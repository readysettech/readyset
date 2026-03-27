extern crate proc_macro;

use std::mem;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenTree};
use quote::{ToTokens, quote};
use syn::{Error, Ident, ItemFn, parse_macro_input, parse_quote};

/// Mark the given test as a "slow" test, meaning it won't be run if slow tests should not be run.
///
/// # Examples
///
/// The following test will only be run if the `RUN_SLOW_TESTS` environment variable is set:
///
/// ```no_run
/// # use test_utils_proc_macros::slow;
/// #[test]
/// #[slow]
/// fn my_slow_test() {
///     std::thread::sleep(10000);
/// }
/// ```
#[proc_macro_attribute]
pub fn slow(_args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let orig_body = *item.block;
    let body = parse_quote! {{
        if ::test_utils::skip_slow_tests() {
            eprintln!("Not running slow tests; skipping");
            return;
        }
        #orig_body
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    result.into_token_stream().into()
}

/// Annotate that the given test should be skipped when running in the nightly pipeline with
/// `flaky_finder`
///
/// # Examples
///
/// The following test will not be run if the `FLAKY_FINDER` environment variable is set:
///
/// ```no_run
/// # use test_utils_proc_macros::skip_flaky_finder;
/// #[test]
/// #[skip_flaky_finder]
/// fn my_flaky_test() {
///     assert_eq!(1, 1);
/// }
/// ```
#[proc_macro_attribute]
pub fn skip_flaky_finder(_args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let orig_body = *item.block;
    let body = parse_quote! {{
        if ::test_utils::skip_with_flaky_finder() {
            return;
        }
        #orig_body
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    result.into_token_stream().into()
}

// ── Structured upstream parsing ──────────────────────────────────────────

/// Known MySQL versions, as (numeric_id, variant_name) pairs.
const MYSQL_VERSIONS: &[(u32, &str)] = &[(57, "mysql57"), (80, "mysql80"), (84, "mysql84")];

/// Known PostgreSQL versions, as (numeric_id, variant_name) pairs.
const POSTGRES_VERSIONS: &[(u32, &str)] = &[(13, "postgres13"), (15, "postgres15")];

/// Minimum MySQL version that supports flags (gtid, mrbr, etc.).
const MYSQL_MODERN_MIN: u32 = 80;

#[derive(Debug, Clone, Copy)]
enum Family {
    Mysql,
    Postgres,
}

impl Family {
    fn name(&self) -> &'static str {
        match self {
            Family::Mysql => "mysql",
            Family::Postgres => "postgres",
        }
    }
}

#[derive(Debug)]
enum VersionSet {
    All,
    Modern,
    Single(u32),
}

#[derive(Debug, Clone, Copy)]
enum Flag {
    Gtid,
    Nogtid,
    Mrbr,
    MrbrGtid,
}

impl Flag {
    fn name(&self) -> &'static str {
        match self {
            Flag::Gtid => "gtid",
            Flag::Nogtid => "nogtid",
            Flag::Mrbr => "mrbr",
            Flag::MrbrGtid => "mrbr_gtid",
        }
    }
}

/// Known flag names, used to detect flags misplaced in the version-set position.
const KNOWN_FLAGS: &[&str] = &["gtid", "nogtid", "mrbr", "mrbr_gtid"];

/// Result of parsing `#[upstream(...)]` arguments, with token spans preserved for error reporting.
#[derive(Debug)]
struct ParsedArgs {
    family: Family,
    version_set: VersionSet,
    flag: Option<Flag>,
    /// Span of the version-set token (or call_site if defaulted to All).
    version_span: proc_macro2::Span,
    /// Span of the flag token (or call_site if absent).
    flag_span: proc_macro2::Span,
}

/// Parse `#[upstream(family, version_set, flag)]` arguments.
fn parse_structured_args(args: proc_macro2::TokenStream) -> Result<ParsedArgs, Error> {
    let tokens: Vec<TokenTree> = args
        .into_iter()
        .filter(|tt| !matches!(tt, TokenTree::Punct(p) if p.as_char() == ','))
        .collect();

    if tokens.is_empty() {
        return Err(Error::new(
            Span::call_site(),
            "expected a database family (mysql or postgres)",
        ));
    }

    // Token 0: family
    let family = match &tokens[0] {
        TokenTree::Ident(ident) => match ident.to_string().as_str() {
            "mysql" => Family::Mysql,
            "postgres" => Family::Postgres,
            other => {
                return Err(Error::new(
                    ident.span(),
                    format!("unknown database family '{other}'; expected 'mysql' or 'postgres'"),
                ));
            }
        },
        other => {
            return Err(Error::new(
                other.span(),
                "expected a database family identifier",
            ));
        }
    };

    // Token 1: optional version set
    let (version_set, version_span) = if tokens.len() >= 2 {
        match &tokens[1] {
            TokenTree::Ident(ident) => {
                let s = ident.to_string();
                match s.as_str() {
                    "all" => (VersionSet::All, ident.span()),
                    "modern" => {
                        if matches!(family, Family::Postgres) {
                            return Err(Error::new(
                                ident.span(),
                                "'modern' version set is only valid for mysql",
                            ));
                        }
                        (VersionSet::Modern, ident.span())
                    }
                    other => {
                        let hint = if KNOWN_FLAGS.contains(&other) {
                            match family {
                                Family::Mysql => format!(
                                    "; '{other}' is a flag — \
                                     did you mean #[upstream(mysql, modern, {other})]?",
                                ),
                                Family::Postgres => format!(
                                    "; '{other}' is a flag, but flags are only valid for mysql",
                                ),
                            }
                        } else {
                            String::new()
                        };
                        return Err(Error::new(
                            ident.span(),
                            format!(
                                "unknown version set '{other}'; \
                                 expected 'all', 'modern', or an integer version number{hint}"
                            ),
                        ));
                    }
                }
            }
            TokenTree::Literal(lit) => {
                let ver: u32 = lit
                    .to_string()
                    .replace('_', "")
                    .trim_end_matches(|c: char| c.is_alphabetic())
                    .parse()
                    .map_err(|_| Error::new(lit.span(), "expected an integer version number"))?;
                (VersionSet::Single(ver), lit.span())
            }
            other => {
                return Err(Error::new(
                    other.span(),
                    "expected a version set ('all', 'modern') or integer version number",
                ));
            }
        }
    } else {
        (VersionSet::All, Span::call_site())
    };

    // Token 2: optional flag
    let (flag, flag_span) = if tokens.len() >= 3 {
        let (flag, span) = match &tokens[2] {
            TokenTree::Ident(ident) => match ident.to_string().as_str() {
                "gtid" => (Flag::Gtid, ident.span()),
                "nogtid" => (Flag::Nogtid, ident.span()),
                "mrbr" => (Flag::Mrbr, ident.span()),
                "mrbr_gtid" => (Flag::MrbrGtid, ident.span()),
                other => {
                    return Err(Error::new(
                        ident.span(),
                        format!(
                            "unknown flag '{other}'; \
                             expected 'gtid', 'nogtid', 'mrbr', or 'mrbr_gtid'"
                        ),
                    ));
                }
            },
            other => {
                return Err(Error::new(other.span(), "expected a flag identifier"));
            }
        };
        if matches!(family, Family::Postgres) {
            return Err(Error::new(
                span,
                "flags are only valid for mysql, not postgres",
            ));
        }
        (Some(flag), span)
    } else {
        (None, Span::call_site())
    };

    if tokens.len() > 3 {
        return Err(Error::new(
            tokens[3].span(),
            "unexpected extra arguments; expected at most: family, version_set, flag",
        ));
    }

    Ok(ParsedArgs {
        family,
        version_set,
        flag,
        version_span,
        flag_span,
    })
}

/// A resolved variant: the generated identifier plus the structured metadata that produced it.
#[derive(Debug)]
struct ResolvedVariant {
    ident: Ident,
    family: Family,
    version: u32,
    flag: Option<Flag>,
}

/// Resolve parsed arguments into a list of concrete variants with structured metadata.
fn resolve_variants(args: &ParsedArgs) -> Result<Vec<ResolvedVariant>, Error> {
    let versions: &[(u32, &str)] = match args.family {
        Family::Mysql => MYSQL_VERSIONS,
        Family::Postgres => POSTGRES_VERSIONS,
    };
    let family_name = args.family.name();

    let filtered: Vec<(u32, &str)> = match &args.version_set {
        VersionSet::All => versions.to_vec(),
        VersionSet::Modern => versions
            .iter()
            .filter(|(ver, _)| *ver >= MYSQL_MODERN_MIN)
            .copied()
            .collect(),
        VersionSet::Single(target) => {
            let found = versions.iter().find(|(ver, _)| ver == target);
            match found {
                Some(entry) => vec![*entry],
                None => {
                    let valid: Vec<String> = versions.iter().map(|(v, _)| v.to_string()).collect();
                    return Err(Error::new(
                        args.version_span,
                        format!(
                            "unknown version '{target}' for {family_name}; \
                             valid versions are: {}",
                            valid.join(", ")
                        ),
                    ));
                }
            }
        }
    };

    if let Some(f) = args.flag {
        for (ver, name) in &filtered {
            if *ver < MYSQL_MODERN_MIN {
                return Err(Error::new(
                    args.flag_span,
                    format!(
                        "flag '{}' is not valid for {name} \
                         (only MySQL 8.0+ supports flags)",
                        f.name()
                    ),
                ));
            }
        }
    }

    // Use version_span for generated idents so errors in downstream macros
    // point back to the version token in the #[upstream(...)] attribute.
    Ok(filtered
        .iter()
        .map(|(ver, base)| {
            let name = match args.flag {
                Some(f) => format!("{base}_{}", f.name()),
                None => base.to_string(),
            };
            ResolvedVariant {
                ident: Ident::new(&name, args.version_span),
                family: args.family,
                version: *ver,
                flag: args.flag,
            }
        })
        .collect())
}

/// Generate a separate copy of a test for each upstream database variant.
///
/// Each variant becomes a nested module, making it filterable via nextest:
///
/// ```sh
/// cargo nextest run -E 'test(/:mysql80:/)'
/// ```
///
/// # Structured syntax
///
/// The first argument is a database family (`mysql` or `postgres`). Optional second and third
/// arguments narrow the version set and add a flag:
///
/// ```rust,ignore
/// #[upstream(mysql)]                    // all MySQL versions
/// #[upstream(mysql, modern)]            // MySQL 8.0+
/// #[upstream(mysql, modern, gtid)]      // MySQL 8.0+ with gtid flag
/// #[upstream(mysql, 84, gtid)]          // single version with flag
/// #[upstream(postgres)]                 // all PostgreSQL versions
/// #[upstream(postgres, 15)]             // single PostgreSQL version
/// ```
///
/// # Limitations
///
/// The version set is a single token — either a named group (`all`, `modern`) or one integer
/// version number. There is no way to specify an arbitrary multi-version list like
/// `#[upstream(mysql, 80, 84, gtid)]`. If a new subset is needed, the options are:
///
/// 1. Add a named version set (e.g. `v8` for all 8.x versions) to [`MYSQL_VERSIONS`] /
///    [`parse_structured_args`].
/// 2. Support a list syntax such as `#[upstream(mysql, [80, 84], gtid)]`.
///
/// # Expansion example
///
/// ```rust,ignore
/// #[upstream(mysql, modern)]
/// #[test]
/// fn my_test() { assert!(true); }
///
/// // Expands to (simplified):
/// pub mod my_test {
///     pub mod mysql80 { ... fn my_test() { assert!(true); } }
///     pub mod mysql84 { ... }
///     // __xp_ variants (nightly-only, auto-expanded):
///     pub mod mysql80__xp_mrbr   { ... }
///     pub mod mysql80__xp_gtid   { ... }
///     pub mod mysql80__xp_nogtid { ... }
///     pub mod mysql80__xp_mrbr_gtid { ... }
///     // ... same four for mysql84
/// }
/// ```
///
/// When combined with [`tags`], `#[tags(...)]` must be outermost so it runs first.
#[proc_macro_attribute]
pub fn upstream(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as proc_macro2::TokenStream);
    let item = parse_macro_input!(item as ItemFn);

    let parsed = match parse_structured_args(args) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error().into(),
    };
    let explicit_variants: Vec<ResolvedVariant> = match resolve_variants(&parsed) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };

    // Auto-expand MySQL 8.0+ variants with an `__xp_` marker so CI can
    // filter them out (they only run in the nightly pipeline). Explicitly
    // declared variants (e.g. `mysql80_gtid`) always run in CI.
    //
    // The `__xp_` prefix (short for "expanded") uses double underscores to
    // avoid collisions with user-defined test names.
    //
    // Expansion adds the missing flags (mrbr, gtid) as __xp_ variants:
    //   mysql80           → + __xp_mrbr, __xp_gtid, __xp_nogtid, __xp_mrbr_gtid
    //   mysql80_mrbr      → + __xp_mrbr_gtid
    //   mysql80_gtid      → + __xp_mrbr_gtid
    //   mysql80_mrbr_gtid → (nothing, fully specified)
    //   mysql80_nogtid    → (nothing, nogtid is terminal)
    //
    // Variants containing "__xp_" or "nogtid" are never expanded further.
    let variants: Vec<Ident> = explicit_variants
        .into_iter()
        .flat_map(|rv| {
            let span = rv.ident.span();
            let mut out = vec![rv.ident];

            let is_mysql_modern =
                matches!(rv.family, Family::Mysql) && rv.version >= MYSQL_MODERN_MIN;
            let is_nogtid = matches!(rv.flag, Some(Flag::Nogtid));

            if !is_mysql_modern || is_nogtid {
                return out;
            }

            let has_mrbr = matches!(rv.flag, Some(Flag::Mrbr | Flag::MrbrGtid));
            let has_gtid = matches!(rv.flag, Some(Flag::Gtid | Flag::MrbrGtid));

            // Base is the version-only name (e.g. "mysql80") for constructing expanded idents.
            let base = MYSQL_VERSIONS
                .iter()
                .find(|(v, _)| *v == rv.version)
                .expect("version was already validated")
                .1;

            match (has_mrbr, has_gtid) {
                // mysql80 → all four expanded variants
                (false, false) => {
                    for suffix in ["__xp_mrbr", "__xp_gtid", "__xp_nogtid", "__xp_mrbr_gtid"] {
                        out.push(Ident::new(&format!("{base}{suffix}"), span));
                    }
                }
                // mysql80_mrbr or mysql80_gtid → only __xp_mrbr_gtid
                (true, false) | (false, true) => {
                    out.push(Ident::new(&format!("{base}__xp_mrbr_gtid"), span));
                }
                // mysql80_mrbr_gtid → fully specified, nothing to add
                (true, true) => {}
            }

            out
        })
        .collect();

    let fn_name = &item.sig.ident;
    let attrs = &item.attrs;

    // If #[tags(...)] is still present in attrs, it means `upstream` was applied *before* `tags`,
    // which is the wrong order. `tags` must be outermost so it runs first.
    if let Some(tags_attr) = attrs.iter().find(|attr| attr.path().is_ident("tags")) {
        return Error::new_spanned(
            tags_attr,
            "#[tags(...)] must be placed before (above) #[upstream(...)], not after it",
        )
        .to_compile_error()
        .into();
    }

    let asyncness = &item.sig.asyncness;
    let ret_type = &item.sig.output;
    let inputs = &item.sig.inputs;
    let body = &item.block;

    let variant_modules = variants.iter().map(|variant| {
        quote! {
            #[allow(non_snake_case)]
            pub mod #variant {
                use super::*;
                // Explicit import so it takes priority over both the glob import and the
                // prelude, avoiding the ambiguity that `use super::*` + prelude causes.
                #[allow(unused_imports)]
                use ::test_utils::pretty_assertions::assert_eq;
                #(#attrs)*
                #asyncness fn #fn_name(#inputs) #ret_type #body
            }
        }
    });

    // If #[tags] already wrapped us, fn_name is "test" and the outer module exists.
    if fn_name == "test" {
        quote! { #(#variant_modules)* }.into()
    } else {
        quote! {
            pub mod #fn_name {
                use super::*;
                #(#variant_modules)*
            }
        }
        .into()
    }
}

/// List of valid test that can be used with the `tags` attribute. See descriptions in [`tags`].
const VALID_TAGS: &[&str] = &["serial", "no_retry", "slow", "very_slow"];

/// "Tag" a test by moving it into a module. This is a lightweight version of the [test-tag] crate
/// with special support for the `serial` tag.
///
/// ```rust,ignore
/// // This:
/// #[tags(serial, slow)]
/// #[test]
/// fn test_foobar() {
///     // Test code here
/// }
///
/// // Will turn into this:
/// pub mod test_foobar {
///     pub mod serial {
///         pub mod slow {
///             #[test]
///             fn test() {
///                 // Test code here
///             }
///         }
///     }
/// }
/// ```
///
/// This allows test runs to target specific tags using the `:tag:` pattern, like so:
///
/// ```sh
/// cargo test -- :serial:
/// cargo nextest run -E 'test(/:serial:/)'
/// ```
///
/// If one of the tags is `serial`, we add the [`serial_test::serial`] attribute so this can be run
/// with `cargo test` using `serial_test`'s in-process mutual exclusion. Nextest's filter-based
/// groups can be used for mutual exclusion by adding a `test(/:serial:/)` filter to the group
/// definition.
///
/// Note that we rename the test function itself to `test` and put all the tag modules into an outer
/// module with the original test name primarily so that regardless of the ordering of the tags,
/// they will always be identifiable as `:tag:`, not requiring `:tag` for the last or `tag:` for the
/// first.
///
/// When combined with [`upstream`], `#[tags(...)]` must be the outermost (topmost) attribute so
/// that it runs first and renames the function to `test`. The `#[upstream]` macro detects this
/// and skips creating a redundant outer module.
///
/// ## Supported Tags
///
/// The following tags are supported (defined in [`VALID_TAGS`]):
///
/// - `serial`: Indicates that tests must be run serially (not in parallel with other serial tests for
///   the same upstream; see .config/nextest.toml for test groups)
/// - `no_retry`: For generative tests such as proptests which should not be retried if they fail
/// - `slow`: For tests that are expected to take a long time to run. See [`slow`]
/// - `very_slow`: For tests that are extremely slow; these are prioritized to run first so they
///   don't become long-pole tests that delay CI completion
///
/// [test-tag]: https://crates.io/crates/test-tag
#[proc_macro_attribute]
pub fn tags(args: TokenStream, item: TokenStream) -> TokenStream {
    let groups = parse_macro_input!(args as proc_macro2::TokenStream);
    let serial_groups = if groups.is_empty() {
        parse_quote! { serial }
    } else {
        groups.clone()
    };

    let mut item = parse_macro_input!(item as ItemFn);

    let mut tags: Vec<_> = groups
        .into_iter()
        .filter_map(|tt| match tt {
            TokenTree::Ident(ident) => Some(ident),
            _ => None,
        })
        .collect();

    // Validate that all tags are known
    for tag in &tags {
        let tag_str = tag.to_string();
        if !VALID_TAGS.contains(&tag_str.as_str()) {
            let err_msg = format!(
                "Unknown test tag '{}'. Valid tags are: {}",
                tag_str,
                VALID_TAGS.join(", ")
            );
            let error = Error::new(tag.span(), err_msg);
            return error.to_compile_error().into();
        }
    }

    // Special handling for `serial`: add the in-process mutex from `serial_test`
    let uses_serial = tags.iter().any(|tag| tag == "serial");
    if uses_serial {
        let serial_attr = parse_quote! {
            #[::test_utils::serial_test::serial(#serial_groups)]
        };
        item.attrs.push(serial_attr);
    }

    let uses_slow = tags.iter().any(|tag| tag == "slow" || tag == "very_slow");
    if uses_slow {
        let slow_attr = parse_quote! {
            #[::test_utils::slow]
        };
        item.attrs.push(slow_attr);
    }

    let test_name = mem::replace(&mut item.sig.ident, Ident::new("test", Span::call_site()));
    tags.insert(0, test_name);

    // In addition to adding the attribute, we also need to make `serial_test` resolve in scope,
    // because the macro adds a call out to the crate for a runtime function.
    let mut out = if uses_serial {
        quote! {
            use ::test_utils::serial_test;
            use ::test_utils::pretty_assertions::assert_eq;
            #item
        }
    } else {
        quote! {
            use ::test_utils::pretty_assertions::assert_eq;
            #item
        }
    };

    for group in tags.into_iter().rev() {
        out = quote! {
            pub mod #group {
                use super::*;
                #out
            }
        };
    }

    // XXX(mvzink): If anybody gets annoyed that their test fn name is becoming a module instead, we
    // might want to do, as a final step, the same thing test-tag does here:
    //
    // ```
    // use example_exprs_eval_same_as_mysql::serial::test as example_exprs_eval_same_as_mysql;
    // ```
    out.into_token_stream().into()
}
