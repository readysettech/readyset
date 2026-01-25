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

/// List of valid test that can be used with the `tags` attribute. See descriptions in [`tags`].
const VALID_TAGS: &[&str] = &[
    "serial",
    "no_retry",
    "slow",
    "very_slow",
    "mysql_upstream",
    "mysql8_upstream",
    "postgres_upstream",
    "postgres15_upstream",
];

/// "Tag" a test by moving it into a module. This is a lightweight version of the [test-tag] crate
/// with special support for the `serial` tag.
///
/// ```rust,ignore
/// // This:
/// #[tags(serial, mysql8_upstream)]
/// #[test]
/// fn test_foobar() {
///     // Test code here
/// }
///
/// // Will turn into this:
/// pub mod test_foobar {
///     pub mod serial {
///         pub mod mysql8_upstream {
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
/// cargo test -- :mysql8_upstream:
/// cargo nextest run -E 'test(/:mysql8_upstream:/)'
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
/// - `mysql_upstream`: For tests that can run against any supported MySQL version
/// - `mysql8_upstream`: For tests that specifically target MySQL 8.x features or behavior
/// - `postgres_upstream`: For tests that can run against any supported PostgreSQL version
/// - `postgres15_upstream`: For tests that specifically target PostgreSQL 15.x features or behavior
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
    // use example_exprs_eval_same_as_mysql::serial::mysql_upstream::test as example_exprs_eval_same_as_mysql;
    // ```
    out.into_token_stream().into()
}
