extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, ToTokens};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{parse_macro_input, parse_quote, Attribute, Ident, ItemFn, Path};

/// Mark the given test as a "slow" test, meaning it won't be run if slow tests should not be run.
///
/// # Examples
///
/// The following test will only be run if the `RUN_SLOW_TESTS` environment variable is set:
///
/// ```
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
/// ```
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

// XXX `quote::ToTokens` turns the output into `proc_macro2::TokenStream`, which requires a further
// conversion to `proc_macro::TokenStream`. Unfortunately, clippy thinks these are the same thing
// and complains that there's a reduntant `.into()` call, so we ignore it.
#[allow(clippy::useless_conversion)]
#[proc_macro_attribute]
pub fn serial(args: TokenStream, item: TokenStream) -> TokenStream {
    let group = parse_macro_input!(args as Option<Ident>);
    let mut item = parse_macro_input!(item as ItemFn);
    let name = item.sig.ident;
    item.sig.ident = if let Some(ref group) = group {
        format_ident!("{}_serial_{}", name, group)
    } else {
        format_ident!("{}_serial", name)
    };
    item.attrs.push(Attribute {
        pound_token: syn::token::Pound(group.span()),
        style: syn::AttrStyle::Outer,
        bracket_token: syn::token::Bracket(group.span()),
        meta: syn::Meta::List(syn::MetaList {
            path: Path {
                leading_colon: None,
                segments: Punctuated::from_iter(vec![
                    syn::PathSegment {
                        ident: Ident::new("serial_test", group.span()),
                        arguments: syn::PathArguments::None,
                    },
                    syn::PathSegment {
                        ident: Ident::new("serial", group.span()),
                        arguments: syn::PathArguments::None,
                    },
                ]),
            },
            delimiter: syn::MacroDelimiter::Paren(syn::token::Paren(group.span())),
            tokens: group.into_token_stream().into(),
        }),
    });
    item.into_token_stream().into()
}
