extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::parse::Parse;
use syn::{parse_macro_input, parse_quote, Ident, ItemFn};

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

#[derive(Clone, Debug)]
struct ParallelGroupArgs {
    group: Ident,
}

impl Parse for ParallelGroupArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let group = input.parse()?;
        Ok(ParallelGroupArgs { group })
    }
}

/// Mark the given test as belonging to the given parallelism group
#[proc_macro_attribute]
pub fn parallel_group(args: TokenStream, item: TokenStream) -> TokenStream {
    let ParallelGroupArgs { group } = parse_macro_input!(args as ParallelGroupArgs);
    let item = parse_macro_input!(item as ItemFn);

    let acquire = if item.sig.asyncness.is_some() {
        quote! {
            #group.acquire_async().await
        }
    } else {
        quote! {
            #group.acquire()
        }
    };

    let inner_body = item.block;
    let body = parse_quote! {{
        let _permit = #acquire;
        #inner_body
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    quote!(#result).into()
}
