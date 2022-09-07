extern crate proc_macro;

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::{parse_macro_input, parse_quote, ItemFn};

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
