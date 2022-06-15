extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn clustertest(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);

    let result = quote! {
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #input_fn
    };
    result.into()
}
