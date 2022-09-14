extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, ItemFn};

#[proc_macro_attribute]
pub fn clustertest(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);

    let fn_block = *input_fn.block;
    let fn_name = input_fn.sig.ident.to_string();

    // Enable readyset_tracing test logging for all #[clustertest]s and log the test name as a debug
    // log
    let fn_block_with_tracing = parse_quote! {{
        ::readyset_tracing::init_test_logging();
        tracing::debug!("Starting {}", #fn_name);
        #fn_block
    }};

    let test_with_tracing = ItemFn {
        block: Box::new(fn_block_with_tracing),
        ..input_fn
    };

    let result = quote! {
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #test_with_tracing
    };
    result.into()
}
