extern crate proc_macro;
use std::mem;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parser, parse_macro_input, ItemFn, LitStr, Stmt};

/// Introduces a failpoint at the beginning of the function this attribute macro
/// is applied to. This failpoint can use all failpoint actions except `return`.
///
/// Example:
///
/// ```rust
/// use failpoint_proc_macros::failpoint;
///
/// #[failpoint("rpc-failure")]
/// fn failpoint_test() {
///     println!("Hello there!");
/// }
/// ```
///
/// This is converted into:
/// ```rust
/// use failpoint_proc_macros::failpoint;
///
/// fn failpoint_test() {
///     #[cfg(feature = "failure_injection")]
///     fail::fail_point!("rpc-failure");
///     println!("Hello there!");
/// }
/// ```
// TODO(justin) Support return failpoint action.
#[proc_macro_attribute]
pub fn failpoint(attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut input_fn = parse_macro_input!(input as ItemFn);
    let failpoint_name = parse_macro_input!(attr as LitStr);

    // Get the set of statements for the feature and macro invocation.
    let new_stmts_str = format!(
        r#"
        #[cfg(feature = "failure_injection")]
        fail::fail_point!("{}");
        "#,
        failpoint_name.value()
    );

    let new_stmts: Vec<Stmt> = syn::Block::parse_within.parse_str(&new_stmts_str).unwrap();
    // Prefix the existing statements with `new_stmts`.
    let old_stmts = mem::replace(&mut input_fn.block.stmts, new_stmts);
    input_fn.block.stmts.extend(old_stmts);

    let result = quote! {
        #input_fn
    };
    result.into()
}
