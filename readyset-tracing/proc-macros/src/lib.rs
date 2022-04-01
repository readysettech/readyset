//! This crate contains procedural macro attributes for instrumenting functions to participate in
//! distributed tracing with [`readyset-tracing`].
//!
//! The macros defined in this crate differ from those defined in the base [`tracing`] crate in two
//! main ways:
//!
//! * They properly participate in [`presampled`] tracing, by conditionally enabling spans that
//!   should be sampled in the most performant possible way.
//! * They fully participate in *distributed* tracing
//!
//! See the documentation for the individual macros exported by this crate for more information.
//!
//! Note that this crate should generally not be depended on directly - instead depend on the
//! [`readyset-tracing`] crate, which re-exports all the macros defined here.
//!
//! [`readyset-tracing`]: http://docs/rustdoc/readyset_tracing/index.html
//! [`tracing`]: https://crates.io/crates/tracing
//! [`presampled`]: http://docs/rustdoc/readyset_tracing/presampled/index.html

#![feature(drain_filter)]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, parse_quote, AttrStyle, Expr, FnArg, Ident, ItemFn, LitInt, LitStr, Pat,
    Path, Token,
};

mod kw {
    syn::custom_keyword!(fields);
    syn::custom_keyword!(level);
    syn::custom_keyword!(target);
    syn::custom_keyword!(name);
}

#[derive(Clone, Debug)]
enum Level {
    Str(LitStr),
    Int(LitInt),
    Path(Path),
}

impl Parse for Level {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(LitStr) {
            Ok(Self::Str(input.parse()?))
        } else if lookahead.peek(LitInt) {
            Ok(Self::Int(input.parse()?))
        } else if lookahead.peek(Ident) {
            Ok(Self::Path(input.parse()?))
        } else {
            Err(lookahead.error())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldKind {
    Debug,
    Display,
    Value,
}

#[derive(Clone, Debug)]
struct Field {
    name: Punctuated<Ident, Token![.]>,
    value: Option<Expr>,
    kind: FieldKind,
}

impl Parse for Field {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut kind = FieldKind::Value;
        if input.peek(Token![%]) {
            input.parse::<Token![%]>()?;
            kind = FieldKind::Display;
        } else if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            kind = FieldKind::Debug;
        }

        let name = Punctuated::parse_separated_nonempty_with(input, Ident::parse)?;
        let value = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            if input.peek(Token![%]) {
                input.parse::<Token![%]>()?;
                kind = FieldKind::Display;
            } else if input.peek(Token![?]) {
                input.parse::<Token![?]>()?;
                kind = FieldKind::Debug;
            }
            Some(input.parse()?)
        } else {
            None
        };

        Ok(Self { name, value, kind })
    }
}

#[derive(Clone, Debug)]
struct Fields(Punctuated<Field, Token![,]>);

impl Parse for Fields {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _ = input.parse::<kw::fields>();
        let content;
        let _ = syn::parenthesized!(content in input);
        let fields = content.parse_terminated(Field::parse)?;
        Ok(Self(fields))
    }
}

#[derive(Clone, Debug, Default)]
struct InstrumentArgs {
    name: Option<LitStr>,
    level: Option<Level>,
    target: Option<LitStr>,
    fields: Option<Fields>,
}

impl InstrumentArgs {
    pub fn level(&self) -> impl ToTokens {
        fn is_level(lit: &LitInt, expected: u64) -> bool {
            match lit.base10_parse::<u64>() {
                Ok(value) => value == expected,
                Err(_) => false,
            }
        }

        match &self.level {
            Some(Level::Str(ref lit)) if lit.value().eq_ignore_ascii_case("trace") => {
                quote!(TRACE)
            }
            Some(Level::Str(ref lit)) if lit.value().eq_ignore_ascii_case("debug") => {
                quote!(DEBUG)
            }
            Some(Level::Str(ref lit)) if lit.value().eq_ignore_ascii_case("info") => {
                quote!(INFO)
            }
            Some(Level::Str(ref lit)) if lit.value().eq_ignore_ascii_case("warn") => {
                quote!(WARN)
            }
            Some(Level::Str(ref lit)) if lit.value().eq_ignore_ascii_case("error") => {
                quote!(ERROR)
            }
            Some(Level::Int(ref lit)) if is_level(lit, 1) => quote!(TRACE),
            Some(Level::Int(ref lit)) if is_level(lit, 2) => quote!(DEBUG),
            Some(Level::Int(ref lit)) if is_level(lit, 3) => quote!(INFO),
            Some(Level::Int(ref lit)) if is_level(lit, 4) => quote!(WARN),
            Some(Level::Int(ref lit)) if is_level(lit, 5) => quote!(ERROR),
            Some(Level::Path(ref pat)) => quote!(#pat),
            Some(_) => quote! {
                compile_error!(
                    "unknown verbosity level, expected one of \"trace\", \
                     \"debug\", \"info\", \"warn\", or \"error\", or a number 1-5"
                )
            },
            None => quote!(INFO),
        }
    }

    pub fn target(&self) -> impl ToTokens {
        if let Some(ref target) = self.target {
            quote!(#target)
        } else {
            quote!(module_path!())
        }
    }

    fn fields(&self) -> impl ToTokens {
        let fields = match &self.fields {
            Some(fields) => fields,
            None => return quote!(),
        };
        let fields = fields.0.iter().map(|Field { name, value, kind }| {
            let kind_token = match kind {
                FieldKind::Debug => quote!(?),
                FieldKind::Display => quote!(%),
                FieldKind::Value => quote!(),
            };
            if let Some(value) = value {
                quote! {
                    #name = #kind_token #value
                }
            } else {
                quote! {
                    #kind_token #name
                }
            }
        });
        quote! {
            #(#fields),*
        }
    }
}

impl Parse for InstrumentArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(kw::name) {
                if args.name.is_some() {
                    return Err(input.error("expected only a single `name` argument"));
                }
                args.name = Some(input.parse::<KvArg<kw::name, LitStr>>()?.value)
            } else if lookahead.peek(LitStr) {
                if args.name.is_some() {
                    return Err(input.error("expected only a single `name` argument"));
                }
                args.name = Some(input.parse()?)
            } else if lookahead.peek(kw::level) {
                if args.level.is_some() {
                    return Err(input.error("expected only a single `level` argument"));
                }
                args.level = Some(input.parse::<KvArg<kw::level, Level>>()?.value)
            } else if lookahead.peek(kw::target) {
                if args.target.is_some() {
                    return Err(input.error("expected only a single `target` argument"));
                }
                args.target = Some(input.parse::<KvArg<kw::target, LitStr>>()?.value)
            } else if lookahead.peek(kw::fields) {
                if args.fields.is_some() {
                    return Err(input.error("expected only a single `fields` argument"));
                }
                args.fields = Some(input.parse()?)
            } else if lookahead.peek(Token![,]) {
                let _ = input.parse::<Token![,]>()?;
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(args)
    }
}

struct KvArg<K, V> {
    value: V,
    _p: std::marker::PhantomData<K>,
}

impl<K: Parse, V: Parse> Parse for KvArg<K, V> {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let _ = input.parse::<K>()?;
        let _ = input.parse::<Token![=]>()?;
        let value = input.parse()?;
        Ok(Self {
            value,
            _p: std::marker::PhantomData,
        })
    }
}

fn instrument_local(
    args: TokenStream,
    item: TokenStream,
    span_macro: impl ToTokens,
) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(args as InstrumentArgs);
    let target = args.target();
    let level = args.level();
    let name = match &args.name {
        Some(n) => n.value(),
        None => item.sig.ident.to_string(),
    };
    let fields = args.fields();

    let inner_block = *item.block;
    let (dot_await, inner_body) = if item.sig.asyncness.is_some() {
        (
            quote!(.await),
            quote! {{
                async move { #inner_block }
            }},
        )
    } else {
        (quote!(), quote!(#inner_block))
    };

    let body = parse_quote! {{
        let span = readyset_tracing::#span_macro(target: #target, #level, #name, #fields);
        ::readyset_tracing::presampled::instrument_if_enabled(#inner_body, span) #dot_await
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    quote!(#result).into()
}

/// Instruments a function to create and enter a *root* `tracing` span every time the function is
/// called, as long as sampling is enabled.
///
/// Using this function is essentially equivalent to constructing a span via [`root_span!`], and
/// instrumenting the body via [`instrument_if_enabled`]
///
/// Unless overridden, a span with `info` level will be generated, the span's name will be the name
/// of the function, the span's target will be the current module path, and *no* fields will be
/// passed to the span. The level of the span, the span's name, the span's target, and the span's
/// fields can all be overridden by passing arguments to the `instrument_root` attribute macro,
/// similar to tracing's `instrument` macro.
///
/// # Asyncness
///
/// Note that because conditional instrumentation only works on futures, the function must either be
/// an `async fn`, *or* return a type that implements the [`Future`] trait.
///
/// # Examples
///
/// Instrumenting a function:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_root;
/// #[instrument_root]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the level of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_root;
/// #[instrument_root(level = "debug")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the name of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_root;
/// #[instrument_root(name = "another_name")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the target of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_root;
/// #[instrument_root(target = "my_target")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Fields can be passed to the span by referencing the arguments to the function, using the same
/// syntax as [`tracing::span!`]
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_root;
/// #[instrument_root(fields(%arg, another_arg = %(arg + 1)))]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// [`root_span!`]: http://docs/rustdoc/readyset_tracing/macro.root_span.html
/// [`instrument_if_enabled`]: http://docs/rustdoc/readyset_tracing/presampled/fn.instrument_if_enabled.html
/// [`tracing::span!`]: http://docs/rustdoc/tracing/span/index.html
#[proc_macro_attribute]
pub fn instrument_root(args: TokenStream, item: TokenStream) -> TokenStream {
    instrument_local(args, item, quote!(root_span!))
}

/// Instruments a function to create and enter a *child* `tracing` span every time the function is
/// called, as long as sampling is enabled.
///
/// Using this function is essentially equivalent to constructing a span via [`child_span!`], and
/// instrumenting the body via [`instrument_if_enabled`]
///
/// Unless overridden, a span with `info` level will be generated, the span's name will be the name
/// of the function, the span's target will be the current module path, and *no* fields will be
/// passed to the span. The level of the span, the span's name, the span's target, and the span's
/// fields can all be overridden by passing arguments to the `instrument_child` attribute macro,
/// similar to tracing's `instrument` macro.
///
/// # Asyncness
///
/// Note that because conditional instrumentation only works on futures, the function must either be
/// an `async fn`, *or* return a type that implements the [`Future`] trait.
///
/// # Examples
///
/// Instrumenting a function:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_child;
/// #[instrument_child]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the level of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_child;
/// #[instrument_child(level = "debug")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the name of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_child;
/// #[instrument_child(name = "another_name")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the target of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_child;
/// #[instrument_child(target = "my_target")]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Fields can be passed to the span by referencing the arguments to the function, using the same
/// syntax as [`tracing::span!`]
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_child;
/// #[instrument_child(fields(%arg, another_arg = %(arg + 1)))]
/// async fn my_function(arg: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// [`child_span!`]: http://docs/rustdoc/readyset_tracing/macro.child_span.html
/// [`instrument_if_enabled`]: http://docs/rustdoc/readyset_tracing/presampled/fn.instrument_if_enabled.html
/// [`tracing::span!`]: http://docs/rustdoc/tracing/span/index.html
#[proc_macro_attribute]
pub fn instrument_child(args: TokenStream, item: TokenStream) -> TokenStream {
    instrument_local(args, item, quote!(child_span!))
}

/// Instruments a function to create and enter a *remote* `tracing` span every time the function is
/// called, as long as sampling is enabled.
///
/// To use this macro, exactly one argument must be annotated with the `#[instrumented]` attribute -
/// that argument's type will then be wrapped with [`Instrumented`], and it will be used to extract
/// the context for participating in distributed tracing. For example, using `instrument_remote`
/// like so:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote]
/// async fn my_fn(#[instrumented] request: u32) {}
/// ```
///
/// will actually produce a function with a signature like:
///
/// ```
/// use readyset_tracing::propagation::Instrumented;
/// async fn my_fn(request: Instrumented<u32>) {}
/// ```
///
/// Unless overridden, a span with `info` level will be generated, the span's name will be the name
/// of the function, the span's target will be the current module path, and *no* fields will be
/// passed to the span. The level of the span, the span's name, the span's target, and the span's
/// fields can all be overridden by passing arguments to the `instrument_remote` attribute macro,
/// similar to tracing's `instrument` macro.
///
/// # Asyncness
///
/// Note that because conditional instrumentation only works on futures, the function must either be
/// an `async fn`, *or* return a type that implements the [`Future`] trait.
///
/// # Examples
///
/// Instrumenting a function:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote]
/// async fn my_function(#[instrumented] request: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the level of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote(level = "debug")]
/// async fn my_function(#[instrumented] request: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the name of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote(name = "another_name")]
/// async fn my_function(#[instrumented] request: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Overriding the target of the span:
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote(target = "my_target")]
/// async fn my_function(#[instrumented] request: usize) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// Fields can be passed to the span by referencing the arguments to the function, using the same
/// syntax as [`tracing::span!`]
///
/// ```
/// # use readyset_tracing_proc_macros::instrument_remote;
/// #[instrument_remote(fields(%arg, another_arg = %format!("arg is {}", arg)))]
/// async fn my_function(#[instrumented] request: usize, arg: String) {
///     tracing::info!("Inside my function");
/// }
/// ```
///
/// [`Instrumented`]: http://docs/rustdoc/readyset_tracing/propagation/struct.Instrumented.html
/// [`instrument_if_enabled`]: http://docs/rustdoc/readyset_tracing/presampled/fn.instrument_if_enabled.html
/// [`tracing::span!`]: http://docs/rustdoc/tracing/span/index.html
#[proc_macro_attribute]
pub fn instrument_remote(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(args as InstrumentArgs);
    let target = args.target();
    let level = args.level();
    let name = match &args.name {
        Some(n) => n.value(),
        None => item.sig.ident.to_string(),
    };
    let fields = args.fields();

    let mut instrumented_arg = None;
    for arg in &mut item.sig.inputs {
        if let FnArg::Typed(arg) = arg {
            if arg
                .attrs
                .drain_filter(|attr| {
                    attr.style == AttrStyle::Outer && attr.path == parse_quote!(instrumented)
                })
                .count()
                > 0
            {
                if instrumented_arg.is_some() {
                    return quote_spanned!(arg.span() => compile_error!(
                        "Expected only one argument with the #[instrumented] argument, but found more than one"
                    ))
                    .into();
                }
                match &mut *arg.pat {
                    Pat::Ident(ident) => {
                        instrumented_arg = Some(ident.ident.clone());
                        let inner_ty = (*arg.ty).clone();
                        arg.ty = Box::new(
                            parse_quote!(::readyset_tracing::propagation::Instrumented<#inner_ty>),
                        );
                    }
                    pat => {
                        return quote_spanned!(pat.span() => compile_error!(
                            "Destructuring is not supported for the #[instrumented] argument"
                        ))
                        .into()
                    }
                }
            }
        }
    }

    let instrumented_arg = if let Some(instrumented_arg) = instrumented_arg {
        instrumented_arg
    } else {
        return quote_spanned!(item.sig.inputs.span() => compile_error!(
            "Expected at least one argument with the #[instrumented] attribute"
        ))
        .into();
    };

    let inner_block = *item.block;
    let (dot_await, inner_body) = if item.sig.asyncness.is_some() {
        (
            quote!(.await),
            quote! {{
                async move { #inner_block }
            }},
        )
    } else {
        (quote!(), quote!(#inner_block))
    };

    let body = parse_quote! {{
        let (span, #instrumented_arg) = readyset_tracing::remote_span!(
            #instrumented_arg,
            target: #target,
            #level,
            #name,
            #fields
        );
        ::readyset_tracing::presampled::instrument_if_enabled(#inner_body, span) #dot_await
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    quote!(#result).into()
}
