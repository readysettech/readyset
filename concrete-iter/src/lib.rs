//! A macro to generate a *concrete* iterator type for a function that can return one of
//! multiple different kinds of iterator.
//!
//! See [`concrete_iter`] for more information.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote_spanned, ToTokens};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::visit::{self, Visit};
use syn::visit_mut::{self, VisitMut};
use syn::{
    parse_macro_input, parse_quote, parse_quote_spanned, Arm, Expr, GenericArgument, GenericParam,
    Ident, ItemEnum, ItemFn, ItemImpl, Path, PathArguments, ReturnType, Type, TypeParam,
    TypeParamBound, Variant,
};

/// Try to sniff out the `T` in `impl Iterator<Item = T> + ...`.
///
/// Returns `None` if we were unable to do so
fn impl_iterator_item_type(output: &ReturnType) -> Option<&Type> {
    match output {
        ReturnType::Type(_, ty) => match ty.as_ref() {
            Type::ImplTrait(it) => {
                let generic_args = it.bounds.iter().find_map(|b| match b {
                    TypeParamBound::Trait(tb)
                        if tb.path.segments.len() == 1
                            && tb.path.segments.first().unwrap().ident == "Iterator" =>
                    {
                        match &tb.path.segments.last().unwrap().arguments {
                            PathArguments::AngleBracketed(args) => Some(&args.args),
                            _ => None,
                        }
                    }
                    _ => None,
                })?;

                generic_args.iter().find_map(|arg| match arg {
                    GenericArgument::Binding(b) if b.ident == "Item" => Some(&b.ty),
                    _ => None,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// Is the given macro invocation an invocation of `concrete_iter!`?
fn is_concrete_iter(m: &syn::Macro) -> bool {
    m.path.get_ident().iter().any(|id| *id == "concrete_iter")
}

/// Count the number of times the given `ItemFn` called `concrete_iter!`.
///
/// We will generate an enum type with variants corresponding to each of these callsites.
fn count_variants(item: &ItemFn) -> usize {
    #[derive(Default)]
    struct CountVariantsVisitor(usize);

    impl<'ast> Visit<'ast> for CountVariantsVisitor {
        fn visit_macro(&mut self, i: &'ast syn::Macro) {
            if is_concrete_iter(i) {
                self.0 += 1;
            }

            // Keep going, in case there's a macro call inside the macro call for some reason
            visit::visit_macro(self, i)
        }
    }

    let mut v = CountVariantsVisitor::default();
    v.visit_item_fn(item);
    v.0
}

/// Expand the `concrete_iter!` macro invocations in the body of the given `ItemFn`, replacing them
/// with construction of the corresponding enum variants.
fn expand_concrete_iter(item: &mut ItemFn) {
    #[derive(Default)]
    struct ExpandConcreteIterVisitor {
        idx: usize,
    }

    impl VisitMut for ExpandConcreteIterVisitor {
        fn visit_expr_mut(&mut self, i: &mut Expr) {
            let mac_span_and_arg = match i {
                Expr::Macro(m) if is_concrete_iter(&m.mac) => {
                    Some((m.mac.span(), m.mac.parse_body::<Expr>().unwrap()))
                }
                _ => None,
            };

            if let Some((mac_span, arg)) = mac_span_and_arg {
                let variant_name = Ident::new(&format!("Variant{}", self.idx), mac_span);
                let variant_path: Path =
                    parse_quote_spanned!(mac_span => ConcreteIter::#variant_name);

                *i = parse_quote_spanned!(i.span() => #variant_path(#arg));

                self.idx += 1;
            }

            visit_mut::visit_expr_mut(self, i)
        }
    }

    ExpandConcreteIterVisitor::default().visit_item_fn_mut(item);
}

/// Generate the actual definition for the enum type we're going to return, with the given number of
/// variants (as returned by `count_variants`)
fn generate_enum(num_variants: usize) -> ItemEnum {
    let type_param_names = (0..num_variants)
        .map(|idx| -> TypeParam {
            let name = Ident::new(&format!("T{idx}"), Span::mixed_site());
            parse_quote!(#name)
        })
        .collect::<Vec<_>>();

    let generic_params: Punctuated<_, Comma> = type_param_names
        .iter()
        .cloned()
        .map(GenericParam::Type)
        .collect();

    let variants: Punctuated<_, Comma> = type_param_names
        .iter()
        .enumerate()
        .map(|(i, tp)| -> Variant {
            let name = Ident::new(&format!("Variant{i}"), Span::mixed_site());
            parse_quote!(#name(#tp))
        })
        .collect();

    parse_quote! {
        enum ConcreteIter<#generic_params> {
            #variants
        }
    }
}

/// Generate an `impl Iterator` for the enum type, with the given number of variants and `Item`
/// type.
fn generate_iterator_impl(num_variants: usize, item_ty: Type) -> ItemImpl {
    let type_param_names = (0..num_variants)
        .map(|idx| Ident::new(&format!("T{idx}"), Span::mixed_site()))
        .collect::<Vec<_>>();

    let generic_param_bounds: Punctuated<_, Comma> = type_param_names
        .iter()
        .map(|tn| -> GenericParam { parse_quote!(#tn : ::std::iter::Iterator<Item = #item_ty>) })
        .collect();

    let generic_param_args: Punctuated<_, Comma> = type_param_names
        .iter()
        .map(|tn| -> GenericParam { parse_quote!(#tn) })
        .collect();

    let match_arms: Vec<_> = (0..num_variants)
        .map(|idx| -> Arm {
            let variant_name = Ident::new(&format!("Variant{idx}"), Span::mixed_site());
            parse_quote! {
                Self::#variant_name(x) => x.next()
            }
        })
        .collect();

    parse_quote! {
        impl<#generic_param_bounds> ::std::iter::Iterator for ConcreteIter<#generic_param_args> {
            type Item = #item_ty;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    #(#match_arms),*
                }
            }
        }
    }
}

/// A macro to generate a *concrete* iterator type for a function that can return one of multiple
/// different kinds of iterator.
///
/// This macro attempts to solve the same kinds of problems as `Box<dyn Iterator<Item = T> + '_>` or
/// `itertools::Either`, but without the performance pitfalls of the former, or the lack of
/// readability and maintainability of the latter.
///
/// Just add `#[concrete_iter]` to your function, which must return a type like
/// `impl Iterator<Item = T> + ...`, and wrap all the values you want to return as concrete
/// iterators in `concrete_iter!`. The macro will generate an enum for you with variants
/// corresponding to each of the points where you called `concrete_iter!` in your function, an
/// `impl Iterator` for that enum, and replace those calls to `concrete_iter!` with those enum
/// variants.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use std::iter;
///
/// use concrete_iter::concrete_iter;
///
/// // An enum type, which can contain some number of `u32`s in different ways
/// enum MyEnum {
///     SingleVal(u32),
///     LotsOfVals(Vec<u32>),
///     SetOfVals(HashSet<u32>),
///     Nothing,
/// }
///
/// impl MyEnum {
///     /// Iterate over all the `u32`s in `self`.
///     ///
///     /// Returns a concrete iterator type - no allocations, no vtable, no boxing!
///     #[concrete_iter]
///     pub fn vals(&self) -> impl Iterator<Item = u32> + '_ {
///         match *self {
///             Self::SingleVal(x) => concrete_iter!(iter::once(x)),
///             Self::LotsOfVals(ref vs) => concrete_iter!(vs.iter().copied()),
///             Self::SetOfVals(ref vs) => concrete_iter!(vs.iter().copied()),
///             Self::Nothing => concrete_iter!(iter::empty::<u32>()),
///         }
///     }
/// }
///
/// assert_eq!(MyEnum::SingleVal(1).vals().collect::<Vec<_>>(), vec![1]);
/// assert_eq!(
///     MyEnum::LotsOfVals(vec![1, 2, 3]).vals().collect::<Vec<_>>(),
///     vec![1, 2, 3]
/// );
/// assert_eq!(
///     MyEnum::SetOfVals([4, 5, 6].into())
///         .vals()
///         .collect::<HashSet<_>>(),
///     [4, 5, 6].into()
/// );
/// ```
#[proc_macro_attribute]
pub fn concrete_iter(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as ItemFn);
    let item_ty = if let Some(item_ty) = impl_iterator_item_type(&item.sig.output) {
        item_ty.clone()
    } else {
        return quote_spanned!(item.sig.output.span() =>
            compile_error!("Functions annotated with `#[concrete_iter]` must return `impl Iterator<Item = ...>`");
        ).into();
    };

    let num_variants = count_variants(&item);

    expand_concrete_iter(&mut item);
    let enum_defn = generate_enum(num_variants);
    let iterator_impl = generate_iterator_impl(num_variants, item_ty);

    let orig_body = *item.block;
    let body = parse_quote! {{
        #enum_defn
        #iterator_impl
        #orig_body
    }};

    let result = ItemFn {
        block: Box::new(body),
        ..item
    };

    result.into_token_stream().into()
}
