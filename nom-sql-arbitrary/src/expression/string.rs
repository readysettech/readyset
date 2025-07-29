//! Module that holds all the functions to get a proptest [`Strategy`]
//! that generates [`Expr`]s that resolve to a string value.
// TODO(fran): Make collation configurable.

use proptest::prop_oneof;
use proptest::strategy::Strategy;
use readyset_sql::{ast::*, Dialect};

use crate::expression::util::{case_when, cast, coalesce, if_null};
use crate::expression::ExprStrategy;

/// Produces a [`Strategy`] that generates a non-base (neither literal nor column) string
/// [`Expr`], using other kinds of [`Expr`] provided by the given [`ExprStrategy`]
pub(super) fn generate_string(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
    let case_when = case_when(es.string.clone(), es.bool.clone());
    prop_oneof![case_when, string_cast(es.clone()), call::call(es, dialect)]
}

/// Produces a [`Strategy`] that generates a string [`Expr::Cast`].
// TODO(fran): Add missing types + make this into a macro
fn string_cast(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let string = cast(es.string, SqlType::Text, false);
    let integer = cast(es.integer, SqlType::Text, false);
    let float = cast(es.float, SqlType::Text, false);
    let timestamp = cast(es.timestamp, SqlType::Text, false);
    prop_oneof![string, integer, float, timestamp]
}

/// Helper module to group all the [`Strategy`]s that generate string [`Expr::Call`].
mod call {
    use super::*;

    /// Produces a [`Strategy`] that generates a string [`Expr::Call`].
    pub(super) fn call(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
        match dialect {
            Dialect::PostgreSQL => prop_oneof![
                coalesce(es.string.clone()),
                concat(es.clone()),
                concat_ws(es.clone()),
                split_part(es.clone()),
                substring(es)
            ]
            .boxed(),
            Dialect::MySQL => prop_oneof![
                if_null(es.string.clone()),
                coalesce(es.string.clone()),
                concat(es.clone()),
                concat_ws(es.clone()),
                substring(es)
            ]
            .boxed(),
        }
    }

    /// Produces a [`Strategy`] that generates a string [`Expr::Call`] with
    /// [`BuiltinFunction::Concat`].
    fn concat(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let random_expr = prop_oneof![
            es.string.clone(),
            es.integer.clone(),
            es.bool.clone(),
            es.timestamp.clone()
        ];
        let concats = proptest::collection::vec(random_expr, 0..3);
        (es.string, concats).prop_map(|(s, mut exprs): (_, Vec<Expr>)| {
            exprs.insert(0, s);
            Expr::Call(FunctionExpr::Call {
                name: "concat".into(),
                arguments: Some(exprs),
            })
        })
    }

    /// Produces a [`Strategy`] that generates a string [`Expr::Call`] with
    /// [`BuiltinFunction::ConcatWs`].
    fn concat_ws(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let random_expr = prop_oneof![
            es.string.clone(),
            es.integer.clone(),
            es.bool.clone(),
            es.timestamp.clone()
        ];
        let concat_wss = proptest::collection::vec(random_expr, 0..3);
        (es.string, concat_wss).prop_map(|(s, mut exprs): (_, Vec<Expr>)| {
            exprs.insert(0, s);
            Expr::Call(FunctionExpr::Call {
                name: "concat_ws".into(),
                arguments: Some(exprs),
            })
        })
    }

    /// Produces a [`Strategy`] that generates a string [`Expr::Call`] with
    /// [`BuiltinFunction::SplitPart`].
    fn split_part(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        (es.string.clone(), es.string.clone(), es.integer).prop_map(|(s1, s2, int)| {
            Expr::Call(FunctionExpr::Call {
                name: "split_part".into(),
                arguments: Some(vec![s1, s2, int]),
            })
        })
    }

    /// Produces a [`Strategy`] that generates a string [`Expr::Call`] with
    /// [`BuiltinFunction::Substring`].
    fn substring(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let opt_int = proptest::option::of(es.integer.clone());
        (es.string, opt_int.clone(), opt_int).prop_map(|(s, int1, int2)| {
            let mut args = Vec::new();
            args.push(s);
            if let Some(int1) = int1 {
                args.push(int1);
            }
            if let Some(int2) = int2 {
                args.push(int2);
            }
            Expr::Call(FunctionExpr::Call {
                name: "substring".into(),
                arguments: Some(args),
            })
        })
    }
}
