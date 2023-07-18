//! Module that holds all the functions to get a proptest [`Strategy`]
//! that generates [`Expr`]s that resolve to an integer value.
//TODO(fran):
//  - Generate different types of integer (signed, unsigned; tiny, small, etc)
//  - Have a specific recursive integer generator for negative numbers (to use with ROUND func).

use nom_sql::{BinaryOperator, Dialect, Expr, Literal, SqlType};
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};

use crate::expression::util::{case_when, cast};
use crate::expression::ExprStrategy;

/// Produces a [`Strategy`] that generates a non-base (neither literal nor column) integer [`Expr`],
/// using other kinds of [`Expr`] provided by the given [`ExprStrategy`]
pub(super) fn generate_integer(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
    let case_when = case_when(es.integer.clone(), es.bool.clone());
    prop_oneof![
        case_when,
        integer_cast(es.clone()),
        op(es.clone()),
        call::call(es, dialect)
    ]
}

/// Produces a [`Strategy`] that generates an integer [`Expr::Cast`].
// TODO(fran): Add missing types + make this into a macro
fn integer_cast(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let string = cast(es.string, SqlType::Int4, false);
    let integer = cast(es.integer, SqlType::Int4, false);
    let float = cast(es.float, SqlType::Int4, false);
    let timestamp = cast(es.timestamp, SqlType::Int4, false);
    prop_oneof![string, integer, float, timestamp]
}

/// Produces a [`Strategy`] that generates an integer [`Expr::Op`].
fn op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let op = prop_oneof![
        Just(BinaryOperator::Add),
        Just(BinaryOperator::Subtract),
        Just(BinaryOperator::Multiply),
        Just(BinaryOperator::Divide)
    ];
    prop_oneof![
        // Choose a random op ~94% of the time
        16 => (es.integer.clone(), es.integer.clone(), op).prop_map(|(n1, n2, op)| Expr::BinaryOp {
            lhs: Box::new(n1),
            op,
            rhs: Box::new(n2),
        }),
        // Choose a manually configured divide expression that resolves to an error ~6% of the time.
        // This is done to guarantee that we get a divide expression that divides by zero, as this
        // would be very unlikely to be generated if we generate random expressions at both sides
        // of the operator.
        1 => es.integer.prop_map(|n| Expr::BinaryOp {
            lhs: Box::new(n),
            op: BinaryOperator::Divide,
            rhs: Box::new(Expr::Literal(Literal::Integer(0))),
        })
    ]
}

/// Helper module to group all the [`Strategy`]s that produce integer [`Expr::Call`].
mod call {
    use nom_sql::{Dialect, Expr, FunctionExpr, Literal};
    use proptest::prelude::Strategy;
    use proptest::prop_oneof;
    use proptest::strategy::Just;

    use crate::expression::util::{coalesce, if_null};
    use crate::expression::ExprStrategy;

    /// Produces a [`Strategy`] that generates an integer [`Expr::Call`].
    pub(super) fn call(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
        match dialect {
            Dialect::PostgreSQL => prop_oneof![coalesce(es.integer.clone()), round(es),].boxed(),
            Dialect::MySQL => prop_oneof![
                coalesce(es.integer.clone()),
                if_null(es.integer.clone()),
                day_of_week(es.clone()),
                month(es.clone()),
                round(es)
            ]
            .boxed(),
        }
    }

    /// Produces a [`Strategy`] that generates an integer [`Expr::Call`] with
    /// [`BuiltinFunction::DayOfWeek`].
    fn day_of_week(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        es.timestamp.prop_map(|dt| {
            Expr::Call(FunctionExpr::Call {
                name: "dayofweek".into(),
                arguments: vec![dt],
            })
        })
    }

    /// Produces a [`Strategy`] that generates an integer [`Expr::Call`] with
    /// [`BuiltinFunction::Month`].
    fn month(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        es.timestamp.prop_map(|dt| {
            Expr::Call(FunctionExpr::Call {
                name: "month".into(),
                arguments: vec![dt],
            })
        })
    }

    /// Produces a [`Strategy`] that generates an integer [`Expr::Call`] with
    /// [`BuiltinFunction::Round`] with a negative precision.
    fn round(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        // TODO(fran): Use recursive expr instead of manually building a negative or zero literal
        //  one. This is tricky as negative integers might become positive through + operator.
        (es.float, i64::MIN..=0).prop_flat_map(|(n1, neg_or_zero)| {
            prop_oneof![
                Just(Expr::Call(FunctionExpr::Call {
                    name: "round".into(),
                    arguments: vec![n1.clone(), Expr::Literal(Literal::Integer(neg_or_zero))],
                })),
                Just(Expr::Call(FunctionExpr::Call {
                    name: "round".into(),
                    arguments: vec![n1]
                }))
            ]
        })
    }
}
