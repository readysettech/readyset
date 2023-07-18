//! Module that holds all the functions to get a proptest [`Strategy`]
//! that generates [`Expr`]s that resolve to a float value.
//TODO(fran):
//  - Generate different types of float (double, numeric)

use nom_sql::{BinaryOperator, Dialect, Expr, Float, Literal, SqlType};
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};

use crate::expression::util::{case_when, cast};
use crate::expression::ExprStrategy;

/// Produces a [`Strategy`] that generates a non-base (neither literal nor column) float [`Expr`],
/// using other kinds of [`Expr`] provided by the given [`ExprStrategy`]
pub(super) fn generate_float(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
    let case_when = case_when(es.float.clone(), es.bool.clone());
    prop_oneof![
        case_when,
        float_cast(es.clone()),
        op(es.clone()),
        call::call(es, dialect)
    ]
}

/// Produces a [`Strategy`] that generates a float [`Expr::Cast`].
// TODO(fran): Add missing types + make this into a macro
fn float_cast(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let string = cast(es.string, SqlType::Float, false);
    let integer = cast(es.integer, SqlType::Float, false);
    let float = cast(es.float, SqlType::Float, false);
    let timestamp = cast(es.timestamp, SqlType::Float, false);
    prop_oneof![string, integer, float, timestamp]
}

/// Produces a [`Strategy`] that generates a float [`Expr::Op`].
fn op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let op = prop_oneof![
        Just(BinaryOperator::Add),
        Just(BinaryOperator::Subtract),
        Just(BinaryOperator::Multiply),
        Just(BinaryOperator::Divide)
    ];
    prop_oneof![
        // Choose a random op ~94% of the time
        16 => (es.float.clone(), es.float.clone(), op).prop_map(|(f1, f2, op)| Expr::BinaryOp {
            lhs: Box::new(f1),
            op,
            rhs: Box::new(f2),
        }),
        // Choose a manually configured divide expression that resolves to an error ~6% of the time.
        // This is done to guarantee that we get a divide expression that divides by zero, as this
        // would be very unlikely to be generated if we generate random expressions at both sides
        // of the operator.
        1 => es.float.prop_map(|f| Expr::BinaryOp {
            lhs: Box::new(f),
            op: BinaryOperator::Divide,
            rhs: Box::new(Expr::Literal(Literal::Float(Float {
                value: 0.0,
                precision: u8::MAX
            }))),
        })
    ]
}

/// Helper module to group all the [`Strategy`]s that generate float [`Expr::Call`].
mod call {
    use nom_sql::{Dialect, Expr, FunctionExpr};
    use proptest::prelude::Strategy;
    use proptest::prop_oneof;

    use crate::expression::util::{coalesce, if_null};
    use crate::expression::ExprStrategy;

    /// Produces a [`Strategy`] that generates a float [`Expr::Call`].
    pub(super) fn call(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
        match dialect {
            Dialect::PostgreSQL => prop_oneof![round(es.clone()), coalesce(es.float)].boxed(),
            Dialect::MySQL => prop_oneof![
                if_null(es.float.clone()),
                coalesce(es.float.clone()),
                round(es)
            ]
            .boxed(),
        }
    }

    /// Produces a [`Strategy`] that generates a float [`Expr::Call`] with
    /// [`BuiltinFunction::Round`].
    fn round(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        (es.float, es.integer).prop_map(|(f, n)| {
            Expr::Call(FunctionExpr::Call {
                name: "round".into(),
                arguments: vec![f, n],
            })
        })
    }
}
