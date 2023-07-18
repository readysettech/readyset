//! Module that holds all the functions to get a proptest [`Strategy`]
//! that generates [`Expr`]s that resolve to a boolean value.

use nom_sql::{Dialect, Expr, SqlType};
use proptest::prop_oneof;
use proptest::strategy::Strategy;

use crate::expression::util::{case_when, cast};
use crate::expression::ExprStrategy;

/// Produces a [`Strategy`] that generates a non-base (neither literal nor column) boolean [`Expr`],
/// using other kinds of [`Expr`] provided by the given [`ExprStrategy`]
pub(super) fn generate_bool(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
    let case_when = case_when(es.bool.clone(), es.bool.clone());
    prop_oneof![
        op::op(es.clone()),
        bool_cast(es.clone()),
        call::call(es, dialect),
        case_when
    ]
}

/// Produces a [`Strategy`] that generates an [`Expr::Cast`].
// TODO(fran): Add missing types + try to make this into a macro
fn bool_cast(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let string = cast(es.string, SqlType::Bool, false);
    let integer = cast(es.integer, SqlType::Bool, false);
    let float = cast(es.float, SqlType::Bool, false);
    let timestamp = cast(es.timestamp, SqlType::Bool, false);
    prop_oneof![string, integer, float, timestamp]
}

/// Helper module to group all the [`Strategy`]s that generate boolean [`Expr::Op`].
// TODO(fran): Add OpAll, OpAny, and operators that predicate over JSON
mod op {
    use nom_sql::{BinaryOperator, Expr};
    use proptest::prop_oneof;
    use proptest::strategy::{Just, Strategy};

    use crate::expression::ExprStrategy;

    /// Produces a [`Strategy`] that generates a boolean [`Expr::Op`].
    pub(super) fn op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        prop_oneof![like_op(es.clone()), bool_op(es.clone()), comparison_op(es)]
    }

    /// Produces a [`Strategy`] that generates a boolean [`Expr::Op`], where the [`BinaryOperator`]s
    /// being used are comparison ones (<, <=, =, =>, >).
    fn comparison_op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let op = prop_oneof![
            Just(BinaryOperator::Less),
            Just(BinaryOperator::LessOrEqual),
            Just(BinaryOperator::Equal),
            Just(BinaryOperator::NotEqual),
            Just(BinaryOperator::GreaterOrEqual),
            Just(BinaryOperator::Greater),
        ];
        // TODO(fran): Add other comparable types
        let number = prop_oneof![es.integer.clone(), es.float.clone()];
        // TODO(fran): Add other comparable types.
        let random_comp = prop_oneof![
            (number.clone(), number.clone(), op.clone()).prop_map(|(n1, n2, op)| Expr::BinaryOp {
                lhs: Box::new(n1),
                op,
                rhs: Box::new(n2),
            }),
            (es.timestamp.clone(), es.timestamp, op).prop_map(|(t1, t2, op)| {
                Expr::BinaryOp {
                    lhs: Box::new(t1),
                    op,
                    rhs: Box::new(t2),
                }
            })
        ];
        let equal_comp = prop_oneof![Just(BinaryOperator::Equal), Just(BinaryOperator::NotEqual)];
        // TODO(fran): Add other comparable types.
        let equal_comp = prop_oneof![
            (number.clone(), equal_comp.clone()).prop_map(|(n, op)| Expr::BinaryOp {
                lhs: Box::new(n.clone()),
                op,
                rhs: Box::new(n),
            }),
            (number, equal_comp).prop_map(|(n, op)| Expr::BinaryOp {
                lhs: Box::new(n.clone()),
                op,
                rhs: Box::new(n),
            })
        ];
        prop_oneof![
            // Choose a random op 95% of the type
            19 => random_comp,
            // Choose a manually configured equal expression that resolves to true 5% of the time.
            // This is done to guarantee that we get an equal expression that resolves to true, as this
            // would be very unlikely to be generated if we generate random expressions at both sides
            // of the operator.
            1 => equal_comp
        ]
    }

    /// Produces a [`Strategy`] that generates a boolean [`Expr::Op`], where the [`BinaryOperator`]
    /// is a boolean operator (AND, OR).
    fn bool_op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let op = prop_oneof![Just(BinaryOperator::And), Just(BinaryOperator::Or)];
        (es.bool.clone(), es.bool, op).prop_map(|(b1, b2, op)| Expr::BinaryOp {
            lhs: Box::new(b1),
            op,
            rhs: Box::new(b2),
        })
    }

    /// Produces a [`Strategy`] that generates a boolean [`Expr::Op`], where the [`BinaryOperator`]
    /// is a string comparison one (LIKE, ILIKE).
    fn like_op(es: ExprStrategy) -> impl Strategy<Value = Expr> {
        let op = prop_oneof![Just(BinaryOperator::Like), Just(BinaryOperator::ILike)];
        let random_comp =
            (es.string.clone(), es.string.clone(), op.clone()).prop_map(|(s1, s2, op)| {
                Expr::BinaryOp {
                    lhs: Box::new(s1),
                    op,
                    rhs: Box::new(s2),
                }
            });
        let equal_comp = (es.string, op).prop_map(|(s, op)| Expr::BinaryOp {
            lhs: Box::new(s.clone()),
            op,
            rhs: Box::new(s),
        });
        prop_oneof![
            // Choose a random op 50% of the time
            2 => random_comp,
            // Choose a manually configured LIKE/ILIKE expression that resolves to true, 50% of the time.
            // This is done to guarantee that we get a LIKE/ILIKE expression that resolves to true, as this
            // would be very unlikely to be generated if we generate random expressions at both sides
            // of the operator.
            1 => equal_comp
        ]
    }
}

/// Helper module to group all the [`Strategy`]s that produce boolean [`Expr::Call`].
// TODO(fran): Add JSON functions that return boolean values
mod call {
    use nom_sql::{Dialect, Expr};
    use proptest::prop_oneof;
    use proptest::strategy::Strategy;

    use crate::expression::util::{coalesce, if_null};
    use crate::expression::ExprStrategy;

    /// Produces a [`Strategy`] that generates a boolean [`Expr::Call`].
    pub(super) fn call(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
        match dialect {
            Dialect::PostgreSQL => coalesce(es.bool).boxed(),
            Dialect::MySQL => prop_oneof![if_null(es.bool.clone()), coalesce(es.bool)].boxed(),
        }
    }
}
