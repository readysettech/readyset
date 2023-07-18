//! Module that holds all the functions to get a proptest [`Strategy`]
//! that generates [`Expr`]s that resolve to a timestamp value.
use nom_sql::{Dialect, Expr, SqlType};
use proptest::prop_oneof;
use proptest::strategy::Strategy;

use crate::expression::util::{case_when, cast};
use crate::expression::ExprStrategy;

/// Produces a [`Strategy`] that generates a non-base (neither literal nor column) timestamp
/// [`Expr`], using other kinds of [`Expr`] provided by the given [`ExprStrategy`]
pub(super) fn generate_timestamp(
    es: ExprStrategy,
    dialect: &Dialect,
) -> impl Strategy<Value = Expr> {
    let case_when = case_when(es.timestamp.clone(), es.bool.clone());
    prop_oneof![
        case_when,
        timestamp_cast(es.clone()),
        call::call(es, dialect)
    ]
}

/// Produces a [`Strategy`] that generates a timestamp [`Expr::Cast`].
// TODO(fran): Add missing types + make this into a macro
fn timestamp_cast(es: ExprStrategy) -> impl Strategy<Value = Expr> {
    let string = cast(es.string, SqlType::Timestamp, false);
    let integer = cast(es.integer, SqlType::Timestamp, false);
    let float = cast(es.float, SqlType::Timestamp, false);
    let timestamp = cast(es.timestamp, SqlType::Timestamp, false);
    prop_oneof![string, integer, float, timestamp]
}

/// Helper module to group all the [`Strategy`]s that generate timestamp [`Expr::Call`].
mod call {
    use nom_sql::{Dialect, Expr};
    use proptest::prelude::Strategy;
    use proptest::prop_oneof;

    use crate::expression::util::{coalesce, if_null};
    use crate::expression::ExprStrategy;

    /// Produces a [`Strategy`] that generates a timestamp [`Expr::Call`].
    pub(super) fn call(es: ExprStrategy, dialect: &Dialect) -> impl Strategy<Value = Expr> {
        match dialect {
            Dialect::PostgreSQL => coalesce(es.timestamp).boxed(),
            Dialect::MySQL => {
                prop_oneof![if_null(es.timestamp.clone()), coalesce(es.timestamp)].boxed()
            }
        }
    }
}
