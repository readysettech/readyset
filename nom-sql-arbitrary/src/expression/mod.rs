//! Module that provides proptest [`Strategy`]s for
//! [`Expr`] AST structs.
// TODO(fran): Generate more complex `Expr`:
//   - Arrays
//   - JSON
use nom_sql::{Dialect, Expr};
use proptest::arbitrary::{any, any_with};
use proptest::strategy::{BoxedStrategy, Just, Strategy};

use crate::expression::bool::generate_bool;
use crate::expression::expr::{generate_base_expr, ExprSpec};
use crate::expression::float::generate_float;
use crate::expression::integer::generate_integer;
use crate::expression::string::generate_string;
use crate::expression::timestamp::generate_timestamp;

mod bool;
mod expr;
mod float;
mod integer;
mod string;
mod timestamp;
mod util;

/// A struct that holds the [`Strategy`]s to generate
/// [`Expr`] of different kinds.
// TODO(fran): Expand the kinds of `Expr` being generated
#[derive(Debug, Clone)]
pub struct ExprStrategy {
    pub bool: BoxedStrategy<Expr>,
    pub string: BoxedStrategy<Expr>,
    pub integer: BoxedStrategy<Expr>,
    pub float: BoxedStrategy<Expr>,
    pub timestamp: BoxedStrategy<Expr>,
}

/// Produces a [`Strategy`] that generates [`ExprStrategy`] structs, where
/// the underlying [`Expr`] of said structs get recursively more complex.
#[allow(dead_code)]
fn arbitrary_expr_strategy(dialect: Dialect) -> impl Strategy<Value = ExprStrategy> {
    Just(ExprStrategy {
        bool: generate_base_expr(ExprSpec::Bool).boxed(),
        string: generate_base_expr(ExprSpec::String(
            any_with::<String>("[a-zA-Z0-9]{0,8}".into()).boxed(),
        ))
        .boxed(),
        integer: generate_base_expr(ExprSpec::UnsignedInt(any::<u32>().boxed())).boxed(),
        float: generate_base_expr(ExprSpec::Float(any::<f32>().boxed())).boxed(),
        timestamp: generate_base_expr(ExprSpec::Timestamp).boxed(),
    })
    .prop_recursive(3, 32, 3, move |inner| {
        inner.prop_map(move |e| ExprStrategy {
            // TODO(fran): Replace this with respective recursive function for each type
            bool: generate_bool(e.clone(), &dialect).boxed(),
            string: generate_string(e.clone(), &dialect).boxed(),
            integer: generate_integer(e.clone(), &dialect).boxed(),
            float: generate_float(e.clone(), &dialect).boxed(),
            timestamp: generate_timestamp(e, &dialect).boxed(),
        })
    })
}
