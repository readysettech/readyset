#![warn(clippy::dbg_macro)]
#![feature(or_patterns)]

extern crate nom;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

pub use self::arithmetic::{Arithmetic, ArithmeticBase, ArithmeticItem, ArithmeticOperator};
pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::common::{
    BinaryOperator, FieldDefinitionExpression, ItemPlaceholder, Literal, Real, SqlType, TableKey,
};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::condition::{ConditionBase, ConditionExpression, ConditionTree};
pub use self::create::{CreateTableStatement, CreateViewStatement, SelectSpecification};
pub use self::delete::DeleteStatement;
pub use self::expression::{Expression, FunctionExpression};
pub use self::insert::InsertStatement;
pub use self::join::{JoinConstraint, JoinOperator, JoinRightSide};
pub use self::order::{OrderClause, OrderType};
pub use self::parser::*;
pub use self::select::{GroupByClause, JoinClause, LimitClause, SelectStatement};
pub use self::set::SetStatement;
pub use self::table::Table;
pub use self::update::UpdateStatement;

pub mod analysis;
pub mod parser;

#[macro_use]
mod keywords;
mod alter;
mod arithmetic;
mod case;
mod column;
mod common;
mod compound_select;
mod condition;
mod create;
mod create_table_options;
mod delete;
mod drop;
mod expression;
mod insert;
mod join;
mod order;
mod select;
mod set;
mod table;
mod update;
