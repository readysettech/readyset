#![warn(clippy::dbg_macro)]
#![feature(box_patterns)]

extern crate nom;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::common::{
    FieldDefinitionExpression, ItemPlaceholder, Literal, Real, SqlType, TableKey,
};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::create::{CreateTableStatement, CreateViewStatement, SelectSpecification};
pub use self::delete::DeleteStatement;
pub use self::dialect::Dialect;
pub use self::expression::{
    BinaryOperator, Expression, FunctionExpression, InValue, UnaryOperator,
};
pub use self::insert::InsertStatement;
pub use self::join::{JoinConstraint, JoinOperator, JoinRightSide};
pub use self::order::{OrderClause, OrderType};
pub use self::parser::*;
pub use self::select::{
    CommonTableExpression, GroupByClause, JoinClause, LimitClause, SelectStatement,
};
pub use self::set::SetStatement;
pub use self::table::Table;
pub use self::update::UpdateStatement;

pub mod analysis;
pub mod parser;

#[macro_use]
mod dialect;

mod alter;
mod case;
mod column;
mod common;
mod compound_select;
mod create;
mod create_table_options;
mod delete;
mod drop;
mod expression;
mod insert;
mod join;
mod keywords;
mod order;
mod select;
mod set;
mod table;
mod update;
