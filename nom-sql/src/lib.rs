#![deny(macro_use_extern_crate)]
#![feature(box_patterns)]

#[allow(macro_use_extern_crate)]
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

pub use sql_identifier::SqlIdentifier;

pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::common::{
    Double, FieldDefinitionExpression, Float, ItemPlaceholder, Literal, SqlType, TableKey,
};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::create::{
    CachedQueryInner, CreateCachedQueryStatement, CreateTableStatement, CreateViewStatement,
    SelectSpecification,
};
pub use self::delete::DeleteStatement;
pub use self::dialect::Dialect;
pub use self::drop::DropCachedQueryStatement;
pub use self::explain::ExplainStatement;
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
pub use self::show::ShowStatement;
pub use self::table::Table;
pub use self::update::UpdateStatement;
pub use self::use_statement::UseStatement;

pub mod parser;

#[macro_use]
mod dialect;
#[macro_use]
mod macros;

mod alter;
pub mod analysis;
mod case;
mod column;
mod common;
mod compound_select;
mod create;
mod create_table_options;
mod delete;
mod drop;
mod explain;
mod expression;
mod insert;
mod join;
mod keywords;
mod order;
mod rename;
mod select;
mod set;
mod show;
mod sql_identifier;
mod table;
mod transaction;
mod update;
mod use_statement;
