#![deny(macro_use_extern_crate)]
#![feature(box_patterns)]

#[allow(macro_use_extern_crate)]
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

pub use self::alter::{AlterColumnOperation, AlterTableDefinition, AlterTableStatement};
pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::common::{FieldDefinitionExpr, FieldReference, IndexType, TableKey};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::create::{
    CacheInner, CreateCacheStatement, CreateTableStatement, CreateViewStatement,
    SelectSpecification,
};
pub use self::delete::DeleteStatement;
pub use self::dialect::Dialect;
pub use self::drop::{
    DropAllCachesStatement, DropCacheStatement, DropTableStatement, DropViewStatement,
};
pub use self::explain::ExplainStatement;
pub use self::expression::{BinaryOperator, Expr, FunctionExpr, InValue, UnaryOperator};
pub use self::insert::InsertStatement;
pub use self::join::{JoinConstraint, JoinOperator, JoinRightSide};
pub use self::literal::{
    embedded_literal, literal, raw_string_literal, utf8_string_literal, Double, Float,
    ItemPlaceholder, Literal, QuotingStyle,
};
pub use self::order::{OrderClause, OrderType};
pub use self::parser::*;
pub use self::select::{CommonTableExpr, GroupByClause, JoinClause, SelectStatement};
pub use self::set::{
    PostgresParameterScope, PostgresParameterValue, PostgresParameterValueInner, SetNames,
    SetPostgresParameter, SetPostgresParameterValue, SetStatement, SetVariables, Variable,
    VariableScope,
};
pub use self::show::ShowStatement;
pub use self::sql_identifier::SqlIdentifier;
pub use self::sql_type::SqlType;
pub use self::table::{replicator_table_list, Table, TableExpr};
pub use self::update::UpdateStatement;
pub use self::use_statement::UseStatement;

pub mod parser;

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
mod literal;
mod order;
mod rename;
mod select;
mod set;
mod show;
mod sql_identifier;
mod sql_type;
mod table;
mod transaction;
mod update;
mod use_statement;
pub mod whitespace;
