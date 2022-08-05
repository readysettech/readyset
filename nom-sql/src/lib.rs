#![deny(macro_use_extern_crate)]
#![feature(box_patterns)]

#[allow(macro_use_extern_crate)]
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

use nom::error::{ErrorKind, FromExternalError, ParseError};
use nom::IResult;
use nom_locate::LocatedSpan;
pub use sql_identifier::SqlIdentifier;

pub use self::alter::{AlterColumnOperation, AlterTableDefinition, AlterTableStatement};
pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::common::{FieldDefinitionExpr, FieldReference, IndexType, SqlType, TableKey};
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
mod table;
mod transaction;
mod update;
mod use_statement;
pub mod whitespace;

type Span<'a> = LocatedSpan<&'a [u8]>;
type Error<'a> = NomSqlError<Span<'a>>;
type NomSqlResult<'a, O> = IResult<Span<'a>, O, Error<'a>>;

#[derive(Debug)]
struct NomSqlError<I> {
    input: I,
    kind: ErrorKind,
    offset: usize,
    line: u32,
}

impl<'a> ParseError<Span<'a>> for NomSqlError<Span<'a>> {
    fn from_error_kind(input: Span<'a>, kind: ErrorKind) -> Self {
        Self {
            input,
            kind,
            offset: input.location_offset(),
            line: input.location_line(),
        }
    }

    fn append(input: Span<'a>, kind: ErrorKind, other: Self) -> Self {
        //println!("input: {:?}, kind: {:?}, other: {:?}", input, kind, other);
        other
    }

    fn from_char(input: Span<'a>, _: char) -> Self {
        Self::from_error_kind(input, ErrorKind::Char)
    }

    // TODO what if both branches got equally far i.e. self.offset == other.offset?
    fn or(self, other: Self) -> Self {
        if self.offset > other.offset {
            self
        } else {
            other
        }
    }
}

impl<'a> FromExternalError<Span<'a>, Utf8Error> for NomSqlError<Span<'a>> {
    
}

impl<'a> From<nom::error::Error<Span<'a>>> for NomSqlError<Span<'a>> {
    fn from(e: nom::error::Error<Span<'a>>) -> Self {
        Self {
            input: e.input,
            kind: e.code,
            offset: e.input.location_offset(),
            line: e.input.location_line(),
        }
    }
}
