#![deny(macro_use_extern_crate)]
#![allow(incomplete_features)]
#![feature(box_patterns)]
#![allow(macro_use_extern_crate)]

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

use std::fmt::{Debug, Formatter};

use nom::error::{ErrorKind, FromExternalError, ParseError};
use nom::{AsBytes, Err, HexDisplay, IResult};
use nom_locate::LocatedSpan;

pub use self::alter::{
    AlterColumnOperation, AlterTableDefinition, AlterTableStatement, ReplicaIdentity,
};
pub use self::column::{Column, ColumnConstraint, ColumnSpecification};
pub use self::comment::CommentStatement;
pub use self::common::{FieldDefinitionExpr, FieldReference, IndexType, TableKey};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::create::{
    CacheInner, CreateCacheStatement, CreateTableBody, CreateTableStatement, CreateViewStatement,
    SelectSpecification,
};
pub use self::create_table_options::CreateTableOption;
pub use self::delete::DeleteStatement;
pub use self::dialect::{Dialect, DialectDisplay};
pub use self::drop::{
    DropAllCachesStatement, DropCacheStatement, DropTableStatement, DropViewStatement,
};
pub use self::explain::ExplainStatement;
pub use self::expression::{
    BinaryOperator, CaseWhenBranch, Expr, FunctionExpr, InValue, UnaryOperator,
};
pub use self::insert::InsertStatement;
pub use self::join::{JoinConstraint, JoinOperator, JoinRightSide};
pub use self::literal::{
    embedded_literal, literal, raw_string_literal, utf8_string_literal, Double, Float,
    ItemPlaceholder, Literal, QuotingStyle,
};
pub use self::order::{OrderBy, OrderClause, OrderType};
pub use self::parser::*;
pub use self::select::{CommonTableExpr, GroupByClause, JoinClause, LimitClause, SelectStatement};
pub use self::set::{
    PostgresParameterScope, PostgresParameterValue, PostgresParameterValueInner, SetNames,
    SetPostgresParameter, SetPostgresParameterValue, SetStatement, SetVariables, Variable,
    VariableScope,
};
pub use self::show::ShowStatement;
pub use self::sql_identifier::SqlIdentifier;
pub use self::sql_type::{EnumVariants, SqlType, SqlTypeArbitraryOptions};
pub use self::table::{
    replicator_table_list, NonReplicatedRelation, NotReplicatedReason, Relation, TableExpr,
    TableExprInner,
};
pub use self::transaction::StartTransactionStatement;
pub use self::update::UpdateStatement;
pub use self::use_statement::UseStatement;

pub mod parser;

#[macro_use]
mod macros;

mod alter;
pub mod analysis;
mod column;
mod comment;
mod common;
mod compound_select;
mod create;
mod create_table_options;
mod delete;
mod dialect;
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

pub type NomSqlResult<I, O> = IResult<LocatedSpan<I>, O, NomSqlError<I>>;

#[derive(PartialEq, Eq)]
pub struct NomSqlError<I: AsBytes> {
    pub input: LocatedSpan<I>,
    pub kind: ErrorKind,
}

impl<I: AsBytes> ParseError<LocatedSpan<I>> for NomSqlError<I> {
    fn from_error_kind(input: LocatedSpan<I>, kind: ErrorKind) -> Self {
        Self { input, kind }
    }

    fn append(_input: LocatedSpan<I>, _kind: ErrorKind, other: Self) -> Self {
        other
    }

    fn from_char(input: LocatedSpan<I>, _: char) -> Self {
        Self::from_error_kind(input, ErrorKind::Char)
    }

    /// Used by branching combinators when no branch succeeds, to decide which error to propagate.
    /// The error propagated by the combinator is from the branch that made it furthest through
    /// the input. If multiple branches reach the same furthest character in the input, then the
    /// error propagated is from the branch tried last.
    fn or(self, other: Self) -> Self {
        if self.input.location_offset() > other.input.location_offset() {
            self
        } else {
            other
        }
    }
}

impl<I: AsBytes, E> FromExternalError<LocatedSpan<I>, E> for NomSqlError<I> {
    fn from_external_error(input: LocatedSpan<I>, kind: ErrorKind, _e: E) -> Self {
        NomSqlError { input, kind }
    }
}

impl<I: AsBytes + Debug> Debug for NomSqlError<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // If the input sequence is valid UTF-8, convert it and the position info
        // Else, fall back to the equivalent of the default debug impl
        match std::str::from_utf8(self.input.fragment().as_bytes()) {
            Ok(input) => f
                .debug_struct("NomSqlError")
                .field("input", &input)
                .field("offset", &self.input.location_offset())
                .field("line", &self.input.location_line())
                .field("col", &self.input.get_utf8_column())
                .field("kind", &self.kind)
                .finish(),
            Err(_) => f
                .debug_struct("NomSqlError")
                .field("input", &self.input)
                .field("kind", &self.kind)
                .finish(),
        }
    }
}

/// Converts a NomSqlResult into a nom::IResult, discarding any position information provided by the
/// nom_locate crate. Useful for unit tests.
pub fn to_nom_result<I: AsBytes + Copy, O>(res: NomSqlResult<I, O>) -> IResult<I, O> {
    res.map(|(input, output)| (*input, output))
        .map_err(|err| match err {
            Err::Incomplete(needed) => Err::Incomplete(needed),
            Err::Error(error) => Err::Error(nom::error::Error::new(*error.input, error.kind)),
            Err::Failure(error) => Err::Failure(nom::error::Error::new(*error.input, error.kind)),
        })
}

#[allow(dead_code)]
pub fn dbg_dmp<'a, F, O>(
    mut f: F,
    context: &'static str,
) -> impl FnMut(LocatedSpan<&'a [u8]>) -> NomSqlResult<&'a [u8], O>
where
    O: Debug,
    F: FnMut(LocatedSpan<&'a [u8]>) -> NomSqlResult<&'a [u8], O>,
{
    move |i: LocatedSpan<&'a [u8]>| match f(i) {
        Err(e) => {
            println!("{context}: Error({e:?}) at:\n{}", i.to_hex(8));
            Err(e)
        }
        Ok((i, o)) => {
            println!("{context}: Ok(({i:?}, {o:?})) at:\n{}", i.to_hex(8));
            Ok((i, o))
        }
    }
}

#[cfg(test)]
mod tests {
    use nom::branch::alt;
    use nom::bytes::complete::tag;
    use nom::combinator::{map, not};

    use super::*;
    use crate::whitespace::whitespace0;

    #[test]
    fn it_takes_the_greater_of_two_locations() {
        fn parse_aa(i: LocatedSpan<&str>) -> NomSqlResult<&str, LocatedSpan<&str>> {
            let (i, _) = tag("a")(i)?;
            tag("a")(i)
        }

        fn parse_bb(i: LocatedSpan<&str>) -> NomSqlResult<&str, LocatedSpan<&str>> {
            let (i, _) = tag("b")(i)?;
            tag("b")(i)
        }

        fn parser(i: LocatedSpan<&str>) -> NomSqlResult<&str, LocatedSpan<&str>> {
            alt((parse_aa, parse_bb))(i)
        }

        let err = parser(LocatedSpan::new("ab")).unwrap_err();
        assert_eq!(
            err,
            Err::Error(NomSqlError {
                input: unsafe { LocatedSpan::new_from_raw_offset(1, 1, "b", ()) },
                kind: ErrorKind::Tag
            })
        );
    }

    #[test]
    fn it_takes_the_last_of_two_equal_locations() {
        fn parser(i: LocatedSpan<&str>) -> NomSqlResult<&str, ()> {
            alt((map(tag("a"), |_| ()), not(tag("b"))))(i)
        }

        let err = parser(LocatedSpan::new("b")).unwrap_err();
        assert_eq!(
            err,
            Err::Error(NomSqlError {
                input: unsafe { LocatedSpan::new_from_raw_offset(0, 1, "b", ()) },
                kind: ErrorKind::Not
            })
        );
    }

    #[test]
    fn it_handles_line_endings() {
        fn parser(i: LocatedSpan<&str>) -> NomSqlResult<&str, LocatedSpan<&str>> {
            let (i, _) = whitespace0(i)?;
            tag("a")(i)
        }

        let err = parser(LocatedSpan::new(" \r\n \r \n \n\r\t\t\r\n\rb")).unwrap_err();
        assert_eq!(
            err,
            Err::Error(NomSqlError {
                input: unsafe { LocatedSpan::new_from_raw_offset(15, 5, "b", ()) },
                kind: ErrorKind::Tag
            })
        );
    }
}
