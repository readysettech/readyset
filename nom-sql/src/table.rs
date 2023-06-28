use std::fmt::Display;
use std::hash::Hash;
use std::str;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::terminated;
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{as_alias, ws_sep_comma};
use crate::select::nested_selection;
use crate::whitespace::whitespace0;
use crate::{Dialect, NomSqlResult, SelectStatement, SqlIdentifier};

/// A (potentially schema-qualified) name for a relation
///
/// This type is (perhaps surprisingly) quite pervasive - it's used as not only the names for tables
/// and views, but also all nodes in the graph (both MIR and dataflow).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub struct Relation {
    /// The optional schema for the relation.
    ///
    /// Note that this name is maximally general - what MySQL calls a "database" is actually much
    /// closer to a schema (and in fact you can call it a `SCHEMA` in mysql commands!).
    pub schema: Option<SqlIdentifier>,

    /// The name of the relation itself
    pub name: SqlIdentifier,
}

impl From<SqlIdentifier> for Relation {
    fn from(name: SqlIdentifier) -> Self {
        Relation { name, schema: None }
    }
}

impl From<&SqlIdentifier> for Relation {
    fn from(name: &SqlIdentifier) -> Self {
        Relation {
            name: name.clone(),
            schema: None,
        }
    }
}

impl<'a> From<&'a str> for Relation {
    fn from(t: &str) -> Relation {
        Relation {
            name: t.into(),
            schema: None,
        }
    }
}

impl From<String> for Relation {
    fn from(t: String) -> Self {
        Relation {
            name: t.into(),
            schema: None,
        }
    }
}

impl<'a> From<&'a String> for Relation {
    fn from(s: &'a String) -> Self {
        Self::from(s.as_str())
    }
}

impl Relation {
    pub fn display(&self, dialect: Dialect) -> impl Display + Copy + '_ {
        fmt_with(move |f| {
            if let Some(schema) = &self.schema {
                write!(f, "{}.", dialect.quote_identifier(schema))?;
            }
            write!(f, "{}", dialect.quote_identifier(&self.name))
        })
    }

    /// Like [`display()`](Self::display) except the schema and table name will not be quoted.
    ///
    /// This should not be used to emit SQL code and instead should mostly be for error messages.
    pub fn display_unquoted(&self) -> impl Display + Copy + '_ {
        fmt_with(move |f| {
            if let Some(schema) = &self.schema {
                write!(f, "{schema}.")?;
            }
            write!(f, "{}", self.name)
        })
    }
}

/// An expression for a table in a query
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub enum TableExprInner {
    Table(Relation),
    Subquery(Box<SelectStatement>),
}

impl TableExprInner {
    pub fn as_table(&self) -> Option<&Relation> {
        if let Self::Table(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn try_into_table(self) -> Result<Relation, Self> {
        if let Self::Table(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    pub fn display(&self, dialect: Dialect) -> impl Display + Copy + '_ {
        fmt_with(move |f| match self {
            TableExprInner::Table(t) => write!(f, "{}", t.display(dialect)),
            TableExprInner::Subquery(sq) => write!(f, "({})", sq.display(dialect)),
        })
    }
}

/// An expression for a table in the `FROM` clause of a query, with optional alias
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub struct TableExpr {
    pub inner: TableExprInner,
    pub alias: Option<SqlIdentifier>,
}

/// Constructs a [`TableExpr`] with no alias
impl From<Relation> for TableExpr {
    fn from(table: Relation) -> Self {
        Self {
            inner: TableExprInner::Table(table),
            alias: None,
        }
    }
}

impl TableExpr {
    pub fn display(&self, dialect: Dialect) -> impl Display + Copy + '_ {
        fmt_with(move |f| {
            write!(f, "{}", self.inner.display(dialect))?;

            if let Some(alias) = &self.alias {
                write!(f, " AS {}", dialect.quote_identifier(alias))?;
            }

            Ok(())
        })
    }
}

// Parse a reference to a named schema.table
pub fn relation(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Relation> {
    move |i| {
        let (i, schema) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, name) = dialect.identifier()(i)?;
        Ok((i, Relation { schema, name }))
    }
}

// Parse list of table names.
pub fn table_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Relation>> {
    move |i| separated_list1(ws_sep_comma, relation(dialect))(i)
}

fn subquery(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SelectStatement> {
    move |i| {
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, stmt) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        Ok((i, stmt))
    }
}

fn table_expr_inner(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableExprInner> {
    move |i| {
        alt((
            map(relation(dialect), TableExprInner::Table),
            map(subquery(dialect), |sq| {
                TableExprInner::Subquery(Box::new(sq))
            }),
        ))(i)
    }
}

pub fn table_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableExpr> {
    move |i| {
        let (i, inner) = table_expr_inner(dialect)(i)?;
        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, TableExpr { inner, alias }))
    }
}

pub fn table_expr_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<TableExpr>> {
    move |i| separated_list1(ws_sep_comma, table_expr(dialect))(i)
}

// Parse a reference to a named schema.table or schema.* as used by the replicator to identify
// tables to replicate
pub fn replicator_table_reference(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Relation> {
    move |i| {
        let (i, schema) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, name) = alt((
            dialect.identifier(),
            map(tag("*"), |_| SqlIdentifier::from("*")),
        ))(i)?;
        Ok((i, Relation { schema, name }))
    }
}

// Parse list of table names as used by the replicator to identify tables to replicate
pub fn replicator_table_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Relation>> {
    move |i| separated_list1(ws_sep_comma, replicator_table_reference(dialect))(i)
}
