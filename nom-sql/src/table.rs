use std::fmt::Display;
use std::hash::Hash;
use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::terminated;
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{as_alias, ws_sep_comma};
use crate::{Dialect, SqlIdentifier};

/// A (potentially schema-qualified) name for a relation
///
/// This type is (perhaps surprisingly) quite pervasive - it's used as not only the names for tables
/// and views, but also all nodes in the graph (both MIR and dataflow).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Relation {
    /// The optional schema for the relation.
    ///
    /// Note that this name is maximally general - what MySQL calls a "database" is actually much
    /// closer to a schema (and in fact you can call it a `SCHEMA` in mysql commands!).
    pub schema: Option<SqlIdentifier>,

    /// The name of the relation itself
    pub name: SqlIdentifier,
}

impl Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "`{}`.", schema)?;
        }
        write!(f, "`{}`", self.name)?;
        Ok(())
    }
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

/// An expression for a table in the `FROM` clause of a query, with optional alias
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub struct TableExpr {
    pub table: Relation,
    pub alias: Option<SqlIdentifier>,
}

impl Display for TableExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS `{alias}`")?;
        }
        Ok(())
    }
}

/// Constructs a [`TableExpr`] with no alias
impl From<Relation> for TableExpr {
    fn from(table: Relation) -> Self {
        Self { table, alias: None }
    }
}

// Parse a reference to a named schema.table
pub fn table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Relation> {
    move |i| {
        let (i, schema) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, name) = dialect.identifier()(i)?;
        Ok((i, Relation { schema, name }))
    }
}

// Parse list of table names.
pub fn table_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Relation>> {
    move |i| separated_list1(ws_sep_comma, table_reference(dialect))(i)
}

pub fn table_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableExpr> {
    move |i| {
        let (i, table) = table_reference(dialect)(i)?;
        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, TableExpr { table, alias }))
    }
}

pub fn table_expr_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<TableExpr>> {
    move |i| separated_list1(ws_sep_comma, table_expr(dialect))(i)
}

// Parse a reference to a named schema.table or schema.* as used by the replicator to identify
// tables to replicate
pub fn replicator_table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Relation> {
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
pub fn replicator_table_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Relation>> {
    move |i| separated_list1(ws_sep_comma, replicator_table_reference(dialect))(i)
}
