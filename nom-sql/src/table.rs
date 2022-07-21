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

#[derive(Clone, Debug, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Table {
    pub schema: Option<SqlIdentifier>,
    pub name: SqlIdentifier,
}

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "`{}`.", schema)?;
        }
        write!(f, "`{}`", self.name)?;
        Ok(())
    }
}

// `schema` is ignored for now as we do not support multiple DBs/namespaces
impl Hash for Table {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

// `schema` is ignored for now as we do not support multiple DBs/namespaces
impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl From<SqlIdentifier> for Table {
    fn from(name: SqlIdentifier) -> Self {
        Table { name, schema: None }
    }
}

impl From<&SqlIdentifier> for Table {
    fn from(name: &SqlIdentifier) -> Self {
        Table {
            name: name.clone(),
            schema: None,
        }
    }
}

impl<'a> From<&'a str> for Table {
    fn from(t: &str) -> Table {
        Table {
            name: t.into(),
            schema: None,
        }
    }
}

/// An expression for a table in the `FROM` clause of a query, with optional alias
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub struct TableExpr {
    pub table: Table,
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
impl From<Table> for TableExpr {
    fn from(table: Table) -> Self {
        Self { table, alias: None }
    }
}

// Parse a reference to a named schema.table
pub fn table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Table> {
    move |i| {
        let (i, schema) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, name) = dialect.identifier()(i)?;
        Ok((i, Table { schema, name }))
    }
}

// Parse list of table names.
pub fn table_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Table>> {
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
pub fn replicator_table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Table> {
    move |i| {
        let (i, schema) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, name) = alt((
            dialect.identifier(),
            map(tag("*"), |_| SqlIdentifier::from("*")),
        ))(i)?;
        Ok((i, Table { schema, name }))
    }
}

// Parse list of table names as used by the replicator to identify tables to replicate
pub fn replicator_table_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Table>> {
    move |i| separated_list1(ws_sep_comma, replicator_table_reference(dialect))(i)
}
