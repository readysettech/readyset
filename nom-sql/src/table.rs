use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::{fmt, str};

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
use crate::{Dialect, DialectDisplay, NomSqlResult, SelectStatement, SqlIdentifier};

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

impl DialectDisplay for Relation {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
        fmt_with(move |f| {
            if let Some(schema) = &self.schema {
                write!(f, "{}.", dialect.quote_identifier(schema))?;
            }
            write!(f, "{}", dialect.quote_identifier(&self.name))
        })
    }
}

impl Relation {
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
    // TODO: re-enable after SelectStatement round-trips
    #[weight(0)]
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
}

impl DialectDisplay for TableExprInner {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
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

impl DialectDisplay for TableExpr {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NotReplicatedReason {
    /// Configuration indicates that a table is not being replicated because
    /// it was excluded by configuration.
    Configuration,
    /// TableDropped indicates that the table that was snapshotted has been dropped
    /// and because of this it is no longer being replicated.
    TableDropped,
    /// Partitioned indicates that the table is a partitioned table which are not
    /// supported by ReadySet.
    Partitioned,
    /// UnsupportedType indicates that a column type in the table is not supported.
    /// This will only reference the first unsupported type. If there are more than
    /// one in a single table they will not be mentioned.
    UnsupportedType(String),
    /// OtherError indicates that an error was observed that caused the replication to fail but
    /// was not one of the previous type and was unexpected.
    OtherError(String),
    /// Default is a generic and is used when one of the above enums are not need.
    Default,
}

impl NotReplicatedReason {
    pub fn description(&self) -> String {
        match self {
                NotReplicatedReason::Configuration => "The table was either excluded from replicated-tables or included in replication-tables-ignore option.".to_string(),
                NotReplicatedReason::TableDropped => "Table has been dropped.".to_string(),
                NotReplicatedReason::Partitioned => "Partitioned tables are not supported.".to_string(),
                NotReplicatedReason::UnsupportedType(reason) => {
                    let prefix = "Unsupported type:";
                    if let Some(start) = reason.find(prefix) {
                        let start_offset = start + prefix.len();
                        let type_name_raw = &reason[start_offset..];
                        let type_name = type_name_raw.trim();  // Trim whitespace 
                        format!("Column type {} is not supported.", type_name)
                    } else {
                        "Column type unknown is not supported.".to_string()
                    }
                },
                NotReplicatedReason::OtherError(error) => format!("An unexpected replication error occurred: {}", error),
                NotReplicatedReason::Default => "No specific reason provided.".to_string(),
            }
    }

    pub fn from_string(reason: &String) -> Self {
        if reason.contains("Unsupported type:") {
            NotReplicatedReason::UnsupportedType(reason.to_string())
        } else {
            NotReplicatedReason::OtherError(reason.to_string())
        }
    }
}
impl Debug for NotReplicatedReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration => write!(f, "Configuration"),
            Self::TableDropped => write!(f, "TableDropped"),
            Self::Partitioned => write!(f, "Partitioned"),
            Self::UnsupportedType(s) => write!(f, "UnsupportedType({})", s),
            Self::OtherError(s) => write!(f, "OtherError({})", s),
            Self::Default => write!(f, ""),
        }
    }
}

/// NonReplicatedRelations is a struct that wraps Relations with a reason why
/// it is not a replicated relation.
#[derive(Clone, Serialize, Deserialize)]
pub struct NonReplicatedRelation {
    pub name: Relation,
    pub reason: NotReplicatedReason,
}

impl NonReplicatedRelation {
    pub fn new(name: Relation) -> Self {
        NonReplicatedRelation {
            name,
            reason: NotReplicatedReason::Default,
        }
    }
}
impl Hash for NonReplicatedRelation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}
impl Debug for NonReplicatedRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NonReplicatedRelation {{ name: {:?}, reason: {:?} }}",
            self.name, self.reason
        )
    }
}

impl PartialEq for NonReplicatedRelation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for NonReplicatedRelation {}
