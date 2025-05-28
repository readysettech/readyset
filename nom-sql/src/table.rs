use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::terminated;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::{as_alias, ws_sep_comma};
use crate::dialect::DialectParser;
use crate::index_hint::index_hint_list;
use crate::select::nested_selection;
use crate::whitespace::whitespace0;
use crate::NomSqlResult;

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
        let (i, stmt) = nested_selection(dialect, false)(i)?;
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
        let (i, _) = opt(index_hint_list(dialect))(i)?;
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
