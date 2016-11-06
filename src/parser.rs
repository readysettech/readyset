use nom::IResult;
use std::str;

use select::*;

#[derive(Clone, Debug, PartialEq)]
pub enum SqlQuery {
    Select(SelectStatement),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConditionBase {
    Field(String),
    Literal(String),
    Placeholder,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConditionTree {
    pub operator: String,
    pub left: Option<Box<ConditionExpression>>,
    pub right: Option<Box<ConditionExpression>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    Expr(ConditionBase),
}

/// Parse sequence of SQL statements, divided by semicolons or newlines
// named!(pub query_list<&[u8], Vec<SqlQuery> >,
//    many1!(map_res!(selection, |s| { SqlQuery::Select(s) }))
// );

pub fn parse_query(input: &str) -> Result<SqlQuery, &str> {
    // we process all queries in lowercase to avoid having to deal with capitalization in the
    // parser.
    let q_lower = input.to_lowercase();

    // TODO(malte): appropriately pass through errors from nom
    match selection(&q_lower.into_bytes()) {
        IResult::Done(_, o) => Ok(SqlQuery::Select(o)),
        IResult::Error(_) => Err("parse error"),
        IResult::Incomplete(_) => Err("incomplete query"),
    }
}
