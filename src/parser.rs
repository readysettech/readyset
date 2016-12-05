use nom::IResult;
use std::str;

pub use common::{FieldExpression, Operator};
use insert::*;
use select::*;

#[derive(Clone, Debug, PartialEq)]
pub enum SqlQuery {
    Insert(InsertStatement),
    Select(SelectStatement),
}

/// Parse sequence of SQL statements, divided by semicolons or newlines
// named!(pub query_list<&[u8], Vec<SqlQuery> >,
//    many1!(map_res!(selection, |s| { SqlQuery::Select(s) }))
// );

pub fn parse_query(input: &str) -> Result<SqlQuery, &str> {
    // we process all queries in lowercase to avoid having to deal with capitalization in the
    // parser.
    let q_bytes = String::from(input).into_bytes();

    // TODO(malte): appropriately pass through errors from nom
    match insertion(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::Insert(o)),
        _ => (),
    };

    match selection(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::Select(o)),
        _ => (),
    };

    Err("failed to parse query")
}
