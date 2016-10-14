use nom::{IResult, alphanumeric, space};
use std::str;

use select::*;

#[derive(Debug, PartialEq)]
pub enum SqlQuery {
    Select(SelectStatement),
}

#[derive(Debug, PartialEq)]
pub struct ConditionTree {
    pub field: String,
    pub expr: String,
}

/// Parse rule for a comma-separated list.
named!(pub csvlist<&[u8], Vec<&str> >,
       many0!(
           map_res!(
               chain!(
                   fieldname: alphanumeric ~
                   opt!(
                       chain!(
                           tag!(",") ~
                           space?,
                           ||{}
                       )
                   ),
                   ||{ fieldname }
               ),
               str::from_utf8
           )
       )
);

/// Parse list of columns/fields.
/// XXX(malte): add support for named table notation
named!(pub fieldlist<&[u8], Vec<&str> >,
       alt!(
           tag!("*") => { |_| vec!["ALL".into()] }
         | csvlist
       )
);

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
