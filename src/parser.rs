use nom::{IResult, alphanumeric, digit, space};
use std::str;
use std::str::FromStr;

use select::*;

#[derive(Debug, PartialEq)]
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

/// Parse an unsigned integer.
named!(pub unsigned_number<&[u8], u64>,
    map_res!(
        map_res!(digit, str::from_utf8),
        FromStr::from_str
    )
);

/// Parse binary comparison operators
named!(pub binary_comparison_operator<&[u8], &str>,
    dbg_dmp!(map_res!(
        dbg_dmp!(alt_complete!(  tag_s!(b"=")
             | tag_s!(b"<")
             | tag_s!(b">")
             | tag_s!(b"<>")
             | tag_s!(b">=")
             | tag_s!(b"<=")
             | tag_s!(b"LIKE")
             | tag_s!(b"NOT_LIKE")
        )),
        str::from_utf8
    )
));

/// Parse logical operators
named!(pub binary_logical_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
               tag_s!(b"and")
             | tag_s!(b"or")
        ),
        str::from_utf8
    )
);

/// Parse unary comparison operators
named!(pub unary_comparison_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
               tag_s!(b"NOT")
             | tag_s!(b"-") // ??? (number neg)
             | tag_s!(b"ISNULL")
        ),
        str::from_utf8
    )
);

/// Parse unary comparison operators
named!(pub unary_negation_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
              tag_s!(b"NOT")
            | tag_s!(b"!")
        ),
        str::from_utf8
    )
);

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
       alt_complete!(
           tag!("*") => { |_| vec!["ALL".into()] }
         | csvlist
       )
);

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
