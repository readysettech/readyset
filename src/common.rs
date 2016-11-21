use nom::{alphanumeric, digit, eof, is_alphanumeric, line_ending, multispace, space};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq)]
pub enum Operator {
    Not,
    And,
    Or,
    Like,
    NotLike,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

/// Parse an unsigned integer.
named!(pub unsigned_number<&[u8], u64>,
    map_res!(
        map_res!(digit, str::from_utf8),
        FromStr::from_str
    )
);

/// Parse a terminator that ends a SQL statement.
named!(pub statement_terminator,
    delimited!(opt!(multispace),
               alt_complete!(tag!(";") | line_ending | eof),
               opt!(multispace)
    )
);

/// Parse binary comparison operators
named!(pub binary_comparison_operator<&[u8], Operator>,
    alt_complete!(
           map!(caseless_tag!("like"), |_| Operator::Like)
         | map!(caseless_tag!("not_like"), |_| Operator::NotLike)
         | map!(caseless_tag!("<>"), |_| Operator::NotEqual)
         | map!(caseless_tag!(">="), |_| Operator::GreaterOrEqual)
         | map!(caseless_tag!("<="), |_| Operator::LessOrEqual)
         | map!(caseless_tag!("="), |_| Operator::Equal)
         | map!(caseless_tag!("<"), |_| Operator::Less)
         | map!(caseless_tag!(">"), |_| Operator::Greater)
    )
);

/// Parse logical operators
named!(pub binary_logical_operator<&[u8], Operator>,
    alt_complete!(
           map!(caseless_tag!("and"), |_| Operator::And)
         | map!(caseless_tag!("or"), |_| Operator::Or)
    )
);

/// Parse unary comparison operators
named!(pub unary_comparison_operator<&[u8], &str>,
    map_res!(
        alt_complete!(
               tag_s!(b"ISNULL")
             | tag_s!(b"NOT")
             | tag_s!(b"-") // ??? (number neg)
        ),
        str::from_utf8
    )
);

/// Parse unary comparison operators
named!(pub unary_negation_operator<&[u8], Operator>,
    alt_complete!(
          map!(caseless_tag!("not"), |_| Operator::Not)
        | map!(caseless_tag!("!"), |_| Operator::Not)
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
                           multispace?,
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

/// Parse a list of values (e.g., for INSERT syntax).
/// XXX(malte): support non-placeholder values
named!(pub valuelist<&[u8], Vec<&str> >,
       many0!(
           map_res!(
               chain!(
                   val: alt_complete!(tag_s!(b"?") | alphanumeric) ~
                   opt!(
                       chain!(
                           tag!(",") ~
                           multispace?,
                           ||{}
                       )
                   ),
                   || { val }
               ),
               str::from_utf8
           )
       )
);

/// Parse a reference to a named table
/// XXX(malte): add support for schema.table notation
named!(pub table_reference<&[u8], &str>,
    chain!(
        table: map_res!(alphanumeric, str::from_utf8) ~
        opt!(
            chain!(
                space ~
                caseless_tag!("as") ~
                space ~
                alias: map_res!(alphanumeric, str::from_utf8),
                || { println!("got alias: {} -> {}", table, alias); alias }
            )
        ),
        || { table }
    )
);
