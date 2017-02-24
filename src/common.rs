use nom::{alphanumeric, digit, eof, is_alphanumeric, line_ending, multispace};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;
use std::str::FromStr;

use column::{Column, FunctionExpression};
use keywords::sql_keyword;
use table::Table;

#[derive(Clone, Debug, Hash, PartialEq)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FieldExpression {
    All,
    Seq(Vec<Column>),
}

impl Default for FieldExpression {
    fn default() -> FieldExpression {
        FieldExpression::All
    }
}

#[inline]
pub fn is_sql_identifier(chr: u8) -> bool {
    // XXX(malte): dot should not be in here once we have proper alias handling
    is_alphanumeric(chr) || chr == '_' as u8
}

#[inline]
pub fn is_fp_number(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '.' as u8
}

/// Parses a floating point number (very badly).
named!(pub fp_number<&[u8], &[u8]>,
// f64::from_str(&format!("{}.{}", integral, fractional)).unwrap()
//      integral: map_res!(take_while1!(is_digit), str::from_utf8) ~
//      fractional: map_res!(take_while!(is_digit), str::from_utf8),
    take_while1!(is_fp_number)
);

named!(pub column_function<&[u8], FunctionExpression>,
    alt_complete!(
        chain!(
            caseless_tag!("count") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::Count(columns)
            }
        )
    |   chain!(
            caseless_tag!("sum") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::Sum(columns)
            }
        )
    |   chain!(
            caseless_tag!("avg") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::Avg(columns)
            }
        )
    |   chain!(
            caseless_tag!("max") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::Max(columns)
            }
        )
    |   chain!(
            caseless_tag!("min") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::Min(columns)
            }
        )
    |   chain!(
            caseless_tag!("group_concat") ~
            columns: delimited!(tag!("("), field_expr, tag!(")")),
            || {
                FunctionExpression::GroupConcat(columns)
            }
        )
    )
);

/// Parses a SQL column identifier in the table.column format
named!(pub column_identifier<&[u8], Column>,
    alt_complete!(
        chain!(
            function: column_function ~
            alias: opt!(chain!(
                multispace ~
                caseless_tag!("as") ~
                multispace ~
                alias: map_res!(sql_identifier, str::from_utf8),
                || { alias }
            )),
            || {
                Column {
                    name: match alias {
                        Some(a) => String::from(a),
                        None => String::from("anon_fn"),
                    },
                    table: None,
                    function: Some(function),
                }
            }
        )
        | chain!(
            table: opt!(
                chain!(
                    tbl_name: map_res!(sql_identifier, str::from_utf8) ~
                    tag!("."),
                    || { tbl_name }
                )
            ) ~
            column: map_res!(sql_identifier, str::from_utf8),
            || {
                Column {
                    name: String::from(column),
                    table: match table {
                        None => None,
                        Some(t) => Some(String::from(t)),
                    },
                    function: None,
                }
            }
        )
    )
);

/// Parses a SQL identifier (alphanumeric and "_").
named!(pub sql_identifier<&[u8], &[u8]>,
    alt_complete!(
          chain!(
                not!(peek!(sql_keyword)) ~
                ident: take_while1!(is_sql_identifier),
                || { ident }
          )
        | delimited!(tag!("`"), take_while1!(is_sql_identifier), tag!("`"))
        | delimited!(tag!("'"), take_while1!(is_sql_identifier), tag!("'"))
    )
);

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
           map!(caseless_tag!("not_like"), |_| Operator::NotLike)
         | map!(caseless_tag!("like"), |_| Operator::Like)
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
named!(pub field_list<&[u8], Vec<Column> >,
       many0!(
           chain!(
               fieldname: column_identifier ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       ||{}
                   ))
               ),
               || { fieldname }
           )
       )
);

/// Parse list of columns/fields.
/// XXX(malte): add support for named table notation
named!(pub field_expr<&[u8], FieldExpression>,
       alt_complete!(
           tag!("*") => { |_| FieldExpression::All }
         | map!(field_list, |v| FieldExpression::Seq(v))
       )
);

/// Parse list of table names.
/// XXX(malte): add support for aliases
named!(pub table_list<&[u8], Vec<Table> >,
       many0!(
           chain!(
               table: table_reference ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       || {}
                   ))
               ),
               || { table }
           )
       )
);

/// Parse a list of values (e.g., for INSERT syntax).
/// XXX(malte): proper value type
named!(pub value_list<&[u8], Vec<&str> >,
       many0!(
           map_res!(
               chain!(
                   val: alt_complete!(
                         tag_s!(b"?")
                       | tag_s!(b"CURRENT_TIMESTAMP")
                       | delimited!(tag!("'"), alphanumeric, tag!("'"))
                       | fp_number
                   ) ~
                   opt!(
                       complete!(chain!(
                           multispace? ~
                           tag!(",") ~
                           multispace?,
                           ||{}
                       ))
                   ),
                   || { val }
               ),
               str::from_utf8
           )
       )
);

/// Parse a reference to a named table
/// XXX(malte): add support for schema.table notation
named!(pub table_reference<&[u8], Table>,
    chain!(
        table: map_res!(sql_identifier, str::from_utf8) ~
        alias: opt!(
            complete!(chain!(
                multispace ~
                caseless_tag!("as") ~
                multispace ~
                alias: map_res!(sql_identifier, str::from_utf8),
                || { String::from(alias) }
            ))
        ),
        || {
            Table {
                name: String::from(table),
                alias: alias,
            }
        }
    )
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_identifiers() {
        let id1 = b"foo";
        let id2 = b"f_o_o";
        let id3 = b"foo12";
        let id4 = b":fo oo";
        let id5 = b"primary ";
        let id6 = b"`primary`";

        assert!(sql_identifier(id1).is_done());
        assert!(sql_identifier(id2).is_done());
        assert!(sql_identifier(id3).is_done());
        assert!(sql_identifier(id4).is_err());
        assert!(sql_identifier(id5).is_err());
        assert!(sql_identifier(id6).is_done());
    }

    #[test]
    fn simple_column_function() {
        let qs = b"max(addr_id)";

        let res = column_identifier(qs);
        let expected_fields = FieldExpression::Seq(vec![Column::from("addr_id")]);
        let expected_fn = Some(FunctionExpression::Max(expected_fields));
        let expected = Column {
            name: String::from("anon_fn"),
            table: None,
            function: expected_fn,
        };
        assert_eq!(res.unwrap().1, expected);
    }
}
