use nom::{alphanumeric, digit, eof, is_alphanumeric, line_ending, multispace};
use nom::{IResult, Err, ErrorKind, Needed};
use std::fmt::{self, Display};
use std::str;
use std::str::FromStr;

use arithmetic::{arithmetic_expression, ArithmeticExpression};
use column::{Column, FunctionExpression};
use keywords::sql_keyword;
use table::Table;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SqlType {
    Char(u16),
    Varchar(u16),
    Int(u16),
    Bigint(u16),
    Tinyint(u16),
    Blob,
    Longblob,
    Mediumblob,
    Tinyblob,
    Double,
    Float,
    Real,
    Tinytext,
    Mediumtext,
    Text,
    Date,
    Timestamp,
    Varbinary(u16),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Integer(i64),
    String(String),
    Blob(Vec<u8>),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<String> for Literal {
    fn from(s: String) -> Self {
        Literal::String(s)
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(s: &'a str) -> Self {
        Literal::String(String::from(s))
    }
}

impl ToString for Literal {
    fn to_string(&self) -> String {
        match *self {
            Literal::Null => "NULL".to_string(),
            Literal::Integer(ref i) => format!("{}", i),
            Literal::String(ref s) => s.clone(),
            Literal::Blob(ref bv) => {
                format!(
                    "{}",
                    bv.iter()
                        .map(|v| format!("{:x}", v))
                        .collect::<Vec<String>>()
                        .join(" ")
                )
            }
            Literal::CurrentTime => "CURRENT_TIME".to_string(),
            Literal::CurrentDate => "CURRENT_DATE".to_string(),
            Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
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
    In,
}

impl Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            Operator::Not => "not",
            Operator::And => "and",
            Operator::Or => "or",
            Operator::Like => "like",
            Operator::NotLike => "not_like",
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::Greater => ">",
            Operator::GreaterOrEqual => ">=",
            Operator::Less => "<",
            Operator::LessOrEqual => "<=",
            Operator::In => "in",
        };
        write!(f, "{}", op)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey(Vec<Column>),
    UniqueKey(Option<String>, Vec<Column>),
    FulltextKey(Option<String>, Vec<Column>),
    Key(String, Vec<Column>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FieldExpression {
    All,
    AllInTable(String),
    Arithmetic(ArithmeticExpression),
    Col(Column),
    Literal(Literal),
}

impl Display for FieldExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldExpression::All => write!(f, "*"),
            FieldExpression::AllInTable(ref table) => write!(f, "{}.*", table),
            FieldExpression::Arithmetic(ref expr) => write!(f, "{:?}", expr),
            FieldExpression::Col(ref col) => write!(f, "{}", col.name.as_str()),
            FieldExpression::Literal(ref lit) => write!(f, "{}", lit.to_string()),
        }
    }
}

impl Default for FieldExpression {
    fn default() -> FieldExpression {
        FieldExpression::All
    }
}

#[inline]
pub fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '_' as u8
}

/// Parses the arguments for an agregation function, and also returns whether the distinct flag is
/// present.
named!(pub function_arguments<&[u8], (Column, bool)>,
       chain!(
           distinct: opt!(chain!(
               caseless_tag!("distinct") ~
               multispace,
               ||{}
           )) ~
           column: column_identifier_no_alias,
           || {
               (column, distinct.is_some())
           }
       )
);

named!(pub column_function<&[u8], FunctionExpression>,
    alt_complete!(
        chain!(
            caseless_tag!("count(*)"),
            || {
                FunctionExpression::CountStar
            }
        )
    |   chain!(
            caseless_tag!("count") ~
            args: delimited!(tag!("("), function_arguments, tag!(")")),
            || {
                FunctionExpression::Count(args.0.clone(), args.1)
            }
        )
    |   chain!(
            caseless_tag!("sum") ~
            args: delimited!(tag!("("), function_arguments, tag!(")")),
            || {
                FunctionExpression::Sum(args.0.clone(), args.1)
            }
        )
    |   chain!(
            caseless_tag!("avg") ~
            args: delimited!(tag!("("), function_arguments, tag!(")")),
            || {
                FunctionExpression::Avg(args.0.clone(), args.1)
            }
        )
    |   chain!(
            caseless_tag!("max") ~
            args: delimited!(tag!("("), function_arguments, tag!(")")),
            || {
                FunctionExpression::Max(args.0.clone())
            }
        )
    |   chain!(
            caseless_tag!("min") ~
            args: delimited!(tag!("("), function_arguments, tag!(")")),
            || {
                FunctionExpression::Min(args.0.clone())
            }
        )
    |   chain!(
            caseless_tag!("group_concat") ~
            spec: delimited!(tag!("("),
                       complete!(chain!(
                               column: column_identifier_no_alias ~
                               seperator: opt!(
                                   chain!(
                                       multispace? ~
                                       caseless_tag!("separator") ~
                                       sep: delimited!(tag!("'"), opt!(alphanumeric), tag!("'")) ~
                                       multispace?,
                                       || { sep.unwrap_or("".as_bytes()) }
                                   )
                               ),
                               || { (column, seperator) }
                       )),
                       tag!(")")),
            || {
                let (ref col, ref sep) = spec;
                let sep = match *sep {
                    // default separator is a comma, see MySQL manual §5.7
                    None => String::from(","),
                    Some(s) => String::from_utf8(s.to_vec()).unwrap(),
                };

                FunctionExpression::GroupConcat(col.clone(), sep)
            }
        )
    )
);

/// Parses a SQL column identifier in the table.column format
named!(pub column_identifier_no_alias<&[u8], Column>,
    alt_complete!(
        chain!(
            function: column_function,
            || {
                Column {
                    name: format!("{}", function),
                    alias: None,
                    table: None,
                    function: Some(Box::new(function)),
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
                    alias: None,
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

/// Parses a SQL column identifier in the table.column format
named!(pub column_identifier<&[u8], Column>,
    alt_complete!(
        chain!(
            function: column_function ~
            alias: opt!(as_alias),
            || {
                Column {
                    name: match alias {
                        None => format!("{}", function),
                        Some(a) => String::from(a),
                    },
                    alias: match alias {
                        None => None,
                        Some(a) => Some(String::from(a)),
                    },
                    table: None,
                    function: Some(Box::new(function)),
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
            column: map_res!(sql_identifier, str::from_utf8) ~
            alias: opt!(as_alias),
            || {
                Column {
                    name: String::from(column),
                    alias: match alias {
                        None => None,
                        Some(a) => Some(String::from(a)),
                    },
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
        | delimited!(tag!("["), take_while1!(is_sql_identifier), tag!("]"))
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
         | map!(caseless_tag!("!="), |_| Operator::NotEqual)
         | map!(caseless_tag!("<>"), |_| Operator::NotEqual)
         | map!(caseless_tag!(">="), |_| Operator::GreaterOrEqual)
         | map!(caseless_tag!("<="), |_| Operator::LessOrEqual)
         | map!(caseless_tag!("="), |_| Operator::Equal)
         | map!(caseless_tag!("<"), |_| Operator::Less)
         | map!(caseless_tag!(">"), |_| Operator::Greater)
         | map!(caseless_tag!("in"), |_| Operator::In)
    )
);

/// Parse rule for AS-based aliases for SQL entities.
named!(pub as_alias<&[u8], &str>,
    complete!(
        chain!(
            multispace ~
            opt!(chain!(caseless_tag!("as") ~ multispace, ||{})) ~
            alias: map_res!(sql_identifier, str::from_utf8),
            || { alias }
        )
    )
);

/// Parse rule for a comma-separated list of fields without aliases.
named!(pub field_list<&[u8], Vec<Column> >,
       many0!(
           chain!(
               fieldname: column_identifier_no_alias ~
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

/// Parse list of column/field definitions.
named!(pub field_definition_expr<&[u8], Vec<FieldExpression>>,
       many0!(
           chain!(
               field: alt_complete!(
                   chain!(
                       tag!("*"),
                       || {
                           FieldExpression::All
                       }
                   )
                 | chain!(
                     table: table_reference ~
                     tag!(".*"),
                     || {
                         FieldExpression::AllInTable(table.name.clone())
                     }
                 )
                 | chain!(
                     expr: arithmetic_expression,
                     || {
                         FieldExpression::Arithmetic(expr)
                     }
                 )
                 | chain!(
                     literal: literal,
                     || {
                         FieldExpression::Literal(literal)
                     }
                 )
                 | chain!(
                     column: column_identifier,
                     || {
                         FieldExpression::Col(column)
                     }
                 )
               ) ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       ||{}
                   ))
               ),
               || { field }
           )
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

/// Integer literal value
named!(pub integer_literal<&[u8], Literal>,
    chain!(
        val: delimited!(opt!(multispace), digit, opt!(multispace)),
        || {
            let intval = i64::from_str(str::from_utf8(val).unwrap()).unwrap();
            Literal::Integer(intval)
        }
    )
);

/// String literal value
named!(pub string_literal<&[u8], Literal>,
    chain!(
        val: delimited!(opt!(multispace),
                 alt_complete!(
                       delimited!(tag!("\""), opt!(take_until!("\"")), tag!("\""))
                     | delimited!(tag!("'"), opt!(take_until!("'")), tag!("'"))
                 ),
                 opt!(multispace)
              ),
        || {
            let val = val.unwrap_or("".as_bytes());
            let s = String::from(str::from_utf8(val).unwrap());
            Literal::String(s)
        }
    )
);

/// Any literal value.
named!(pub literal<&[u8], Literal>,
    alt_complete!(
          integer_literal
        | string_literal
        | chain!(caseless_tag!("NULL"), || Literal::Null)
        | chain!(caseless_tag!("CURRENT_TIMESTAMP"), || Literal::CurrentTimestamp)
        | chain!(caseless_tag!("CURRENT_DATE"), || Literal::CurrentDate)
        | chain!(caseless_tag!("CURRENT_TIME"), || Literal::CurrentTime)
//        | float_literal
    )
);

/// Parse a list of values (e.g., for INSERT syntax).
named!(pub value_list<&[u8], Vec<Literal> >,
       many0!(
           chain!(
               val: literal ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       ||{}
                   ))
               ),
               || { val }
           )
       )
);

/// Parse a reference to a named table, with an optional alias
/// TODO(malte): add support for schema.table notation
named!(pub table_reference<&[u8], Table>,
    chain!(
        table: map_res!(sql_identifier, str::from_utf8) ~
        alias: opt!(as_alias),
        || {
            Table {
                name: String::from(table),
                alias: match alias {
                    Some(a) => Some(String::from(a)),
                    None => None,
                }
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
        let expected = Column {
            name: String::from("max(addr_id)"),
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::Max(Column::from("addr_id")))),
        };
        assert_eq!(res.unwrap().1, expected);
    }
}
