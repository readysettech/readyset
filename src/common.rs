use nom::{alphanumeric, digit, is_alphanumeric, line_ending, multispace, Compare, IResult};
use nom::types::CompleteByteSlice;
use std::fmt::{self, Display};
use std::str;
use std::str::FromStr;

use arithmetic::{arithmetic_expression, ArithmeticExpression};
use column::{Column, FunctionExpression};
use keywords::{escape_if_keyword, sql_keyword};
use table::Table;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
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
    Longtext,
    Text,
    Date,
    DateTime(u16),
    Timestamp,
    Binary(u16),
    Varbinary(u16),
    Enum(Vec<Literal>),
    Decimal(u8, u8),
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SqlType::Bool => write!(f, "BOOL"),
            SqlType::Char(len) => write!(f, "CHAR({})", len),
            SqlType::Varchar(len) => write!(f, "VARCHAR({})", len),
            SqlType::Int(len) => write!(f, "INT({})", len),
            SqlType::Bigint(len) => write!(f, "BIGINT({})", len),
            SqlType::Tinyint(len) => write!(f, "TINYINT({})", len),
            SqlType::Blob => write!(f, "BLOB"),
            SqlType::Longblob => write!(f, "LONGBLOB"),
            SqlType::Mediumblob => write!(f, "MEDIUMBLOB"),
            SqlType::Tinyblob => write!(f, "TINYBLOB"),
            SqlType::Double => write!(f, "DOUBLE"),
            SqlType::Float => write!(f, "FLOAT"),
            SqlType::Real => write!(f, "REAL"),
            SqlType::Tinytext => write!(f, "TINYTEXT"),
            SqlType::Mediumtext => write!(f, "MEDIUMTEXT"),
            SqlType::Longtext => write!(f, "LONGTEXT"),
            SqlType::Text => write!(f, "TEXT"),
            SqlType::Date => write!(f, "DATE"),
            SqlType::DateTime(len) => write!(f, "DATETIME({})", len),
            SqlType::Timestamp => write!(f, "TIMESTAMP"),
            SqlType::Binary(len) => write!(f, "BINARY({})", len),
            SqlType::Varbinary(len) => write!(f, "VARBINARY({})", len),
            SqlType::Enum(_) => write!(f, "ENUM(...)"),
            SqlType::Decimal(m, d) => write!(f, "DECIMAL({}, {})", m, d),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Real {
    pub integral: i32,
    pub fractional: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Integer(i64),
    FixedPoint(Real),
    String(String),
    Blob(Vec<u8>),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
    Placeholder,
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
            Literal::FixedPoint(ref f) => format!("{}.{}", f.integral, f.fractional),
            Literal::String(ref s) => format!("'{}'", s.replace('\'', "''")),
            Literal::Blob(ref bv) => format!(
                "{}",
                bv.iter()
                    .map(|v| format!("{:x}", v))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
            Literal::CurrentTime => "CURRENT_TIME".to_string(),
            Literal::CurrentDate => "CURRENT_DATE".to_string(),
            Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            Literal::Placeholder => "?".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct LiteralExpression {
    pub value: Literal,
    pub alias: Option<String>,
}

impl From<Literal> for LiteralExpression {
    fn from(l: Literal) -> Self {
        LiteralExpression {
            value: l,
            alias: None,
        }
    }
}

impl fmt::Display for LiteralExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.alias {
            Some(ref alias) => write!(f, "{} AS {}", self.value.to_string(), alias),
            None => write!(f, "{}", self.value.to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
    Is,
}

impl Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            Operator::Not => "NOT",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT_LIKE",
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::Greater => ">",
            Operator::GreaterOrEqual => ">=",
            Operator::Less => "<",
            Operator::LessOrEqual => "<=",
            Operator::In => "IN",
            Operator::Is => "IS",
        };
        write!(f, "{}", op)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey(Vec<Column>),
    UniqueKey(Option<String>, Vec<Column>),
    FulltextKey(Option<String>, Vec<Column>),
    Key(String, Vec<Column>),
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TableKey::PrimaryKey(ref columns) => {
                write!(f, "PRIMARY KEY ")?;
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::UniqueKey(ref name, ref columns) => {
                write!(f, "UNIQUE KEY ")?;
                if let Some(ref name) = *name {
                    write!(f, "{} ", escape_if_keyword(name))?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::FulltextKey(ref name, ref columns) => {
                write!(f, "FULLTEXT KEY ")?;
                if let Some(ref name) = *name {
                    write!(f, "{} ", escape_if_keyword(name))?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::Key(ref name, ref columns) => {
                write!(f, "KEY {} ", escape_if_keyword(name))?;
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FieldDefinitionExpression {
    All,
    AllInTable(String),
    Col(Column),
    Value(FieldValueExpression),
}

impl Display for FieldDefinitionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldDefinitionExpression::All => write!(f, "*"),
            FieldDefinitionExpression::AllInTable(ref table) => {
                write!(f, "{}.*", escape_if_keyword(table))
            }
            FieldDefinitionExpression::Col(ref col) => write!(f, "{}", col),
            FieldDefinitionExpression::Value(ref val) => write!(f, "{}", val),
        }
    }
}

impl Default for FieldDefinitionExpression {
    fn default() -> FieldDefinitionExpression {
        FieldDefinitionExpression::All
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FieldValueExpression {
    Arithmetic(ArithmeticExpression),
    Literal(LiteralExpression),
}

impl Display for FieldValueExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldValueExpression::Arithmetic(ref expr) => write!(f, "{}", expr),
            FieldValueExpression::Literal(ref lit) => write!(f, "{}", lit),
        }
    }
}

#[inline]
pub fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '_' as u8
}

#[inline]
fn len_as_u16(len: CompleteByteSlice) -> u16 {
    match str::from_utf8(*len) {
        Ok(s) => match u16::from_str(s) {
            Ok(v) => v,
            Err(e) => panic!(e),
        },
        Err(e) => panic!(e),
    }
}

named!(pub precision<CompleteByteSlice, (u8, Option<u8>)>,
    delimited!(tag!("("),
               do_parse!(
                   m: digit >>
                   d: opt!(do_parse!(
                             tag!(",") >>
                             opt_multispace >>
                             d: digit >>
                             (d)
                        )) >>
                   ((m.0[0], d.map(|r| r.0[0])))
               ),
               tag!(")"))
);

/// A SQL type specifier.
named!(pub type_identifier<CompleteByteSlice, SqlType>,
    alt!(
          do_parse!(
              tag_no_case!("bool") >>
              (SqlType::Bool)
          )
        | do_parse!(
              tag_no_case!("mediumtext") >>
              (SqlType::Mediumtext)
          )
        | do_parse!(
              tag_no_case!("timestamp") >>
              _len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
              opt_multispace >>
              (SqlType::Timestamp)
          )
         | do_parse!(
               tag_no_case!("varbinary") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt_multispace >>
               (SqlType::Varbinary(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("mediumblob") >>
               (SqlType::Mediumblob)
           )
         | do_parse!(
               tag_no_case!("longblob") >>
               (SqlType::Longblob)
           )
         | do_parse!(
               tag_no_case!("tinyblob") >>
               (SqlType::Tinyblob)
           )
         | do_parse!(
               tag_no_case!("tinytext") >>
               (SqlType::Tinytext)
           )
         | do_parse!(
               tag_no_case!("varchar") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt_multispace >>
               _binary: opt!(tag_no_case!("binary")) >>
               (SqlType::Varchar(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("binary") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt_multispace >>
               (SqlType::Binary(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("varbinary") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt_multispace >>
               (SqlType::Varbinary(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("tinyint") >>
               len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
               opt_multispace >>
               _signed: opt!(alt!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Tinyint(len.map(|l|len_as_u16(l)).unwrap_or(1)))
           )
         | do_parse!(
               tag_no_case!("bigint") >>
               len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
               opt_multispace >>
               _signed: opt!(alt!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Bigint(len.map(|l|len_as_u16(l)).unwrap_or(1)))
           )
         | do_parse!(
               tag_no_case!("double") >>
               opt_multispace >>
               _signed: opt!(alt!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Double)
           )
         | do_parse!(
               tag_no_case!("float") >>
               opt_multispace >>
               _prec: opt!(precision) >>
               opt_multispace >>
               (SqlType::Float)
           )
         | do_parse!(
               tag_no_case!("blob") >>
               (SqlType::Blob)
           )
         | do_parse!(
               tag_no_case!("datetime") >>
               fsp: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
               (SqlType::DateTime(match fsp {
                   Some(fsp) => len_as_u16(fsp),
                   None => 0 as u16,
               }))
           )
         | do_parse!(
               tag_no_case!("date") >>
               (SqlType::Date)
           )
         | do_parse!(
               tag_no_case!("real") >>
               opt_multispace >>
               _signed: opt!(alt!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Real)
           )
         | do_parse!(
               tag_no_case!("text") >>
               (SqlType::Text)
           )
         | do_parse!(
               tag_no_case!("longtext") >>
               (SqlType::Longtext)
           )
         | do_parse!(
               tag_no_case!("char") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt_multispace >>
               _binary: opt!(tag_no_case!("binary")) >>
               (SqlType::Char(len_as_u16(len)))
           )
         | do_parse!(
               alt!(tag_no_case!("integer") | tag_no_case!("int") | tag_no_case!("smallint")) >>
               len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
               opt_multispace >>
               _signed: opt!(alt!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Int(match len {
                   Some(len) => len_as_u16(len),
                   None => 32 as u16,
               }))
           )
         | do_parse!(
               tag_no_case!("enum") >>
               variants: delimited!(tag!("("), value_list, tag!(")")) >>
               opt_multispace >>
               (SqlType::Enum(variants))
           )
         | do_parse!(
               // TODO(malte): not strictly ok to treat DECIMAL and NUMERIC as identical; the
               // former has "at least" M precision, the latter "exactly".
               // See https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
               alt!(tag_no_case!("decimal") | tag_no_case!("numeric")) >>
               prec: opt!(precision) >>
               opt_multispace >>
               (match prec {
                   None => SqlType::Decimal(32, 0),
                   Some((m, None)) => SqlType::Decimal(m, 0),
                   Some((m, Some(d))) => SqlType::Decimal(m, d),
                })
           )
       )
);

/// Parses the arguments for an agregation function, and also returns whether the distinct flag is
/// present.
named!(pub function_arguments<CompleteByteSlice, (Column, bool)>,
       do_parse!(
           distinct: opt!(do_parse!(
               tag_no_case!("distinct") >>
               multispace >>
               ()
           )) >>
           column: column_identifier_no_alias >>
           (column, distinct.is_some())
       )
);

named!(pub column_function<CompleteByteSlice, FunctionExpression>,
    alt!(
        do_parse!(
            tag_no_case!("count(*)") >>
            (FunctionExpression::CountStar)
        )
    |   do_parse!(
            tag_no_case!("count") >>
            args: delimited!(tag!("("), function_arguments, tag!(")")) >>
            (FunctionExpression::Count(args.0.clone(), args.1))
        )
    |   do_parse!(
            tag_no_case!("sum") >>
            args: delimited!(tag!("("), function_arguments, tag!(")")) >>
            (FunctionExpression::Sum(args.0.clone(), args.1))
        )
    |   do_parse!(
            tag_no_case!("avg") >>
            args: delimited!(tag!("("), function_arguments, tag!(")")) >>
            (FunctionExpression::Avg(args.0.clone(), args.1))
        )
    |   do_parse!(
            tag_no_case!("max") >>
            args: delimited!(tag!("("), function_arguments, tag!(")")) >>
            (FunctionExpression::Max(args.0.clone()))
        )
    |   do_parse!(
            tag_no_case!("min") >>
            args: delimited!(tag!("("), function_arguments, tag!(")")) >>
            (FunctionExpression::Min(args.0.clone()))
        )
    |   do_parse!(
            tag_no_case!("group_concat") >>
            spec: delimited!(tag!("("),
                       do_parse!(
                               column: column_identifier_no_alias >>
                               seperator: opt!(
                                   do_parse!(
                                       opt_multispace >>
                                       tag_no_case!("separator") >>
                                       sep: delimited!(tag!("'"), opt!(alphanumeric), tag!("'")) >>
                                       opt_multispace >>
                                       (sep.unwrap_or(CompleteByteSlice(&[])))
                                   )
                               ) >>
                               (column, seperator)
                       ),
                       tag!(")")) >>
            ({
                let (ref col, ref sep) = spec;
                let sep = match *sep {
                    // default separator is a comma, see MySQL manual §5.7
                    None => String::from(","),
                    Some(s) => String::from_utf8(s.to_vec()).unwrap(),
                };

                FunctionExpression::GroupConcat(col.clone(), sep)
            })
        )
    )
);

/// Parses a SQL column identifier in the table.column format
named!(pub column_identifier_no_alias<CompleteByteSlice, Column>,
    alt!(
        do_parse!(
            function: column_function >>
            (Column {
                name: format!("{}", function),
                alias: None,
                table: None,
                function: Some(Box::new(function)),
            })
        )
        | do_parse!(
            table: opt!(
                do_parse!(
                    tbl_name: sql_identifier >>
                    tag!(".") >>
                    (str::from_utf8(*tbl_name).unwrap())
                )
            ) >>
            column: sql_identifier >>
            (Column {
                name: String::from(str::from_utf8(*column).unwrap()),
                alias: None,
                table: match table {
                    None => None,
                    Some(t) => Some(String::from(t)),
                },
                function: None,
            })
        )
    )
);

/// Parses a SQL column identifier in the table.column format
named!(pub column_identifier<CompleteByteSlice, Column>,
    alt!(
        do_parse!(
            function: column_function >>
            alias: opt!(as_alias) >>
            (Column {
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
            })
        )
        | do_parse!(
            table: opt!(
                do_parse!(
                    tbl_name: sql_identifier >>
                    tag!(".") >>
                    (str::from_utf8(*tbl_name).unwrap())
                )
            ) >>
            column: sql_identifier >>
            alias: opt!(as_alias) >>
            (Column {
                name: String::from_utf8(column.to_vec()).unwrap(),
                alias: match alias {
                    None => None,
                    Some(a) => Some(String::from(a)),
                },
                table: match table {
                    None => None,
                    Some(t) => Some(String::from(t)),
                },
                function: None,
            })
        )
    )
);

/// Parses a SQL identifier (alphanumeric and "_").
named!(pub sql_identifier<CompleteByteSlice, CompleteByteSlice>,
    alt!(
          do_parse!(
                not!(peek!(sql_keyword)) >>
                ident: take_while1!(is_sql_identifier) >>
                (ident)
          )
        | delimited!(tag!("`"), take_while1!(is_sql_identifier), tag!("`"))
        | delimited!(tag!("["), take_while1!(is_sql_identifier), tag!("]"))
    )
);

/// Parse an unsigned integer.
named!(pub unsigned_number<CompleteByteSlice, u64>,
    do_parse!(
        d: digit >>
        (FromStr::from_str(str::from_utf8(*d).unwrap()).unwrap())
    )
);

/// Parse a terminator that ends a SQL statement.
named!(pub statement_terminator<CompleteByteSlice, ()>,
    do_parse!(
        delimited!(
            opt_multispace,
            alt!(tag!(";") | line_ending | eof!()),
            opt_multispace
        ) >>
        ()
    )
);

named!(pub opt_multispace<CompleteByteSlice, Option<CompleteByteSlice>>,
       opt!(multispace)
);

/// Parse binary comparison operators
named!(pub binary_comparison_operator<CompleteByteSlice, Operator>,
    alt!(
           map!(tag_no_case!("not_like"), |_| Operator::NotLike)
         | map!(tag_no_case!("like"), |_| Operator::Like)
         | map!(tag_no_case!("!="), |_| Operator::NotEqual)
         | map!(tag_no_case!("<>"), |_| Operator::NotEqual)
         | map!(tag_no_case!(">="), |_| Operator::GreaterOrEqual)
         | map!(tag_no_case!("<="), |_| Operator::LessOrEqual)
         | map!(tag_no_case!("="), |_| Operator::Equal)
         | map!(tag_no_case!("<"), |_| Operator::Less)
         | map!(tag_no_case!(">"), |_| Operator::Greater)
         | map!(tag_no_case!("in"), |_| Operator::In)
    )
);

/// Parse rule for AS-based aliases for SQL entities.
named!(pub as_alias<CompleteByteSlice, &str>,
    do_parse!(
        multispace >>
        opt!(do_parse!(tag_no_case!("as") >> multispace >> ())) >>
        alias: sql_identifier >>
        (str::from_utf8(*alias).unwrap())
    )
);

named!(field_value_expr<CompleteByteSlice, FieldValueExpression>,
    alt!(
        map!(literal, |l| FieldValueExpression::Literal(LiteralExpression {
            value: l.into(),
            alias: None,
        }))
        | map!(arithmetic_expression, |ae| FieldValueExpression::Arithmetic(ae))
    )
);

named!(assignment_expr<CompleteByteSlice, (Column, FieldValueExpression) >,
    do_parse!(
        column: column_identifier_no_alias >>
        opt_multispace >>
        tag!("=") >>
        opt_multispace >>
        value: field_value_expr >>
        (column, value)
    )
);

named!(pub assignment_expr_list<CompleteByteSlice, Vec<(Column, FieldValueExpression)> >,
       many1!(
           do_parse!(
               field_value: assignment_expr >>
               opt!(
                   do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   )
               ) >>
               (field_value)
           )
       )
);

/// Parse rule for a comma-separated list of fields without aliases.
named!(pub field_list<CompleteByteSlice, Vec<Column> >,
       many0!(
           do_parse!(
               fieldname: column_identifier_no_alias >>
               opt!(
                   do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   )
               ) >>
               (fieldname)
           )
       )
);

/// Parse list of column/field definitions.
named!(pub field_definition_expr<CompleteByteSlice, Vec<FieldDefinitionExpression>>,
       many0!(
           do_parse!(
               field: alt!(
                   do_parse!(
                       tag!("*") >>
                       (FieldDefinitionExpression::All)
                   )
                 | do_parse!(
                     table: table_reference >>
                     tag!(".*") >>
                     (FieldDefinitionExpression::AllInTable(table.name.clone()))
                 )
                 | do_parse!(
                     expr: arithmetic_expression >>
                     (FieldDefinitionExpression::Value(
                             FieldValueExpression::Arithmetic(expr)))
                 )
                 | do_parse!(
                     literal: literal_expression >>
                     (FieldDefinitionExpression::Value(
                             FieldValueExpression::Literal(literal)))
                 )
                 | do_parse!(
                     column: column_identifier >>
                     (FieldDefinitionExpression::Col(column))
                 )
               ) >>
               opt!(
                   do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   )
               ) >>
               (field)
           )
       )
);

/// Parse list of table names.
/// XXX(malte): add support for aliases
named!(pub table_list<CompleteByteSlice, Vec<Table> >,
       many0!(
           do_parse!(
               table: table_reference >>
               opt!(
                   do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   )
               ) >>
               (table)
           )
       )
);

/// Integer literal value
named!(pub integer_literal<CompleteByteSlice, Literal>,
    do_parse!(
        sign: opt!(tag!("-")) >>
        val: digit >>
        ({
            let mut intval = i64::from_str(str::from_utf8(*val).unwrap()).unwrap();
            if sign.is_some() {
                intval *= -1;
            }
            Literal::Integer(intval)
        })
    )
);

/// Floating point literal value
named!(pub float_literal<CompleteByteSlice, Literal>,
    do_parse!(
        sign: opt!(tag!("-")) >>
        mant: digit >>
        tag!(".") >>
        frac: digit >>
        ({
            let unpack = |v: &[u8]| -> i32 {
                i32::from_str(str::from_utf8(v).unwrap()).unwrap()
            };
            Literal::FixedPoint(Real {
                integral: if sign.is_some() {
                    -1 * unpack(mant.0)
                } else {
                    unpack(mant.0)
                },
                fractional: unpack(frac.0) as i32,
            })
        })
    )
);

/// String literal value

fn raw_string_quoted(input: CompleteByteSlice, quote: u8) -> IResult<CompleteByteSlice, Vec<u8>> {
    let quote_slice: &[u8] = &[quote];
    let double_quote_slice: &[u8] = &[quote, quote];
    let backslash_quote: &[u8] = &[b'\\', quote];
    delimited!(input,
        tag!(quote_slice),
        fold_many0!(
            alt!(
                is_not!(backslash_quote) |
                map!(tag!(double_quote_slice), |_| CompleteByteSlice(quote_slice)) |
                map!(tag!("\\\\"), |_| CompleteByteSlice(&b"\\"[..])) |
                map!(tag!("\\b"), |_| CompleteByteSlice(&b"\x7f"[..])) |
                map!(tag!("\\r"), |_| CompleteByteSlice(&b"\r"[..])) |
                map!(tag!("\\n"), |_| CompleteByteSlice(&b"\n"[..])) |
                map!(tag!("\\t"), |_| CompleteByteSlice(&b"\t"[..])) |
                map!(tag!("\\0"), |_| CompleteByteSlice(&b"\0"[..])) |
                map!(tag!("\\Z"), |_| CompleteByteSlice(&b"\x1A"[..])) |
                do_parse!(
                        _escape: tag!("\\") >>
                        escaped: take!(1) >>
                        (escaped ))
            ),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: CompleteByteSlice| {
                acc.extend(bytes.0);
                acc
            }
        ),
        tag!(quote_slice)
    )
}

named!(raw_string_singlequoted< CompleteByteSlice, Vec<u8> >, call!(raw_string_quoted, b'\''));
named!(raw_string_doublequoted< CompleteByteSlice, Vec<u8> >, call!(raw_string_quoted, b'"'));

named!(pub string_literal<CompleteByteSlice, Literal>,
       map!(alt!(raw_string_singlequoted | raw_string_doublequoted),
             |bytes| match String::from_utf8(bytes) {
                 Ok(s) => Literal::String(s),
                 Err(err) => Literal::Blob(err.into_bytes())
             }
           )
);

/// Any literal value.
named!(pub literal<CompleteByteSlice, Literal>,
    alt!(
          float_literal
        | integer_literal
        | string_literal
        | do_parse!(tag_no_case!("NULL") >> (Literal::Null))
        | do_parse!(tag_no_case!("CURRENT_TIMESTAMP") >> (Literal::CurrentTimestamp))
        | do_parse!(tag_no_case!("CURRENT_DATE") >> (Literal::CurrentDate))
        | do_parse!(tag_no_case!("CURRENT_TIME") >> (Literal::CurrentTime))
        | do_parse!(tag_no_case!("?") >> (Literal::Placeholder))
    )
);

named!(pub literal_expression<CompleteByteSlice, LiteralExpression>,
    do_parse!(
        literal: delimited!(opt!(tag!("(")), literal, opt!(tag!(")"))) >>
        alias: opt!(as_alias) >>
        (LiteralExpression {
            value: literal,
            alias: alias.map(|a| a.to_string()),
        })
    )
);

/// Parse a list of values (e.g., for INSERT syntax).
named!(pub value_list<CompleteByteSlice, Vec<Literal> >,
       many0!(
           do_parse!(
               opt_multispace >>
               val: literal >>
               opt!(
                   do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   )
               ) >>
               (val)
           )
       )
);

/// Parse a reference to a named table, with an optional alias
/// TODO(malte): add support for schema.table notation
named!(pub table_reference<CompleteByteSlice, Table>,
    do_parse!(
        table: sql_identifier >>
        alias: opt!(as_alias) >>
        (Table {
            name: String::from(str::from_utf8(*table).unwrap()),
            alias: match alias {
                Some(a) => Some(String::from(a)),
                None => None,
            }
        })
    )
);

/// Parse rule for a comment part.
named!(pub parse_comment<CompleteByteSlice, String>,
    do_parse!(
        opt_multispace >>
        tag_no_case!("comment") >>
        multispace >>
        comment: delimited!(tag!("'"), take_until!("'"), tag!("'")) >>
        (String::from(str::from_utf8(*comment).unwrap()))
    )
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_identifiers() {
        let id1 = CompleteByteSlice(b"foo");
        let id2 = CompleteByteSlice(b"f_o_o");
        let id3 = CompleteByteSlice(b"foo12");
        let id4 = CompleteByteSlice(b":fo oo");
        let id5 = CompleteByteSlice(b"primary ");
        let id6 = CompleteByteSlice(b"`primary`");

        assert!(sql_identifier(id1).is_ok());
        assert!(sql_identifier(id2).is_ok());
        assert!(sql_identifier(id3).is_ok());
        assert!(sql_identifier(id4).is_err());
        assert!(sql_identifier(id5).is_err());
        assert!(sql_identifier(id6).is_ok());
    }

    #[test]
    fn sql_types() {
        let ok = ["bool", "integer(16)", "datetime(16)"];
        let not_ok = ["varchar"];

        let res_ok: Vec<_> = ok
            .iter()
            .map(|t| type_identifier(CompleteByteSlice(t.as_bytes())).unwrap().1)
            .collect();
        let res_not_ok: Vec<_> = not_ok
            .iter()
            .map(|t| type_identifier(CompleteByteSlice(t.as_bytes())).is_ok())
            .collect();

        assert_eq!(
            res_ok,
            vec![SqlType::Bool, SqlType::Int(16), SqlType::DateTime(16)]
        );

        assert!(res_not_ok.into_iter().all(|r| r == false));
    }

    #[test]
    fn simple_column_function() {
        let qs = b"max(addr_id)";

        let res = column_identifier(CompleteByteSlice(qs));
        let expected = Column {
            name: String::from("max(addr_id)"),
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::Max(Column::from("addr_id")))),
        };
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn comment_data() {
        let res = parse_comment(CompleteByteSlice(b" COMMENT 'test'"));
        assert_eq!(res.unwrap().1, "test");
    }

    #[test]
    fn literal_string_single_backslash_escape() {
        let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
        for quote in [&b"'"[..], &b"\""[..]].iter() {
            let quoted = &[quote, &all_escaped[..], quote].concat();
            let res = string_literal(CompleteByteSlice(quoted));
            let expected = Literal::String("\0\'\"\x7F\n\r\t\x1a\\%_".to_string());
            assert_eq!(res, Ok((CompleteByteSlice(&b""[..]), expected)));
        }
    }

    #[test]
    fn literal_string_single_quote() {
        let res = string_literal(CompleteByteSlice(b"'a''b'"));
        let expected = Literal::String("a'b".to_string());
        assert_eq!(res, Ok((CompleteByteSlice(&b""[..]), expected)));
    }

    #[test]
    fn literal_string_double_quote() {
        let res = string_literal(CompleteByteSlice(br#""a""b""#));
        let expected = Literal::String(r#"a"b"#.to_string());
        assert_eq!(res, Ok((CompleteByteSlice(&b""[..]), expected)));
    }
}
