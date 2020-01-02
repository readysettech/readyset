use nom::branch::alt;
use nom::character::complete::{alphanumeric1, digit1, line_ending, multispace0, multispace1};
use nom::character::is_alphanumeric;
use nom::combinator::map;
use nom::{IResult, InputLength};
use std::fmt::{self, Display};
use std::str;
use std::str::FromStr;

use arithmetic::{arithmetic_expression, ArithmeticExpression};
use case::case_when_column;
use column::{Column, FunctionArguments, FunctionExpression};
use keywords::{escape_if_keyword, sql_keyword};
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::opt;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use table::Table;
use nom::error::{ParseError, ErrorKind};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Char(u16),
    Varchar(u16),
    Int(u16),
    UnsignedInt(u16),
    Bigint(u16),
    UnsignedBigint(u16),
    Tinyint(u16),
    UnsignedTinyint(u16),
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
            SqlType::UnsignedInt(len) => write!(f, "INT({}) UNSIGNED", len),
            SqlType::Bigint(len) => write!(f, "BIGINT({})", len),
            SqlType::UnsignedBigint(len) => write!(f, "BIGINT({}) UNSIGNED", len),
            SqlType::Tinyint(len) => write!(f, "TINYINT({})", len),
            SqlType::UnsignedTinyint(len) => write!(f, "TINYINT({}) UNSIGNED", len),
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
    UnsignedInteger(u64),
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

impl From<u64> for Literal {
    fn from(i: u64) -> Self {
        Literal::UnsignedInteger(i)
    }
}

impl From<i32> for Literal {
    fn from(i: i32) -> Self {
        Literal::Integer(i.into())
    }
}

impl From<u32> for Literal {
    fn from(i: u32) -> Self {
        Literal::UnsignedInteger(i.into())
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
            Literal::UnsignedInteger(ref i) => format!("{}", i),
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
    is_alphanumeric(chr) || chr == '_' as u8 || chr == '@' as u8
}

#[inline]
fn len_as_u16(len: &[u8]) -> u16 {
    match str::from_utf8(len) {
        Ok(s) => match u16::from_str(s) {
            Ok(v) => v,
            Err(e) => panic!(e),
        },
        Err(e) => panic!(e),
    }
}

fn precision_helper(i: &[u8]) -> IResult<&[u8], (u8, Option<u8>)> {
    let (remaining_input, (m, d)) = tuple((
        digit1,
        opt(preceded(tag(","), preceded(multispace0, digit1))),
    ))(i)?;

    Ok((remaining_input, (m[0], d.map(|r| r[0]))))
}

pub fn precision(i: &[u8]) -> IResult<&[u8], (u8, Option<u8>)> {
    delimited(tag("("), precision_helper, tag(")"))(i)
}

fn opt_signed(i: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    opt(alt((tag_no_case("unsigned"), tag_no_case("signed"))))(i)
}

fn delim_digit(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(tag("("), digit1, tag(")"))(i)
}

// TODO: rather than copy paste these functions, should create a function that returns a parser
// based on the sql int type, just like nom does
fn tiny_int(i: &[u8]) -> IResult<&[u8], SqlType> {
    let (remaining_input, (_, len, _, signed)) = tuple((
        tag_no_case("tinyint"),
        opt(delim_digit),
        multispace0,
        opt_signed,
    ))(i)?;

    match signed {
        Some(sign) => {
            if str::from_utf8(sign)
                .unwrap()
                .eq_ignore_ascii_case("unsigned")
            {
                Ok((
                    remaining_input,
                    SqlType::UnsignedTinyint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
                ))
            } else {
                Ok((
                    remaining_input,
                    SqlType::Tinyint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
                ))
            }
        }
        None => Ok((
            remaining_input,
            SqlType::Tinyint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
        )),
    }
}

// TODO: rather than copy paste these functions, should create a function that returns a parser
// based on the sql int type, just like nom does
fn big_int(i: &[u8]) -> IResult<&[u8], SqlType> {
    let (remaining_input, (_, len, _, signed)) = tuple((
        tag_no_case("bigint"),
        opt(delim_digit),
        multispace0,
        opt_signed,
    ))(i)?;

    match signed {
        Some(sign) => {
            if str::from_utf8(sign)
                .unwrap()
                .eq_ignore_ascii_case("unsigned")
            {
                Ok((
                    remaining_input,
                    SqlType::UnsignedBigint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
                ))
            } else {
                Ok((
                    remaining_input,
                    SqlType::Bigint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
                ))
            }
        }
        None => Ok((
            remaining_input,
            SqlType::Bigint(len.map(|l| len_as_u16(l)).unwrap_or(1)),
        )),
    }
}

// TODO: rather than copy paste these functions, should create a function that returns a parser
// based on the sql int type, just like nom does
fn sql_int_type(i: &[u8]) -> IResult<&[u8], SqlType> {
    let (remaining_input, (_, len, _, signed)) = tuple((
        alt((
            tag_no_case("integer"),
            tag_no_case("int"),
            tag_no_case("smallint"),
        )),
        opt(delim_digit),
        multispace0,
        opt_signed,
    ))(i)?;

    match signed {
        Some(sign) => {
            if str::from_utf8(sign)
                .unwrap()
                .eq_ignore_ascii_case("unsigned")
            {
                Ok((
                    remaining_input,
                    SqlType::UnsignedInt(len.map(|l| len_as_u16(l)).unwrap_or(32)),
                ))
            } else {
                Ok((
                    remaining_input,
                    SqlType::Int(len.map(|l| len_as_u16(l)).unwrap_or(32)),
                ))
            }
        }
        None => Ok((
            remaining_input,
            SqlType::Int(len.map(|l| len_as_u16(l)).unwrap_or(32)),
        )),
    }
}

// TODO(malte): not strictly ok to treat DECIMAL and NUMERIC as identical; the
// former has "at least" M precision, the latter "exactly".
// See https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
fn decimal_or_numeric(i: &[u8]) -> IResult<&[u8], SqlType> {
    let (remaining_input, precision) = delimited(
        alt((tag_no_case("decimal"), tag_no_case("numeric"))),
        opt(precision),
        multispace0,
    )(i)?;

    match precision {
        None => Ok((remaining_input, SqlType::Decimal(32, 0))),
        Some((m, None)) => Ok((remaining_input, SqlType::Decimal(m, 0))),
        Some((m, Some(d))) => Ok((remaining_input, SqlType::Decimal(m, d))),
    }
}

fn type_identifier_first_half(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((
        tiny_int,
        big_int,
        sql_int_type,
        map(tag_no_case("bool"), |_| SqlType::Bool),
        map(
            tuple((
                tag_no_case("char"),
                delim_digit,
                multispace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Char(len_as_u16(t.1)),
        ),
        map(tag_no_case("date"), |_| SqlType::Date),
        map(preceded(tag_no_case("datetime"), opt(delim_digit)), |fsp| {
            SqlType::DateTime(match fsp {
                Some(fsp) => len_as_u16(fsp),
                None => 0 as u16,
            })
        }),
        map(
            tuple((tag_no_case("double"), multispace0, opt_signed)),
            |_| SqlType::Double,
        ),
        map(
            terminated(
                preceded(
                    tag_no_case("enum"),
                    delimited(tag("("), value_list, tag(")")),
                ),
                multispace0,
            ),
            |v| SqlType::Enum(v),
        ),
        map(
            tuple((
                tag_no_case("float"),
                multispace0,
                opt(precision),
                multispace0,
            )),
            |_| SqlType::Float,
        ),
        map(
            tuple((tag_no_case("real"), multispace0, opt_signed)),
            |_| SqlType::Real,
        ),
        map(tag_no_case("text"), |_| SqlType::Text),
        map(
            tuple((tag_no_case("timestamp"), opt(delim_digit), multispace0)),
            |_| SqlType::Timestamp,
        ),
        map(
            tuple((
                tag_no_case("varchar"),
                delim_digit,
                multispace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Varchar(len_as_u16(t.1)),
        ),
        decimal_or_numeric,
    ))(i)
}

fn type_identifier_second_half(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((
        map(
            tuple((tag_no_case("binary"), delim_digit, multispace0)),
            |t| SqlType::Binary(len_as_u16(t.1)),
        ),
        map(tag_no_case("blob"), |_| SqlType::Blob),
        map(tag_no_case("longblob"), |_| SqlType::Longblob),
        map(tag_no_case("mediumblob"), |_| SqlType::Mediumblob),
        map(tag_no_case("mediumtext"), |_| SqlType::Mediumtext),
        map(tag_no_case("longtext"), |_| SqlType::Longtext),
        map(tag_no_case("tinyblob"), |_| SqlType::Tinyblob),
        map(tag_no_case("tinytext"), |_| SqlType::Tinytext),
        map(
            tuple((tag_no_case("varbinary"), delim_digit, multispace0)),
            |t| SqlType::Varbinary(len_as_u16(t.1)),
        ),
    ))(i)
}

// A SQL type specifier.
pub fn type_identifier(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((type_identifier_first_half, type_identifier_second_half))(i)
}

// Parses the arguments for an aggregation function, and also returns whether the distinct flag is
// present.
pub fn function_arguments(i: &[u8]) -> IResult<&[u8], (FunctionArguments, bool)> {
    let distinct_parser = opt(tuple((tag_no_case("distinct"), multispace1)));
    let args_parser = alt((
        map(case_when_column, |cw| FunctionArguments::Conditional(cw)),
        map(column_identifier_no_alias, |c| FunctionArguments::Column(c)),
    ));
    let (remaining_input, (distinct, args)) = tuple((distinct_parser, args_parser))(i)?;
    Ok((remaining_input, (args, distinct.is_some())))
}

fn group_concat_fx_helper(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let ws_sep = preceded(multispace0, tag_no_case("separator"));
    let (remaining_input, sep) = delimited(
        ws_sep,
        delimited(tag("'"), opt(alphanumeric1), tag("'")),
        multispace0,
    )(i)?;

    Ok((remaining_input, sep.unwrap_or(&[0u8; 0])))
}

fn group_concat_fx(i: &[u8]) -> IResult<&[u8], (Column, Option<&[u8]>)> {
    pair(column_identifier_no_alias, opt(group_concat_fx_helper))(i)
}

fn delim_fx_args(i: &[u8]) -> IResult<&[u8], (FunctionArguments, bool)> {
    delimited(tag("("), function_arguments, tag(")"))(i)
}

pub fn column_function(i: &[u8]) -> IResult<&[u8], FunctionExpression> {
    let delim_group_concat_fx = delimited(tag("("), group_concat_fx, tag(")"));
    alt((
        map(tag_no_case("count(*)"), |_| FunctionExpression::CountStar),
        map(preceded(tag_no_case("count"), delim_fx_args), |args| {
            FunctionExpression::Count(args.0.clone(), args.1)
        }),
        map(preceded(tag_no_case("sum"), delim_fx_args), |args| {
            FunctionExpression::Sum(args.0.clone(), args.1)
        }),
        map(preceded(tag_no_case("avg"), delim_fx_args), |args| {
            FunctionExpression::Avg(args.0.clone(), args.1)
        }),
        map(preceded(tag_no_case("max"), delim_fx_args), |args| {
            FunctionExpression::Max(args.0.clone())
        }),
        map(preceded(tag_no_case("min"), delim_fx_args), |args| {
            FunctionExpression::Min(args.0.clone())
        }),
        map(
            preceded(tag_no_case("group_concat"), delim_group_concat_fx),
            |spec| {
                let (ref col, ref sep) = spec;
                let sep = match *sep {
                    // default separator is a comma, see MySQL manual §5.7
                    None => String::from(","),
                    Some(s) => String::from_utf8(s.to_vec()).unwrap(),
                };
                FunctionExpression::GroupConcat(FunctionArguments::Column(col.clone()), sep)
            },
        ),
    ))(i)
}

// Parses a SQL column identifier in the table.column format
named!(pub column_identifier_no_alias<&[u8], Column>,
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
                    (tbl_name)
                )
            ) >>
            column: sql_identifier >>
            (Column {
                name: str::from_utf8(column).unwrap().to_owned(), // TODO: is unwrap fine?
                alias: None,
                table: match table {
                    None => None,
                    Some(t) => Some(str::from_utf8(t).unwrap().to_owned()) // TODO: is unwrap fine?
                },
                function: None,
            })
        )
    )
);

// Parses a SQL column identifier in the table.column format
named!(pub column_identifier<&[u8], Column>,
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
                    (tbl_name)
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
                    Some(t) => Some(str::from_utf8(t).unwrap().to_owned()), // TODO: is unwrap fine?
                },
                function: None,
            })
        )
    )
);

// Parses a SQL identifier (alphanumeric1 and "_").
named!(pub sql_identifier<&[u8], &[u8]>,
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

// Parse an unsigned integer.
named!(pub unsigned_number<&[u8], u64>,
    do_parse!(
        d: digit1 >>
        (FromStr::from_str(str::from_utf8(d).unwrap()).unwrap())
    )
);

pub fn eof<I: Copy + InputLength, E: ParseError<I>>(input: I) -> IResult<I, I, E> {
    if input.input_len() == 0 {
        Ok((input, input))
    } else {
        Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Eof)))
    }
}

// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) = delimited(multispace0, alt((tag(";"), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

// Parse binary comparison operators
named!(pub binary_comparison_operator<&[u8], Operator>,
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

// Parse rule for AS-based aliases for SQL entities.
named!(pub as_alias<&[u8], &str>,
    do_parse!(
        multispace1 >>
        opt!(do_parse!(tag_no_case!("as") >> multispace1 >> ())) >>
        alias: sql_identifier >>
        (str::from_utf8(alias).unwrap())
    )
);

named!(field_value_expr<&[u8], FieldValueExpression>,
    alt!(
        map!(literal, |l| FieldValueExpression::Literal(LiteralExpression {
            value: l.into(),
            alias: None,
        }))
        | map!(arithmetic_expression, |ae| FieldValueExpression::Arithmetic(ae))
    )
);

named!(assignment_expr<&[u8], (Column, FieldValueExpression) >,
    do_parse!(
        column: column_identifier_no_alias >>
        multispace0 >>
        tag!("=") >>
        multispace0 >>
        value: field_value_expr >>
        (column, value)
    )
);

named!(pub assignment_expr_list<&[u8], Vec<(Column, FieldValueExpression)> >,
       many1!(
           do_parse!(
               field_value: assignment_expr >>
               opt!(
                   do_parse!(
                       multispace0 >>
                       tag!(",") >>
                       multispace0 >>
                       ()
                   )
               ) >>
               (field_value)
           )
       )
);

// Parse rule for a comma-separated list of fields without aliases.
named!(pub field_list<&[u8], Vec<Column> >,
       many0!(
           do_parse!(
               fieldname: column_identifier_no_alias >>
               opt!(
                   do_parse!(
                       multispace0 >>
                       tag!(",") >>
                       multispace0 >>
                       ()
                   )
               ) >>
               (fieldname)
           )
       )
);

// Parse list of column/field definitions.
named!(pub field_definition_expr<&[u8], Vec<FieldDefinitionExpression>>,
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
                       multispace0 >>
                       tag!(",") >>
                       multispace0 >>
                       ()
                   )
               ) >>
               (field)
           )
       )
);

// Parse list of table names.
// XXX(malte): add support for aliases
named!(pub table_list<&[u8], Vec<Table> >,
       many0!(
           do_parse!(
               table: table_reference >>
               opt!(
                   do_parse!(
                       multispace0 >>
                       tag!(",") >>
                       multispace0 >>
                       ()
                   )
               ) >>
               (table)
           )
       )
);

// Integer literal value
named!(pub integer_literal<&[u8], Literal>,
    do_parse!(
        sign: opt!(tag!("-")) >>
        val: digit1 >>
        ({
            let mut intval = i64::from_str(str::from_utf8(val).unwrap()).unwrap();
            if sign.is_some() {
                intval *= -1;
            }
            Literal::Integer(intval)
        })
    )
);

// Floating point literal value
named!(pub float_literal<&[u8], Literal>,
    do_parse!(
        sign: opt!(tag!("-")) >>
        mant: digit1 >>
        tag!(".") >>
        frac: digit1 >>
        ({
            let unpack = |v: &[u8]| -> i32 {
                i32::from_str(str::from_utf8(v).unwrap()).unwrap()
            };
            Literal::FixedPoint(Real {
                integral: if sign.is_some() {
                    -1 * unpack(mant)
                } else {
                    unpack(mant)
                },
                fractional: unpack(frac) as i32,
            })
        })
    )
);

// TODO: get this function to compile
/// String literal value
/*
fn raw_string_quoted(input: &[u8], quote: u8) -> IResult<&[u8], &[u8]> {
    let quote_slice: &[u8] = &[quote];
    let double_quote_slice: &[u8] = &[quote, quote];
    let backslash_quote: &[u8] = &[b'\\', quote];
    delimited!(
        input,
        tag!(quote_slice),
        fold_many0!(
            alt!(
                is_not!(backslash_quote)
                    | map!(tag!(double_quote_slice), |_| quote_slice)
                    | map!(tag!("\\\\"), |_| &b"\\"[..])
                    | map!(tag!("\\b"), |_| &b"\x7f"[..])
                    | map!(tag!("\\r"), |_| &b"\r"[..])
                    | map!(tag!("\\n"), |_| &b"\n"[..])
                    | map!(tag!("\\t"), |_| &b"\t"[..])
                    | map!(tag!("\\0"), |_| &b"\0"[..])
                    | map!(tag!("\\Z"), |_| &b"\x1A"[..])
                    | do_parse!(_escape: tag!("\\") >> escaped: take!(1) >> (escaped))
            ),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            }
        ),
        tag!(quote_slice)
    )
}
*/

//named!(raw_string_singlequoted< &[u8], Vec<u8> >, call!(raw_string_quoted, b'\''));
//named!(raw_string_doublequoted< &[u8], Vec<u8> >, call!(raw_string_quoted, b'"'));

named!(raw_string_singlequoted< &[u8], Vec<u8> >,
    delimited!(
        tag!("\'"),
        fold_many0!(
            tag!("hello"),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            }
        ),
        tag!("\'")
    )
);

named!(raw_string_doublequoted< &[u8], Vec<u8> >,
    delimited!(
        tag!("\""),
        fold_many0!(
            tag!("hello"),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            }
        ),
        tag!("\"")
    )
);

named!(pub string_literal<&[u8], Literal>,
       map!(alt!(raw_string_singlequoted | raw_string_doublequoted),
             |bytes| match String::from_utf8(bytes) {
                 Ok(s) => Literal::String(s),
                 Err(err) => Literal::Blob(err.into_bytes())
             }
           )
);

// Any literal value.
named!(pub literal<&[u8], Literal>,
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

named!(pub literal_expression<&[u8], LiteralExpression>,
    do_parse!(
        literal: delimited!(opt!(tag!("(")), literal, opt!(tag!(")"))) >>
        alias: opt!(as_alias) >>
        (LiteralExpression {
            value: literal,
            alias: alias.map(|a| a.to_string()),
        })
    )
);

// Parse a list of values (e.g., for INSERT syntax).
named!(pub value_list<&[u8], Vec<Literal> >,
       many0!(
           do_parse!(
               multispace0 >>
               val: literal >>
               opt!(
                   do_parse!(
                       multispace0 >>
                       tag!(",") >>
                       multispace0 >>
                       ()
                   )
               ) >>
               (val)
           )
       )
);

// Parse a reference to a named table, with an optional alias
// TODO(malte): add support for schema.table notation
named!(pub table_reference<&[u8], Table>,
    do_parse!(
        table: sql_identifier >>
        alias: opt!(as_alias) >>
        (Table {
            name: String::from(str::from_utf8(table).unwrap()),
            alias: match alias {
                Some(a) => Some(String::from(a)),
                None => None,
            }
        })
    )
);

// Parse rule for a comment part.
named!(pub parse_comment<&[u8], String>,
    do_parse!(
        multispace0 >>
        tag_no_case!("comment") >>
        multispace1 >>
        comment: delimited!(tag!("'"), take_until!("'"), tag!("'")) >>
        (String::from(str::from_utf8(comment).unwrap()))
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
            .map(|t| type_identifier(t.as_bytes()).unwrap().1)
            .collect();
        let res_not_ok: Vec<_> = not_ok
            .iter()
            .map(|t| type_identifier(t.as_bytes()).is_ok())
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

        let res = column_identifier(qs);
        let expected = Column {
            name: String::from("max(addr_id)"),
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::Max(
                FunctionArguments::Column(Column::from("addr_id")),
            ))),
        };
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn comment_data() {
        let res = parse_comment(b" COMMENT 'test'");
        assert_eq!(res.unwrap().1, "test");
    }

    #[test]
    fn literal_string_single_backslash_escape() {
        let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
        for quote in [&b"'"[..], &b"\""[..]].iter() {
            let quoted = &[quote, &all_escaped[..], quote].concat();
            let res = string_literal(quoted);
            let expected = Literal::String("\0\'\"\x7F\n\r\t\x1a\\%_".to_string());
            assert_eq!(res, Ok((&b""[..], expected)));
        }
    }

    #[test]
    fn literal_string_single_quote() {
        let res = string_literal(b"'a''b'");
        let expected = Literal::String("a'b".to_string());
        assert_eq!(res, Ok((&b""[..], expected)));
    }

    #[test]
    fn literal_string_double_quote() {
        let res = string_literal(br#""a""b""#);
        let expected = Literal::String(r#"a"b"#.to_string());
        assert_eq!(res, Ok((&b""[..], expected)));
    }

    #[test]
    fn terminated_by_semicolon() {
        let res = statement_terminator(b"   ;  ");
        assert_eq!(res, Ok((&b""[..], ())));
    }
}
