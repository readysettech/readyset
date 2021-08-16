use std::hash::{Hash, Hasher};
use std::str;
use std::str::FromStr;

use itertools::Itertools;
use launchpad::arbitrary::{
    arbitrary_naive_time, arbitrary_positive_naive_date, arbitrary_timestamp_naive_date_time,
};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take, take_until, take_while1};
use nom::character::complete::{digit1, line_ending, multispace0, multispace1};
use nom::character::is_alphanumeric;
use nom::combinator::{map, not, peek};
use nom::combinator::{map_parser, map_res};
use nom::combinator::{opt, recognize};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{fold_many0, many0, many1, separated_list};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{char, complete, do_parse, map, named, opt, tag_no_case, IResult, InputLength};
use proptest::prelude as prop;
use proptest::prop_oneof;
use proptest::strategy::Strategy;
use std::fmt::{self, Display};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::expression::expression;
use crate::keywords::{escape_if_keyword, sql_keyword};
use crate::table::Table;
use crate::{Expression, FunctionExpression};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum SqlType {
    Bool,
    Char(#[strategy(1..255u16)] u16),
    Varchar(#[strategy(1..255u16)] u16),
    Int(#[strategy(1..255u16)] u16),
    UnsignedInt(#[strategy(1..255u16)] u16),
    Bigint(#[strategy(1..255u16)] u16),
    UnsignedBigint(#[strategy(1..255u16)] u16),
    Tinyint(#[strategy(1..255u16)] u16),
    UnsignedTinyint(#[strategy(1..255u16)] u16),
    Smallint(#[strategy(1..255u16)] u16),
    UnsignedSmallint(#[strategy(1..255u16)] u16),
    #[weight(0)]
    Blob,
    #[weight(0)]
    Longblob,
    #[weight(0)]
    Mediumblob,
    #[weight(0)]
    Tinyblob,
    Double,
    Float,
    Real,
    Tinytext,
    Mediumtext,
    Longtext,
    Text,
    Date,
    DateTime(#[strategy(1..=6u16)] u16),
    Time,
    Timestamp,
    #[weight(0)]
    Binary(u16),
    #[weight(0)]
    Varbinary(u16),
    #[weight(0)]
    Enum(Vec<Literal>),
    #[weight(0)]
    Decimal(#[strategy(1..=30u8)] u8, #[strategy(1..=#0)] u8),
}

impl SqlType {
    /// Returns a proptest strategy to generate *numeric* [`SqlType`]s - signed or unsigned, floats
    /// or reals
    pub fn arbitrary_numeric_type() -> impl Strategy<Value = SqlType> {
        use SqlType::*;

        prop_oneof![
            prop::Just(Int(32)),
            prop::Just(UnsignedInt(32)),
            prop::Just(Bigint(64)),
            prop::Just(UnsignedBigint(64)),
            prop::Just(Tinyint(8)),
            prop::Just(UnsignedTinyint(8)),
            prop::Just(Smallint(16)),
            prop::Just(UnsignedSmallint(16)),
            prop::Just(Double),
            prop::Just(Float),
            prop::Just(Real),
        ]
    }
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
            SqlType::Smallint(len) => write!(f, "SMALLINT({})", len),
            SqlType::UnsignedSmallint(len) => write!(f, "SMALLINT({}) UNSIGNED", len),
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
            SqlType::Time => write!(f, "TIME"),
            SqlType::Timestamp => write!(f, "TIMESTAMP"),
            SqlType::Binary(len) => write!(f, "BINARY({})", len),
            SqlType::Varbinary(len) => write!(f, "VARBINARY({})", len),
            SqlType::Enum(_) => write!(f, "ENUM(...)"),
            SqlType::Decimal(m, d) => write!(f, "DECIMAL({}, {})", m, d),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Arbitrary)]
pub struct Real {
    pub value: f64,
    pub precision: u8,
}

impl PartialEq for Real {
    fn eq(&self, other: &Self) -> bool {
        self.value.to_bits() == other.value.to_bits() && self.precision == other.precision
    }
}

impl Eq for Real {}

impl Hash for Real {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.value.to_bits());
        state.write_u8(self.precision);
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ItemPlaceholder {
    QuestionMark,
    DollarNumber(u32),
    ColonNumber(u32),
}

impl ToString for ItemPlaceholder {
    fn to_string(&self) -> String {
        match *self {
            ItemPlaceholder::QuestionMark => "?".to_string(),
            ItemPlaceholder::DollarNumber(ref i) => format!("${}", i),
            ItemPlaceholder::ColonNumber(ref i) => format!(":{}", i),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum Literal {
    Null,
    Integer(i64),
    FixedPoint(Real),
    String(String),
    #[weight(0)]
    Blob(Vec<u8>),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
    Placeholder(ItemPlaceholder),
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<u64> for Literal {
    fn from(i: u64) -> Self {
        Literal::Integer(i as _)
    }
}

impl From<i32> for Literal {
    fn from(i: i32) -> Self {
        Literal::Integer(i.into())
    }
}

impl From<u32> for Literal {
    fn from(i: u32) -> Self {
        Literal::Integer(i.into())
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

impl Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::FixedPoint(fp) => {
                let precision = if fp.precision < 30 { fp.precision } else { 30 };
                let fstr = format!("{:.*}", precision as usize, fp.value);
                // Trim all trailing zeros, but leave one after the dot if this is a whole number
                let res = fstr.trim_end_matches('0');
                if res.ends_with('.') {
                    write!(f, "{}0", res)
                } else {
                    write!(f, "{}", res)
                }
            }
            Literal::String(ref s) => {
                write!(f, "'{}'", s.replace('\'', "''").replace('\\', "\\\\"))
            }
            Literal::Blob(ref bv) => write!(
                f,
                "{}",
                bv.iter()
                    .map(|v| format!("{:x}", v))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
            Literal::CurrentTime => write!(f, "CURRENT_TIME"),
            Literal::CurrentDate => write!(f, "CURRENT_DATE"),
            Literal::CurrentTimestamp => write!(f, "CURRENT_TIMESTAMP"),
            Literal::Placeholder(item) => write!(f, "{}", item.to_string()),
        }
    }
}

impl Literal {
    pub fn arbitrary_with_type(sql_type: &SqlType) -> impl Strategy<Value = Self> + 'static {
        use proptest::prelude::*;

        match sql_type {
            SqlType::Bool => prop_oneof![Just(Self::Integer(0)), Just(Self::Integer(1)),].boxed(),
            SqlType::Char(_)
            | SqlType::Varchar(_)
            | SqlType::Tinytext
            | SqlType::Mediumtext
            | SqlType::Longtext
            | SqlType::Text => any::<String>().prop_map(Self::String).boxed(),
            SqlType::Int(_) => any::<i32>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedInt(_) => any::<u32>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::Bigint(_) => any::<i64>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedBigint(_) => any::<u64>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::Tinyint(_) => any::<i8>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedTinyint(_) => any::<u8>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::Smallint(_) => any::<i16>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::UnsignedSmallint(_) => {
                any::<u16>().prop_map(|i| Self::Integer(i as _)).boxed()
            }
            SqlType::Blob
            | SqlType::Longblob
            | SqlType::Mediumblob
            | SqlType::Tinyblob
            | SqlType::Binary(_)
            | SqlType::Varbinary(_) => any::<Vec<u8>>().prop_map(Self::Blob).boxed(),
            SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => {
                any::<Real>().prop_map(Self::FixedPoint).boxed()
            }
            SqlType::Date => arbitrary_positive_naive_date()
                .prop_map(|nd| Self::String(nd.format("%Y-%m-%d").to_string()))
                .boxed(),
            SqlType::DateTime(_) | SqlType::Timestamp => arbitrary_timestamp_naive_date_time()
                .prop_map(|ndt| Self::String(ndt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .boxed(),
            SqlType::Time => arbitrary_naive_time()
                .prop_map(|nt| Self::String(nt.format("%H:%M:%S").to_string()))
                .boxed(),
            SqlType::Enum(_) => unimplemented!("Enums aren't implemented yet"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey(Vec<Column>),
    UniqueKey(Option<String>, Vec<Column>),
    FulltextKey(Option<String>, Vec<Column>),
    Key(String, Vec<Column>),
    ForeignKey {
        name: Option<String>,
        index_name: Option<String>,
        columns: Vec<Column>,
        target_table: Table,
        target_columns: Vec<Column>,
    },
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableKey::PrimaryKey(columns) => {
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
            TableKey::UniqueKey(name, columns) => {
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
            TableKey::FulltextKey(name, columns) => {
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
            TableKey::Key(name, columns) => {
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
            TableKey::ForeignKey {
                name,
                index_name,
                columns: column,
                target_table,
                target_columns: target_column,
            } => {
                write!(
                    f,
                    "CONSTRAINT {} FOREIGN KEY {}({}) REFERENCES {} ({})",
                    name.as_deref().unwrap_or(""),
                    index_name.as_deref().unwrap_or(""),
                    column.iter().map(|c| escape_if_keyword(&c.name)).join(", "),
                    target_table,
                    target_column
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .join(", "),
                )
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // NOTE(grfn): do we actually care about this?
pub enum FieldDefinitionExpression {
    All,
    AllInTable(String),
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// Constructs a [`FieldDefinitionExpression::Expression`] without an alias
impl From<Expression> for FieldDefinitionExpression {
    fn from(expr: Expression) -> Self {
        FieldDefinitionExpression::Expression { expr, alias: None }
    }
}

/// Constructs a [`FieldDefinitionExpression::Expression`] based on an [`Expression::Column`] for
/// the column and without an alias
impl From<Column> for FieldDefinitionExpression {
    fn from(col: Column) -> Self {
        FieldDefinitionExpression::Expression {
            expr: Expression::Column(col),
            alias: None,
        }
    }
}

impl From<Literal> for FieldDefinitionExpression {
    fn from(lit: Literal) -> Self {
        FieldDefinitionExpression::from(Expression::Literal(lit))
    }
}

impl Display for FieldDefinitionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldDefinitionExpression::All => write!(f, "*"),
            FieldDefinitionExpression::AllInTable(ref table) => {
                write!(f, "{}.*", escape_if_keyword(table))
            }
            FieldDefinitionExpression::Expression { expr, alias } => {
                write!(f, "{}", expr)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for FieldDefinitionExpression {
    fn default() -> FieldDefinitionExpression {
        FieldDefinitionExpression::All
    }
}

pub enum Sign {
    Unsigned,
    Signed,
}

#[inline]
pub fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_' || chr == b'@'
}

fn digit_as_u16(len: &[u8]) -> IResult<&[u8], u16> {
    match str::from_utf8(len) {
        Ok(s) => match u16::from_str(s) {
            Ok(v) => Ok((len, v)),
            Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
                len,
                ErrorKind::LengthValue,
            ))),
        },
        Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
            len,
            ErrorKind::LengthValue,
        ))),
    }
}

fn digit_as_u8(len: &[u8]) -> IResult<&[u8], u8> {
    match str::from_utf8(len) {
        Ok(s) => match u8::from_str(s) {
            Ok(v) => Ok((len, v)),
            Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
                len,
                ErrorKind::LengthValue,
            ))),
        },
        Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
            len,
            ErrorKind::LengthValue,
        ))),
    }
}

pub(crate) fn opt_delimited<I: Clone, O1, O2, O3, E: ParseError<I>, F, G, H>(
    first: F,
    second: G,
    third: H,
) -> impl Fn(I) -> IResult<I, O2, E>
where
    F: Fn(I) -> IResult<I, O1, E>,
    G: Fn(I) -> IResult<I, O2, E>,
    H: Fn(I) -> IResult<I, O3, E>,
{
    move |input: I| {
        let first_ = &first;
        let second_ = &second;
        let third_ = &third;

        let inp = input.clone();
        match second(input) {
            Ok((i, o)) => Ok((i, o)),
            _ => delimited(first_, second_, third_)(inp),
        }
    }
}

fn precision_helper(i: &[u8]) -> IResult<&[u8], (u8, Option<u8>)> {
    let (remaining_input, (m, d)) = tuple((
        digit1,
        opt(preceded(tag(","), preceded(multispace0, digit1))),
    ))(i)?;

    let m = digit_as_u8(m)?.1;
    // Manual map allowed for nom error propagation.
    #[allow(clippy::manual_map)]
    let d = match d {
        None => None,
        Some(v) => Some(digit_as_u8(v)?.1),
    };

    Ok((remaining_input, (m, d)))
}

pub fn precision(i: &[u8]) -> IResult<&[u8], (u8, Option<u8>)> {
    delimited(tag("("), precision_helper, tag(")"))(i)
}

fn opt_signed(i: &[u8]) -> IResult<&[u8], Option<Sign>> {
    opt(alt((
        map(tag_no_case("unsigned"), |_| Sign::Unsigned),
        map(tag_no_case("signed"), |_| Sign::Signed),
    )))(i)
}

fn delim_digit(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(tag("("), digit1, tag(")"))(i)
}

fn delim_u16(i: &[u8]) -> IResult<&[u8], u16> {
    map_parser(delim_digit, digit_as_u16)(i)
}

fn int_type<'a, F, G>(
    tag: &str,
    mk_unsigned: F,
    mk_signed: G,
    default_len: u16,
    i: &'a [u8],
) -> IResult<&'a [u8], SqlType>
where
    F: Fn(u16) -> SqlType + 'static,
    G: Fn(u16) -> SqlType + 'static,
{
    let (remaining_input, (_, len, _, signed)) =
        tuple((tag_no_case(tag), opt(delim_u16), multispace0, opt_signed))(i)?;

    let len = len.unwrap_or(default_len);

    if let Some(Sign::Unsigned) = signed {
        Ok((remaining_input, mk_unsigned(len)))
    } else {
        Ok((remaining_input, mk_signed(len)))
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
        |i| int_type("tinyint", SqlType::UnsignedTinyint, SqlType::Tinyint, 8, i),
        |i| {
            int_type(
                "smallint",
                SqlType::UnsignedSmallint,
                SqlType::Smallint,
                16,
                i,
            )
        },
        |i| int_type("integer", SqlType::UnsignedInt, SqlType::Int, 32, i),
        |i| int_type("int", SqlType::UnsignedInt, SqlType::Int, 32, i),
        |i| int_type("bigint", SqlType::UnsignedBigint, SqlType::Bigint, 64, i),
        map(tag_no_case("bool"), |_| SqlType::Bool),
        map(
            tuple((
                tag_no_case("char"),
                delim_u16,
                multispace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Char(t.1),
        ),
        map(preceded(tag_no_case("datetime"), opt(delim_u16)), |fsp| {
            SqlType::DateTime(fsp.unwrap_or(0_u16))
        }),
        map(tag_no_case("date"), |_| SqlType::Date),
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
            SqlType::Enum,
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
                alt((
                    // The alt expects the same type to be returned for both entries,
                    // so both have to be tuples with same number of elements
                    tuple((tag_no_case("varchar"), multispace0, multispace0)),
                    tuple((
                        tag_no_case("character"),
                        multispace1,
                        tag_no_case("varying"),
                    )),
                )),
                delim_u16,
                multispace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Varchar(t.1),
        ),
        map(tag_no_case("time"), |_| SqlType::Time),
    ))(i)
}

fn type_identifier_second_half(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((
        decimal_or_numeric,
        map(
            tuple((tag_no_case("binary"), delim_u16, multispace0)),
            |t| SqlType::Binary(t.1),
        ),
        map(tag_no_case("blob"), |_| SqlType::Blob),
        map(tag_no_case("longblob"), |_| SqlType::Longblob),
        map(tag_no_case("mediumblob"), |_| SqlType::Mediumblob),
        map(tag_no_case("mediumtext"), |_| SqlType::Mediumtext),
        map(tag_no_case("longtext"), |_| SqlType::Longtext),
        map(tag_no_case("tinyblob"), |_| SqlType::Tinyblob),
        map(tag_no_case("tinytext"), |_| SqlType::Tinytext),
        map(
            tuple((tag_no_case("varbinary"), delim_u16, multispace0)),
            |t| SqlType::Varbinary(t.1),
        ),
    ))(i)
}

// A SQL type specifier.
pub fn type_identifier(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((type_identifier_first_half, type_identifier_second_half))(i)
}

// Parses the arguments for an aggregation function, and also returns whether the distinct flag is
// present.
pub fn function_arguments(i: &[u8]) -> IResult<&[u8], (Expression, bool)> {
    let distinct_parser = opt(tuple((tag_no_case("distinct"), multispace1)));
    let (remaining_input, (distinct, args)) = tuple((distinct_parser, expression))(i)?;
    Ok((remaining_input, (args, distinct.is_some())))
}

fn group_concat_fx_helper(i: &[u8]) -> IResult<&[u8], String> {
    let ws_sep = delimited(multispace0, tag_no_case("separator"), multispace0);
    let (remaining_input, sep) = delimited(
        ws_sep,
        opt(map_res(
            alt((raw_string_single_quoted, raw_string_double_quoted)),
            String::from_utf8,
        )),
        multispace0,
    )(i)?;

    Ok((remaining_input, sep.unwrap_or_default()))
}

fn group_concat_fx(i: &[u8]) -> IResult<&[u8], (Column, Option<String>)> {
    pair(column_identifier_no_alias, opt(group_concat_fx_helper))(i)
}

fn delim_fx_args(i: &[u8]) -> IResult<&[u8], (Expression, bool)> {
    delimited(tag("("), function_arguments, tag(")"))(i)
}

named!(cast(&[u8]) -> FunctionExpression, do_parse!(
    complete!(tag_no_case!("cast"))
        >> multispace0
        >> complete!(char!('('))
        >> arg: expression
        >> multispace1
        >> complete!(tag_no_case!("as"))
        >> multispace1
        >> type_: type_identifier
        >> multispace0
        >> complete!(char!(')'))
        >> (FunctionExpression::Cast(Box::new(arg), type_))
));

pub fn column_function(i: &[u8]) -> IResult<&[u8], FunctionExpression> {
    let delim_group_concat_fx = delimited(tag("("), group_concat_fx, tag(")"));
    alt((
        map(tag_no_case("count(*)"), |_| FunctionExpression::CountStar),
        map(preceded(tag_no_case("count"), delim_fx_args), |args| {
            FunctionExpression::Count {
                expr: Box::new(args.0.clone()),
                distinct: args.1,
                count_nulls: false,
            }
        }),
        map(preceded(tag_no_case("sum"), delim_fx_args), |args| {
            FunctionExpression::Sum {
                expr: Box::new(args.0.clone()),
                distinct: args.1,
            }
        }),
        map(preceded(tag_no_case("avg"), delim_fx_args), |args| {
            FunctionExpression::Avg {
                expr: Box::new(args.0.clone()),
                distinct: args.1,
            }
        }),
        map(preceded(tag_no_case("max"), delim_fx_args), |args| {
            FunctionExpression::Max(Box::new(args.0))
        }),
        map(preceded(tag_no_case("min"), delim_fx_args), |args| {
            FunctionExpression::Min(Box::new(args.0))
        }),
        cast,
        map(
            preceded(tag_no_case("group_concat"), delim_group_concat_fx),
            |spec| {
                let (ref col, sep) = spec;
                let separator = match sep {
                    // default separator is a comma, see MySQL manual §5.7
                    None => String::from(","),
                    Some(s) => s,
                };
                FunctionExpression::GroupConcat {
                    expr: Box::new(Expression::Column(col.clone())),
                    separator,
                }
            },
        ),
        map(
            tuple((
                sql_identifier,
                multispace0,
                tag("("),
                separated_list(tag(","), delimited(multispace0, expression, multispace0)),
                tag(")"),
            )),
            |tuple| {
                let (name, _, _, arguments, _) = tuple;
                FunctionExpression::Call {
                    name: name.to_string(),
                    arguments,
                }
            },
        ),
    ))(i)
}

// Parses a SQL column identifier in the table.column format
pub fn column_identifier_no_alias(i: &[u8]) -> IResult<&[u8], Column> {
    let table_parser = pair(opt(terminated(sql_identifier, tag("."))), sql_identifier);
    alt((
        map(column_function, |f| Column {
            name: format!("{}", f),
            table: None,
            function: Some(Box::new(f)),
        }),
        map(table_parser, |tup| Column {
            name: tup.1.to_string(),
            table: tup.0.map(|t| t.to_string()),
            function: None,
        }),
    ))(i)
}

#[cfg(feature = "postgres")]
pub fn sql_identifier(i: &[u8]) -> IResult<&[u8], &str> {
    map_res(
        alt((
            preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
            delimited(tag("\""), take_while1(is_sql_identifier), tag("\"")),
        )),
        str::from_utf8,
    )(i)
}

// Parses a SQL identifier (alphanumeric1 and "_").
#[cfg(not(feature = "postgres"))]
pub fn sql_identifier(i: &[u8]) -> IResult<&[u8], &str> {
    map_res(
        alt((
            preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
            delimited(tag("`"), take_while1(is_sql_identifier), tag("`")),
            delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
        )),
        str::from_utf8,
    )(i)
}

// Parse an unsigned integer.
pub fn unsigned_number(i: &[u8]) -> IResult<&[u8], u64> {
    map_res(map_res(digit1, str::from_utf8), u64::from_str)(i)
}

pub(crate) fn eof<I: Copy + InputLength, E: ParseError<I>>(input: I) -> IResult<I, I, E> {
    if input.input_len() == 0 {
        Ok((input, input))
    } else {
        Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Eof)))
    }
}

// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) =
        delimited(multispace0, alt((tag(";"), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

// Parse rule for AS-based aliases for SQL entities.
pub fn as_alias(i: &[u8]) -> IResult<&[u8], &str> {
    map(
        tuple((
            multispace1,
            opt(pair(tag_no_case("as"), multispace1)),
            sql_identifier,
        )),
        |a| a.2,
    )(i)
}

fn assignment_expr(i: &[u8]) -> IResult<&[u8], (Column, Expression)> {
    separated_pair(
        column_identifier_no_alias,
        delimited(multispace0, tag("="), multispace0),
        expression,
    )(i)
}

/// Whitespace surrounded optionally on either side by a comma
pub(crate) fn ws_sep_comma(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(multispace0, tag(","), multispace0)(i)
}

pub(crate) fn ws_sep_equals<'a, I>(i: I) -> IResult<I, I>
where
    I: nom::InputTakeAtPosition + nom::InputTake + nom::Compare<&'a str>,
    // Compare required by tag
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
    // AsChar and Clone required by multispace0
{
    delimited(multispace0, tag("="), multispace0)(i)
}

pub fn assignment_expr_list(i: &[u8]) -> IResult<&[u8], Vec<(Column, Expression)>> {
    many1(terminated(assignment_expr, opt(ws_sep_comma)))(i)
}

// Parse rule for a comma-separated list of fields without aliases.
pub fn field_list(i: &[u8]) -> IResult<&[u8], Vec<Column>> {
    many0(terminated(column_identifier_no_alias, opt(ws_sep_comma)))(i)
}

named!(
    expression_field(&[u8]) -> FieldDefinitionExpression,
    do_parse!(
        expr: expression
            >> alias: opt!(map!(as_alias, |a| a.to_owned()))
            >> (FieldDefinitionExpression::Expression { expr, alias })
    )
);

// Parse list of column/field definitions.
pub fn field_definition_expr(i: &[u8]) -> IResult<&[u8], Vec<FieldDefinitionExpression>> {
    terminated(
        separated_list(
            ws_sep_comma,
            alt((
                map(tag("*"), |_| FieldDefinitionExpression::All),
                map(terminated(table_reference, tag(".*")), |t| {
                    FieldDefinitionExpression::AllInTable(t.name)
                }),
                expression_field,
            )),
        ),
        opt(ws_sep_comma),
    )(i)
}

// Parse list of table names.
// XXX(malte): add support for aliases
pub fn table_list(i: &[u8]) -> IResult<&[u8], Vec<Table>> {
    many1(terminated(schema_table_reference, opt(ws_sep_comma)))(i)
}

// Integer literal value
pub fn integer_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(
        pair(
            opt(tag("-")),
            map_res(map_res(digit1, str::from_utf8), i64::from_str),
        ),
        |tup| {
            let mut intval = tup.1;
            if (tup.0).is_some() {
                intval *= -1;
            }
            Literal::Integer(intval)
        },
    )(i)
}

#[allow(clippy::type_complexity)]
pub fn float(i: &[u8]) -> IResult<&[u8], (Option<&[u8]>, &[u8], &[u8], &[u8])> {
    tuple((opt(tag("-")), digit1, tag("."), digit1))(i)
}

// Floating point literal value
#[allow(clippy::type_complexity)]
pub fn float_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(
        pair(
            peek(float),
            map_res(map_res(recognize(float), str::from_utf8), f64::from_str),
        ),
        |f| {
            let (_, _, _, frac) = f.0;
            Literal::FixedPoint(Real {
                value: f.1,
                precision: frac.len() as _,
            })
        },
    )(i)
}

/// String literal value
fn raw_string_quoted(input: &[u8], is_single_quote: bool) -> IResult<&[u8], Vec<u8>> {
    // TODO: clean up these assignments. lifetimes and temporary values made it difficult
    let quote_slice: &[u8] = if is_single_quote { b"\'" } else { b"\"" };
    let double_quote_slice: &[u8] = if is_single_quote { b"\'\'" } else { b"\"\"" };
    let backslash_quote: &[u8] = if is_single_quote { b"\\\'" } else { b"\\\"" };
    delimited(
        tag(quote_slice),
        fold_many0(
            alt((
                is_not(backslash_quote),
                map(tag(double_quote_slice), |_| -> &[u8] {
                    if is_single_quote {
                        b"\'"
                    } else {
                        b"\""
                    }
                }),
                map(tag("\\\\"), |_| &b"\\"[..]),
                map(tag("\\b"), |_| &b"\x7f"[..]),
                map(tag("\\r"), |_| &b"\r"[..]),
                map(tag("\\n"), |_| &b"\n"[..]),
                map(tag("\\t"), |_| &b"\t"[..]),
                map(tag("\\0"), |_| &b"\0"[..]),
                map(tag("\\Z"), |_| &b"\x1A"[..]),
                preceded(tag("\\"), take(1usize)),
            )),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            },
        ),
        tag(quote_slice),
    )(input)
}

fn raw_string_single_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, true)
}

fn raw_string_double_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, false)
}

#[cfg(feature = "postgres")]
pub fn string_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(raw_string_single_quoted, |bytes| {
        match String::from_utf8(bytes) {
            Ok(s) => Literal::String(s),
            Err(err) => Literal::Blob(err.into_bytes()),
        }
    })(i)
}

#[cfg(not(feature = "postgres"))]
pub fn string_literal(i: &[u8]) -> IResult<&[u8], Literal> {
    map(
        alt((raw_string_single_quoted, raw_string_double_quoted)),
        |bytes| match String::from_utf8(bytes) {
            Ok(s) => Literal::String(s),
            Err(err) => Literal::Blob(err.into_bytes()),
        },
    )(i)
}

// Any literal value.
pub fn literal(i: &[u8]) -> IResult<&[u8], Literal> {
    alt((
        float_literal,
        integer_literal,
        string_literal,
        map(tag_no_case("null"), |_| Literal::Null),
        map(tag_no_case("current_timestamp"), |_| {
            Literal::CurrentTimestamp
        }),
        map(tag_no_case("current_date"), |_| Literal::CurrentDate),
        map(tag_no_case("current_time"), |_| Literal::CurrentTime),
        map(tag("?"), |_| {
            Literal::Placeholder(ItemPlaceholder::QuestionMark)
        }),
        map(
            preceded(
                tag(":"),
                map_res(map_res(digit1, str::from_utf8), u32::from_str),
            ),
            |num| Literal::Placeholder(ItemPlaceholder::ColonNumber(num)),
        ),
        map(
            preceded(
                tag("$"),
                map_res(map_res(digit1, str::from_utf8), u32::from_str),
            ),
            |num| Literal::Placeholder(ItemPlaceholder::DollarNumber(num)),
        ),
    ))(i)
}

// Parse a list of values (e.g., for INSERT syntax).
pub fn value_list(i: &[u8]) -> IResult<&[u8], Vec<Literal>> {
    many0(delimited(multispace0, literal, opt(ws_sep_comma)))(i)
}

// Parse a reference to a named schema.table, with an optional alias
pub fn schema_table_reference(i: &[u8]) -> IResult<&[u8], Table> {
    map(
        tuple((
            opt(pair(sql_identifier, tag("."))),
            sql_identifier,
            opt(as_alias),
        )),
        |tup| Table {
            name: String::from(tup.1),
            alias: tup.2.map(String::from),
            schema: tup.0.map(|(s, _)| s.to_string()),
        },
    )(i)
}

named!(pub(crate) if_not_exists(&[u8]) -> bool, map!(opt!(do_parse!(
    tag_no_case!("if")
        >> multispace1
        >> tag_no_case!("not")
        >> multispace1
        >> tag_no_case!("exists")
        >> multispace1
        >> (())
    )), |o| o.is_some()));

// Parse a reference to a named table, with an optional alias
pub fn table_reference(i: &[u8]) -> IResult<&[u8], Table> {
    map(pair(sql_identifier, opt(as_alias)), |tup| Table {
        name: String::from(tup.0),
        alias: tup.1.map(String::from),
        schema: None,
    })(i)
}

// Parse rule for a comment part.
pub fn parse_comment(i: &[u8]) -> IResult<&[u8], String> {
    map(
        preceded(
            delimited(multispace0, tag_no_case("comment"), multispace1),
            map_res(
                delimited(tag("'"), take_until("'"), tag("'")),
                str::from_utf8,
            ),
        ),
        String::from,
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;
    use launchpad::hash::hash;
    use proptest::prop_assume;
    use test_strategy::proptest;

    fn test_opt_delimited_fn_call(i: &str) -> IResult<&[u8], &[u8]> {
        opt_delimited(tag("("), tag("abc"), tag(")"))(i.as_bytes())
    }

    #[test]
    fn opt_delimited_tests() {
        // let ok1 = IResult::Ok(("".as_bytes(), "abc".as_bytes()));
        assert_eq!(
            test_opt_delimited_fn_call("abc"),
            IResult::Ok(("".as_bytes(), "abc".as_bytes()))
        );
        assert_eq!(
            test_opt_delimited_fn_call("(abc)"),
            IResult::Ok(("".as_bytes(), "abc".as_bytes()))
        );
        assert!(test_opt_delimited_fn_call("(abc").is_err());
        assert_eq!(
            test_opt_delimited_fn_call("abc)"),
            IResult::Ok((")".as_bytes(), "abc".as_bytes()))
        );
        assert!(test_opt_delimited_fn_call("ab").is_err());
    }

    #[test]
    fn sql_types() {
        let ok = ["bool", "integer(16)", "datetime(16)"];
        let not_ok = ["varchar"];

        let res_ok: Vec<_> = ok
            .iter()
            .map(|t| type_identifier(t.as_bytes()).unwrap().1)
            .collect();

        assert_eq!(
            res_ok,
            vec![SqlType::Bool, SqlType::Int(16), SqlType::DateTime(16)]
        );

        assert!(not_ok
            .iter()
            .map(|t| type_identifier(t.as_bytes()).is_ok())
            .all(|r| !r));
    }

    #[test]
    fn group_concat() {
        let qs = b"group_concat(x separator ', ')";
        let expected = FunctionExpression::GroupConcat {
            expr: Box::new(Expression::Column(Column::from("x"))),
            separator: ", ".to_owned(),
        };
        let res = column_function(qs);
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn simple_generic_function() {
        let qlist = [
            "coalesce(a,b,c)".as_bytes(),
            "coalesce (a,b,c)".as_bytes(),
            "coalesce(a ,b,c)".as_bytes(),
            "coalesce(a, b,c)".as_bytes(),
        ];
        for q in qlist.iter() {
            let res = column_function(q);
            let expected = FunctionExpression::Call {
                name: "coalesce".to_string(),
                arguments: vec![
                    Expression::Column(Column::from("a")),
                    Expression::Column(Column::from("b")),
                    Expression::Column(Column::from("c")),
                ],
            };
            assert_eq!(res, Ok((&b""[..], expected)));
        }
    }

    #[test]
    fn nested_function_call() {
        let res = column_function(b"max(cast(foo as int))");
        let (rem, res) = res.unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            res,
            FunctionExpression::Max(Box::new(Expression::Call(FunctionExpression::Cast(
                Box::new(Expression::Column("foo".into())),
                SqlType::Int(32)
            ))))
        )
    }

    #[test]
    fn generic_function_with_int_literal() {
        let (_, res) = column_function(b"ifnull(x, 0)").unwrap();
        assert_eq!(
            res,
            FunctionExpression::Call {
                name: "ifnull".to_owned(),
                arguments: vec![
                    Expression::Column(Column::from("x")),
                    Expression::Literal(Literal::Integer(0))
                ]
            }
        );
    }

    #[test]
    fn comment_data() {
        let res = parse_comment(b" COMMENT 'test'");
        assert_eq!(res.unwrap().1, "test");
    }

    #[test]
    fn literal_string_single_quote() {
        let res = string_literal(b"'a''b'");
        let expected = Literal::String("a'b".to_string());
        assert_eq!(res, Ok((&b""[..], expected)));
    }

    #[test]
    fn terminated_by_semicolon() {
        let res = statement_terminator(b"   ;  ");
        assert_eq!(res, Ok((&b""[..], ())));
    }

    #[test]
    fn float_formatting_strips_trailing_zeros() {
        let f = Literal::FixedPoint(Real {
            value: 1.5,
            precision: u8::MAX,
        });
        assert_eq!(f.to_string(), "1.5");
    }

    #[test]
    fn float_formatting_leaves_zero_after_dot() {
        let f = Literal::FixedPoint(Real {
            value: 0.0,
            precision: u8::MAX,
        });
        assert_eq!(f.to_string(), "0.0");
    }

    #[test]
    fn float_lots_of_zeros() {
        let res = float_literal(b"1.500000000000000000000000000000")
            .unwrap()
            .1;
        match res {
            Literal::FixedPoint(Real { value, .. }) => {
                assert_approx_eq!(value, 1.5);
            }
            _ => unreachable!(),
        }
    }

    #[proptest]
    fn real_hash_matches_eq(real1: Real, real2: Real) {
        assert_eq!(real1 == real2, hash(&real1) == hash(&real2));
    }

    #[proptest]
    fn literal_to_string_parse_round_trip(lit: Literal) {
        prop_assume!(!matches!(lit, Literal::FixedPoint(_)));
        let s = lit.to_string();
        eprintln!("to_string(): {}", s);
        assert_eq!(literal(s.as_bytes()).unwrap().1, lit)
    }
}

#[cfg(not(feature = "postgres"))]
#[cfg(test)]
mod tests_mysql {
    use super::*;

    #[test]
    fn cast() {
        let qs = b"cast(`lp`.`start_ddtm` as date)";
        let expected = FunctionExpression::Cast(
            Box::new(Expression::Column(Column {
                table: Some("lp".to_owned()),
                name: "start_ddtm".to_owned(),
                function: None,
            })),
            SqlType::Date,
        );
        let res = column_function(qs);
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn simple_generic_function_with_literal() {
        let qlist = [
            "coalesce(\"a\",b,c)".as_bytes(),
            "coalesce (\"a\",b,c)".as_bytes(),
            "coalesce(\"a\" ,b,c)".as_bytes(),
            "coalesce(\"a\", b,c)".as_bytes(),
        ];
        for q in qlist.iter() {
            let res = column_function(q);
            let expected = FunctionExpression::Call {
                name: "coalesce".to_string(),
                arguments: vec![
                    Expression::Literal(Literal::String("a".to_owned())),
                    Expression::Column(Column::from("b")),
                    Expression::Column(Column::from("c")),
                ],
            };
            assert_eq!(res, Ok((&b""[..], expected)));
        }
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
    fn literal_string_double_quote() {
        let res = string_literal(br#""a""b""#);
        let expected = Literal::String(r#"a"b"#.to_string());
        assert_eq!(res, Ok((&b""[..], expected)));
    }
}

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests_postgres {
    use super::*;

    #[test]
    fn cast() {
        let qs = b"cast(\"lp\".\"start_ddtm\" as date)";
        let expected = FunctionExpression::Cast(
            Box::new(Expression::Column(Column {
                table: Some("lp".to_owned()),
                name: "start_ddtm".to_owned(),
                function: None,
            })),
            SqlType::Date,
        );
        let res = column_function(qs);
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn simple_generic_function_with_literal() {
        let qlist = [
            "coalesce('a',b,c)".as_bytes(),
            "coalesce ('a',b,c)".as_bytes(),
            "coalesce('a' ,b,c)".as_bytes(),
            "coalesce('a', b,c)".as_bytes(),
        ];
        for q in qlist.iter() {
            let res = column_function(q);
            let expected = FunctionExpression::Call {
                name: "coalesce".to_string(),
                arguments: vec![
                    Expression::Literal(Literal::String("a".to_owned())),
                    Expression::Column(Column::from("b")),
                    Expression::Column(Column::from("c")),
                ],
            };
            assert_eq!(res, Ok((&b""[..], expected)));
        }
    }

    #[test]
    fn literal_string_single_backslash_escape() {
        let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
        let quote = &b"'"[..];
        let quoted = &[quote, &all_escaped[..], quote].concat();
        let res = string_literal(quoted);
        let expected = Literal::String("\0\'\"\x7F\n\r\t\x1a\\%_".to_string());
        assert_eq!(res, Ok((&b""[..], expected)));
    }

    #[test]
    fn sql_identifiers() {
        let id1 = b"foo";
        let id2 = b"f_o_o";
        let id3 = b"foo12";
        let id4 = b":fo oo";
        let id5 = b"primary ";
        let id6 = b"\"primary\"";

        assert!(sql_identifier(id1).is_ok());
        assert!(sql_identifier(id2).is_ok());
        assert!(sql_identifier(id3).is_ok());
        assert!(sql_identifier(id4).is_err());
        assert!(sql_identifier(id5).is_err());
        assert!(sql_identifier(id6).is_ok());
    }
}
