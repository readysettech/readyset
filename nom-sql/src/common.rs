use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::str;
use std::str::FromStr;

use itertools::Itertools;
use launchpad::arbitrary::{
    arbitrary_decimal, arbitrary_json, arbitrary_naive_time, arbitrary_positive_naive_date,
    arbitrary_timestamp_naive_date_time, arbitrary_uuid,
};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{digit1, line_ending, multispace0, multispace1};
use nom::combinator::{map, peek};
use nom::combinator::{map_parser, map_res};
use nom::combinator::{opt, recognize};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many0, many1, separated_list};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{call, do_parse, map, named, opt, tag_no_case, IResult, InputLength};
use proptest::prelude as prop;
use proptest::prop_oneof;
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::dialect::Dialect;
use crate::expression::expression;
use crate::keywords::escape_if_keyword;
use crate::table::Table;
use crate::{Expression, FunctionExpression};
use eui48::{MacAddress, MacAddressFormat};
use rust_decimal::Decimal;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum SqlType {
    Bool,
    Char(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Varchar(#[strategy(1..255u16)] u16),
    Int(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Bigint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedBigint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Tinyint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedTinyint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Smallint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedSmallint(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
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
    Numeric(Option<(u16, Option<u8>)>),
    Tinytext,
    Mediumtext,
    Longtext,
    Text,
    Date,
    DateTime(#[strategy(proptest::option::of(1..=6u16))] Option<u16>),
    Time,
    Timestamp,
    #[weight(0)]
    Binary(Option<u16>),
    #[weight(0)]
    Varbinary(u16),
    #[weight(0)]
    Enum(Vec<Literal>),
    #[weight(0)]
    Decimal(#[strategy(1..=30u8)] u8, #[strategy(1..=# 0)] u8),
    Json,
    Jsonb,
    ByteArray,
    MacAddr,
    Uuid,
}

impl SqlType {
    /// Returns a proptest strategy to generate *numeric* [`SqlType`]s - signed or unsigned, floats
    /// or reals
    pub fn arbitrary_numeric_type() -> impl Strategy<Value = SqlType> {
        use SqlType::*;

        prop_oneof![
            prop::Just(Int(None)),
            prop::Just(UnsignedInt(None)),
            prop::Just(Bigint(None)),
            prop::Just(UnsignedBigint(None)),
            prop::Just(Tinyint(None)),
            prop::Just(UnsignedTinyint(None)),
            prop::Just(Smallint(None)),
            prop::Just(UnsignedSmallint(None)),
            prop::Just(Double),
            prop::Just(Float),
            prop::Just(Real),
        ]
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let write_with_len = |f: &mut fmt::Formatter, name, len| {
            write!(f, "{}", name)?;

            if let Some(len) = len {
                write!(f, "({})", len)?;
            }
            Ok(())
        };

        match *self {
            SqlType::Bool => write!(f, "BOOL"),
            SqlType::Char(len) => write_with_len(f, "CHAR", len),
            SqlType::Varchar(len) => write!(f, "VARCHAR({})", len),
            SqlType::Int(len) => write_with_len(f, "INT", len),
            SqlType::UnsignedInt(len) => {
                write_with_len(f, "INT", len)?;
                write!(f, " UNSIGNED")
            }
            SqlType::Bigint(len) => write_with_len(f, "BIGINT", len),
            SqlType::UnsignedBigint(len) => {
                write_with_len(f, "BIGINT", len)?;
                write!(f, " UNSIGNED")
            }
            SqlType::Tinyint(len) => write_with_len(f, "TINYINT", len),
            SqlType::UnsignedTinyint(len) => {
                write_with_len(f, "TINYINT", len)?;
                write!(f, " UNSIGNED")
            }
            SqlType::Smallint(len) => write_with_len(f, "SMALLINT", len),
            SqlType::UnsignedSmallint(len) => {
                write_with_len(f, "SMALLINT", len)?;
                write!(f, " UNSIGNED")
            }
            SqlType::Blob => write!(f, "BLOB"),
            SqlType::Longblob => write!(f, "LONGBLOB"),
            SqlType::Mediumblob => write!(f, "MEDIUMBLOB"),
            SqlType::Tinyblob => write!(f, "TINYBLOB"),
            SqlType::Double => write!(f, "DOUBLE"),
            SqlType::Float => write!(f, "FLOAT"),
            SqlType::Real => write!(f, "REAL"),
            SqlType::Numeric(precision) => match precision {
                Some((prec, Some(scale))) => write!(f, "NUMERIC({}, {})", prec, scale),
                Some((prec, _)) => write!(f, "NUMERIC({})", prec),
                _ => write!(f, "NUMERIC"),
            },
            SqlType::Tinytext => write!(f, "TINYTEXT"),
            SqlType::Mediumtext => write!(f, "MEDIUMTEXT"),
            SqlType::Longtext => write!(f, "LONGTEXT"),
            SqlType::Text => write!(f, "TEXT"),
            SqlType::Date => write!(f, "DATE"),
            SqlType::DateTime(len) => write_with_len(f, "DATETIME", len),
            SqlType::Time => write!(f, "TIME"),
            SqlType::Timestamp => write!(f, "TIMESTAMP"),
            SqlType::Binary(len) => write_with_len(f, "BINARY", len),
            SqlType::Varbinary(len) => write!(f, "VARBINARY({})", len),
            SqlType::Enum(ref variants) => write!(f, "ENUM({})", variants.iter().join(", ")),
            SqlType::Decimal(m, d) => write!(f, "DECIMAL({}, {})", m, d),
            SqlType::Json => write!(f, "JSON"),
            SqlType::Jsonb => write!(f, "JSONB"),
            SqlType::ByteArray => write!(f, "BYTEA"),
            SqlType::MacAddr => write!(f, "MACADDR"),
            SqlType::Uuid => write!(f, "UUID"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Arbitrary)]
pub struct Float {
    pub value: f32,
    pub precision: u8,
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.value.to_bits() == other.value.to_bits() && self.precision == other.precision
    }
}

impl Eq for Float {}

impl Hash for Float {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.value.to_bits());
        state.write_u8(self.precision);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Arbitrary)]
pub struct Double {
    pub value: f64,
    pub precision: u8,
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.value.to_bits() == other.value.to_bits() && self.precision == other.precision
    }
}

impl Eq for Double {}

impl Hash for Double {
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
    /// Represents an `f32` floating-point number.
    /// This distinction was introduced to avoid numeric error when transforming
    /// a `[Literal]` into another type (`[DataType]` or `[mysql::Value]`), an back.
    /// As an example, if we read an `f32` from a binlog, we would be transforming that
    /// `f32` into an `f64` (thus, potentially introducing numeric error) if this type
    /// didn't exist.
    Float(Float),
    Double(Double),
    Numeric(i128, u32),
    String(String),
    #[weight(0)]
    Blob(Vec<u8>),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
    // Even though `ByteArray` has the same inner representation as `Blob`,
    // we want to distinguish them, so then we can avoid doing a trial-and-error
    // to try to determine to which DataType it corresponds to.
    // Having this here makes it easy to parse PostgreSQL byte array literals, and
    // then just store the `Vec<u8>` into a DataType without testing if it's a valid
    // String or not.
    ByteArray(Vec<u8>),
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
        macro_rules! write_real {
            ($real:expr, $prec:expr) => {{
                let precision = if $prec < 30 { $prec } else { 30 };
                let fstr = format!("{:.*}", precision as usize, $real);
                // Trim all trailing zeros, but leave one after the dot if this is a whole number
                let res = fstr.trim_end_matches('0');
                if res.ends_with('.') {
                    write!(f, "{}0", res)
                } else {
                    write!(f, "{}", res)
                }
            }};
        }
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(float) => write_real!(float.value, float.precision),
            Literal::Double(double) => write_real!(double.value, double.precision),
            Literal::Numeric(val, scale) => {
                write!(f, "{}", Decimal::from_i128_with_scale(*val, *scale))
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
            Literal::ByteArray(b) => {
                write!(f, "E'\\x{}'", b.iter().map(|v| format!("{:x}", v)).join(""))
            }
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
            | SqlType::ByteArray
            | SqlType::Longblob
            | SqlType::Mediumblob
            | SqlType::Tinyblob
            | SqlType::Binary(_)
            | SqlType::Varbinary(_) => any::<Vec<u8>>().prop_map(Self::Blob).boxed(),
            SqlType::Float => any::<Float>().prop_map(Self::Float).boxed(),
            SqlType::Double | SqlType::Real | SqlType::Decimal(_, _) => {
                any::<Double>().prop_map(Self::Double).boxed()
            }
            SqlType::Numeric(_) => arbitrary_decimal()
                .prop_map(|d| Self::Numeric(d.mantissa(), d.scale()))
                .boxed(),
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
            SqlType::Json | SqlType::Jsonb => arbitrary_json()
                .prop_map(|v| Self::String(v.to_string()))
                .boxed(),
            SqlType::MacAddr => any::<[u8; 6]>()
                .prop_map(|bytes| {
                    // We know the length and format of the bytes, so this should always be parsable as a `MacAddress`.
                    #[allow(clippy::unwrap_used)]
                    Self::String(
                        MacAddress::from_bytes(&bytes[..])
                            .unwrap()
                            .to_string(MacAddressFormat::HexString),
                    )
                })
                .boxed(),
            SqlType::Uuid => arbitrary_uuid()
                .prop_map(|uuid| Self::String(uuid.to_string()))
                .boxed(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

impl Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
    SetDefault,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Cascade => "CASCADE",
                Self::SetNull => "SET NULL",
                Self::Restrict => "RESTRICT",
                Self::NoAction => "NO ACTION",
                Self::SetDefault => "SET DEFAULT",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<Column>,
    },
    UniqueKey {
        name: Option<String>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    FulltextKey(Option<String>, Vec<Column>),
    Key {
        name: String,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    ForeignKey {
        name: Option<String>,
        index_name: Option<String>,
        columns: Vec<Column>,
        target_table: Table,
        target_columns: Vec<Column>,
        on_delete: Option<ReferentialAction>,
    },
    CheckConstraint {
        name: Option<String>,
        expr: Expression,
        enforced: Option<bool>,
    },
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableKey::PrimaryKey { name, columns } => {
                write!(f, "PRIMARY KEY ")?;
                if let Some(name) = name {
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
            TableKey::UniqueKey {
                name,
                columns,
                index_type,
            } => {
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
                )?;
                if let Some(index_type) = index_type {
                    write!(f, " USING {}", index_type)?;
                }
                Ok(())
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
            TableKey::Key {
                name,
                columns,
                index_type,
            } => {
                write!(f, "KEY {} ", escape_if_keyword(name))?;
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| escape_if_keyword(&c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(index_type) = index_type {
                    write!(f, " USING {}", index_type)?;
                }
                Ok(())
            }
            TableKey::ForeignKey {
                name,
                index_name,
                columns: column,
                target_table,
                target_columns: target_column,
                on_delete,
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
                        .join(", ")
                )?;
                if let Some(on_delete) = on_delete {
                    write!(f, " ON DELETE {}", on_delete)?;
                }
                Ok(())
            }
            TableKey::CheckConstraint {
                name,
                expr,
                enforced,
            } => {
                write!(f, "CONSTRAINT",)?;
                if let Some(name) = name {
                    write!(f, " {}", name)?;
                }

                write!(f, " CHECK {}", expr)?;

                if let Some(enforced) = enforced {
                    if !enforced {
                        write!(f, " NOT")?;
                    }
                    write!(f, " ENFORCED")?;
                }

                Ok(())
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
                    write!(f, " AS {}", escape_if_keyword(alias))?;
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

pub fn numeric_precision(i: &[u8]) -> IResult<&[u8], (u16, Option<u8>)> {
    delimited(tag("("), numeric_precision_inner, tag(")"))(i)
}

pub fn numeric_precision_inner(i: &[u8]) -> IResult<&[u8], (u16, Option<u8>)> {
    let (remaining_input, (m, _, d)) = tuple((
        digit1,
        multispace0,
        opt(preceded(tag(","), preceded(multispace0, digit1))),
    ))(i)?;

    let m = digit_as_u16(m)?.1;
    d.map(|v| digit_as_u8(v))
        .transpose()
        .map(|v| (remaining_input, (m, v.map(|digit| digit.1))))
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
    i: &'a [u8],
) -> IResult<&'a [u8], SqlType>
where
    F: Fn(Option<u16>) -> SqlType + 'static,
    G: Fn(Option<u16>) -> SqlType + 'static,
{
    let (remaining_input, (_, len, _, signed)) =
        tuple((tag_no_case(tag), opt(delim_u16), multispace0, opt_signed))(i)?;

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

fn type_identifier_first_half(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SqlType> {
    move |i| {
        alt((
            |i| int_type("tinyint", SqlType::UnsignedTinyint, SqlType::Tinyint, i),
            |i| int_type("smallint", SqlType::UnsignedSmallint, SqlType::Smallint, i),
            |i| int_type("integer", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("int", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("bigint", SqlType::UnsignedBigint, SqlType::Bigint, i),
            map(tag_no_case("bool"), |_| SqlType::Bool),
            map(preceded(tag_no_case("datetime"), opt(delim_u16)), |fsp| {
                SqlType::DateTime(fsp)
            }),
            map(tag_no_case("date"), |_| SqlType::Date),
            map(
                tuple((
                    tag_no_case("double"),
                    opt(preceded(multispace1, tag_no_case("precision"))),
                    multispace0,
                    opt(precision),
                    multispace0,
                    opt_signed,
                )),
                |_| SqlType::Double,
            ),
            map(
                tuple((tag_no_case("numeric"), multispace0, opt(numeric_precision))),
                |t| SqlType::Numeric(t.2),
            ),
            map(
                terminated(
                    preceded(
                        tag_no_case("enum"),
                        delimited(tag("("), value_list(dialect), tag(")")),
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
            map(
                tuple((
                    tag_no_case("char"),
                    opt(delim_u16),
                    multispace0,
                    opt(tag_no_case("binary")),
                )),
                |t| SqlType::Char(t.1),
            ),
        ))(i)
    }
}

fn type_identifier_second_half(i: &[u8]) -> IResult<&[u8], SqlType> {
    alt((
        map(tag_no_case("time"), |_| SqlType::Time),
        decimal_or_numeric,
        map(
            tuple((tag_no_case("binary"), opt(delim_u16), multispace0)),
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
        map(tag_no_case("bytea"), |_| SqlType::ByteArray),
        map(tag_no_case("macaddr"), |_| SqlType::MacAddr),
        map(tag_no_case("uuid"), |_| SqlType::Uuid),
        map(tag_no_case("jsonb"), |_| SqlType::Jsonb),
        map(tag_no_case("json"), |_| SqlType::Json),
    ))(i)
}

// A SQL type specifier.
pub fn type_identifier(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SqlType> {
    move |i| {
        alt((
            type_identifier_first_half(dialect),
            type_identifier_second_half,
        ))(i)
    }
}

// Parses the arguments for an aggregation function, and also returns whether the distinct flag is
// present.
pub fn agg_function_arguments(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], (Expression, bool)> {
    move |i| {
        let distinct_parser = opt(tuple((tag_no_case("distinct"), multispace1)));
        let (remaining_input, (distinct, args)) = tuple((distinct_parser, expression(dialect)))(i)?;
        Ok((remaining_input, (args, distinct.is_some())))
    }
}

fn group_concat_fx_helper(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], String> {
    move |i| {
        let ws_sep = delimited(multispace0, tag_no_case("separator"), multispace0);
        let (i, sep) = delimited(
            ws_sep,
            opt(map_res(
                move |i| dialect.string_literal()(i),
                String::from_utf8,
            )),
            multispace0,
        )(i)?;

        Ok((i, sep.unwrap_or_default()))
    }
}

fn group_concat_fx(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Column, Option<String>)> {
    move |i| {
        pair(
            column_identifier_no_alias(dialect),
            opt(group_concat_fx_helper(dialect)),
        )(i)
    }
}

fn agg_fx_args(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Expression, bool)> {
    move |i| delimited(tag("("), agg_function_arguments(dialect), tag(")"))(i)
}

fn delim_fx_args(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Expression>> {
    move |i| {
        delimited(
            tag("("),
            separated_list(
                tag(","),
                delimited(multispace0, expression(dialect), multispace0),
            ),
            tag(")"),
        )(i)
    }
}

pub fn column_function(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FunctionExpression> {
    move |i| {
        alt((
            map(tag_no_case("count(*)"), |_| FunctionExpression::CountStar),
            map(
                preceded(tag_no_case("count"), agg_fx_args(dialect)),
                |args| FunctionExpression::Count {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                    count_nulls: false,
                },
            ),
            map(preceded(tag_no_case("sum"), agg_fx_args(dialect)), |args| {
                FunctionExpression::Sum {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("avg"), agg_fx_args(dialect)), |args| {
                FunctionExpression::Avg {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("max"), agg_fx_args(dialect)), |args| {
                FunctionExpression::Max(Box::new(args.0))
            }),
            map(preceded(tag_no_case("min"), agg_fx_args(dialect)), |args| {
                FunctionExpression::Min(Box::new(args.0))
            }),
            map(
                preceded(
                    tag_no_case("group_concat"),
                    delimited(tag("("), group_concat_fx(dialect), tag(")")),
                ),
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
                    dialect.function_identifier(),
                    multispace0,
                    delim_fx_args(dialect),
                )),
                |(name, _, arguments)| FunctionExpression::Call {
                    name: name.to_string(),
                    arguments,
                },
            ),
        ))(i)
    }
}

// Parses a SQL column identifier in the table.column format
pub fn column_identifier_no_alias(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Column> {
    move |i| {
        alt((
            map(column_function(dialect), |f| Column {
                name: format!("{}", f),
                table: None,
                function: Some(Box::new(f)),
            }),
            map(
                pair(
                    opt(terminated(dialect.identifier(), tag("."))),
                    dialect.identifier(),
                ),
                |tup| Column {
                    name: tup.1.to_string(),
                    table: tup.0.map(|t| t.to_string()),
                    function: None,
                },
            ),
        ))(i)
    }
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
pub fn as_alias(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Cow<str>> {
    move |i| {
        map(
            tuple((
                multispace1,
                opt(pair(tag_no_case("as"), multispace1)),
                dialect.identifier(),
            )),
            |a| a.2,
        )(i)
    }
}

fn assignment_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Column, Expression)> {
    move |i| {
        separated_pair(
            column_identifier_no_alias(dialect),
            delimited(multispace0, tag("="), multispace0),
            expression(dialect),
        )(i)
    }
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

pub fn assignment_expr_list(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(Column, Expression)>> {
    move |i| many1(terminated(assignment_expr(dialect), opt(ws_sep_comma)))(i)
}

// Parse rule for a comma-separated list of fields without aliases.
pub fn field_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Column>> {
    move |i| {
        many0(terminated(
            column_identifier_no_alias(dialect),
            opt(ws_sep_comma),
        ))(i)
    }
}

fn expression_field(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], FieldDefinitionExpression> {
    move |i| {
        do_parse!(
            i,
            expr: call!(expression(dialect))
                >> alias: opt!(map!(as_alias(dialect), |a| a.to_string()))
                >> (FieldDefinitionExpression::Expression { expr, alias })
        )
    }
}

// Parse list of column/field definitions.
pub fn field_definition_expr(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<FieldDefinitionExpression>> {
    move |i| {
        terminated(
            separated_list(
                ws_sep_comma,
                alt((
                    map(tag("*"), |_| FieldDefinitionExpression::All),
                    map(terminated(table_reference(dialect), tag(".*")), |t| {
                        FieldDefinitionExpression::AllInTable(t.name)
                    }),
                    expression_field(dialect),
                )),
            ),
            opt(ws_sep_comma),
        )(i)
    }
}

// Parse list of table names.
pub fn table_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Table>> {
    move |i| {
        many1(terminated(
            schema_table_reference(dialect),
            opt(ws_sep_comma),
        ))(i)
    }
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
            Literal::Double(Double {
                value: f.1,
                precision: frac.len() as _,
            })
        },
    )(i)
}

// Any literal value.
pub fn literal(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Literal> {
    move |i| {
        alt((
            float_literal,
            integer_literal,
            map(dialect.string_literal(), |bytes| {
                match String::from_utf8(bytes) {
                    Ok(s) => Literal::String(s),
                    Err(err) => Literal::Blob(err.into_bytes()),
                }
            }),
            map(dialect.bytes_literal(), Literal::ByteArray),
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
}

// Parse a list of values (e.g., for INSERT syntax).
pub fn value_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Literal>> {
    move |i| many0(delimited(multispace0, literal(dialect), opt(ws_sep_comma)))(i)
}

// Parse a reference to a named schema.table, with an optional alias
pub fn schema_table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Table> {
    move |i| {
        map(
            tuple((
                opt(pair(dialect.identifier(), tag("."))),
                dialect.identifier(),
                opt(as_alias(dialect)),
            )),
            |tup| Table {
                name: String::from(tup.1),
                alias: tup.2.map(String::from),
                schema: tup.0.map(|(s, _)| s.to_string()),
            },
        )(i)
    }
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
pub fn table_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Table> {
    move |i| {
        map(pair(dialect.identifier(), opt(as_alias(dialect))), |tup| {
            Table {
                name: String::from(tup.0),
                alias: tup.1.map(String::from),
                schema: None,
            }
        })(i)
    }
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
            .map(|t| type_identifier(Dialect::MySQL)(t.as_bytes()).unwrap().1)
            .collect();

        assert_eq!(
            res_ok,
            vec![
                SqlType::Bool,
                SqlType::Int(Some(16)),
                SqlType::DateTime(Some(16))
            ]
        );

        assert!(not_ok
            .iter()
            .map(|t| type_identifier(Dialect::MySQL)(t.as_bytes()).is_ok())
            .all(|r| !r));
    }

    #[test]
    fn group_concat() {
        let qs = b"group_concat(x separator ', ')";
        let expected = FunctionExpression::GroupConcat {
            expr: Box::new(Expression::Column(Column::from("x"))),
            separator: ", ".to_owned(),
        };
        let res = column_function(Dialect::MySQL)(qs);
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
            let res = column_function(Dialect::MySQL)(q);
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
        let res = test_parse!(column_function(Dialect::MySQL), b"max(min(foo))");
        assert_eq!(
            res,
            FunctionExpression::Max(Box::new(Expression::Call(FunctionExpression::Min(
                Box::new(Expression::Column("foo".into()))
            ))))
        )
    }

    #[test]
    fn nested_cast() {
        let res = test_parse!(column_function(Dialect::MySQL), b"max(cast(foo as int))");
        assert_eq!(
            res,
            FunctionExpression::Max(Box::new(Expression::Cast {
                expr: Box::new(Expression::Column("foo".into())),
                ty: SqlType::Int(None),
                postgres_style: false,
            }))
        )
    }

    #[test]
    fn generic_function_with_int_literal() {
        let (_, res) = column_function(Dialect::MySQL)(b"ifnull(x, 0)").unwrap();
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
    fn terminated_by_semicolon() {
        let res = statement_terminator(b"   ;  ");
        assert_eq!(res, Ok((&b""[..], ())));
    }

    #[test]
    fn float_formatting_strips_trailing_zeros() {
        let f = Literal::Double(Double {
            value: 1.5,
            precision: u8::MAX,
        });
        assert_eq!(f.to_string(), "1.5");
    }

    #[test]
    fn float_formatting_leaves_zero_after_dot() {
        let f = Literal::Double(Double {
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
            Literal::Double(Double { value, .. }) => {
                assert_approx_eq!(value, 1.5);
            }
            _ => unreachable!(),
        }
    }

    #[proptest]
    fn real_hash_matches_eq(real1: Double, real2: Double) {
        assert_eq!(real1 == real2, hash(&real1) == hash(&real2));
    }

    #[proptest]
    fn literal_to_string_parse_round_trip(lit: Literal) {
        prop_assume!(!matches!(
            lit,
            Literal::Double(_) | Literal::Float(_) | Literal::ByteArray(_) | Literal::Numeric(_, _)
        ));
        let s = lit.to_string();
        assert_eq!(literal(Dialect::MySQL)(s.as_bytes()).unwrap().1, lit)
    }

    #[test]
    fn json_type() {
        for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
            let res = type_identifier(dialect)(b"json");
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Json);
        }
    }

    mod mysql {
        use super::*;

        #[test]
        fn cast() {
            let qs = b"cast(`lp`.`start_ddtm` as date)";
            let expected = Expression::Cast {
                expr: Box::new(Expression::Column(Column {
                    table: Some("lp".to_owned()),
                    name: "start_ddtm".to_owned(),
                    function: None,
                })),
                ty: SqlType::Date,
                postgres_style: false,
            };
            let res = expression(Dialect::MySQL)(qs);
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
                let res = column_function(Dialect::MySQL)(q);
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
        fn double_with_lens() {
            let qs = b"double(16,12)";
            let res = type_identifier(Dialect::MySQL)(qs);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Double);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn cast() {
            let qs = b"cast(\"lp\".\"start_ddtm\" as date)";
            let expected = Expression::Cast {
                expr: Box::new(Expression::Column(Column {
                    table: Some("lp".to_owned()),
                    name: "start_ddtm".to_owned(),
                    function: None,
                })),
                ty: SqlType::Date,
                postgres_style: false,
            };
            let res = expression(Dialect::PostgreSQL)(qs);
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
                let res = column_function(Dialect::PostgreSQL)(q);
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
        fn numeric() {
            let qs = b"NUMERIC";
            let res = type_identifier(Dialect::PostgreSQL)(qs);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(None));
        }

        #[test]
        fn numeric_with_precision() {
            let qs = b"NUMERIC(10)";
            let res = type_identifier(Dialect::PostgreSQL)(qs);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, None))));
        }

        #[test]
        fn numeric_with_precision_and_scale() {
            let qs = b"NUMERIC(10, 20)";
            let res = type_identifier(Dialect::PostgreSQL)(qs);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, Some(20)))));
        }

        #[test]
        fn macaddr_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"macaddr");
            assert_eq!(res, SqlType::MacAddr);
        }

        #[test]
        fn uuid_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"uuid");
            assert_eq!(res, SqlType::Uuid);
        }

        #[test]
        fn json_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"json");
            assert_eq!(res, SqlType::Json);
        }

        #[test]
        fn jsonb_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"jsonb");
            assert_eq!(res, SqlType::Jsonb);
        }
    }
}
