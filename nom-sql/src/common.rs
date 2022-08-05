use std::fmt::{self, Display};
use std::hash::Hash;
use std::ops::{Range, RangeFrom, RangeTo};
use std::str::{self, FromStr, Utf8Error};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{digit1, line_ending};
use nom::combinator::{map, map_parser, map_res, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{fold_many0, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{IResult, InputLength};
use proptest::strategy::Strategy;
use proptest::{prelude as prop, prop_oneof};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::dialect::Dialect;
use crate::expression::expression;
use crate::table::Table;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{literal, Expr, FunctionExpr, Literal, NomSqlResult, Span, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub enum SqlType {
    Bool,
    Char(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Varchar(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
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
    TimestampTz,
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
    Inet,
    Uuid,
    Bit(Option<u16>),
    Varbit(Option<u16>),
    Serial,
    BigSerial,
    Array(Box<SqlType>),
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
            SqlType::Varchar(len) => write_with_len(f, "VARCHAR", len),
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
            SqlType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            SqlType::Binary(len) => write_with_len(f, "BINARY", len),
            SqlType::Varbinary(len) => write!(f, "VARBINARY({})", len),
            SqlType::Enum(ref variants) => write!(f, "ENUM({})", variants.iter().join(", ")),
            SqlType::Decimal(m, d) => write!(f, "DECIMAL({}, {})", m, d),
            SqlType::Json => write!(f, "JSON"),
            SqlType::Jsonb => write!(f, "JSONB"),
            SqlType::ByteArray => write!(f, "BYTEA"),
            SqlType::MacAddr => write!(f, "MACADDR"),
            SqlType::Inet => write!(f, "INET"),
            SqlType::Uuid => write!(f, "UUID"),
            SqlType::Bit(n) => {
                write!(f, "BIT")?;
                if let Some(size) = n {
                    write!(f, "({})", size)?;
                }
                Ok(())
            }
            SqlType::Varbit(n) => write_with_len(f, "VARBIT", n),
            SqlType::Serial => write!(f, "SERIAL"),
            SqlType::BigSerial => write!(f, "BIGSERIAL"),
            SqlType::Array(ref t) => write!(f, "{}[]", t),
        }
    }
}

impl FromStr for SqlType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        type_identifier(Dialect::MySQL)(Span::new(s.as_bytes()))
            .map(|(_, s)| s)
            .map_err(|_| "failed to parse")
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
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    UniqueKey {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    FulltextKey {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    Key {
        name: SqlIdentifier,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    ForeignKey {
        name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        target_table: Table,
        target_columns: Vec<Column>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    CheckConstraint {
        name: Option<SqlIdentifier>,
        expr: Expr,
        enforced: Option<bool>,
    },
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableKey::PrimaryKey { name, columns } => {
                write!(f, "PRIMARY KEY ")?;
                if let Some(name) = name {
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
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
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(index_type) = index_type {
                    write!(f, " USING {}", index_type)?;
                }
                Ok(())
            }
            TableKey::FulltextKey { name, columns } => {
                write!(f, "FULLTEXT KEY ")?;
                if let Some(ref name) = *name {
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::Key {
                name,
                columns,
                index_type,
            } => {
                write!(f, "KEY `{}` ", name)?;
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
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
                on_update,
            } => {
                write!(
                    f,
                    "CONSTRAINT `{}` FOREIGN KEY {}({}) REFERENCES {} ({})",
                    name.as_deref().unwrap_or(""),
                    index_name.as_deref().unwrap_or(""),
                    column.iter().map(|c| format!("`{}`", c.name)).join(", "),
                    target_table,
                    target_column
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .join(", ")
                )?;
                if let Some(on_delete) = on_delete {
                    write!(f, " ON DELETE {}", on_delete)?;
                }
                if let Some(on_update) = on_update {
                    write!(f, " ON UPDATE {}", on_update)?;
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
                    write!(f, " `{}`", name)?;
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // NOTE(grfn): do we actually care about this?
pub enum FieldDefinitionExpr {
    All,
    AllInTable(Table),
    Expr {
        expr: Expr,
        alias: Option<SqlIdentifier>,
    },
}

/// Constructs a [`FieldDefinitionExpr::Expr`] without an alias
impl From<Expr> for FieldDefinitionExpr {
    fn from(expr: Expr) -> Self {
        FieldDefinitionExpr::Expr { expr, alias: None }
    }
}

/// Constructs a [`FieldDefinitionExpr::Expr`] based on an [`Expr::Column`] for
/// the column and without an alias
impl From<Column> for FieldDefinitionExpr {
    fn from(col: Column) -> Self {
        FieldDefinitionExpr::Expr {
            expr: Expr::Column(col),
            alias: None,
        }
    }
}

impl From<Literal> for FieldDefinitionExpr {
    fn from(lit: Literal) -> Self {
        FieldDefinitionExpr::from(Expr::Literal(lit))
    }
}

impl Display for FieldDefinitionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldDefinitionExpr::All => write!(f, "*"),
            FieldDefinitionExpr::AllInTable(ref table) => {
                write!(f, "{}.*", table)
            }
            FieldDefinitionExpr::Expr { expr, alias } => {
                write!(f, "{}", expr)?;
                if let Some(alias) = alias {
                    write!(f, " AS `{}`", alias)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for FieldDefinitionExpr {
    fn default() -> FieldDefinitionExpr {
        FieldDefinitionExpr::All
    }
}

pub enum Sign {
    Unsigned,
    Signed,
}

/// A reference to a field in a query, usable in either the `GROUP BY` or `ORDER BY` clauses of the
/// query
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum FieldReference {
    /// A reference to a field in the `SELECT` list by its (1-based) index.
    Numeric(u64),
    /// An expression
    Expr(Expr),
}

impl Display for FieldReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldReference::Numeric(n) => write!(f, "{}", n),
            FieldReference::Expr(expr) => write!(f, "{}", expr),
        }
    }
}

fn digit_as_u16(len: Span) -> NomSqlResult<u16> {
    match str::from_utf8(*len) {
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

fn digit_as_u8(len: Span) -> NomSqlResult<u8> {
    match str::from_utf8(*len) {
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

pub fn str_from_utf8_span(i: Span) -> Result<&str, Utf8Error> {
    str::from_utf8(*i)
}

pub(crate) fn opt_delimited<I: Clone, O1, O2, O3, E: ParseError<I>, F, G, H>(
    mut first: F,
    mut second: G,
    mut third: H,
) -> impl FnMut(I) -> IResult<I, O2, E>
where
    F: FnMut(I) -> IResult<I, O1, E>,
    G: FnMut(I) -> IResult<I, O2, E>,
    H: FnMut(I) -> IResult<I, O3, E>,
{
    move |input: I| {
        let inp = input.clone();
        match second(input) {
            Ok((i, o)) => Ok((i, o)),
            _ => {
                let first_ = &mut first;
                let second_ = &mut second;
                let third_ = &mut third;
                delimited(first_, second_, third_)(inp)
            }
        }
    }
}

fn precision_helper(i: Span) -> NomSqlResult<(u8, Option<u8>)> {
    let (remaining_input, (m, d)) = tuple((
        digit1,
        opt(preceded(tag(","), preceded(whitespace0, digit1))),
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

pub fn precision(i: Span) -> NomSqlResult<(u8, Option<u8>)> {
    delimited(tag("("), precision_helper, tag(")"))(i)
}

pub fn numeric_precision(i: Span) -> NomSqlResult<(u16, Option<u8>)> {
    delimited(tag("("), numeric_precision_inner, tag(")"))(i)
}

pub fn numeric_precision_inner(i: Span) -> NomSqlResult<(u16, Option<u8>)> {
    let (remaining_input, (m, _, d)) = tuple((
        digit1,
        whitespace0,
        opt(preceded(tag(","), preceded(whitespace0, digit1))),
    ))(i)?;

    let m = digit_as_u16(m)?.1;
    d.map(digit_as_u8)
        .transpose()
        .map(|v| (remaining_input, (m, v.map(|digit| digit.1))))
}

fn opt_signed(i: Span) -> NomSqlResult<Option<Sign>> {
    opt(alt((
        map(tag_no_case("unsigned"), |_| Sign::Unsigned),
        map(tag_no_case("signed"), |_| Sign::Signed),
    )))(i)
}

fn delim_digit(i: Span) -> NomSqlResult<Span> {
    delimited(tag("("), digit1, tag(")"))(i)
}

fn delim_u16(i: Span) -> NomSqlResult<u16> {
    map_parser(delim_digit, digit_as_u16)(i)
}

fn int_type<'a, F, G>(
    tag: &str,
    mk_unsigned: F,
    mk_signed: G,
    i: Span<'a>,
) -> NomSqlResult<'a, SqlType>
where
    F: Fn(Option<u16>) -> SqlType + 'static,
    G: Fn(Option<u16>) -> SqlType + 'static,
{
    let (remaining_input, (_, len, _, signed)) =
        tuple((tag_no_case(tag), opt(delim_u16), whitespace0, opt_signed))(i)?;

    if let Some(Sign::Unsigned) = signed {
        Ok((remaining_input, mk_unsigned(len)))
    } else {
        Ok((remaining_input, mk_signed(len)))
    }
}

// TODO(malte): not strictly ok to treat DECIMAL and NUMERIC as identical; the
// former has "at least" M precision, the latter "exactly".
// See https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
fn decimal_or_numeric(i: Span) -> NomSqlResult<SqlType> {
    let (remaining_input, precision) = delimited(
        alt((tag_no_case("decimal"), tag_no_case("numeric"))),
        opt(precision),
        whitespace0,
    )(i)?;

    match precision {
        None => Ok((remaining_input, SqlType::Decimal(32, 0))),
        Some((m, None)) => Ok((remaining_input, SqlType::Decimal(m, 0))),
        Some((m, Some(d))) => Ok((remaining_input, SqlType::Decimal(m, d))),
    }
}

fn opt_without_time_zone(i: Span) -> NomSqlResult<()> {
    map(
        opt(preceded(
            whitespace1,
            tuple((
                tag_no_case("without"),
                whitespace1,
                tag_no_case("time"),
                whitespace1,
                tag_no_case("zone"),
            )),
        )),
        |_| (),
    )(i)
}

fn type_identifier_first_half(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<SqlType> {
    move |i| {
        alt((
            |i| int_type("tinyint", SqlType::UnsignedTinyint, SqlType::Tinyint, i),
            |i| int_type("smallint", SqlType::UnsignedSmallint, SqlType::Smallint, i),
            |i| int_type("integer", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("int", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("bigint", SqlType::UnsignedBigint, SqlType::Bigint, i),
            map(alt((tag_no_case("boolean"), tag_no_case("bool"))), |_| {
                SqlType::Bool
            }),
            map(preceded(tag_no_case("datetime"), opt(delim_u16)), |fsp| {
                SqlType::DateTime(fsp)
            }),
            map(tag_no_case("date"), |_| SqlType::Date),
            map(
                tuple((
                    tag_no_case("double"),
                    opt(preceded(whitespace1, tag_no_case("precision"))),
                    whitespace0,
                    opt(precision),
                    whitespace0,
                    opt_signed,
                )),
                |_| SqlType::Double,
            ),
            map(
                tuple((tag_no_case("numeric"), whitespace0, opt(numeric_precision))),
                |t| SqlType::Numeric(t.2),
            ),
            map(
                terminated(
                    preceded(
                        tag_no_case("enum"),
                        delimited(tag("("), value_list(dialect), tag(")")),
                    ),
                    whitespace0,
                ),
                SqlType::Enum,
            ),
            map(
                tuple((
                    tag_no_case("float"),
                    whitespace0,
                    opt(precision),
                    whitespace0,
                )),
                |_| SqlType::Float,
            ),
            map(
                tuple((tag_no_case("real"), whitespace0, opt_signed)),
                |_| SqlType::Real,
            ),
            map(tag_no_case("text"), |_| SqlType::Text),
            map(
                tuple((
                    tag_no_case("timestamp"),
                    opt(preceded(whitespace0, delim_digit)),
                    preceded(
                        whitespace1,
                        tuple((
                            tag_no_case("with"),
                            whitespace1,
                            tag_no_case("time"),
                            whitespace1,
                            tag_no_case("zone"),
                        )),
                    ),
                )),
                |_| SqlType::TimestampTz,
            ),
            map(
                tuple((
                    tag_no_case("timestamp"),
                    opt(preceded(whitespace0, delim_digit)),
                    opt_without_time_zone,
                )),
                |_| SqlType::Timestamp,
            ),
            map(
                tuple((
                    alt((
                        map(
                            tuple((
                                map(tag_no_case("varchar"), |x: Span| *x),
                                map(whitespace0, |_| "".as_bytes()),
                            )),
                            |_| (),
                        ),
                        map(
                            tuple((
                                map(tag_no_case("character"), |x: Span| *x),
                                map(whitespace1, |_| "".as_bytes()),
                                map(tag_no_case("varying"), |x: Span| *x),
                            )),
                            |_| (),
                        ),
                    )),
                    opt(delim_u16),
                    whitespace0,
                    opt(tag_no_case("binary")),
                )),
                |t| SqlType::Varchar(t.1),
            ),
            map(
                tuple((
                    tag_no_case("char"),
                    opt(delim_u16),
                    whitespace0,
                    opt(tag_no_case("binary")),
                )),
                |t| SqlType::Char(t.1),
            ),
        ))(i)
    }
}

fn type_identifier_second_half(i: Span) -> NomSqlResult<SqlType> {
    alt((
        map(
            terminated(tag_no_case("time"), opt_without_time_zone),
            |_| SqlType::Time,
        ),
        decimal_or_numeric,
        map(
            tuple((tag_no_case("binary"), opt(delim_u16), whitespace0)),
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
            tuple((tag_no_case("varbinary"), delim_u16, whitespace0)),
            |t| SqlType::Varbinary(t.1),
        ),
        map(tag_no_case("bytea"), |_| SqlType::ByteArray),
        map(tag_no_case("macaddr"), |_| SqlType::MacAddr),
        map(tag_no_case("inet"), |_| SqlType::Inet),
        map(tag_no_case("uuid"), |_| SqlType::Uuid),
        map(tag_no_case("jsonb"), |_| SqlType::Jsonb),
        map(tag_no_case("json"), |_| SqlType::Json),
        map(
            tuple((
                alt((
                    // The alt expects the same type to be returned for both entries,
                    // so both have to be tuples with same number of elements
                    map(tuple((tag_no_case("varbit"), whitespace0)), |_| ()),
                    map(
                        tuple((
                            tag_no_case("bit"),
                            whitespace1,
                            tag_no_case("varying"),
                            whitespace0,
                        )),
                        |_| (),
                    ),
                )),
                opt(delim_u16),
            )),
            |t| SqlType::Varbit(t.1),
        ),
        map(tuple((tag_no_case("bit"), opt(delim_u16))), |t| {
            SqlType::Bit(t.1)
        }),
        map(tag_no_case("serial"), |_| SqlType::Serial),
        map(tag_no_case("bigserial"), |_| SqlType::BigSerial),
    ))(i)
}

fn array_suffix(i: Span) -> NomSqlResult<()> {
    let (i, _) = tag("[")(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _len) = opt(digit1)(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _) = tag("]")(i)?;
    Ok((i, ()))
}

fn type_identifier_no_arrays(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<SqlType> {
    move |i| {
        alt((
            type_identifier_first_half(dialect),
            type_identifier_second_half,
        ))(i)
    }
}

// A SQL type specifier.
pub fn type_identifier(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<SqlType> {
    move |i| {
        // need to pull a bit of a trick here to properly recursive-descent arrays since they're a
        // suffix. First, we parse the type, then we parse any number of `[]` or `[<n>]` suffixes,
        // and use those to construct the multiple levels of `SqlType::Array`
        let (i, ty) = type_identifier_no_arrays(dialect)(i)?;
        fold_many0(
            array_suffix,
            move || ty.clone(),
            |t, _| SqlType::Array(Box::new(t)),
        )(i)
    }
}

// Parses the arguments for an aggregation function, and also returns whether the distinct flag is
// present.
pub fn agg_function_arguments(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<(Expr, bool)> {
    move |i| {
        let distinct_parser = opt(tuple((tag_no_case("distinct"), whitespace1)));
        let (remaining_input, (distinct, args)) = tuple((distinct_parser, expression(dialect)))(i)?;
        Ok((remaining_input, (args, distinct.is_some())))
    }
}

fn group_concat_fx_helper(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<String> {
    move |i| {
        let ws_sep = delimited(whitespace0, tag_no_case("separator"), whitespace0);
        let (i, sep) = delimited(
            ws_sep,
            opt(map_res(
                move |i| dialect.string_literal()(i),
                String::from_utf8,
            )),
            whitespace0,
        )(i)?;

        Ok((i, sep.unwrap_or_default()))
    }
}

fn group_concat_fx(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<(Column, Option<String>)> {
    move |i| {
        pair(
            column_identifier_no_alias(dialect),
            opt(group_concat_fx_helper(dialect)),
        )(i)
    }
}

fn agg_fx_args(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<(Expr, bool)> {
    move |i| delimited(tag("("), agg_function_arguments(dialect), tag(")"))(i)
}

fn delim_fx_args(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<Vec<Expr>> {
    move |i| {
        delimited(
            tag("("),
            separated_list0(
                tag(","),
                delimited(whitespace0, expression(dialect), whitespace0),
            ),
            tag(")"),
        )(i)
    }
}

pub fn column_function(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<FunctionExpr> {
    move |i| {
        alt((
            map(tag_no_case("count(*)"), |_| FunctionExpr::CountStar),
            map(
                preceded(tag_no_case("count"), agg_fx_args(dialect)),
                |args| FunctionExpr::Count {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                    count_nulls: false,
                },
            ),
            map(preceded(tag_no_case("sum"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Sum {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("avg"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Avg {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("max"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Max(Box::new(args.0))
            }),
            map(preceded(tag_no_case("min"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Min(Box::new(args.0))
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
                    FunctionExpr::GroupConcat {
                        expr: Box::new(Expr::Column(col.clone())),
                        separator,
                    }
                },
            ),
            map(
                tuple((
                    dialect.function_identifier(),
                    whitespace0,
                    delim_fx_args(dialect),
                )),
                |(name, _, arguments)| FunctionExpr::Call {
                    name: name.to_string(),
                    arguments,
                },
            ),
        ))(i)
    }
}

// Parses a SQL column identifier in the db/schema.table.column format
pub fn column_identifier_no_alias(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<Column> {
    move |i| {
        let (i, id1) = opt(terminated(
            dialect.identifier(),
            delimited(whitespace0, tag("."), whitespace0),
        ))(i)?;
        let (i, id2) = opt(terminated(
            dialect.identifier(),
            delimited(whitespace0, tag("."), whitespace0),
        ))(i)?;
        // Do we have a 'db/schema.table.' or 'table.' qualifier?
        let table = match (id1, id2) {
            (Some(db), Some(t)) => Some(Table {
                schema: Some(db),
                name: t,
            }),
            // (None, Some(t)) should be unreachable
            (Some(t), None) | (None, Some(t)) => Some(Table::from(t)),
            (None, None) => None,
        };
        let (i, name) = dialect.identifier()(i)?;
        Ok((i, Column { name, table }))
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
pub fn statement_terminator(i: Span) -> NomSqlResult<()> {
    let (remaining_input, _) =
        delimited(whitespace0, alt((tag(";"), line_ending, eof)), whitespace0)(i)?;

    Ok((remaining_input, ()))
}

/// Parser combinator that applies the given parser,
/// and then tries to match for a statement terminator.
pub fn terminated_with_statement_terminator<F, O>(parser: F) -> impl FnOnce(Span) -> NomSqlResult<O>
where
    F: Fn(Span) -> NomSqlResult<O>,
{
    move |i| terminated(parser, statement_terminator)(i)
}

// Parse rule for AS-based aliases for SQL entities.
pub fn as_alias(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<SqlIdentifier> {
    move |i| {
        map(
            tuple((
                whitespace1,
                opt(pair(tag_no_case("as"), whitespace1)),
                dialect.identifier(),
            )),
            |a| a.2,
        )(i)
    }
}

fn assignment_expr(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<(Column, Expr)> {
    move |i| {
        separated_pair(
            column_identifier_no_alias(dialect),
            delimited(whitespace0, tag("="), whitespace0),
            expression(dialect),
        )(i)
    }
}

/// Whitespace surrounded optionally on either side by a comma
pub(crate) fn ws_sep_comma(i: Span) -> NomSqlResult<()> {
    map_res(delimited(whitespace0, tag(","), whitespace0), |_| Ok(()))(i)
}

pub(crate) fn ws_sep_equals(i: Span) -> NomSqlResult<()> {
    map_res(delimited(whitespace0, tag("="), whitespace0), |_| Ok(()))(i)
}

pub fn assignment_expr_list(
    dialect: Dialect,
) -> impl Fn(Span) -> NomSqlResult<Vec<(Column, Expr)>> {
    move |i| separated_list1(ws_sep_comma, assignment_expr(dialect))(i)
}

// Parse rule for a comma-separated list of fields without aliases.
pub fn field_list(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<Vec<Column>> {
    move |i| separated_list0(ws_sep_comma, column_identifier_no_alias(dialect))(i)
}

fn expression_field(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<FieldDefinitionExpr> {
    move |i| {
        let (i, expr) = expression(dialect)(i)?;
        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, FieldDefinitionExpr::Expr { expr, alias }))
    }
}

fn all_in_table(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<FieldDefinitionExpr> {
    move |i| {
        let (i, ident1) = terminated(dialect.identifier(), tag("."))(i)?;
        let (i, ident2) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, _) = tag("*")(i)?;

        let table = match ident2 {
            Some(name) => Table {
                schema: Some(ident1),
                name,
            },
            None => Table {
                schema: None,
                name: ident1,
            },
        };

        Ok((i, FieldDefinitionExpr::AllInTable(table)))
    }
}

// Parse list of column/field definitions.
pub fn field_definition_expr(
    dialect: Dialect,
) -> impl Fn(Span) -> NomSqlResult<Vec<FieldDefinitionExpr>> {
    move |i| {
        terminated(
            separated_list0(
                ws_sep_comma,
                alt((
                    map(tag("*"), |_| FieldDefinitionExpr::All),
                    all_in_table(dialect),
                    expression_field(dialect),
                )),
            ),
            opt(ws_sep_comma),
        )(i)
    }
}

// Parse a list of values (e.g., for INSERT syntax).
pub fn value_list(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<Vec<Literal>> {
    move |i| separated_list0(ws_sep_comma, literal(dialect))(i)
}

pub(crate) fn if_not_exists(i: Span) -> NomSqlResult<bool> {
    let (i, s) = opt(move |i| {
        let (i, _) = tag_no_case("if")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("not")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("exists")(i)?;
        let (i, _) = whitespace1(i)?;

        Ok((i, ()))
    })(i)?;

    Ok((i, s.is_some()))
}

// Parse rule for a comment part.
pub fn parse_comment(i: Span) -> NomSqlResult<String> {
    map(
        preceded(
            delimited(whitespace0, tag_no_case("comment"), whitespace1),
            map_res(
                map(delimited(tag("'"), take_until("'"), tag("'")), |x| *x),
                str::from_utf8,
            ),
        ),
        String::from,
    )(i)
}

pub fn field_reference(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<FieldReference> {
    move |i| {
        match dialect {
            Dialect::PostgreSQL => map(expression(dialect), FieldReference::Expr)(i),
            // Only MySQL supports numeric field references (postgresql considers them integer
            // literals, I'm pretty sure)
            Dialect::MySQL => alt((
                map(
                    map_res(
                        map_res(map(digit1, |x: Span| *x), str::from_utf8),
                        u64::from_str,
                    ),
                    FieldReference::Numeric,
                ),
                map(expression(dialect), FieldReference::Expr),
            ))(i),
        }
    }
}

pub fn field_reference_list(
    dialect: Dialect,
) -> impl Fn(Span) -> NomSqlResult<Vec<FieldReference>> {
    move |i| separated_list0(ws_sep_comma, field_reference(dialect))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_opt_delimited_fn_call(i: &str) -> IResult<&[u8], &[u8]> {
        opt_delimited(tag("("), tag("abc"), tag(")"))(i.as_bytes())
    }

    #[test]
    fn qualified_column_with_spaces() {
        let res = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"foo . bar");
        assert_eq!(
            res,
            Column {
                table: Some("foo".into()),
                name: "bar".into(),
            }
        )
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
    }

    #[test]
    fn boolean_bool() {
        let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"boolean");
        assert_eq!(res, SqlType::Bool);
    }

    #[test]
    fn group_concat() {
        let qs = b"group_concat(x separator ', ')";
        let expected = FunctionExpr::GroupConcat {
            expr: Box::new(Expr::Column(Column::from("x"))),
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
            let expected = FunctionExpr::Call {
                name: "coalesce".to_string(),
                arguments: vec![
                    Expr::Column(Column::from("a")),
                    Expr::Column(Column::from("b")),
                    Expr::Column(Column::from("c")),
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
            FunctionExpr::Max(Box::new(Expr::Call(FunctionExpr::Min(Box::new(
                Expr::Column("foo".into())
            )))))
        )
    }

    #[test]
    fn nested_cast() {
        let res = test_parse!(column_function(Dialect::MySQL), b"max(cast(foo as int))");
        assert_eq!(
            res,
            FunctionExpr::Max(Box::new(Expr::Cast {
                expr: Box::new(Expr::Column("foo".into())),
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
            FunctionExpr::Call {
                name: "ifnull".to_owned(),
                arguments: vec![
                    Expr::Column(Column::from("x")),
                    Expr::Literal(Literal::Integer(0))
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
            let expected = Expr::Cast {
                expr: Box::new(Expr::Column(Column {
                    table: Some("lp".into()),
                    name: "start_ddtm".into(),
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
                let expected = FunctionExpr::Call {
                    name: "coalesce".to_string(),
                    arguments: vec![
                        Expr::Literal(Literal::String("a".to_owned())),
                        Expr::Column(Column::from("b")),
                        Expr::Column(Column::from("c")),
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

        #[test]
        fn table_qualifier() {
            let res1 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"`t`.`c`");
            let res2 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"t.c");
            let expected = Column {
                name: "c".into(),
                table: Some("t".into()),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }

        #[test]
        fn db_table_qualifier() {
            let res1 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"`db`.`t`.`c`");
            let res2 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"db.t.c");
            let expected = Column {
                name: "c".into(),
                table: Some(Table {
                    schema: Some("db".into()),
                    name: "t".into(),
                }),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn cast() {
            let qs = b"cast(\"lp\".\"start_ddtm\" as date)";
            let expected = Expr::Cast {
                expr: Box::new(Expr::Column(Column {
                    table: Some("lp".into()),
                    name: "start_ddtm".into(),
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
                let expected = FunctionExpr::Call {
                    name: "coalesce".to_string(),
                    arguments: vec![
                        Expr::Literal(Literal::String("a".to_owned())),
                        Expr::Column(Column::from("b")),
                        Expr::Column(Column::from("c")),
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
        fn inet_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"inet");
            assert_eq!(res, SqlType::Inet);
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

        #[test]
        fn bit_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit");
            assert_eq!(res, SqlType::Bit(None));
        }

        #[test]
        fn bit_with_size_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit(10)");
            assert_eq!(res, SqlType::Bit(Some(10)));
        }

        #[test]
        fn bit_varying_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit varying");
            assert_eq!(res, SqlType::Varbit(None));
        }

        #[test]
        fn bit_varying_with_size_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit varying(10)");
            assert_eq!(res, SqlType::Varbit(Some(10)));
        }

        #[test]
        fn timestamp_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamp");
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_with_prec_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamp (5)");
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_without_timezone_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp without time zone"
            );
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_with_prec_without_timezone_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp (5)   without time zone"
            );
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_tz_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp with time zone"
            );
            assert_eq!(res, SqlType::TimestampTz);
        }

        #[test]
        fn timestamp_tz_with_prec_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp (5)    with time zone"
            );
            assert_eq!(res, SqlType::TimestampTz);
        }

        #[test]
        fn serial_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"serial");
            assert_eq!(res, SqlType::Serial);
        }

        #[test]
        fn bigserial_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bigserial");
            assert_eq!(res, SqlType::BigSerial);
        }

        #[test]
        fn varchar_without_length() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"varchar");
            assert_eq!(res, SqlType::Varchar(None));
        }

        #[test]
        fn character_varying() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"character varying");
            assert_eq!(res, SqlType::Varchar(None));
        }

        #[test]
        fn character_varying_with_length() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"character varying(20)"
            );
            assert_eq!(res, SqlType::Varchar(Some(20)));
        }

        #[test]
        fn time_without_time_zone() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"time without time zone"
            );
            assert_eq!(res, SqlType::Time);
        }

        #[test]
        fn int_array() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"int[]");
            assert_eq!(res, SqlType::Array(Box::new(SqlType::Int(None))));
        }

        #[test]
        fn double_nested_array() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"text[][]");
            assert_eq!(
                res,
                SqlType::Array(Box::new(SqlType::Array(Box::new(SqlType::Text))))
            );
        }

        #[test]
        fn arrays_with_length() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"float[4][5]");
            assert_eq!(
                res,
                SqlType::Array(Box::new(SqlType::Array(Box::new(SqlType::Float))))
            );
        }

        #[test]
        fn table_qualifier() {
            let res1 = test_parse!(
                column_identifier_no_alias(Dialect::PostgreSQL),
                b"\"t\".\"c\""
            );
            let res2 = test_parse!(column_identifier_no_alias(Dialect::PostgreSQL), b"t.c");
            let expected = Column {
                name: "c".into(),
                table: Some("t".into()),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }

        #[test]
        fn db_table_qualifier() {
            let res1 = test_parse!(
                column_identifier_no_alias(Dialect::PostgreSQL),
                b"\"db\".\"t\".\"c\""
            );
            let res2 = test_parse!(column_identifier_no_alias(Dialect::PostgreSQL), b"db.t.c");
            let expected = Column {
                name: "c".into(),
                table: Some(Table {
                    schema: Some("db".into()),
                    name: "t".into(),
                }),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }
    }
}
