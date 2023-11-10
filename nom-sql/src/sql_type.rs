// TODO(jasobrown) double check this
#![allow(clippy::non_canonical_partial_ord_impl)]

use std::ops::Deref;
use std::str::FromStr;
use std::{fmt, str};

use failpoint_macros::set_failpoint;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::digit1;
#[cfg(feature = "failure_injection")]
use nom::combinator::fail;
use nom::combinator::{map, map_parser, opt, value};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{fold_many0, separated_list0};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use proptest::arbitrary::Arbitrary;
use proptest::prelude::any_with;
use proptest::sample::SizeRange;
use proptest::strategy::{BoxedStrategy, Strategy};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use triomphe::ThinArc;

use crate::common::{ws_sep_comma, Sign};
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, DialectDisplay, NomSqlResult, Relation};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IntervalFields {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
}

impl fmt::Display for IntervalFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntervalFields::Year => write!(f, "YEAR"),
            IntervalFields::Month => write!(f, "MONTH"),
            IntervalFields::Day => write!(f, "DAY"),
            IntervalFields::Hour => write!(f, "HOUR"),
            IntervalFields::Minute => write!(f, "MINUTE"),
            IntervalFields::Second => write!(f, "SECOND"),
            IntervalFields::YearToMonth => write!(f, "YEAR TO MONTH"),
            IntervalFields::DayToHour => write!(f, "DAY TO HOUR"),
            IntervalFields::DayToMinute => write!(f, "DAY TO MINUTE"),
            IntervalFields::DayToSecond => write!(f, "DAY TO SECOND"),
            IntervalFields::HourToMinute => write!(f, "HOUR TO MINUTE"),
            IntervalFields::HourToSecond => write!(f, "HOUR TO SECOND"),
            IntervalFields::MinuteToSecond => write!(f, "MINUTE TO SECOND"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Char(Option<u16>),
    VarChar(Option<u16>),
    Int(Option<u16>),
    UnsignedInt(Option<u16>),
    BigInt(Option<u16>),
    UnsignedBigInt(Option<u16>),
    TinyInt(Option<u16>),
    UnsignedTinyInt(Option<u16>),
    SmallInt(Option<u16>),
    UnsignedSmallInt(Option<u16>),
    Int2,
    Int4,
    Int8,
    Blob,
    LongBlob,
    MediumBlob,
    TinyBlob,
    Double,
    Float,
    Real,
    Numeric(Option<(u16, Option<u8>)>),
    TinyText,
    MediumText,
    LongText,
    Text,
    Citext,
    QuotedChar,
    Date,
    DateTime(Option<u16>),
    // FIXME(ENG-1832): Parse subsecond digit count.
    Time,
    Timestamp,
    TimestampTz,
    Interval {
        fields: Option<IntervalFields>,
        precision: Option<u16>,
    },
    Binary(Option<u16>),
    VarBinary(u16),
    Enum(EnumVariants),
    Decimal(u8, u8),
    Json,
    Jsonb,
    ByteArray,
    MacAddr,
    Inet,
    Uuid,
    Bit(Option<u16>),
    VarBit(Option<u16>),
    Serial,
    BigSerial,
    Array(Box<SqlType>),

    /// Any other named type
    Other(Relation),
}

/// Options for generating arbitrary [`SqlType`]s
#[derive(Debug, Clone, Copy)]
pub struct SqlTypeArbitraryOptions {
    /// Enable generation of [`SqlType::Array`]. Defaults to `true`
    pub generate_arrays: bool,

    /// Enable generation of [`SqlType::Other`]. Defaults to `false`
    pub generate_other: bool,

    /// Enable generation of [`SqlType::Json`] and [`SqlType::Jsonb`]. Defaults to `true`
    pub generate_json: bool,

    /// Constrain types to only those which are valid for this SQL dialect
    pub dialect: Option<Dialect>,
}

impl Default for SqlTypeArbitraryOptions {
    fn default() -> Self {
        Self {
            generate_arrays: true,
            generate_other: false,
            generate_json: true,
            dialect: None,
        }
    }
}

impl Arbitrary for SqlType {
    type Parameters = SqlTypeArbitraryOptions;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        use proptest::option;
        use proptest::prelude::*;
        use SqlType::*;

        let mut variants = vec![
            Just(Bool).boxed(),
            option::of(1..255u16).prop_map(Char).boxed(),
            (1..255u16).prop_map(|l| VarChar(Some(l))).boxed(),
            Just(Int(None)).boxed(),
            Just(BigInt(None)).boxed(),
            Just(SmallInt(None)).boxed(),
            Just(Double).boxed(),
            Just(Float).boxed(),
            Just(Real).boxed(),
            option::of((1..=65u16).prop_flat_map(|n| {
                (
                    Just(n),
                    if n > 28 {
                        Just(None).boxed()
                    } else {
                        option::of(0..=(n as u8)).boxed()
                    },
                )
            }))
            .prop_map(Numeric)
            .boxed(),
            Just(Text).boxed(),
            Just(Date).boxed(),
            Just(Time).boxed(),
            Just(Timestamp).boxed(),
            (1..=28u8)
                .prop_flat_map(|prec| (1..=prec).prop_map(move |scale| Decimal(prec, scale)))
                .boxed(),
        ];

        if args.generate_json {
            variants.push(Just(Json).boxed());
        }

        if args.dialect.is_none() || args.dialect == Some(Dialect::PostgreSQL) {
            variants.extend([
                Just(Int2).boxed(),
                Just(Int4).boxed(),
                Just(Int8).boxed(),
                Just(VarChar(None)).boxed(),
                Just(ByteArray).boxed(),
                Just(MacAddr).boxed(),
                Just(Inet).boxed(),
                Just(Uuid).boxed(),
                any::<Option<u16>>().prop_map(Bit).boxed(),
                any::<Option<u16>>().prop_map(VarBit).boxed(),
                Just(Serial).boxed(),
                Just(BigSerial).boxed(),
                Just(TimestampTz).boxed(),
                Just(Citext).boxed(),
                Just(QuotedChar).boxed(),
            ]);

            if args.generate_json {
                variants.push(Just(Jsonb).boxed());
            }
        }

        if args.dialect.is_none() || args.dialect == Some(Dialect::MySQL) {
            variants.extend([
                (1..255u16).prop_map(|p| Int(Some(p))).boxed(),
                (1..255u16).prop_map(|p| BigInt(Some(p))).boxed(),
                (1..255u16).prop_map(|p| SmallInt(Some(p))).boxed(),
                option::of(1..255u16).prop_map(TinyInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedSmallInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedBigInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedTinyInt).boxed(),
                Just(TinyText).boxed(),
                Just(MediumText).boxed(),
                Just(LongText).boxed(),
                option::of(1..=6u16).prop_map(DateTime).boxed(),
            ]);
        }

        if args.generate_arrays {
            variants.push(
                any_with::<Box<SqlType>>(SqlTypeArbitraryOptions {
                    generate_arrays: false,
                    ..args
                })
                .prop_map(Array)
                .boxed(),
            );
        }

        if args.generate_other {
            variants.push(any::<Relation>().prop_map(Other).boxed())
        }

        proptest::sample::select(variants)
            .prop_flat_map(|strat| strat)
            .boxed()
    }
}

impl SqlType {
    /// Creates a [`SqlType::Enum`] instance from a sequence of variant names.
    #[inline]
    pub fn from_enum_variants<I>(variants: I) -> Self
    where
        I: IntoIterator<Item = String>,
        I::IntoIter: ExactSizeIterator, // required by `triomphe::ThinArc`
    {
        Self::Enum(variants.into())
    }

    /// Returns whether `self` is any text-containing type.
    #[inline]
    pub fn is_any_text(&self) -> bool {
        use SqlType::*;
        matches!(
            self,
            Text | TinyText | MediumText | LongText | Char(_) | VarChar(_)
        )
    }

    /// Returns the deepest nested type in [`SqlType::Array`], otherwise returns `self`.
    #[inline]
    pub fn innermost_array_type(&self) -> &Self {
        let mut current = self;
        while let Self::Array(ty) = current {
            current = ty;
        }
        current
    }
}

/// Test helpers.
impl SqlType {
    /// Nests this type into an array with the given dimension count.
    pub fn nest_in_array(mut self, dimen: usize) -> Self {
        for _ in 0..dimen {
            self = Self::Array(Box::new(self));
        }
        self
    }
}

impl DialectDisplay for SqlType {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
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
                SqlType::VarChar(len) => write_with_len(f, "VARCHAR", len),
                SqlType::Int(len) => write_with_len(f, "INT", len),
                SqlType::UnsignedInt(len) => {
                    write_with_len(f, "INT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::BigInt(len) => write_with_len(f, "BIGINT", len),
                SqlType::UnsignedBigInt(len) => {
                    write_with_len(f, "BIGINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::TinyInt(len) => write_with_len(f, "TINYINT", len),
                SqlType::UnsignedTinyInt(len) => {
                    write_with_len(f, "TINYINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::SmallInt(len) => write_with_len(f, "SMALLINT", len),
                SqlType::UnsignedSmallInt(len) => {
                    write_with_len(f, "SMALLINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::Int2 => write!(f, "INT2"),
                SqlType::Int4 => write!(f, "INT4"),
                SqlType::Int8 => write!(f, "INT8"),
                SqlType::Blob => write!(f, "BLOB"),
                SqlType::LongBlob => write!(f, "LONGBLOB"),
                SqlType::MediumBlob => write!(f, "MEDIUMBLOB"),
                SqlType::TinyBlob => write!(f, "TINYBLOB"),
                SqlType::Double => match dialect {
                    Dialect::PostgreSQL => write!(f, "DOUBLE PRECISION"),
                    Dialect::MySQL => write!(f, "DOUBLE"),
                },
                SqlType::Float => write!(f, "FLOAT"),
                SqlType::Real => write!(f, "REAL"),
                SqlType::Numeric(precision) => match precision {
                    Some((prec, Some(scale))) => write!(f, "NUMERIC({}, {})", prec, scale),
                    Some((prec, _)) => write!(f, "NUMERIC({})", prec),
                    _ => write!(f, "NUMERIC"),
                },
                SqlType::TinyText => write!(f, "TINYTEXT"),
                SqlType::MediumText => write!(f, "MEDIUMTEXT"),
                SqlType::LongText => write!(f, "LONGTEXT"),
                SqlType::Text => write!(f, "TEXT"),
                SqlType::Citext => write!(f, "CITEXT"),
                SqlType::QuotedChar => write!(f, "\"char\""),
                SqlType::Date => write!(f, "DATE"),
                SqlType::DateTime(subsecond_digits) => {
                    write_with_len(f, "DATETIME", subsecond_digits)
                }
                SqlType::Time => write!(f, "TIME"),
                SqlType::Timestamp => write!(f, "TIMESTAMP"),
                SqlType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
                SqlType::Interval {
                    ref fields,
                    ref precision,
                } => {
                    write!(f, "INTERVAL")?;

                    if let Some(fields) = fields {
                        write!(f, " {fields}")?;
                    }

                    if let Some(precision) = precision {
                        write!(f, " ({precision})")?;
                    }

                    Ok(())
                }
                SqlType::Binary(len) => write_with_len(f, "BINARY", len),
                SqlType::VarBinary(len) => write!(f, "VARBINARY({})", len),
                SqlType::Enum(ref variants) => {
                    write!(f, "ENUM(")?;
                    for (i, variant) in variants.iter().enumerate() {
                        if i != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "'{}'", variant.replace('\'', "''").replace('\\', "\\\\"))?;
                    }
                    write!(f, ")")
                }
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
                SqlType::VarBit(n) => write_with_len(f, "VARBIT", n),
                SqlType::Serial => write!(f, "SERIAL"),
                SqlType::BigSerial => write!(f, "BIGSERIAL"),
                SqlType::Array(ref t) => write!(f, "{}[]", t.display(dialect)),
                SqlType::Other(ref t) => write!(f, "{}", t.display(dialect)),
            }
        })
    }
}

impl FromStr for SqlType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        type_identifier(Dialect::MySQL)(LocatedSpan::new(s.as_bytes()))
            .map(|(_, s)| s)
            .map_err(|_| "failed to parse")
    }
}

/// [`SqlType::Enum`](crate::SqlType::Enum) abstraction over an array of [`String`].
///
/// Clones are O(1) and this is always 1 pointer wide for efficient storage in `SqlType`.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct EnumVariants {
    variants: ThinArc<(), String>,
}

impl Deref for EnumVariants {
    type Target = [String];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.variants.slice
    }
}

// We cannot use `FromIterator` because it does not allow specializing the iterator type.
//
// This impl works for `Vec` and `[T; N]`. It will never conflict with `From<EnumType>` because we
// cannot implement an owned iterator over `triomphe::ThinArc`.
impl<I: IntoIterator<Item = String>> From<I> for EnumVariants
where
    I::IntoIter: ExactSizeIterator,
{
    #[inline]
    fn from(variants: I) -> Self {
        Self {
            variants: ThinArc::from_header_and_iter((), variants.into_iter()),
        }
    }
}

impl fmt::Debug for EnumVariants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialOrd for EnumVariants {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        <[String]>::partial_cmp(self, other)
    }
}

impl Ord for EnumVariants {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        <[String]>::cmp(self, other)
    }
}

impl<'de> Deserialize<'de> for EnumVariants {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialize into a temporary `Vec` since we cannot construct `ThinArc` from an iterator
        // here.
        let result = Vec::<String>::deserialize(deserializer)?;

        Ok(result.into())
    }
}

impl Serialize for EnumVariants {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <[String]>::serialize(self, serializer)
    }
}

impl Arbitrary for EnumVariants {
    type Parameters = (&'static str, SizeRange);

    fn arbitrary_with((regex, len): Self::Parameters) -> Self::Strategy {
        // NOTE: We are required to provide a regex for Arbitrary, otherwise we would just get empty
        // strings.
        // Using hashset to ensure that all variants are unique.
        proptest::collection::hash_set(any_with::<String>(regex.into()), len)
            .prop_map(EnumVariants::from)
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

fn digit_as_u16(len: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u16> {
    match str::from_utf8(&len) {
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

fn digit_as_u8(len: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u8> {
    match str::from_utf8(&len) {
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

fn precision_helper(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u8, Option<u8>)> {
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

pub fn precision(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u8, Option<u8>)> {
    delimited(tag("("), precision_helper, tag(")"))(i)
}

pub fn numeric_precision(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u16, Option<u8>)> {
    delimited(tag("("), numeric_precision_inner, tag(")"))(i)
}

pub fn numeric_precision_inner(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u16, Option<u8>)> {
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

fn opt_signed(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<Sign>> {
    opt(alt((
        map(tag_no_case("unsigned"), |_| Sign::Unsigned),
        map(tag_no_case("signed"), |_| Sign::Signed),
    )))(i)
}

fn delim_digit(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LocatedSpan<&[u8]>> {
    delimited(tag("("), digit1, tag(")"))(i)
}

fn delim_u16(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u16> {
    map_parser(delim_digit, digit_as_u16)(i)
}

fn int_type<'a, F, G>(
    tag: &str,
    mk_unsigned: F,
    mk_signed: G,
    i: LocatedSpan<&'a [u8]>,
) -> NomSqlResult<&'a [u8], SqlType>
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
fn decimal_or_numeric(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    let (i, _) = alt((tag_no_case("decimal"), tag_no_case("numeric")))(i)?;
    let (i, precision) = opt(precision)(i)?;
    let (i, _) = whitespace0(i)?;
    // Parse and drop sign
    let (i, _) = opt_signed(i)?;

    match precision {
        None => Ok((i, SqlType::Decimal(32, 0))),
        Some((m, None)) => Ok((i, SqlType::Decimal(m, 0))),
        Some((m, Some(d))) => Ok((i, SqlType::Decimal(m, d))),
    }
}

fn opt_without_time_zone(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
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

fn enum_type(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        let (i, _) = tag_no_case("enum")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, variants) = separated_list0(ws_sep_comma, dialect.utf8_string_literal())(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;
        Ok((i, SqlType::from_enum_variants(variants)))
    }
}

fn interval_fields(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], IntervalFields> {
    alt((
        value(
            IntervalFields::YearToMonth,
            tuple((
                tag_no_case("YEAR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MONTH"),
            )),
        ),
        value(
            IntervalFields::DayToHour,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("HOUR"),
            )),
        ),
        value(
            IntervalFields::DayToMinute,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MINUTE"),
            )),
        ),
        value(
            IntervalFields::DayToSecond,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(
            IntervalFields::HourToMinute,
            tuple((
                tag_no_case("HOUR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MINUTE"),
            )),
        ),
        value(
            IntervalFields::HourToSecond,
            tuple((
                tag_no_case("HOUR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(
            IntervalFields::MinuteToSecond,
            tuple((
                tag_no_case("MINUTE"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(IntervalFields::Year, tag_no_case("YEAR")),
        value(IntervalFields::Month, tag_no_case("MONTH")),
        value(IntervalFields::Day, tag_no_case("DAY")),
        value(IntervalFields::Hour, tag_no_case("HOUR")),
        value(IntervalFields::Minute, tag_no_case("MINUTE")),
        value(IntervalFields::Second, tag_no_case("SECOND")),
    ))(i)
}

fn interval_type(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    let (i, _) = tag_no_case("interval")(i)?;
    let (i, fields) = opt(preceded(whitespace1, interval_fields))(i)?;
    let (i, precision) = opt(preceded(whitespace0, delim_u16))(i)?;

    Ok((i, SqlType::Interval { fields, precision }))
}

// `alt` has an upper limit on the number of items it supports in tuples, so we have to split out
// the parsing for types into 3 separate functions
// (see https://github.com/Geal/nom/pull/1556)
fn type_identifier_part1(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            interval_type,
            value(SqlType::Int2, tag_no_case("int2")),
            value(SqlType::Int4, tag_no_case("int4")),
            value(SqlType::Int8, tag_no_case("int8")),
            |i| int_type("tinyint", SqlType::UnsignedTinyInt, SqlType::TinyInt, i),
            |i| int_type("smallint", SqlType::UnsignedSmallInt, SqlType::SmallInt, i),
            |i| int_type("integer", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("int", SqlType::UnsignedInt, SqlType::Int, i),
            |i| int_type("bigint", SqlType::UnsignedBigInt, SqlType::BigInt, i),
            map(alt((tag_no_case("boolean"), tag_no_case("bool"))), |_| {
                SqlType::Bool
            }),
            map(
                preceded(tag_no_case("datetime"), opt(delim_u16)),
                SqlType::DateTime,
            ),
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
            enum_type(dialect),
            map(
                tuple((
                    tag_no_case("float"),
                    whitespace0,
                    opt(precision),
                    whitespace0,
                    opt_signed,
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
        ))(i)
    }
}

fn type_identifier_part2(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    alt((
        map(tag_no_case("timestamptz"), |_| SqlType::TimestampTz),
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
                    // The alt expects the same type to be returned for both entries,
                    // so both have to be tuples with same number of elements
                    tuple((
                        map(tag_no_case("varchar"), |i: LocatedSpan<&[u8]>| *i),
                        map(whitespace0, |_| "".as_bytes()),
                        map(whitespace0, |_| "".as_bytes()),
                    )),
                    tuple((
                        map(tag_no_case("character"), |i: LocatedSpan<&[u8]>| *i),
                        map(whitespace1, |_| "".as_bytes()),
                        map(tag_no_case("varying"), |i: LocatedSpan<&[u8]>| *i),
                    )),
                )),
                opt(delim_u16),
                whitespace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::VarChar(t.1),
        ),
        map(
            tuple((
                alt((tag_no_case("character"), tag_no_case("char"))),
                opt(delim_u16),
                whitespace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Char(t.1),
        ),
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
        map(tag_no_case("longblob"), |_| SqlType::LongBlob),
        map(tag_no_case("mediumblob"), |_| SqlType::MediumBlob),
        map(tag_no_case("mediumtext"), |_| SqlType::MediumText),
        map(tag_no_case("longtext"), |_| SqlType::LongText),
        map(tag_no_case("tinyblob"), |_| SqlType::TinyBlob),
        map(tag_no_case("tinytext"), |_| SqlType::TinyText),
        map(
            tuple((tag_no_case("varbinary"), delim_u16, whitespace0)),
            |t| SqlType::VarBinary(t.1),
        ),
        map(tag_no_case("bytea"), |_| SqlType::ByteArray),
        map(tag_no_case("macaddr"), |_| SqlType::MacAddr),
        map(tag_no_case("inet"), |_| SqlType::Inet),
        map(tag_no_case("uuid"), |_| SqlType::Uuid),
        map(tag_no_case("jsonb"), |_| SqlType::Jsonb),
        map(tag_no_case("json"), |_| SqlType::Json),
    ))(i)
}

fn type_identifier_part3(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
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
                |t| SqlType::VarBit(t.1),
            ),
            map(tuple((tag_no_case("bit"), opt(delim_u16))), |t| {
                SqlType::Bit(t.1)
            }),
            map(tag_no_case("serial"), |_| SqlType::Serial),
            map(tag_no_case("bigserial"), |_| SqlType::BigSerial),
            map(tag_no_case("citext"), |_| SqlType::Citext),
            map(tag("\"char\""), |_| SqlType::QuotedChar),
            map(other_type(dialect), SqlType::Other),
        ))(i)
    }
}

fn other_type(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Relation> {
    move |i| match dialect {
        Dialect::PostgreSQL => relation(dialect)(i),
        Dialect::MySQL => Err(nom::Err::Error(ParseError::from_error_kind(
            i,
            ErrorKind::IsNot,
        ))),
    }
}

fn array_suffix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    let (i, _) = tag("[")(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _len) = opt(digit1)(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _) = tag("]")(i)?;
    Ok((i, ()))
}

fn type_identifier_no_arrays(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            type_identifier_part1(dialect),
            type_identifier_part2,
            type_identifier_part3(dialect),
        ))(i)
    }
}

// A SQL type specifier.
pub fn type_identifier(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        set_failpoint!("parse-sql-type", |_| fail(i));
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

/// For CASTs to integer types, MySQL does not support its own standard type names, and instead
/// only supports a handful of CAST-specific keywords; this function returns a parser for them.
pub fn mysql_int_cast_targets() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            map(
                tuple((
                    tag_no_case("signed"),
                    whitespace0,
                    opt(tag_no_case("integer")),
                )),
                |_| SqlType::BigInt(None),
            ),
            map(
                tuple((
                    tag_no_case("unsigned"),
                    whitespace0,
                    opt(tag_no_case("integer")),
                )),
                |_| SqlType::UnsignedBigInt(None),
            ),
        ))(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod arbitrary {
        use proptest::arbitrary::any_with;
        use test_strategy::proptest;

        use super::*;

        #[proptest]
        fn dont_generate_arrays(
            #[strategy(any_with::<SqlType>(SqlTypeArbitraryOptions {
                generate_arrays: false,
                ..Default::default()
            }))]
            ty: SqlType,
        ) {
            assert!(!matches!(ty, SqlType::Array(..)))
        }

        #[proptest]
        fn dont_generate_other(
            #[strategy(any_with::<SqlType>(SqlTypeArbitraryOptions {
                generate_other: false,
                ..Default::default()
            }))]
            ty: SqlType,
        ) {
            assert!(!matches!(ty, SqlType::Other(..)))
        }
    }

    #[test]
    fn sql_types() {
        let type0 = "bigint(20)";
        let type1 = "varchar(255) binary";
        let type2 = "bigint(20) unsigned";
        let type3 = "bigint(20) signed";

        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type0.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigInt(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type1.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::VarChar(Some(255)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigInt(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type3.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigInt(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigInt(Some(20)));

        let ok = [
            "bool",
            "integer(16)",
            "datetime(16)",
            "decimal(6,2) unsigned",
            "numeric(2) unsigned",
            "float unsigned",
        ];

        let res_ok: Vec<_> = ok
            .iter()
            .map(|t| {
                type_identifier(Dialect::MySQL)(LocatedSpan::new(t.as_bytes()))
                    .unwrap()
                    .1
            })
            .collect();

        assert_eq!(
            res_ok,
            vec![
                SqlType::Bool,
                SqlType::Int(Some(16)),
                SqlType::DateTime(Some(16)),
                SqlType::Decimal(6, 2),
                SqlType::Numeric(Some((2, None))),
                SqlType::Float,
            ]
        );
    }

    #[test]
    fn boolean_bool() {
        let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"boolean");
        assert_eq!(res, SqlType::Bool);
    }

    #[test]
    fn json_type() {
        for &dialect in Dialect::ALL {
            let res = type_identifier(dialect)(LocatedSpan::new(b"json"));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Json);
        }
    }

    #[test]
    fn innermost_array_type() {
        fn nest_array(mut ty: SqlType, dimen: usize) -> SqlType {
            for _ in 0..dimen {
                ty = SqlType::Array(Box::new(ty));
            }
            ty
        }

        for ty in [SqlType::Text, SqlType::Bool, SqlType::Double] {
            for dimen in 0..=5 {
                let arr = nest_array(ty.clone(), dimen);
                assert_eq!(arr.innermost_array_type(), &ty);
            }
        }
    }

    mod mysql {
        use super::*;

        #[test]
        fn double_with_lens() {
            let qs = b"double(16,12)";
            let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Double);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn numeric() {
            let qs = b"NUMERIC";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(None));
        }

        #[test]
        fn numeric_with_precision() {
            let qs = b"NUMERIC(10)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, None))));
        }

        #[test]
        fn numeric_with_precision_and_scale() {
            let qs = b"NUMERIC(10, 20)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, Some(20)))));
        }

        #[test]
        fn quoted_char_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"\"char\"");
            assert_eq!(res, SqlType::QuotedChar);
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
            assert_eq!(res, SqlType::VarBit(None));
        }

        #[test]
        fn bit_varying_with_size_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit varying(10)");
            assert_eq!(res, SqlType::VarBit(Some(10)));
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
        fn timestamptz_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamptz");
            assert_eq!(res, SqlType::TimestampTz)
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
            assert_eq!(res, SqlType::VarChar(None));
        }

        #[test]
        fn character_varying() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"character varying");
            assert_eq!(res, SqlType::VarChar(None));
        }

        #[test]
        fn character_varying_with_length() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"character varying(20)"
            );
            assert_eq!(res, SqlType::VarChar(Some(20)));
        }

        #[test]
        fn character_with_length() {
            let qs = b"character(16)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Char(Some(16)));
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
        fn unsupported_other() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"unsupportedtype");
            assert_eq!(res, SqlType::Other("unsupportedtype".into()));
        }

        #[test]
        fn custom_schema_qualified_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"foo.custom");
            assert_eq!(
                res,
                SqlType::Other(Relation {
                    schema: Some("foo".into()),
                    name: "custom".into()
                })
            );
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
        fn citext() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"citext");
            assert_eq!(res, SqlType::Citext);
        }

        #[test]
        fn int_numeric_aliases() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int2"),
                SqlType::Int2
            );
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int4"),
                SqlType::Int4
            );
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int8"),
                SqlType::Int8
            );
        }

        #[test]
        fn interval_type() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"interval"),
                SqlType::Interval {
                    fields: None,
                    precision: None
                }
            );

            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"interval dAY"),
                SqlType::Interval {
                    fields: Some(IntervalFields::Day),
                    precision: None
                }
            );

            assert_eq!(
                test_parse!(
                    type_identifier(Dialect::PostgreSQL),
                    b"INTERVAL hour to mINuTe (4)"
                ),
                SqlType::Interval {
                    fields: Some(IntervalFields::HourToMinute),
                    precision: Some(4),
                }
            );
        }
    }
}
