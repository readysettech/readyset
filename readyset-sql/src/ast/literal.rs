use std::fmt;
use std::hash::Hash;

use bit_vec::BitVec;
use eui48::{MacAddress, MacAddressFormat};
use itertools::Itertools;
use proptest::prelude::Strategy;
use readyset_util::arbitrary::{
    arbitrary_bitvec, arbitrary_date_time, arbitrary_decimal_string_with_digits, arbitrary_ipinet,
    arbitrary_json, arbitrary_naive_time, arbitrary_positive_naive_date,
    arbitrary_timestamp_naive_date_time, arbitrary_uuid,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, ast::*};

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ItemPlaceholder {
    QuestionMark,
    DollarNumber(u32),
    ColonNumber(u32),
}

// Postgres doesn't accept MySQL-style ? placeholders, and MySQL doesn't accept PostgreSQL-style $
// placeholders, but for better or worse we often use each in both dialects in `CREATE CACHE`
// statements. Luckily, sqlparser-rs parses these permissively too, so we accept both here. If that
// changes in sqlparser-rs upstream, we may have to upstream a fix to let the
// [`sqlparser::dialect::Dialect`] determine what placeholders are supported, and override it with a
// custom dialect that supports both.
impl TryFrom<&String> for ItemPlaceholder {
    type Error = AstConversionError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        if value == "?" {
            Ok(Self::QuestionMark)
        } else if let Some(number) = value.strip_prefix("$") {
            Ok(Self::DollarNumber(number.parse().map_err(|_| {
                failed_err!("Could not parse number for placeholder: {number:?}")
            })?))
        } else if let Some(number) = value.strip_prefix(":") {
            Ok(Self::ColonNumber(number.parse().map_err(|_| {
                failed_err!("Could not parse number for placeholder: {number:?}")
            })?))
        } else {
            unsupported!("string is not a PostgreSQL placeholder: {value}")
        }
    }
}

impl TryFrom<&sqlparser::ast::Ident> for ItemPlaceholder {
    type Error = AstConversionError;

    fn try_from(value: &sqlparser::ast::Ident) -> Result<Self, Self::Error> {
        if value.quote_style.is_none() {
            (&value.value).try_into()
        } else {
            failed!("ident is not a placeholder: {value}")
        }
    }
}

impl fmt::Display for ItemPlaceholder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ItemPlaceholder::QuestionMark => write!(f, "?"),
            ItemPlaceholder::DollarNumber(ref i) => write!(f, "${i}"),
            ItemPlaceholder::ColonNumber(ref i) => write!(f, ":{i}"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum Literal {
    Null,
    Boolean(bool),
    /// Signed integer literals
    ///
    /// When parsing, we only return an integer when the value is negative
    Integer(i64),
    /// Unsigned integer literals
    ///
    /// When parsing, we default to signed integer if the integer value has no sign, because mysql
    /// does that and postgres doesn't have unsigned integers
    UnsignedInteger(u64),
    Number(#[strategy(arbitrary_decimal_string_with_digits(65, 30))] String),
    String(String),
    #[weight(0)]
    Blob(Vec<u8>),
    // Even though `ByteArray` has the same inner representation as `Blob`,
    // we want to distinguish them, so then we can avoid doing a trial-and-error
    // to try to determine to which DfValue it corresponds to.
    // Having this here makes it easy to parse PostgreSQL byte array literals, and
    // then just store the `Vec<u8>` into a DfValue without testing if it's a valid
    // String or not.
    ByteArray(Vec<u8>),
    Placeholder(ItemPlaceholder),
    BitVector(#[strategy(arbitrary_bitvec(0..=64))] BitVec),
}

impl From<bool> for Literal {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<u64> for Literal {
    fn from(i: u64) -> Self {
        Literal::UnsignedInteger(i as _)
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

impl From<ItemPlaceholder> for Literal {
    fn from(p: ItemPlaceholder) -> Self {
        Literal::Placeholder(p)
    }
}

impl TryFrom<sqlparser::ast::Value> for Literal {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Value) -> Result<Self, Self::Error> {
        use sqlparser::ast::Value;
        match value {
            Value::Placeholder(ref name) => Ok(Self::Placeholder(name.try_into()?)),
            Value::Boolean(b) => Ok(Self::Boolean(b)),
            Value::Null => Ok(Self::Null),
            Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => Ok(Self::String(s)),
            Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value, .. }) => {
                Ok(Self::String(value))
            }
            Value::Number(s, _unknown) => {
                // TODO(mvzink): Probably should parse as unsigned first and/or fix nom-sql's
                // parsing of numeric literals. sqlparser-rs leaves the number as a string, and its
                // expression parsing will parse `-1` as `UnaryOp::Minus(Value::Number("1"))`.
                // However, to match nom-sql, we usually want a signed integer.
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Self::Integer(i))
                } else if let Ok(i) = s.parse::<u64>() {
                    Ok(Self::UnsignedInteger(i))
                } else {
                    Ok(Self::Number(s))
                }
            }
            Value::EscapedStringLiteral(s) => Ok(Self::String(s)),
            Value::SingleQuotedByteStringLiteral(s) => {
                let mut bitvec = BitVec::new();
                for byte in s.as_bytes() {
                    bitvec.push(*byte == b'1');
                }
                Ok(Self::BitVector(bitvec))
            }
            Value::HexStringLiteral(s) => {
                Ok(Self::ByteArray(hex::decode(s).map_err(|e| {
                    failed_err!("Failed to parse hex literal: {e}")
                })?))
            }
            _ => failed!("unsupported literal {value:?}"),
        }
    }
}

impl TryFrom<sqlparser::ast::ValueWithSpan> for Literal {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::ValueWithSpan) -> Result<Self, Self::Error> {
        value.value.try_into()
    }
}

impl TryFrom<sqlparser::ast::Expr> for Literal {
    type Error = AstConversionError;

    fn try_from(expr: sqlparser::ast::Expr) -> Result<Self, Self::Error> {
        match expr {
            sqlparser::ast::Expr::Value(value) => value.try_into(),
            sqlparser::ast::Expr::Identifier(ref id) => {
                if let Ok(placeholder) = id.try_into() {
                    Ok(Literal::Placeholder(placeholder))
                } else {
                    failed!("unsupported non-literal identifier {id}")
                }
            }
            _ => unsupported!("unexpected non-literal {expr}"),
        }
    }
}

impl TryFrom<sqlparser::ast::Offset> for Literal {
    type Error = AstConversionError;

    fn try_from(offset: sqlparser::ast::Offset) -> Result<Self, Self::Error> {
        let sqlparser::ast::Offset { value, .. } = offset;
        value.try_into()
    }
}

impl DialectDisplay for Literal {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(true) => write!(f, "TRUE"),
            Literal::Boolean(false) => write!(f, "FALSE"),
            Literal::Integer(i) => write!(f, "{i}"),
            Literal::UnsignedInteger(i) => write!(f, "{i}"),
            Literal::Number(s) => write!(f, "{s}"),
            Literal::String(s) => match dialect {
                Dialect::MySQL => display_string_literal(f, s),
                Dialect::PostgreSQL => {
                    let escaped = escape_string_literal(s);
                    if s.len() != escaped.len() {
                        write!(f, "E'{escaped}'")
                    } else {
                        write!(f, "'{escaped}'")
                    }
                }
            },
            Literal::Blob(bv) => write!(
                f,
                "{}",
                bv.iter()
                    .map(|v| format!("{v:x}"))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
            Literal::ByteArray(b) => match dialect {
                Dialect::PostgreSQL => {
                    write!(f, "E'\\x{}'", b.iter().map(|v| format!("{v:x}")).join(""))
                }
                Dialect::MySQL => {
                    write!(f, "X'{}'", b.iter().map(|v| format!("{v:02X}")).join(""))
                }
            },
            Literal::Placeholder(item) => write!(f, "{item}"),
            Literal::BitVector(b) => {
                write!(
                    f,
                    "B'{}'",
                    b.iter().map(|bit| if bit { "1" } else { "0" }).join("")
                )
            }
        })
    }
}

pub(crate) fn display_string_literal(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    write!(f, "'{}'", escape_string_literal(s))
}

fn escape_string_literal(s: &str) -> String {
    s.replace('\'', "''").replace('\\', "\\\\")
}

impl Literal {
    pub fn arbitrary_with_type(
        sql_type: &SqlType,
    ) -> impl Strategy<Value = Self> + 'static + use<> {
        use proptest::prelude::*;

        match sql_type {
            SqlType::Bool => prop_oneof![Just(Self::Integer(0)), Just(Self::Integer(1)),].boxed(),
            SqlType::Char(_)
            | SqlType::VarChar(_)
            | SqlType::TinyText
            | SqlType::MediumText
            | SqlType::LongText
            | SqlType::Text
            | SqlType::Citext => any::<String>().prop_map(Self::String).boxed(),
            SqlType::QuotedChar => any::<i8>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::Int(_) | SqlType::Int4 => {
                any::<i32>().prop_map(|i| Self::Integer(i as _)).boxed()
            }
            SqlType::IntUnsigned(_) => any::<u32>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::BigInt(_) | SqlType::Int8 => {
                any::<i64>().prop_map(|i| Self::Integer(i as _)).boxed()
            }
            SqlType::BigIntUnsigned(_) => any::<u64>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::TinyInt(_) => any::<i8>().prop_map(|i| Self::Integer(i as _)).boxed(),
            SqlType::TinyIntUnsigned(_) => any::<u8>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::SmallInt(_) | SqlType::Int2 => {
                any::<i16>().prop_map(|i| Self::Integer(i as _)).boxed()
            }
            SqlType::SmallIntUnsigned(_) => any::<u16>()
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::MediumInt(_) => ((-1i32 << 23)..(1i32 << 23))
                .prop_map(|i| Self::Integer(i as _))
                .boxed(),
            SqlType::MediumIntUnsigned(_) => (0..(1u32 << 24))
                .prop_map(|i| Self::UnsignedInteger(i as _))
                .boxed(),
            SqlType::Blob
            | SqlType::ByteArray
            | SqlType::LongBlob
            | SqlType::MediumBlob
            | SqlType::TinyBlob
            | SqlType::Binary(_)
            | SqlType::VarBinary(_) => any::<Vec<u8>>().prop_map(Self::Blob).boxed(),
            SqlType::Real | SqlType::Float | SqlType::Double => {
                arbitrary_decimal_string_with_digits(65, 30)
                    .prop_map(Self::Number)
                    .boxed()
            }
            SqlType::Decimal(prec, scale) => {
                arbitrary_decimal_string_with_digits(*prec as u16, *scale)
                    .prop_map(Self::Number)
                    .boxed()
            }
            SqlType::Numeric(prec_scale) => {
                let (prec, scale) = prec_scale
                    .map(|(p, s)| (p, s.unwrap_or(p.min(30) as u8)))
                    .unwrap_or((65, 30));
                arbitrary_decimal_string_with_digits(prec, scale)
                    .prop_map(Self::Number)
                    .boxed()
            }
            SqlType::Date => arbitrary_positive_naive_date()
                .prop_map(|nd| Self::String(nd.format("%Y-%m-%d").to_string()))
                .boxed(),
            SqlType::DateTime(_) | SqlType::Timestamp => arbitrary_timestamp_naive_date_time()
                .prop_map(|ndt| Self::String(ndt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .boxed(),
            SqlType::TimestampTz => arbitrary_date_time()
                .prop_map(|dt| Self::String(dt.format("%Y-%m-%d %H:%M:%S %:z").to_string()))
                .boxed(),
            SqlType::Time => arbitrary_naive_time()
                .prop_map(|nt| Self::String(nt.format("%H:%M:%S").to_string()))
                .boxed(),
            SqlType::Interval { .. } => unimplemented!("Intervals aren't implemented yet"),
            SqlType::Enum(_) => unimplemented!("Enums aren't implemented yet"),
            SqlType::Json | SqlType::Jsonb => arbitrary_json()
                .prop_map(|v| Self::String(v.to_string()))
                .boxed(),
            SqlType::Inet => arbitrary_ipinet()
                .prop_map(|v| Self::String(v.to_string()))
                .boxed(),
            SqlType::MacAddr => any::<[u8; 6]>()
                .prop_map(|bytes| -> Literal {
                    // We know the length and format of the bytes, so this should always be parsable
                    // as a `MacAddress`.
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
            SqlType::Bit(n) => {
                let size = n.unwrap_or(1) as usize;
                arbitrary_bitvec(size..=size)
                    .prop_map(Self::BitVector)
                    .boxed()
            }
            SqlType::VarBit(n) => {
                arbitrary_bitvec(0..n.map(|max_size| max_size as usize).unwrap_or(20_usize))
                    .prop_map(Self::BitVector)
                    .boxed()
            }
            SqlType::Serial => any::<i32>().prop_map(Self::from).boxed(),
            SqlType::BigSerial => any::<i64>().prop_map(Self::from).boxed(),
            SqlType::Array(_) => unimplemented!("Arrays aren't implemented yet"),
            SqlType::Other(ty) => {
                unimplemented!("Other({}) isn't implemented yet", ty.display_unquoted())
            }
            SqlType::Signed
            | SqlType::Unsigned
            | SqlType::SignedInteger
            | SqlType::UnsignedInteger => unimplemented!(
                "This type is only valid in `CAST` and can't be used as a Column Def"
            ),
            SqlType::Point | SqlType::PostgisPoint => {
                unimplemented!("Points aren't implemented yet")
            }
            SqlType::Tsvector => unimplemented!("Tsvector isn't implemented"),
        }
    }

    /// Returns `true` if `self` is a [`String`] literal
    ///
    /// [`String`]: Literal::String
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(..))
    }

    /// Returns `true` if `self` is a [`Placeholder`] literal
    ///
    /// [`Placeholder`]: Literal::Placeholder
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder(_))
    }
}
