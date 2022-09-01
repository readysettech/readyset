use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use itertools::Itertools;
use proptest::strategy::Strategy;
use proptest::{prelude as prop, prop_oneof};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;
use triomphe::ThinArc;

use crate::common::type_identifier;
use crate::{Dialect, Literal};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Arbitrary)]
pub enum SqlType {
    Bool,
    Char(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    VarChar(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    Int(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    BigInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedBigInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    TinyInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedTinyInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    SmallInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    UnsignedSmallInt(#[strategy(proptest::option::of(1..255u16))] Option<u16>),
    #[weight(0)]
    Blob,
    #[weight(0)]
    LongBlob,
    #[weight(0)]
    MediumBlob,
    #[weight(0)]
    TinyBlob,
    Double,
    Float,
    Real,
    Numeric(Option<(u16, Option<u8>)>),
    TinyText,
    MediumText,
    LongText,
    Text,
    Date,
    DateTime(#[strategy(proptest::option::of(1..=6u16))] Option<u16>),
    Time,
    Timestamp,
    TimestampTz,
    #[weight(0)]
    Binary(Option<u16>),
    #[weight(0)]
    VarBinary(u16),
    #[weight(0)]
    Enum(EnumType),
    #[weight(0)]
    Decimal(#[strategy(1..=30u8)] u8, #[strategy(1..=# 0)] u8),
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
}

impl SqlType {
    /// Returns a proptest strategy to generate *numeric* [`SqlType`]s - signed or unsigned, floats
    /// or reals
    pub fn arbitrary_numeric_type() -> impl Strategy<Value = SqlType> {
        use SqlType::*;

        prop_oneof![
            prop::Just(Int(None)),
            prop::Just(UnsignedInt(None)),
            prop::Just(BigInt(None)),
            prop::Just(UnsignedBigInt(None)),
            prop::Just(TinyInt(None)),
            prop::Just(UnsignedTinyInt(None)),
            prop::Just(SmallInt(None)),
            prop::Just(UnsignedSmallInt(None)),
            prop::Just(Double),
            prop::Just(Float),
            prop::Just(Real),
        ]
    }

    /// Creates a [`SqlType::Enum`] instance from a sequence of variant names.
    #[inline]
    pub fn from_enum_variants<I>(variants: I) -> Self
    where
        I: IntoIterator<Item = Literal>,
        I::IntoIter: ExactSizeIterator,
    {
        Self::Enum(variants.into())
    }

    /// Returns whether `self` is any text-containing type.
    #[inline]
    pub fn is_any_text(&self) -> bool {
        use SqlType::*;
        matches!(self, Text | TinyText | MediumText | LongText | VarChar(_))
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
            SqlType::Blob => write!(f, "BLOB"),
            SqlType::LongBlob => write!(f, "LONGBLOB"),
            SqlType::MediumBlob => write!(f, "MEDIUMBLOB"),
            SqlType::TinyBlob => write!(f, "TINYBLOB"),
            SqlType::Double => write!(f, "DOUBLE"),
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
            SqlType::Date => write!(f, "DATE"),
            SqlType::DateTime(len) => write_with_len(f, "DATETIME", len),
            SqlType::Time => write!(f, "TIME"),
            SqlType::Timestamp => write!(f, "TIMESTAMP"),
            SqlType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            SqlType::Binary(len) => write_with_len(f, "BINARY", len),
            SqlType::VarBinary(len) => write!(f, "VARBINARY({})", len),
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
            SqlType::VarBit(n) => write_with_len(f, "VARBIT", n),
            SqlType::Serial => write!(f, "SERIAL"),
            SqlType::BigSerial => write!(f, "BIGSERIAL"),
            SqlType::Array(ref t) => write!(f, "{}[]", t),
        }
    }
}

impl FromStr for SqlType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        type_identifier(Dialect::MySQL)(s.as_bytes())
            .map(|(_, s)| s)
            .map_err(|_| "failed to parse")
    }
}

/// [`SqlType::Enum`](crate::SqlType::Enum) abstraction over an array of [`Literal`].
///
/// Clones are O(1) and this is always 1 pointer wide for efficient storage in `SqlType`.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct EnumType {
    variants: ThinArc<(), Literal>,
}

impl Deref for EnumType {
    type Target = [Literal];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.variants.slice
    }
}

// We cannot use `FromIterator` because it does not allow specializing the iterator type.
//
// This impl works for `Vec` and `[T; N]`. It will never conflict with `From<EnumType>` because we
// cannot implement an owned iterator over `triomphe::ThinArc`.
impl<I: IntoIterator<Item = Literal>> From<I> for EnumType
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

impl fmt::Debug for EnumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialOrd for EnumType {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        <[Literal]>::partial_cmp(self, other)
    }
}

impl<'de> Deserialize<'de> for EnumType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialize into a temporary `Vec` since we cannot construct `ThinArc` from an iterator
        // here.
        let result = Vec::<Literal>::deserialize(deserializer)?;

        Ok(result.into())
    }
}

impl Serialize for EnumType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <[Literal]>::serialize(self, serializer)
    }
}
