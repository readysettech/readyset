use std::fmt;
use std::ops::Deref;

use proptest::{
    prelude::{any_with, Arbitrary, BoxedStrategy, Strategy},
    sample::SizeRange,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use triomphe::ThinArc;

use crate::{ast::*, Dialect, DialectDisplay};

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
    MediumInt(Option<u16>),
    UnsignedMediumInt(Option<u16>),
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
            Just(Text).boxed(),
            Just(Date).boxed(),
            Just(Time).boxed(),
            Just(Timestamp).boxed(),
            option::of((1..=28u16).prop_flat_map(|n| (Just(n), option::of(0..=(n as u8)).boxed())))
                .prop_map(Numeric)
                .boxed(),
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
                option::of(1..255u16).prop_map(MediumInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedSmallInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedBigInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedTinyInt).boxed(),
                option::of(1..255u16).prop_map(UnsignedMediumInt).boxed(),
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
                SqlType::MediumInt(len) => write_with_len(f, "MEDIUMINT", len),
                SqlType::UnsignedMediumInt(len) => {
                    write_with_len(f, "MEDIUMINT", len)?;
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
    #[allow(clippy::non_canonical_partial_ord_impl)]
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
