use std::fmt;
use std::ops::Deref;

use itertools::Itertools;
use proptest::{
    prelude::{any_with, Arbitrary, BoxedStrategy, Strategy},
    sample::SizeRange,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use triomphe::ThinArc;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

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
    // These 4 are used only in MySQL CAST
    Signed,
    Unsigned,
    UnsignedInteger,
    SignedInteger,
    IntUnsigned(Option<u16>),
    BigInt(Option<u16>),
    BigIntUnsigned(Option<u16>),
    TinyInt(Option<u16>),
    TinyIntUnsigned(Option<u16>),
    SmallInt(Option<u16>),
    SmallIntUnsigned(Option<u16>),
    MediumInt(Option<u16>),
    MediumIntUnsigned(Option<u16>),
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

impl TryFromDialect<sqlparser::ast::DataType> for crate::ast::SqlType {
    fn try_from_dialect(
        value: sqlparser::ast::DataType,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::DataType::*;
        match value {
            Int(len) => Ok(Self::Int(len.map(|n| n as u16))),
            TinyInt(len) => Ok(Self::TinyInt(len.map(|n| n as u16))),
            BigIntUnsigned(len) | Int8Unsigned(len) => {
                Ok(Self::BigIntUnsigned(len.map(|n| n as u16)))
            }
            Text => Ok(Self::Text),
            TinyText => Ok(Self::TinyText),
            MediumText => Ok(Self::MediumText),
            LongText => Ok(Self::LongText),
            Character(len) | Char(len) => Ok(Self::Char(len.and_then(character_length_into_u16))),
            Varchar(len) | Nvarchar(len) | CharVarying(len) | CharacterVarying(len) => {
                Ok(Self::VarChar(len.and_then(character_length_into_u16)))
            }
            Uuid => Ok(Self::Uuid),
            CharacterLargeObject(_) | CharLargeObject(_) | Clob(_) => {
                not_yet_implemented!("character large object type")
            }
            Binary(n) => Ok(Self::Binary(n.map(|n| n as u16))),
            Varbinary(binary_length) => {
                if let Some(binary_length) = binary_length {
                    match binary_length {
                        sqlparser::ast::BinaryLength::IntegerLength { length } => {
                            Ok(Self::VarBinary(length as u16))
                        }
                        sqlparser::ast::BinaryLength::Max => unsupported!("MSSQL VARBINARY(MAX)"),
                    }
                } else {
                    Ok(Self::VarBinary(0))
                }
            }
            // TOOD: technically mysql inspects the size and converts to tiny/medium/long
            Blob(_) => Ok(Self::Blob),
            TinyBlob => Ok(Self::TinyBlob),
            MediumBlob => Ok(Self::MediumBlob),
            LongBlob => Ok(Self::LongBlob),
            Bytes(_) => unimplemented!(),
            Numeric(info) => exact_number_info_into_numeric(info)
                .map_err(|e| failed_err!("NUMERIC conversion: {e}")),
            Decimal(info) => exact_number_info_into_decimal(info)
                .map_err(|e| failed_err!("DECIMAL conversion: {e}")),
            BigNumeric(info) => exact_number_info_into_numeric(info)
                .map_err(|e| failed_err!("BIG NUMERIC conversion: {e}")),
            BigDecimal(info) => exact_number_info_into_decimal(info)
                .map_err(|e| failed_err!("BIG DECIMAL conversion: {e}")),
            Dec(info) => {
                exact_number_info_into_decimal(info).map_err(|e| failed_err!("DEC conversion: {e}"))
            }
            Float(_) => Ok(Self::Float),
            TinyIntUnsigned(n) => Ok(Self::TinyIntUnsigned(n.map(|n| n as u16))),
            Int2(_) => Ok(Self::Int2),
            Int2Unsigned(n) => Ok(Self::SmallIntUnsigned(n.map(|n| n as u16))),
            SmallInt(n) => Ok(Self::SmallInt(n.map(|n| n as u16))),
            SmallIntUnsigned(n) => Ok(Self::SmallIntUnsigned(n.map(|n| n as u16))),
            MediumInt(n) => Ok(Self::MediumInt(n.map(|n| n as u16))),
            MediumIntUnsigned(n) => Ok(Self::MediumIntUnsigned(n.map(|n| n as u16))),
            Int4(_) => Ok(Self::Int4),
            Int8(_) => Ok(Self::Int8),
            Int16 | Int32 | Int64 | Int128 | Int256 => unsupported!("INT<size> type"),
            Integer(n) => Ok(Self::Int(n.map(|n| n as u16))),
            IntUnsigned(n) => Ok(Self::IntUnsigned(n.map(|n| n as u16))),
            Int4Unsigned(n) => Ok(Self::IntUnsigned(n.map(|n| n as u16))),
            IntegerUnsigned(n) => Ok(Self::IntUnsigned(n.map(|n| n as u16))),
            UInt8 | UInt16 | UInt32 | UInt64 | UInt128 | UInt256 => unsupported!("UINT<size> type"),
            BigInt(n) => Ok(Self::BigInt(n.map(|n| n as u16))),
            Signed => Ok(Self::Signed),
            SignedInteger => Ok(Self::SignedInteger),
            Unsigned => Ok(Self::Unsigned),
            UnsignedInteger => Ok(Self::UnsignedInteger),
            Float4 | Float8 | Float32 | Float64 => unsupported!("FLOAT<size> type"),
            Real => Ok(Self::Real),
            // XXX we don't support precision on doubles; [MySQL] says it's deprecated, but still present as of 9.1
            // [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/floating-point-types.html
            Double(_info) => Ok(Self::Double),
            DoublePrecision => Ok(Self::Double),
            Bool => Ok(Self::Bool),
            Boolean => Ok(Self::Bool),
            Date => Ok(Self::Date),
            Date32 => unsupported!("DATE32 type"),
            // TODO: Should we support these options?
            Time(_, _timezone_info) => Ok(Self::Time),
            Datetime(n) => Ok(Self::DateTime(n.map(|n| n as u16))),
            Datetime64(_, _) => unsupported!("DATETIME64 type"),
            // Note: We don't support precision on timestamps; and we don't differentiate
            // `TIMESTAMPTZ` from `WITH TIME ZONE`.
            Timestamp(_precision, tz_info) => match tz_info {
                sqlparser::ast::TimezoneInfo::None => Ok(Self::Timestamp),
                sqlparser::ast::TimezoneInfo::WithTimeZone => Ok(Self::TimestampTz),
                sqlparser::ast::TimezoneInfo::WithoutTimeZone => Ok(Self::Timestamp),
                sqlparser::ast::TimezoneInfo::Tz => Ok(Self::TimestampTz),
            },
            Interval => Ok(Self::Time),
            JSON => Ok(Self::Json),
            JSONB => Ok(Self::Jsonb),
            Regclass => unsupported!("REGCLASS type"),
            String(_) => not_yet_implemented!("STRING type"),
            FixedString(_) => unsupported!("FIXEDSTRING type"),
            Bytea => Ok(Self::ByteArray),
            Bit(n) => Ok(Self::Bit(n.map(|n| n as u16))),
            BitVarying(n) | VarBit(n) => Ok(Self::VarBit(n.map(|n| n as u16))),
            Custom(name, _values) => match name.0.iter().exactly_one() {
                Ok(part) => match part.as_ident().map(|ident| ident.value.as_str()) {
                    Some(name) => {
                        if dialect == Dialect::PostgreSQL && name == "char" {
                            Ok(Self::QuotedChar)
                        } else if name.eq_ignore_ascii_case("bigserial") {
                            Ok(Self::BigSerial)
                        } else if name.eq_ignore_ascii_case("citext") {
                            Ok(Self::Citext)
                        } else if name.eq_ignore_ascii_case("inet") {
                            Ok(Self::Inet)
                        } else if name.eq_ignore_ascii_case("macaddr") {
                            Ok(Self::MacAddr)
                        } else if name.eq_ignore_ascii_case("serial") {
                            Ok(Self::Serial)
                        } else {
                            Ok(Self::Other(name.into()))
                        }
                    }
                    None => failed!("invalid custom type name {}", name),
                },
                Err(_) => Ok(Self::Other(name.into_dialect(dialect))),
            },
            Array(def) => Ok(Self::Array(Box::new(def.try_into_dialect(dialect)?))),
            Map(_data_type, _data_type1) => unsupported!("MAP type"),
            Tuple(_vec) => unsupported!("TUPLE type"),
            Nested(_vec) => unsupported!("NESTED type"),
            // XXX bits is a Clickhouse extension for ENUM8/ENUM16
            Enum(variants, _bits) => Ok(Self::Enum(
                variants
                    .into_iter()
                    .map(|variant| match variant {
                        sqlparser::ast::EnumMember::Name(s) => s,
                        // XXX expression is a Clickhouse extension
                        sqlparser::ast::EnumMember::NamedValue(s, _expr) => s,
                    })
                    .collect::<Vec<_>>()
                    .into(),
            )),
            Set(_vec) => unsupported!("SET type"),
            Struct(_vec, _struct_bracket_kind) => unsupported!("STRUCT type"),
            Union(_vec) => unsupported!("UNION type"),
            Nullable(_data_type) => not_yet_implemented!("NULLABLE type"),
            LowCardinality(_data_type) => unsupported!("LOW_CARDINALITY type"),
            Unspecified => unsupported!("UNSPECIFIED"),
            Trigger => skipped!("TRIGGER"),
            AnyType => unsupported!("ANY TYPE"),
            Table(_column_definition_list) => unsupported!("TABLE type"),
            GeometricType(geometric_type_kind) => {
                unsupported!("geometric type {geometric_type_kind}")
            }
        }
    }
}

// XXX: Not written as a `TryFrom` impl because the types don't tell us whether this is supposed to
// be a numeric or a decimal
fn exact_number_info_into_numeric(
    info: sqlparser::ast::ExactNumberInfo,
) -> Result<SqlType, std::num::TryFromIntError> {
    match info {
        sqlparser::ast::ExactNumberInfo::None => Ok(SqlType::Numeric(None)),
        sqlparser::ast::ExactNumberInfo::Precision(precision) => {
            Ok(SqlType::Numeric(Some((precision.try_into()?, None))))
        }
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok(
            SqlType::Numeric(Some((precision.try_into()?, Some(scale.try_into()?)))),
        ),
    }
}

fn exact_number_info_into_decimal(
    info: sqlparser::ast::ExactNumberInfo,
) -> Result<SqlType, std::num::TryFromIntError> {
    match info {
        // TODO(mvzink): this default of 32 matches nom-sql, which is wrong, and varies by dialect
        // (and it should actually be optional like [`SqlType::Numeric`] too)
        sqlparser::ast::ExactNumberInfo::None => Ok(SqlType::Decimal(32, 0)),
        sqlparser::ast::ExactNumberInfo::Precision(precision) => {
            Ok(SqlType::Decimal(precision.try_into()?, 0))
        }
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            Ok(SqlType::Decimal(precision.try_into()?, scale.try_into()?))
        }
    }
}

fn character_length_into_u16(value: sqlparser::ast::CharacterLength) -> Option<u16> {
    match value {
        sqlparser::ast::CharacterLength::IntegerLength { length, unit: _ } => {
            length.try_into().ok()
        }
        sqlparser::ast::CharacterLength::Max => None,
    }
}

impl TryFromDialect<sqlparser::ast::ArrayElemTypeDef> for SqlType {
    fn try_from_dialect(
        value: sqlparser::ast::ArrayElemTypeDef,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::ArrayElemTypeDef;
        match value {
            ArrayElemTypeDef::None => unsupported!("ARRAY<NONE> type"),
            ArrayElemTypeDef::AngleBracket(data_type) => data_type.try_into_dialect(dialect),
            // TODO: Should we explicitly reject numbers in the square brackets?
            ArrayElemTypeDef::SquareBracket(data_type, _) => data_type.try_into_dialect(dialect),
            ArrayElemTypeDef::Parenthesis(data_type) => data_type.try_into_dialect(dialect),
        }
    }
}

impl TryFromDialect<Box<sqlparser::ast::DataType>> for crate::ast::SqlType {
    fn try_from_dialect(
        value: Box<sqlparser::ast::DataType>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        (*value).try_into_dialect(dialect)
    }
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
                option::of(1..255u16).prop_map(IntUnsigned).boxed(),
                option::of(1..255u16).prop_map(SmallIntUnsigned).boxed(),
                option::of(1..255u16).prop_map(BigIntUnsigned).boxed(),
                option::of(1..255u16).prop_map(TinyIntUnsigned).boxed(),
                option::of(1..255u16).prop_map(MediumIntUnsigned).boxed(),
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
                SqlType::IntUnsigned(len) => {
                    write_with_len(f, "INT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::BigInt(len) => write_with_len(f, "BIGINT", len),
                SqlType::BigIntUnsigned(len) => {
                    write_with_len(f, "BIGINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::TinyInt(len) => write_with_len(f, "TINYINT", len),
                SqlType::TinyIntUnsigned(len) => {
                    write_with_len(f, "TINYINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::SmallInt(len) => write_with_len(f, "SMALLINT", len),
                SqlType::SmallIntUnsigned(len) => {
                    write_with_len(f, "SMALLINT", len)?;
                    write!(f, " UNSIGNED")
                }
                SqlType::MediumInt(len) => write_with_len(f, "MEDIUMINT", len),
                SqlType::MediumIntUnsigned(len) => {
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
                SqlType::Signed => write!(f, "SIGNED"),
                SqlType::Unsigned => write!(f, "UNSIGNED"),
                SqlType::SignedInteger => write!(f, "SIGNED INTEGER"),
                SqlType::UnsignedInteger => write!(f, "UNSIGNED INTEGER"),
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
