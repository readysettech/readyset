use std::fmt;

use enum_kinds::EnumKind;
use itertools::Itertools;
use nom_sql::{EnumType, Literal, SqlIdentifier, SqlType};
use serde::{Deserialize, Serialize};

use crate::{Collation, Dialect};

/// Dataflow runtime representation of [`SqlType`].
///
/// Time types contain a `subsecond_digits` property, also known as fractional seconds precision
/// (FSP). It must be between 0 through 6, and defaults to [`Dialect::default_subsecond_digits`].
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, EnumKind)]
#[enum_kind(DfTypeKind)]
pub enum DfType {
    /// Placeholder for when the type is not known.
    ///
    /// In PostgreSQL, `Unknown` is a not-yet-resolved [pseudo type](https://www.postgresql.org/docs/10/datatype-pseudo.html).
    /// PostgreSQL also sometimes considers `Unknown` to be the ["third" boolean type](https://www.postgresql.org/docs/current/datatype-boolean.html),
    /// however this type does not represent that.
    Unknown,

    /// [PostgreSQL `T[]`](https://www.postgresql.org/docs/current/arrays.html).
    Array(Box<DfType>),

    /// [PostgreSQL boolean](https://www.postgresql.org/docs/current/datatype-boolean.html)
    /// or alias to `tinyint(1)` in MySQL.
    ///
    /// In MySQL, instances can be any [`i8`] value.
    Bool,

    /// [`i32`].
    Int,

    /// [`u32`].
    UnsignedInt,

    /// [`i64`].
    BigInt,

    /// [`u64`].
    UnsignedBigInt,

    /// [`i8`].
    TinyInt,

    /// [`u8`].
    UnsignedTinyInt,

    /// [`i16`].
    SmallInt,

    /// [`u16`].
    UnsignedSmallInt,

    /// [`f32`]: a IEEE 754 floating-point 32-bit real value.
    ///
    /// This is either:
    /// - [MySQL `float`](https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html).
    /// - [PostgreSQL `real`](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT)
    Float,

    /// [`f64`]: a IEEE 754 floating-point 64-bit real value.
    ///
    /// This is either:
    /// - [MySQL `double precision`](https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html).
    /// - [PostgreSQL `double precision`](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT).
    Double,

    /// The `DECIMAL` or `NUMERIC` type.
    ///
    /// See:
    /// - [MySQL docs](https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html).
    /// - [PostgreSQL docs](https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html).
    Numeric {
        /// Maximum number of digits a number can have. Default is 10.
        ///
        /// This value can be queried via `numeric_precision` in
        /// [`information_schema.columns`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html).
        prec: u16,

        /// Number of digits to the right of the decimal point. Default is 0.
        ///
        /// This value can be queried via `numeric_scale` in
        /// [`information_schema.columns`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html).
        scale: u8,
    },

    /// Any variable-length character string.
    ///
    /// Despite size limits existing in the `character_octet_length`/`character_maximum_length`
    /// column properties (which vary between SQL types and dialects), we treat all `*text` types
    /// and bare `varchar` as having unlimited length. We are allowed to do so because the upstream
    /// database validates data size for us.
    Text(Collation),

    /// `CHAR(n)`: fixed-length character string.
    // FIXME(ENG-1839): Should have `Option<u16>` to determine how `cast` is done for MySQL. The
    // dialect field provides context for semantics.
    Char(u16, Collation, Dialect),

    /// `VARCHAR(n)`/`CHAR VARYING(n)`: max-length character string.
    VarChar(u16, Collation),

    /// [MySQL `blob`](https://dev.mysql.com/doc/refman/8.0/en/blob.html) or
    /// [PostgreSQL `bytea`](https://www.postgresql.org/docs/current/datatype-binary.html).
    #[doc(alias = "bytea")]
    Blob,

    /// MySQL `binary(n)`: fixed-length binary string.
    Binary(u16),

    /// MySQL `varbinary(n)`: max-length character string.
    VarBinary(u16),

    /// [MySQL `bit(n)`] field.
    ///
    /// The maximum values for MySQL and PostgreSQL are 64 and 83886080 respectively.
    Bit(u16),

    /// [PostgreSQL `varbit`/`bit varying(n)`](https://www.postgresql.org/docs/current/datatype-bit.html).
    ///
    /// The maximum values for MySQL and PostgreSQL are 64 and 83886080 respectively.
    VarBit(Option<u16>),

    /// [MySQL `date`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html).
    Date,

    /// [MySQL `datetime`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html).
    DateTime { subsecond_digits: u16 },

    /// [MySQL `time`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html).
    Time { subsecond_digits: u16 },

    /// [MySQL `timestamp`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html) or
    /// [PostgreSQL `timestamp`](https://www.postgresql.org/docs/current/datatype-datetime.html).
    Timestamp { subsecond_digits: u16 },

    /// [PostgreSQL `timestamptz`/`timestamp with timezone`](https://www.postgresql.org/docs/current/datatype-datetime.html).
    TimestampTz { subsecond_digits: u16 },

    /// [PostgreSQL `macaddr`](https://www.postgresql.org/docs/current/datatype-net-types.html).
    MacAddr,

    /// [PostgreSQL `inet`](https://www.postgresql.org/docs/current/datatype-net-types.html).
    Inet,

    /// [PostgreSQL `uuid`](https://www.postgresql.org/docs/current/datatype-uuid.html).
    Uuid,

    /// MySQL enum.
    ///
    /// PostgreSQL enums are technically not yet supported.
    Enum(EnumType, Dialect),

    /// [MySQL `json`](https://dev.mysql.com/doc/refman/8.0/en/json.html) or
    /// [PostgreSQL `json`](https://www.postgresql.org/docs/current/datatype-json.html).
    // TODO: Change to MySQL/PG-specific variants.
    Json(Dialect),

    /// [PostgreSQL `jsonb`](https://www.postgresql.org/docs/current/datatype-json.html).
    Jsonb,

    /// PostgreSQL fallback type, enabling the adapter to funnel arbitrary types to/from upstream.
    /// These types are uncachable and effectively blackboxes to ReadySet.
    PassThrough(SqlIdentifier),
}

/// Defaults.
impl DfType {
    /// [`DfType::Text`] with the default collation.
    pub const DEFAULT_TEXT: Self = Self::Text(Collation::Utf8);

    pub const DEFAULT_NUMERIC: Self = Self::Numeric {
        prec: Self::DEFAULT_NUMERIC_PREC,
        scale: Self::DEFAULT_NUMERIC_SCALE,
    };

    pub const DEFAULT_NUMERIC_PREC: u16 = 10;
    pub const DEFAULT_NUMERIC_SCALE: u8 = 0;

    pub const DEFAULT_BIT: Self = Self::Bit(1);
}

/// Conversions to/from [`SqlType`].
impl DfType {
    /// Converts from a possible [`SqlType`] reference within the context of a SQL [`Dialect`].
    #[inline]
    pub fn from_sql_type<'a>(ty: impl Into<Option<&'a SqlType>>, dialect: Dialect) -> Self {
        // NOTE: We get a feature gate error if we don't name the lifetime in the signature.
        match ty.into() {
            Some(ty) => Self::from_sql_type_impl(ty, dialect),
            None => Self::Unknown,
        }
    }

    /// Internal implementation of `from_sql_type` to avoid monormorphizing two large functions.
    fn from_sql_type_impl(ty: &SqlType, dialect: Dialect) -> Self {
        use SqlType::*;

        match *ty {
            Array(ref ty) => Self::Array(Box::new(Self::from_sql_type_impl(ty, dialect))),

            // PERF: Cloning enum types is O(1).
            Enum(ref ty) => Self::Enum(ty.clone(), dialect),

            // FIXME(ENG-1650): Convert to `tinyint(1)` for MySQL.
            Bool => Self::Bool,

            Serial => dialect.serial_type(),
            BigSerial => Self::BigInt,

            Int(_) => Self::Int,
            TinyInt(_) => Self::TinyInt,
            SmallInt(_) => Self::SmallInt,
            BigInt(_) => Self::BigInt,
            UnsignedInt(_) => Self::UnsignedInt,
            UnsignedTinyInt(_) => Self::UnsignedTinyInt,
            UnsignedSmallInt(_) => Self::UnsignedSmallInt,
            UnsignedBigInt(_) => Self::UnsignedBigInt,

            Double => Self::Double,
            Float => dialect.float_type(),
            Real => dialect.real_type(),

            // Decimal and Numeric are semantically aliases.
            Numeric(prec) => {
                let (prec, scale) = prec.unwrap_or((Self::DEFAULT_NUMERIC_PREC, None));
                let scale = scale.unwrap_or(Self::DEFAULT_NUMERIC_SCALE);
                Self::Numeric { prec, scale }
            }
            Decimal(prec, scale) => Self::Numeric {
                prec: prec.into(),
                scale,
            },

            // Character string types.
            //
            // `varchar` by itself is an error in MySQL but synonymous with `text` in PostgreSQL.
            Text | TinyText | MediumText | LongText | VarChar(None) => Self::DEFAULT_TEXT,
            VarChar(Some(len)) => Self::VarChar(len, Collation::default()),
            Char(len) => Self::Char(len.unwrap_or(1), Collation::default(), dialect),

            Blob | TinyBlob | MediumBlob | LongBlob | ByteArray => Self::Blob,
            VarBinary(len) => Self::VarBinary(len),
            Binary(len) => Self::Binary(len.unwrap_or(1)),

            Bit(len) => Self::Bit(len.unwrap_or(1)),
            VarBit(len) => Self::VarBit(len),

            Json => Self::Json(dialect),
            Jsonb => Self::Jsonb,

            Date => Self::Date,
            #[allow(clippy::or_fun_call)]
            DateTime(subsecond_digits) => Self::DateTime {
                subsecond_digits: subsecond_digits.unwrap_or(dialect.default_subsecond_digits()),
            },
            Time => Self::Time {
                subsecond_digits: dialect.default_subsecond_digits(),
            },
            Timestamp => Self::Timestamp {
                subsecond_digits: dialect.default_subsecond_digits(),
            },
            TimestampTz => Self::TimestampTz {
                subsecond_digits: dialect.default_subsecond_digits(),
            },

            Uuid => Self::Uuid,
            MacAddr => Self::MacAddr,
            Inet => Self::Inet,
            Citext => Self::Text(Collation::Citext),
            PassThrough(ref id) => Self::PassThrough(id.to_owned()),
        }
    }
}

impl DfType {
    /// Creates a [`DfType::Enum`] instance from a sequence of variant names.
    #[inline]
    pub fn from_enum_variants<I>(variants: I, dialect: Dialect) -> Self
    where
        I: IntoIterator<Item = Literal>,
        I::IntoIter: ExactSizeIterator, // required by `triomphe::ThinArc`
    {
        Self::Enum(variants.into(), dialect)
    }

    /// Returns the PostgreSQL type category for this type
    pub fn pg_category(&self) -> PgTypeCategory {
        match self {
            DfType::Unknown => PgTypeCategory::Unknown,
            DfType::Array(_) => PgTypeCategory::Array,
            DfType::Bool => PgTypeCategory::Boolean,
            DfType::Int
            | DfType::UnsignedInt
            | DfType::BigInt
            | DfType::UnsignedBigInt
            | DfType::TinyInt
            | DfType::UnsignedTinyInt
            | DfType::SmallInt
            | DfType::UnsignedSmallInt
            | DfType::Float
            | DfType::Double
            | DfType::Numeric { .. } => PgTypeCategory::Numeric,
            DfType::Text(_) | DfType::Char(_, _, _) | DfType::VarChar(_, _) => {
                PgTypeCategory::String
            }
            DfType::Blob
            | DfType::Binary(_)
            | DfType::VarBinary(_)
            | DfType::Bit(_)
            | DfType::VarBit(_) => PgTypeCategory::BitString,
            DfType::Date
            | DfType::DateTime { .. }
            | DfType::Time { .. }
            | DfType::Timestamp { .. }
            | DfType::TimestampTz { .. } => PgTypeCategory::DateTime,
            DfType::MacAddr | DfType::Inet => PgTypeCategory::NetworkAddress,
            DfType::Uuid | DfType::Enum(_, _) | DfType::Json(_) | DfType::Jsonb => {
                PgTypeCategory::UserDefined
            }
            DfType::PassThrough(_) => PgTypeCategory::Unknown,
        }
    }

    /// Returns the number of subsecond digits if this is a time type, otherwise [`None`].
    ///
    /// This is also known as fractional seconds precision (FSP). It must be between 0 through 6,
    /// and defaults to [`Dialect::default_subsecond_digits`].
    #[inline]
    pub fn subsecond_digits(&self) -> Option<u16> {
        match *self {
            Self::DateTime { subsecond_digits }
            | Self::Time { subsecond_digits }
            | Self::Timestamp { subsecond_digits }
            | Self::TimestampTz { subsecond_digits } => Some(subsecond_digits),
            _ => None,
        }
    }

    /// Converts the type to an [`Option`] where [`DfType::Unknown`] becomes [`None`].
    #[inline]
    pub fn try_into_known(self) -> Option<Self> {
        if self.is_known() {
            Some(self)
        } else {
            None
        }
    }

    /// Returns `true` if the type carries information (i.e. is not [`DfType::Unknown`]).
    #[inline]
    pub fn is_known(&self) -> bool {
        !self.is_unknown()
    }

    /// Returns `true` if the type does not contain [`DfType::Unknown`].
    #[inline]
    pub fn is_strictly_known(&self) -> bool {
        match self {
            Self::Unknown => false,
            Self::Array(ty) => ty.is_strictly_known(),
            _ => true,
        }
    }

    /// Returns `true` if the type carries no information (i.e. is [`DfType::Unknown`]).
    #[inline]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Returns `true` if this is an enum type.
    #[inline]
    pub fn is_enum(&self) -> bool {
        matches!(self, DfType::Enum { .. })
    }

    /// Returns `true` if this is any `*int` type.
    #[inline]
    pub fn is_any_int(&self) -> bool {
        matches!(
            *self,
            Self::TinyInt
                | Self::UnsignedTinyInt
                | Self::SmallInt
                | Self::UnsignedSmallInt
                | Self::Int
                | Self::UnsignedInt
                | Self::BigInt
                | Self::UnsignedBigInt
        )
    }

    /// Returns `true` if this is any `text` type
    #[inline]
    pub fn is_any_text(&self) -> bool {
        matches!(self, Self::Text(..) | Self::VarChar(..) | Self::Char(..))
    }

    /// Returns `true` if this is any IEEE 754 floating-point type.
    #[inline]
    pub fn is_any_float(&self) -> bool {
        matches!(*self, Self::Float | Self::Double)
    }

    /// Returns the deepest nested type in [`DfType::Array`], otherwise returns `self`.
    #[inline]
    pub fn innermost_array_type(&self) -> &Self {
        let mut current = self;
        while let Self::Array(ty) = current {
            current = ty;
        }
        current
    }

    /// Returns this type by value if not [`DfType::Unknown`], otherwise returns `other`.
    ///
    /// See [`Option::or`].
    #[inline]
    pub fn or(self, other: Self) -> Self {
        if self.is_known() {
            self
        } else {
            other
        }
    }

    /// Returns this type by reference if not [`DfType::Unknown`], otherwise returns `other`.
    #[inline]
    pub fn or_ref<'a>(&'a self, other: &'a Self) -> &'a Self {
        if self.is_known() {
            self
        } else {
            other
        }
    }

    /// Returns this type by value if not [`DfType::Unknown`], otherwise calls `f` and returns the
    /// result.
    ///
    /// See [`Option::or_else`].
    #[inline]
    pub fn or_else<F>(self, f: F) -> Self
    where
        F: FnOnce() -> Self,
    {
        if self.is_known() {
            self
        } else {
            f()
        }
    }

    /// Returns this type by reference if not [`DfType::Unknown`], otherwise calls `f` and returns
    /// the result.
    #[inline]
    pub fn or_else_ref<'a, F>(&'a self, f: F) -> &'a Self
    where
        F: FnOnce() -> &'a Self,
    {
        if self.is_known() {
            self
        } else {
            f()
        }
    }
}

/// Postgresql type category. See [the docs][docs] for more information, and the [official list of
/// type categories][list].
///
/// [docs]: https://www.postgresql.org/docs/current/typeconv-overview.html
/// [list]: https://www.postgresql.org/docs/current/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTypeCategory {
    Array,
    Boolean,
    Composite,
    DateTime,
    Enum,
    Geometric,
    NetworkAddress,
    Numeric,
    Pseudo,
    Range,
    String,
    Timespan,
    UserDefined,
    BitString,
    Unknown,
}

/// Test helpers.
#[cfg(test)]
impl DfType {
    /// Nests this type into an array with the given dimension count.
    fn nest_in_array(mut self, dimen: usize) -> Self {
        for _ in 0..dimen {
            self = Self::Array(Box::new(self));
        }
        self
    }
}

impl Default for DfType {
    #[inline]
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for DfType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // This name is not consistent with `SqlType` but it's only used for logging.
        let kind = DfTypeKind::from(self);

        match *self {
            Self::Unknown
            | Self::Bool
            | Self::TinyInt
            | Self::UnsignedTinyInt
            | Self::SmallInt
            | Self::UnsignedSmallInt
            | Self::Int
            | Self::UnsignedInt
            | Self::BigInt
            | Self::UnsignedBigInt
            | Self::Float
            | Self::Double
            | Self::Text { .. }
            | Self::Blob
            | Self::VarBit(None)
            | Self::Date
            | Self::Inet
            | Self::MacAddr
            | Self::Uuid
            | Self::Json(_)
            | Self::Jsonb => write!(f, "{kind:?}"),

            Self::Array(ref ty) => write!(f, "{ty}[]"),
            Self::PassThrough(ref ty) => write!(f, "PassThrough({ty})"),

            Self::Char(n, ..)
            | Self::VarChar(n, ..)
            | Self::Binary(n)
            | Self::VarBinary(n)
            | Self::Bit(n)
            | Self::VarBit(Some(n))
            | Self::DateTime {
                subsecond_digits: n,
            }
            | Self::Time {
                subsecond_digits: n,
            }
            | Self::Timestamp {
                subsecond_digits: n,
            }
            | Self::TimestampTz {
                subsecond_digits: n,
            } => write!(f, "{kind:?}({n})"),

            Self::Enum(ref variants, _) => write!(f, "{kind:?}({})", variants.iter().join(", ")),
            Self::Numeric { prec, scale } => write!(f, "{kind:?}({prec}, {scale})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn innermost_array_type() {
        for ty in [DfType::DEFAULT_TEXT, DfType::Bool, DfType::Double] {
            for dimen in 0..=5 {
                let arr = ty.clone().nest_in_array(dimen);
                assert_eq!(arr.innermost_array_type(), &ty);
            }
        }
    }
}
