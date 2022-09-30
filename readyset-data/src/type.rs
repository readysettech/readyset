use std::fmt;

use enum_kinds::EnumKind;
use itertools::Itertools;
use nom_sql::{Dialect, EnumType, Literal, SqlType};
// use readyset_errors::{internal, ReadySetError};
use serde::{Deserialize, Serialize};

use crate::Collation;

/// Dataflow runtime representation of [`SqlType`].
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
    // TODO(ENG-1853): Remove dialect once `to_sql_type` is removed.
    Float(Dialect),

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
    // TODO(ENG-1853): Remove dialect once `to_sql_type` is removed.
    #[doc(alias = "bytea")]
    Blob(Dialect),

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
    ///
    /// The parameter determines the fractional seconds part (FSP),
    /// [which has a default value of 0](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html).
    DateTime(u16),

    /// [MySQL `time`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html).
    ///
    /// The parameter determines the fractional seconds part (FSP),
    /// [which has a default value of 0](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html).
    Time(u16),

    /// [MySQL `timestamp`](https://dev.mysql.com/doc/refman/8.0/en/datetime.html) or
    /// [PostgreSQL `timestamp`](https://www.postgresql.org/docs/current/datatype-datetime.html).
    ///
    /// The parameter determines the fractional seconds part (FSP),
    /// [which has a default value of 0](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html).
    Timestamp(u16),

    /// [PostgreSQL `timestamptz`/`timestamp with timezone`]().
    TimestampTz(u16),

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
}

/// Defaults.
impl DfType {
    pub const DEFAULT_NUMERIC: Self = Self::Numeric {
        prec: Self::DEFAULT_NUMERIC_PREC,
        scale: Self::DEFAULT_NUMERIC_SCALE,
    };

    pub const DEFAULT_NUMERIC_PREC: u16 = 10;
    pub const DEFAULT_NUMERIC_SCALE: u8 = 0;
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
        use Dialect::*;
        use SqlType::*;

        match *ty {
            Array(ref ty) => Self::Array(Box::new(Self::from_sql_type_impl(ty, dialect))),

            // PERF: Cloning enum types is O(1).
            Enum(ref ty) => Self::Enum(ty.clone(), dialect),

            // FIXME(ENG-1650): Convert to `tinyint(1)` for MySQL.
            Bool => Self::Bool,

            Serial => match dialect {
                MySQL => Self::UnsignedBigInt,
                PostgreSQL => Self::Int,
            },
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
            Float => match dialect {
                MySQL => Self::Float(dialect),
                PostgreSQL => Self::Double,
            },
            Real => match dialect {
                // TODO: Handle `real_as_float` mode.
                MySQL => Self::Double,
                PostgreSQL => Self::Float(dialect),
            },

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
            Text | TinyText | MediumText | LongText | VarChar(None) => {
                Self::Text(Collation::default())
            }
            VarChar(Some(len)) => Self::VarChar(len, Collation::default()),
            Char(len) => Self::Char(len.unwrap_or(1), Collation::default(), dialect),

            Blob | TinyBlob | MediumBlob | LongBlob | ByteArray => Self::Blob(dialect),
            VarBinary(len) => Self::VarBinary(len),
            Binary(len) => Self::Binary(len.unwrap_or(1)),

            Bit(len) => Self::Bit(len.unwrap_or(1)),
            VarBit(len) => Self::VarBit(len),

            Json => Self::Json(dialect),
            Jsonb => Self::Jsonb,

            Date => Self::Date,
            #[allow(clippy::or_fun_call)]
            DateTime(fsp) => Self::DateTime(fsp.unwrap_or(dialect.default_datetime_precision())),
            Time => Self::Time(dialect.default_datetime_precision()),
            Timestamp => Self::Timestamp(dialect.default_datetime_precision()),
            TimestampTz => Self::TimestampTz(dialect.default_datetime_precision()),

            Uuid => Self::Uuid,
            MacAddr => Self::MacAddr,
            Inet => Self::Inet,
        }
    }

    /// Attempts to convert back to a [`SqlType`].
    ///
    /// This is a lossy conversion: calling [`DfType::from_sql_type`] on the result may not produce
    /// the same exact type as `self`.
    ///
    /// This method is not inlined because it is commonly called, so we don't want to duplicate code
    /// such as any generated jump table.
    // TODO(ENG-1853): Once we have a `DfType`, we should not need to round-trip back to `SqlType`.
    pub fn to_sql_type(&self) -> Option<SqlType> {
        use SqlType::*;

        Some(match *self {
            Self::Unknown => return None,

            Self::Array(ref ty) => Array(Box::new(ty.to_sql_type()?)),

            Self::Bool => Bool,

            Self::TinyInt => TinyInt(None),
            Self::UnsignedTinyInt => UnsignedTinyInt(None),
            Self::SmallInt => SmallInt(None),
            Self::UnsignedSmallInt => UnsignedSmallInt(None),
            Self::Int => Int(None),
            Self::UnsignedInt => UnsignedInt(None),
            Self::BigInt => BigInt(None),
            Self::UnsignedBigInt => UnsignedBigInt(None),

            Self::Float(Dialect::MySQL) => Float,
            Self::Float(Dialect::PostgreSQL) => Real,
            Self::Double => Double,

            Self::Numeric { prec, scale } => Numeric(Some((prec, Some(scale)))),

            Self::Char(n, ..) => Char(Some(n)),
            Self::VarChar(n, ..) => VarChar(Some(n)),
            Self::Text { .. } => Text,

            Self::Binary(n) => Binary(Some(n)),
            Self::VarBinary(n) => VarBinary(n),
            Self::Blob(Dialect::MySQL) => Blob,
            Self::Blob(Dialect::PostgreSQL) => ByteArray,

            Self::Bit(n) => Bit(Some(n)),
            Self::VarBit(n) => VarBit(n),

            Self::Date => Date,
            Self::DateTime(fsp) => DateTime(Some(fsp)),
            Self::Time(_) => Time,
            Self::Timestamp(_) => Timestamp,
            Self::TimestampTz(_) => TimestampTz,

            Self::MacAddr => MacAddr,
            Self::Inet => Inet,
            Self::Uuid => Uuid,

            // PERF: Cloning enum types is O(1).
            Self::Enum(ref ty, _) => Enum(ty.clone()),

            Self::Json(_) => Json,
            Self::Jsonb => Jsonb,
        })
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

    /// Returns the SQL [`Dialect`] to determine this type's expression evaluation semantics.
    #[inline]
    pub fn dialect(&self) -> Option<Dialect> {
        use DfType::*;

        match *self {
            Binary(_) | VarBinary(_) | Date => Some(Dialect::MySQL),
            Array(_) | TimestampTz(_) | Jsonb | Inet | Uuid | MacAddr => Some(Dialect::PostgreSQL),

            Float(d) | Enum(.., d) | Char(.., d) | Blob(d) | Json(d) => Some(d),

            Unknown
            | Bool
            | Int
            | TinyInt
            | SmallInt
            | BigInt
            | UnsignedInt
            | UnsignedTinyInt
            | UnsignedSmallInt
            | UnsignedBigInt
            | Double
            | Numeric { .. }
            | Text { .. }
            | VarChar { .. }
            | Bit(_)
            | VarBit(_)
            | DateTime(_)
            | Time(_)
            | Timestamp(_) => None,
        }
    }

    /// Returns `true` if the type carries information.
    #[inline]
    pub fn is_known(&self) -> bool {
        !self.is_unknown()
    }

    /// Returns `true` if the type carries no information.
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

    /// Returns `true` if this is any IEEE 754 floating-point type.
    #[inline]
    pub fn is_any_float(&self) -> bool {
        matches!(*self, Self::Float(_) | Self::Double)
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
            | Self::Float(_)
            | Self::Double
            | Self::Text { .. }
            // XXX: Should we take into account PostgreSQL and use "ByteArray" or "ByteA"?
            | Self::Blob(_)
            | Self::VarBit(None)
            | Self::Date
            | Self::Inet
            | Self::MacAddr
            | Self::Uuid
            | Self::Json(_)
            | Self::Jsonb => write!(f, "{kind:?}"),

            Self::Array(ref ty) => write!(f, "{ty}[]"),

            Self::Char(n, ..)
            | Self::VarChar(n, ..)
            | Self::Binary(n)
            | Self::VarBinary(n)
            | Self::Bit(n)
            | Self::VarBit(Some(n))
            | Self::DateTime(n)
            | Self::Time(n)
            | Self::Timestamp(n)
            | Self::TimestampTz(n) => write!(f, "{kind:?}({n})"),

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
        for ty in [
            DfType::Text(Collation::default()),
            DfType::Bool,
            DfType::Double,
        ] {
            for dimen in 0..=5 {
                let arr = ty.clone().nest_in_array(dimen);
                assert_eq!(arr.innermost_array_type(), &ty);
            }
        }
    }

    #[test]
    fn to_sql_type() {
        let len: u16 = 42;
        let fsp: u16 = 6;

        let cases: &[(DfType, Option<SqlType>)] = &[
            (DfType::Unknown, None),
            (DfType::Bool, Some(SqlType::Bool)),
            (DfType::Int, Some(SqlType::Int(None))),
            (DfType::UnsignedInt, Some(SqlType::UnsignedInt(None))),
            (DfType::BigInt, Some(SqlType::BigInt(None))),
            (DfType::UnsignedBigInt, Some(SqlType::UnsignedBigInt(None))),
            (DfType::TinyInt, Some(SqlType::TinyInt(None))),
            (
                DfType::UnsignedTinyInt,
                Some(SqlType::UnsignedTinyInt(None)),
            ),
            (DfType::SmallInt, Some(SqlType::SmallInt(None))),
            (
                DfType::UnsignedSmallInt,
                Some(SqlType::UnsignedSmallInt(None)),
            ),
            (DfType::Float(Dialect::MySQL), Some(SqlType::Float)),
            (DfType::Float(Dialect::PostgreSQL), Some(SqlType::Real)),
            (DfType::Double, Some(SqlType::Double)),
            (
                DfType::Numeric {
                    prec: 42,
                    scale: 42,
                },
                Some(SqlType::Numeric(Some((42, Some(42))))),
            ),
            (DfType::Text(Collation::default()), Some(SqlType::Text)),
            (
                DfType::Char(len, Collation::default(), Dialect::MySQL),
                Some(SqlType::Char(Some(len))),
            ),
            (
                DfType::Char(len, Collation::default(), Dialect::PostgreSQL),
                Some(SqlType::Char(Some(len))),
            ),
            (
                DfType::VarChar(len, Collation::default()),
                Some(SqlType::VarChar(Some(len))),
            ),
            (DfType::Blob(Dialect::MySQL), Some(SqlType::Blob)),
            (DfType::Blob(Dialect::PostgreSQL), Some(SqlType::ByteArray)),
            (DfType::Binary(len), Some(SqlType::Binary(Some(len)))),
            (DfType::VarBinary(len), Some(SqlType::VarBinary(len))),
            (DfType::Bit(len), Some(SqlType::Bit(Some(len)))),
            (DfType::VarBit(Some(len)), Some(SqlType::VarBit(Some(len)))),
            (DfType::Date, Some(SqlType::Date)),
            (DfType::DateTime(fsp), Some(SqlType::DateTime(Some(fsp)))),
            (DfType::Time(fsp), Some(SqlType::Time)),
            (DfType::Timestamp(fsp), Some(SqlType::Timestamp)),
            (DfType::TimestampTz(fsp), Some(SqlType::TimestampTz)),
            (DfType::MacAddr, Some(SqlType::MacAddr)),
            (DfType::Inet, Some(SqlType::Inet)),
            (DfType::Uuid, Some(SqlType::Uuid)),
            (DfType::Json(Dialect::MySQL), Some(SqlType::Json)),
            (DfType::Json(Dialect::PostgreSQL), Some(SqlType::Json)),
            (DfType::Jsonb, Some(SqlType::Jsonb)),
        ];

        // Immediate conversion.
        for (df_type, sql_type) in cases {
            assert_eq!(&df_type.to_sql_type(), sql_type);
        }

        // Nested array conversion.
        for (df_type, sql_type) in cases {
            for dimen in 1..=5 {
                let df_type = df_type.clone().nest_in_array(dimen);
                let sql_type = sql_type.clone().map(|ty| ty.nest_in_array(dimen));
                assert_eq!(df_type.to_sql_type(), sql_type);
            }
        }
    }
}
