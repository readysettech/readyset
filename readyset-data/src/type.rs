use std::fmt;

use enum_kinds::EnumKind;
use itertools::Itertools;
use nom_sql::{EnumVariants, Relation, SqlIdentifier, SqlType};
use proptest::arbitrary::{any, any_with, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Just};
use readyset_errors::{unsupported, unsupported_err, ReadySetResult};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{Collation, Dialect};

/// Metadata about a postgresql enum type, optionally stored inside of `DfType::Enum` for enum types
/// that originate in postgres
#[derive(Clone, Arbitrary, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PgEnumMetadata {
    /// The name of the enum type
    pub name: SqlIdentifier,
    /// The postgres schema that the enum type is in
    pub schema: SqlIdentifier,
    /// The postgres `oid` of the enum type
    pub oid: u32,
    /// The postgres `oid` of the type for *arrays* of this enum type
    pub array_oid: u32,
}

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

    /// [`i8`] and Postgres's "char" (with quotes) type.
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
    ///
    /// Note: Postgres additionally has a different type named "char" (*with* the quotes!), which
    /// corresponds to a single 1-byte character. The naming of these two types aren't
    /// consistent across RS's code base, Postgres itself, and the postgres rust crate, so
    /// caution must be taken.
    // FIXME(ENG-1839): Should have `Option<u16>` to determine how `cast` is done for MySQL. The
    // dialect field provides context for semantics.
    Char(u16, Collation),

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

    /// Enum types
    Enum {
        variants: EnumVariants,

        /// Metadata about the enum type for PostgreSQL enums. For MySQL enums, this will always be
        /// `None`.
        metadata: Option<PgEnumMetadata>,
    },

    /// [MySQL `json`](https://dev.mysql.com/doc/refman/8.0/en/json.html) or
    /// [PostgreSQL `json`](https://www.postgresql.org/docs/current/datatype-json.html).
    Json,

    /// [PostgreSQL `jsonb`](https://www.postgresql.org/docs/current/datatype-json.html).
    Jsonb,
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
    /// Converts from a possible [`SqlType`] reference within the context of a SQL [`Dialect`],
    /// given a function to resolve named custom types in the schema
    pub fn from_sql_type<'a, T, R>(
        ty: T,
        dialect: Dialect,
        resolve_custom_type: R,
    ) -> ReadySetResult<Self>
    where
        T: Into<Option<&'a SqlType>>,
        R: Fn(Relation) -> Option<DfType>,
    {
        use SqlType::*;

        let ty = match ty.into() {
            Some(ty) => ty,
            None => return Ok(Self::Unknown),
        };

        Ok(match *ty {
            Array(ref ty) => Self::Array(Box::new(Self::from_sql_type(
                Some(ty.as_ref()),
                dialect,
                resolve_custom_type,
            )?)),

            Enum(ref variants) => Self::Enum {
                // PERF: Cloning variants is O(1).
                variants: variants.clone(),
                metadata: None,
            },

            // FIXME(ENG-1650): Convert to `tinyint(1)` for MySQL.
            Bool => Self::Bool,

            Serial => dialect.serial_type(),
            BigSerial => Self::BigInt,

            Int(_) | Int4 => Self::Int,
            TinyInt(_) => Self::TinyInt,
            SmallInt(_) | Int2 => Self::SmallInt,
            BigInt(_) | Int8 => Self::BigInt,
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
            Char(len) => Self::Char(len.unwrap_or(1), Collation::default()),
            QuotedChar => Self::TinyInt,

            Blob | TinyBlob | MediumBlob | LongBlob | ByteArray => Self::Blob,
            VarBinary(len) => Self::VarBinary(len),
            Binary(len) => Self::Binary(len.unwrap_or(1)),

            Bit(len) => Self::Bit(len.unwrap_or(1)),
            VarBit(len) => Self::VarBit(len),

            Json => Self::Json,
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
            Interval { .. } => unsupported!("Unsupported type: INTERVAL"),
            Uuid => Self::Uuid,
            MacAddr => Self::MacAddr,
            Inet => Self::Inet,
            Citext => Self::Text(Collation::Citext),
            Other(ref id) => resolve_custom_type(id.clone())
                .ok_or_else(|| unsupported_err!("Unsupported type: {}", id.display_unquoted()))?,
        })
    }
}

impl DfType {
    /// Creates a [`DfType::Enum`] instance from a sequence of variant names, a dialect, and
    /// optional metadata about the enum if it originated in a postgresql database
    #[inline]
    pub fn from_enum_variants<I>(variants: I, metadata: Option<PgEnumMetadata>) -> Self
    where
        I: IntoIterator<Item = String>,
        I::IntoIter: ExactSizeIterator, // required by `triomphe::ThinArc`
    {
        Self::Enum {
            variants: variants.into(),
            metadata,
        }
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
            DfType::Text(_) | DfType::Char(..) | DfType::VarChar(..) => PgTypeCategory::String,
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
            DfType::Uuid | DfType::Enum { .. } | DfType::Json | DfType::Jsonb => {
                PgTypeCategory::UserDefined
            }
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

    /// Returns `true` if this is the JSON type in MySQL or PostgreSQL.
    #[inline]
    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }

    /// Returns `true` if this is the PostgreSQL JSONB type.
    #[inline]
    pub fn is_jsonb(&self) -> bool {
        matches!(self, Self::Jsonb)
    }

    /// Returns `true` if this is either the JSON or JSONB type.
    #[inline]
    pub fn is_any_json(&self) -> bool {
        matches!(self, Self::Json | Self::Jsonb)
    }

    /// Returns `true` if this is any JSON-like type.
    #[inline]
    pub fn is_any_json_like(&self) -> bool {
        self.is_any_json() || self.is_any_text()
    }

    /// Returns `true` if this is the boolean type.
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, Self::Bool)
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

    /// Returns `true` if this is any PostgreSQL array type.
    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array { .. })
    }

    /// Returns `true` if this is any MySQL binary type.
    #[inline]
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary(_) | Self::VarBinary(_))
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

impl Arbitrary for DfType {
    type Parameters = ();

    // TODO(fran): Add numeric type. This is tricky since it is dependant on the database
    //  being used.
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::strategy::Strategy;

        let base_type = prop_oneof![
            Just(DfType::Unknown),
            Just(DfType::Bool),
            Just(DfType::Int),
            Just(DfType::UnsignedInt),
            Just(DfType::BigInt),
            Just(DfType::UnsignedBigInt),
            Just(DfType::TinyInt),
            Just(DfType::UnsignedTinyInt),
            Just(DfType::SmallInt),
            Just(DfType::UnsignedSmallInt),
            Just(DfType::Float),
            Just(DfType::Double),
            any::<Collation>().prop_map(DfType::Text),
            (1..255_u16, any::<Collation>()).prop_map(|(char, col)| DfType::Char(char, col)),
            (1..255_u16, any::<Collation>()).prop_map(|(char, col)| DfType::VarChar(char, col)),
            Just(DfType::Blob),
            any::<u16>().prop_map(DfType::Binary),
            any::<u16>().prop_map(DfType::VarBinary),
            any::<u16>().prop_map(DfType::Bit),
            any::<u16>().prop_map(DfType::Bit),
            any::<Option<u16>>().prop_map(DfType::VarBit),
            Just(DfType::Date),
            any::<u16>().prop_map(|subsecond_digits| DfType::DateTime { subsecond_digits }),
            any::<u16>().prop_map(|subsecond_digits| DfType::Time { subsecond_digits }),
            any::<u16>().prop_map(|subsecond_digits| DfType::Timestamp { subsecond_digits }),
            any::<u16>().prop_map(|subsecond_digits| DfType::TimestampTz { subsecond_digits }),
            Just(DfType::MacAddr),
            Just(DfType::Inet),
            Just(DfType::Uuid),
            (
                any_with::<EnumVariants>((".{0, 32}", (0..=20).into())),
                proptest::option::of(any::<PgEnumMetadata>())
            )
                .prop_map(|(variants, metadata)| DfType::Enum { variants, metadata }),
            Just(DfType::Json),
            Just(DfType::Jsonb),
        ];

        base_type
            .prop_recursive(4, 6, 1, |df_type_strat| {
                df_type_strat
                    .prop_map(|df_type| DfType::Array(Box::new(df_type)))
                    .boxed()
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<DfType>;
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
            | Self::Blob
            | Self::VarBit(None)
            | Self::Date
            | Self::Inet
            | Self::MacAddr
            | Self::Uuid
            | Self::Json
            | Self::Jsonb => write!(f, "{kind:?}"),

            Self::Text(collation) => {
                write!(f, "Text")?;
                if !collation.is_utf8() {
                    write!(f, "({collation})")?;
                }
                Ok(())
            }

            Self::Array(ref ty) => write!(f, "{ty}[]"),

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

            Self::Enum {
                ref variants,
                ref metadata,
            } => {
                write!(f, "Enum")?;
                if let Some(PgEnumMetadata { name, schema, .. }) = metadata {
                    write!(f, "[{schema}.{name}]")?;
                }
                write!(f, "({})", variants.iter().join(", "))
            }
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
