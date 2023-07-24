#![feature(box_patterns, iter_order_by)]

use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;
use std::{fmt, io, str};

use ::serde::{Deserialize, Serialize};
use bit_vec::BitVec;
use bytes::BytesMut;
use chrono::{self, DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use cidr::IpInet;
use enum_kinds::EnumKind;
use eui48::{MacAddress, MacAddressFormat};
use itertools::Itertools;
use mysql_time::MySqlTime;
use nom_sql::{Double, Float, Literal, SqlType};
use proptest::prelude::{prop_oneof, Arbitrary};
use readyset_errors::{internal, invalid_err, unsupported, ReadySetError, ReadySetResult};
use readyset_util::arbitrary::{arbitrary_decimal, arbitrary_duration};
use readyset_util::redacted::Sensitive;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use test_strategy::Arbitrary;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};
use uuid::Uuid;

mod array;
mod collation;
pub mod dialect;
mod r#enum;
mod float;
mod integer;
mod serde;
mod text;
mod timestamp;
mod r#type;

pub use ndarray::{ArrayD, IxDyn};

pub use crate::array::Array;
pub use crate::collation::Collation;
pub use crate::dialect::Dialect;
pub use crate::r#type::{DfType, PgEnumMetadata, PgTypeCategory};
pub use crate::serde::TextRef;
pub use crate::text::{Text, TinyText};
pub use crate::timestamp::{TimestampTz, TIMESTAMP_FORMAT, TIMESTAMP_PARSE_FORMAT};

type JsonObject = serde_json::Map<String, JsonValue>;

/// DateTime offsets must be bigger than -86_000 seconds and smaller than 86_000 (not inclusive in
/// either case), and we don't care about seconds, so our maximum offset is gonna be
/// 86_000 - 60 = 85_940.
const MAX_SECONDS_DATETIME_OFFSET: i32 = 85_940;

/// Used to wrap arbitrary Postgres types which aren't natively supported, enabling more extensive
/// proxying support
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PassThrough {
    pub ty: Type,
    pub data: Box<[u8]>,
}

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DfValue` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DfValue::deep_clone` to
/// clone the *value* of a `DfValue` without danger of contention.
#[warn(variant_size_differences)]
#[derive(Clone, Debug, EnumKind)]
#[enum_kind(DfValueKind, derive(Ord, PartialOrd, Hash, Arbitrary))]
#[derive(Default)]
pub enum DfValue {
    /// An empty value.
    #[default]
    None,
    /// A signed integer used to store every SQL integer type up to 64 bits.
    Int(i64),
    /// An unsigned integer used to store every unsigned SQL integer type up to 64 bits.
    UnsignedInt(u64),
    /// A floating point 32-bit real value.
    Float(f32),
    /// A floating point 64-bit real value.
    Double(f64),
    /// A reference-counted string-like value.
    Text(Text),
    /// A tiny string that fits in a pointer
    TinyText(TinyText),
    /// A timestamp with an optional time zone.
    TimestampTz(TimestampTz),
    /// A time duration
    /// NOTE: [`MySqlTime`] is from -838:59:59 to 838:59:59 whereas Postgres time is from 00:00:00
    /// to 24:00:00
    Time(MySqlTime),
    //NOTE(Fran): Using an `Arc` to keep the `DfValue` type 16 bytes long
    /// A byte array
    ByteArray(Arc<Vec<u8>>),
    /// A fixed-point fractional representation.
    Numeric(Arc<Decimal>),
    /// A bit or varbit value.
    BitVector(Arc<BitVec>),
    /// An array of [`DfValue`]s.
    Array(Arc<Array>),
    /// Container type for arbitrary unserialized, unsupported types
    PassThrough(Arc<PassThrough>),
    /// A sentinel maximal value.
    ///
    /// This value is always greater than all other [`DfValue`]s, except itself.
    ///
    /// This is a special value, as it cannot ever be constructed by user supplied values, and
    /// always returns an error when encountered during expression evaluation. Its only use is as
    /// the upper bound in a range query
    Max,
    // NOTE: when adding new DfValue variants:
    //
    // - make sure to always keep Max last - we use the order of the variants to compare
    // - remember to add that variant to:
    //   - The `proptest::Arbitrary` impl for `DfValue`,
    //   - The `example_row` in `src/serde.rs`
}

impl Eq for DfValue {}

impl fmt::Display for DfValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DfValue::None => write!(f, "NULL"),
            DfValue::Text(..) | DfValue::TinyText(..) => {
                let text: &str = <&str>::try_from(self).map_err(|_| fmt::Error)?;
                write!(f, "{}", text)
            }
            DfValue::Int(n) => write!(f, "{}", n),
            DfValue::UnsignedInt(n) => write!(f, "{}", n),
            DfValue::Float(n) => write!(f, "{}", n),
            DfValue::Double(n) => write!(f, "{}", n),
            DfValue::TimestampTz(ref ts) => write!(f, "{}", ts),
            DfValue::Time(ref t) => write!(f, "{}", t),
            DfValue::ByteArray(ref array) => {
                write!(
                    f,
                    "E'\\x{}'",
                    array.iter().map(|byte| format!("{:02x}", byte)).join("")
                )
            }
            DfValue::Numeric(ref d) => write!(f, "{}", d),
            DfValue::BitVector(ref b) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|bit| if bit { "1" } else { "0" }).join("")
                )
            }
            DfValue::Array(ref arr) => write!(f, "{}", arr),
            DfValue::PassThrough(ref p) => {
                write!(f, "[{}:{:x?}]", p.ty.name(), p.data)
            }
            DfValue::Max => f.write_str("MAX"),
        }
    }
}

/// The format for times when parsed as text
pub const TIME_FORMAT: &str = "%H:%M:%S";

impl DfValue {
    /// Construct a new [`DfValue::Array`] containing an empty array
    pub fn empty_array() -> Self {
        // TODO: static singleton empty array?
        DfValue::from(Vec::<DfValue>::new())
    }

    /// Construct a new `DfValue` from the given string value and [`Collation`]
    #[inline]
    pub fn from_str_and_collation(s: &str, collation: Collation) -> Self {
        if let Ok(mut tt) = TinyText::try_from(s) {
            tt.set_collation(collation);
            Self::TinyText(tt)
        } else {
            Self::Text(Text::from_str_with_collation(s, collation))
        }
    }

    /// If this [`DfValue`] represents a string value, return the collation of that string value,
    /// otherwise return None
    #[inline]
    pub fn collation(&self) -> Option<Collation> {
        match self {
            DfValue::Text(t) => Some(t.collation()),
            DfValue::TinyText(tt) => Some(tt.collation()),
            _ => None,
        }
    }

    /// Generates the minimum DfValue corresponding to the type of a given DfValue.
    pub fn min_value(other: &Self) -> Self {
        match other {
            DfValue::None => DfValue::None,
            DfValue::Text(_) | DfValue::TinyText(_) => DfValue::TinyText("".try_into().unwrap()), /* Safe because fits in length */
            DfValue::TimestampTz(_) => DfValue::from(
                FixedOffset::west(-MAX_SECONDS_DATETIME_OFFSET).from_utc_datetime(
                    &NaiveDateTime::new(NaiveDate::MIN, NaiveTime::from_hms(0, 0, 0)),
                ),
            ),
            DfValue::Float(..) => DfValue::Float(f32::MIN),
            DfValue::Double(..) => DfValue::Double(f64::MIN),
            DfValue::Int(_) => DfValue::Int(i64::min_value()),
            DfValue::UnsignedInt(_) => DfValue::UnsignedInt(0),
            DfValue::Time(_) => DfValue::Time(MySqlTime::min_value()),
            DfValue::ByteArray(_) => DfValue::ByteArray(Arc::new(Vec::new())),
            DfValue::Numeric(_) => DfValue::from(Decimal::MIN),
            DfValue::BitVector(_) => DfValue::from(BitVec::new()),
            DfValue::Array(_) => DfValue::empty_array(),
            DfValue::PassThrough(p) => DfValue::PassThrough(Arc::new(PassThrough {
                ty: p.ty.clone(),
                data: [].into(),
            })),
            DfValue::Max => DfValue::None,
        }
    }

    /// Generates the maximum DfValue corresponding to the type of a given DfValue.
    pub fn max_value(other: &Self) -> Self {
        match other {
            DfValue::None => DfValue::None,
            DfValue::TimestampTz(_) => DfValue::from(
                FixedOffset::east(MAX_SECONDS_DATETIME_OFFSET).from_utc_datetime(
                    &NaiveDateTime::new(NaiveDate::MAX, NaiveTime::from_hms(23, 59, 59)),
                ),
            ),
            DfValue::Float(..) => DfValue::Float(f32::MAX),
            DfValue::Double(..) => DfValue::Double(f64::MIN),
            DfValue::Int(_) => DfValue::Int(i64::max_value()),
            DfValue::UnsignedInt(_) => DfValue::UnsignedInt(u64::max_value()),
            DfValue::Time(_) => DfValue::Time(MySqlTime::max_value()),
            DfValue::Numeric(_) => DfValue::from(Decimal::MAX),
            DfValue::TinyText(_)
            | DfValue::Text(_)
            | DfValue::ByteArray(_)
            | DfValue::BitVector(_)
            | DfValue::Array(_)
            | DfValue::PassThrough(_)
            | DfValue::Max => DfValue::Max,
        }
    }

    /// Checks if this value is `DfValue::None`.
    pub fn is_none(&self) -> bool {
        matches!(*self, DfValue::None)
    }

    /// Checks if this value is of an integral data type (i.e., can be converted into integral
    /// types).
    pub fn is_integer(&self) -> bool {
        matches!(*self, DfValue::Int(_) | DfValue::UnsignedInt(_))
    }

    /// Checks if this value is of a real data type (i.e., can be converted into `f32` or `f64`).
    pub fn is_real(&self) -> bool {
        matches!(*self, DfValue::Float(_) | DfValue::Double(_))
    }

    /// Checks if this value is of a string data type (i.e., can be converted into `String` and
    /// `&str`).
    pub fn is_string(&self) -> bool {
        matches!(*self, DfValue::Text(_) | DfValue::TinyText(_))
    }

    /// Checks if this value is of a timestamp data type.
    pub fn is_datetime(&self) -> bool {
        matches!(*self, DfValue::TimestampTz(_))
    }

    /// Checks if this value is of a time data type.
    pub fn is_time(&self) -> bool {
        matches!(*self, DfValue::Time(_))
    }

    /// Checks if this value is of a byte array data type.
    pub fn is_byte_array(&self) -> bool {
        matches!(*self, DfValue::ByteArray(_))
    }

    /// Checks if this value is of a PostgreSQL array.
    pub fn is_array(&self) -> bool {
        matches!(*self, DfValue::Array(_))
    }

    /// Returns `true` if this value is truthy (is not 0, 0.0, '', or NULL).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use readyset_data::DfValue;
    ///
    /// assert!(!DfValue::None.is_truthy());
    /// assert!(!DfValue::Int(0).is_truthy());
    /// assert!(DfValue::Int(1).is_truthy());
    /// ```
    pub fn is_truthy(&self) -> bool {
        match *self {
            DfValue::None | DfValue::Max => false,
            DfValue::Int(x) => x != 0,
            DfValue::UnsignedInt(x) => x != 0,
            DfValue::Float(f) => f != 0.0,
            DfValue::Double(f) => f != 0.0,
            DfValue::Text(ref t) => !t.as_str().is_empty(),
            DfValue::TinyText(ref tt) => !tt.as_bytes().is_empty(),
            DfValue::TimestampTz(ref dt) => {
                dt.to_chrono().naive_local() != NaiveDate::from_ymd(0, 0, 0).and_hms(0, 0, 0)
            }
            DfValue::Time(ref t) => *t != MySqlTime::from_microseconds(0),
            DfValue::ByteArray(ref array) => !array.is_empty(),
            DfValue::Numeric(ref d) => !d.is_zero(),
            DfValue::BitVector(ref bits) => !bits.is_empty(),
            // Truthiness only matters for mysql, and mysql doesn't have arrays, so we can kind of
            // pick whatever we want here - but it makes the most sense to try to limit falsiness to
            // only the things that mysql considers falsey
            DfValue::Array(_) | DfValue::PassThrough(_) => true,
        }
    }

    /// Checks if the given DfValue::Double or DfValue::Float is equal to another DfValue::Double
    /// or DfValue::Float (respectively) under an acceptable error margin. If None is supplied,
    /// we use f32::EPSILON or f64::EPSILON, accordingly.
    pub fn equal_under_error_margin(&self, other: &DfValue, error_margin: Option<f64>) -> bool {
        macro_rules! numeric_comparison {
            ($self_num:expr, $other_num:expr, $error_margin:expr, $epsilon:expr) => {
                // Handle NaN and infinite case.
                if $self_num.is_nan()
                    || $other_num.is_nan()
                    || $self_num.is_infinite()
                    || $other_num.is_infinite()
                {
                    // If either self or other is either NaN or some version of infinite,
                    // compare bitwise instead.
                    $self_num.to_bits() == $other_num.to_bits()
                } else {
                    // Now that we've validated that both floats are valid numbers, and
                    // finite, we can compare within a margin of error.
                    ($self_num - $other_num).abs() < $error_margin.unwrap_or($epsilon)
                }
            };
        }
        match self {
            DfValue::Float(self_float) => match other {
                DfValue::Float(other_float) => {
                    let other_epsilon = error_margin.map(|f| f as f32);
                    numeric_comparison!(self_float, other_float, other_epsilon, f32::EPSILON)
                }
                _ => false,
            },
            DfValue::Double(self_double) => match other {
                DfValue::Double(other_double) => {
                    numeric_comparison!(self_double, other_double, error_margin, f64::EPSILON)
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Returns the SqlType for this DfValue, or None if [`DfValue::None`] (which is valid for any
    /// type)
    pub fn sql_type(&self) -> Option<SqlType> {
        use SqlType::*;
        match self {
            Self::None | Self::PassThrough(_) | Self::Max => None,
            Self::Int(_) => Some(BigInt(None)),
            Self::UnsignedInt(_) => Some(UnsignedBigInt(None)),
            // FIXME: `SqlType::Float` precision can be either single (MySQL) or
            // double (PostgreSQL).
            Self::Float(_) => Some(Float),
            Self::Double(_) => Some(Double),
            Self::Text(_) => Some(Text),
            Self::TinyText(_) => Some(TinyText),
            Self::TimestampTz(_) => Some(TimestampTz), // TODO: Timestamp if no tz
            Self::Time(_) => Some(Time),
            Self::ByteArray(_) => Some(ByteArray),
            Self::Numeric(_) => Some(Numeric(None)),
            Self::BitVector(_) => Some(VarBit(None)),
            // TODO: Once this returns DfType instead of SqlType, an empty array and an array of
            // null should be Array(Unknown) not Unknown.
            Self::Array(vs) => Some(SqlType::Array(Box::new(
                vs.values().find_map(|v| v.sql_type())?,
            ))),
        }
    }

    /// Returns a guess for the [`DfType`] that can represent this [`DfValue`].
    pub fn infer_dataflow_type(&self) -> DfType {
        use DfType::*;

        match self {
            Self::None | Self::PassThrough(_) | Self::Max => Unknown,
            Self::Int(_) => BigInt,
            Self::UnsignedInt(_) => UnsignedBigInt,
            Self::Float(_) => Float,
            Self::Double(_) => Double,
            Self::Text(t) => DfType::Text(t.collation()),
            Self::TinyText(t) => DfType::Text(t.collation()),
            Self::TimestampTz(ts) => {
                let subsecond_digits = u16::from(ts.subsecond_digits());
                if ts.has_timezone() {
                    TimestampTz { subsecond_digits }
                } else {
                    Timestamp { subsecond_digits }
                }
            }
            Self::Time(_) => Time {
                // TODO(ENG-1833): Make this based off of the time value.
                subsecond_digits: 0,
            },
            Self::ByteArray(_) => Blob,
            Self::Numeric(_) => DfType::DEFAULT_NUMERIC,
            Self::BitVector(_) => VarBit(None),
            Self::Array(array) => Array(Box::new(
                array
                    .values()
                    .map(Self::infer_dataflow_type)
                    .find(DfType::is_known)
                    .unwrap_or_default(),
            )),
        }
    }

    /// Returns `true` if the value does not contain a mix of inferred types.
    pub fn is_homogeneous(&self) -> bool {
        match self {
            Self::Array(array) => array.is_homogeneous(),
            _ => true,
        }
    }

    /// Attempt to coerce the given [`DfValue`] to a value of the given [`DfType`].
    ///
    /// Currently, this entails:
    ///
    /// * Coercing values to the type they already are
    /// * Parsing strings
    /// * Changing numeric type sizes (bigint -> int, int -> bigint, etc)
    /// * Converting [`Real`]s with a zero fractional part to an integer
    /// * Removing information from date/time data types (timestamps to dates or times)
    ///
    /// # Examples
    ///
    /// Reals can be converted to integers
    ///
    /// ```rust
    /// use readyset_data::{DfType, DfValue};
    ///
    /// let real = DfValue::Double(123.0);
    /// let int = real.coerce_to(&DfType::Int, &DfType::Unknown).unwrap();
    /// assert_eq!(int, DfValue::Int(123));
    /// ```
    ///
    /// Text can be parsed as a timestamp using the SQL `%Y-%m-%d %H:%M:%S` format:
    ///
    /// ```rust
    /// use std::borrow::Borrow;
    /// use std::convert::TryFrom;
    ///
    /// use chrono::NaiveDate;
    /// use readyset_data::{DfType, DfValue, Dialect};
    ///
    /// let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();
    /// let text = DfValue::from("2021-01-26 10:20:37");
    /// let timestamp = text
    ///     .coerce_to(&DfType::Timestamp { subsecond_digits }, &DfType::Unknown)
    ///     .unwrap();
    /// assert_eq!(
    ///     timestamp,
    ///     DfValue::TimestampTz(NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 20, 37).into())
    /// );
    /// ```
    pub fn coerce_to(&self, to_ty: &DfType, from_ty: &DfType) -> ReadySetResult<DfValue> {
        use crate::text::TextCoerce;

        let mk_err = || ReadySetError::DfValueConversionError {
            src_type: from_ty.to_string(),
            target_type: to_ty.to_string(),
            details: "unsupported".into(),
        };

        // Returns whether `self` can simply use `Clone` for coercion.
        let is_clone_coercible = || -> bool {
            // This check could produce invalid results for heterogenous arrays, but arrays are
            // never coerced with `Clone`.
            debug_assert!(self.is_homogeneous());

            to_ty.is_unknown()
                || self.infer_dataflow_type().try_into_known().as_ref() == Some(to_ty)
        };

        // This lets us avoid repeating this logic in multiple match arms for different int types.
        // Maybe there's a nicer way to do this with a closure instead of a macro, but I haven't
        // been able to find any way to define a closure that takes generic parameters, which is
        // necessary if we want to support both u64 and i64 for both UnsignedInt and Int.
        macro_rules! handle_enum_or_coerce_int {
            ($v:expr, $to_ty:expr, $from_ty:expr) => {
                if let DfType::Enum { variants, .. } = $from_ty {
                    // This will either be u64 or i64, and if it's i64 then negative values will be
                    // converted to 0 anyway, so unwrap_or_default gets us what we want here:
                    let enum_val = u64::try_from(*$v).unwrap_or_default();
                    r#enum::coerce_enum(enum_val, variants, $to_ty, $from_ty)
                } else {
                    integer::coerce_integer(*$v, $to_ty, $from_ty)
                }
            };
        }

        if to_ty.is_unknown() && from_ty.is_unknown() {
            return Ok(self.clone());
        }

        match self {
            DfValue::None => Ok(DfValue::None),
            DfValue::Array(arr) => match to_ty {
                DfType::Array(t) => Ok(DfValue::from(arr.coerce_to(t, from_ty)?)),
                DfType::Text(collation) => Ok(DfValue::from_str_and_collation(
                    &arr.to_string(),
                    *collation,
                )),
                _ => Err(mk_err()),
            },
            _ if is_clone_coercible() => Ok(self.clone()),
            DfValue::Text(t) => t.coerce_to(to_ty, from_ty),
            DfValue::TinyText(tt) => tt.coerce_to(to_ty, from_ty),
            DfValue::TimestampTz(tz) => tz.coerce_to(to_ty),
            DfValue::Int(v) => handle_enum_or_coerce_int!(v, to_ty, from_ty),
            DfValue::UnsignedInt(v) => handle_enum_or_coerce_int!(v, to_ty, from_ty),
            DfValue::Float(f) => float::coerce_f64(f64::from(*f), to_ty, from_ty),
            DfValue::Double(f) => float::coerce_f64(*f, to_ty, from_ty),
            DfValue::Numeric(d) => float::coerce_decimal(d.as_ref(), to_ty, from_ty),
            DfValue::Time(ts) => {
                if let DfType::Text(collation) = to_ty {
                    Ok(DfValue::from_str_and_collation(&ts.to_string(), *collation))
                } else {
                    Err(mk_err())
                }
            }
            DfValue::BitVector(vec) => match to_ty {
                DfType::VarBit(None) => Ok(self.clone()),
                DfType::VarBit(max_size_opt) => match max_size_opt {
                    Some(max_size) if vec.len() > *max_size as usize => Err(mk_err()),
                    _ => Ok(self.clone()),
                },
                _ => Err(mk_err()),
            },
            DfValue::ByteArray(_) | DfValue::Max => Err(mk_err()),
            DfValue::PassThrough(ref p) => Err(ReadySetError::DfValueConversionError {
                src_type: format!("PassThrough[{}]", p.ty),
                target_type: to_ty.to_string(),
                details: "PassThrough items cannot be coerced".into(),
            }),
        }
    }

    /// Mutates the given DfType value to match its underlying database representation for the
    /// given column schema.
    pub fn maybe_coerce_for_table_op(&mut self, col_ty: &DfType) -> ReadySetResult<()> {
        if col_ty.is_enum() {
            *self = self
                .coerce_to(col_ty, &DfType::Unknown)
                // There shouldn't be any cases where this coerce_to call returns an error, but if
                // it does then a value of 0 is generally a safe bet for enum values that don't
                // trigger the happy path:
                .unwrap_or(DfValue::Int(0));
        } else if col_ty.is_array() && col_ty.innermost_array_type().is_enum() {
            *self = self.coerce_to(col_ty, &DfType::Unknown)?;
        }

        Ok(())
    }

    /// If `self` represents any integer value, returns the integer.
    ///
    /// The returned integer is in the range of [`i64::MIN`] through [`u64::MAX`].
    #[inline]
    pub fn as_int(&self) -> Option<i128> {
        match *self {
            Self::Int(i) => Some(i.into()),
            Self::UnsignedInt(i) => Some(i.into()),
            _ => None,
        }
    }

    /// If `self` represents a string value, returns a reference to that string, otherwise returns
    /// `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            DfValue::Text(t) => Some(t.as_str()),
            DfValue::TinyText(t) => Some(t.as_str()),
            _ => None,
        }
    }

    /// If `self` represents a string value, returns a tuple of a reference to that string value and
    /// the [`Collation`], otherwise returns `None`
    pub fn as_str_and_collation(&self) -> Option<(&str, Collation)> {
        match self {
            DfValue::Text(t) => Some((t.as_str(), t.collation())),
            DfValue::TinyText(t) => Some((t.as_str(), t.collation())),
            _ => None,
        }
    }

    /// If `self` is [`DfValue::Array`], return a reference to the underlying [`Array`], otherwise
    /// return a [`ReadySetError::DfValueConversionError`] for all other [`DfValue`] variants.
    pub fn as_array(&self) -> ReadySetResult<&Array> {
        match self {
            DfValue::Array(array) => Ok(array),
            _ => Err(ReadySetError::DfValueConversionError {
                src_type: match self.sql_type() {
                    Some(ty) => ty.display(nom_sql::Dialect::MySQL).to_string(),
                    None => "Null".to_string(),
                },
                target_type: "Array".to_string(),
                details: "".to_string(),
            }),
        }
    }

    /// If `self` is [`DfValue::Text`], [`DfValue::TinyText`] or [`DfValue::ByteArray`], return a
    /// reference to the underlying [`Array`], otherwise return a
    /// [`ReadySetError::DfValueConversionError`] for all other [`DfValue`] variants.
    pub fn as_bytes(&self) -> ReadySetResult<&[u8]> {
        match self {
            DfValue::Text(text) => Ok(text.as_bytes()),
            DfValue::TinyText(text) => Ok(text.as_bytes()),
            DfValue::ByteArray(bytes) => Ok(bytes.as_ref()),
            _ => Err(ReadySetError::DfValueConversionError {
                src_type: match self.sql_type() {
                    Some(ty) => ty.display(nom_sql::Dialect::MySQL).to_string(),
                    None => "Null".to_string(),
                },
                target_type: "ByteArray".to_string(),
                details: "".to_string(),
            }),
        }
    }

    /// Transform this [`DfValue`] in preparation for being serialized to disk as part of an index.
    ///
    /// This function has the property that if `d1 == d2`, then
    /// `serialize(d1.transform_for_serialized_key()) ==
    /// serialize(d2.transform_for_serialized_key())`, which is not necessarily the case for eg
    /// `serialize(d1) == serialize(d2)`.
    #[inline]
    pub fn transform_for_serialized_key(&self) -> Cow<Self> {
        match self.as_str_and_collation() {
            Some((s, collation)) => match collation.normalize(s) {
                Cow::Borrowed(_) => Cow::Borrowed(self),
                Cow::Owned(s) => Cow::Owned(s.into()),
            },
            None => match self {
                DfValue::Float(f) => Cow::Owned((*f as f64).try_into().unwrap()),
                _ => Cow::Borrowed(self),
            },
        }
    }

    /// Returns Some(&self) if self is not [`DfValue::None`]
    ///
    /// # Examples
    ///
    /// ```
    /// use readyset_data::DfValue;
    ///
    /// assert_eq!(DfValue::Int(12).non_null(), Some(&DfValue::Int(12)));
    /// assert_eq!(DfValue::None.non_null(), None);
    /// ```
    pub fn non_null(&self) -> Option<&Self> {
        if self.is_none() {
            None
        } else {
            Some(self)
        }
    }

    /// Returns Some(self) if self is not [`DfValue::None`]
    ///
    /// # Examples
    ///
    /// ```
    /// use readyset_data::DfValue;
    ///
    /// assert_eq!(DfValue::Int(12).into_non_null(), Some(DfValue::Int(12)));
    /// assert_eq!(DfValue::None.into_non_null(), None);
    /// ```
    pub fn into_non_null(self) -> Option<Self> {
        if self.is_none() {
            None
        } else {
            Some(self)
        }
    }

    /// Attempts to convert self to a str and parse as JSON, returning a [`serde_json::Value`]
    /// wrapped in [`ReadySetResult`].
    ///
    /// # Examples
    ///
    /// ```
    /// use readyset_data::DfValue;
    ///
    /// assert_eq!(
    ///     DfValue::from("[1, 2, 3]").to_json(),
    ///     Ok(vec![1, 2, 3].into())
    /// );
    /// assert!(DfValue::from("🤯").to_json().is_err()); // Not a valid JSON string
    /// ```
    pub fn to_json(&self) -> ReadySetResult<JsonValue> {
        let json_str = <&str>::try_from(self)?;
        serde_json::from_str(json_str).map_err(|e| {
            invalid_err!(
                "Could not convert value to JSON: {}. JSON error: {}",
                Sensitive(self),
                Sensitive(&e)
            )
        })
    }

    /// Return a "normalized" version of `self`, for use in serializing lookup keys to rocksdb.
    ///
    /// Normalization may lose data, but it will always be the case that `(x == y) ==
    /// (serialize(x.normalize()) == serialize(y.normalize()))`.
    #[inline]
    pub fn normalize(self) -> Self {
        match self {
            DfValue::Numeric(d) => DfValue::from(d.normalize()),
            _ => self,
        }
    }
}

impl PartialEq for DfValue {
    fn eq(&self, other: &DfValue) -> bool {
        match (self, other) {
            (&DfValue::Text(..), &DfValue::Text(..))
            | (&DfValue::TinyText(..), &DfValue::TinyText(..))
            | (&DfValue::Text(..), &DfValue::TinyText(..))
            | (&DfValue::TinyText(..), &DfValue::Text(..)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let b: &str = <&str>::try_from(other).unwrap();
                #[allow(clippy::unwrap_used)] // All text values have a collation
                let collation = self.collation().unwrap();

                // [note: heterogenous-collations]
                // we use the collation of the lhs here because we have to use *something*, but in
                // practice we will never actually be comparing heterogenously-collated values (both
                // upstreams forbid it)
                collation.compare_strs(a, b) == Ordering::Equal
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), DfValue::TimestampTz(dt)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                let other_dt: Result<TimestampTz, _> = a.parse();
                other_dt.map(|other_dt| other_dt.eq(dt)).unwrap_or(false)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), DfValue::Time(t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                a.parse()
                    .map(|other_t: MySqlTime| t.eq(&other_t))
                    .unwrap_or(false)
            }
            (&DfValue::Int(a), &DfValue::Int(b)) => a == b,
            (&DfValue::UnsignedInt(a), &DfValue::UnsignedInt(b)) => a == b,
            (&DfValue::UnsignedInt(..), &DfValue::Int(..))
            | (&DfValue::Int(..), &DfValue::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();
                a == b
            }
            (&DfValue::Float(fa), &DfValue::Float(fb)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == fb.to_bits()
            }
            (&DfValue::Float(fa), DfValue::Numeric(d)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                d.to_f32()
                    .map(|df| fa.to_bits() == df.to_bits())
                    .unwrap_or(false)
            }
            (&DfValue::Double(fa), &DfValue::Double(fb)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == fb.to_bits()
            }
            (&DfValue::Double(fa), &DfValue::Float(fb))
            | (&DfValue::Float(fb), &DfValue::Double(fa)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == (fb as f64).to_bits()
            }
            (&DfValue::Double(fa), DfValue::Numeric(d)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                d.to_f64()
                    .map(|df| fa.to_bits() == df.to_bits())
                    .unwrap_or(false)
            }
            (DfValue::Numeric(da), DfValue::Numeric(db)) => da == db,
            (&DfValue::Numeric(_), &DfValue::Float(_) | &DfValue::Double(_)) => other == self,
            (
                &DfValue::Time(_) | &DfValue::TimestampTz(_),
                &DfValue::Text(..) | &DfValue::TinyText(..),
            ) => other == self,
            (&DfValue::TimestampTz(tsa), &DfValue::TimestampTz(tsb)) => tsa == tsb,
            (DfValue::Time(ta), DfValue::Time(tb)) => ta == tb,
            (DfValue::ByteArray(array_a), DfValue::ByteArray(array_b)) => {
                array_a.as_ref() == array_b.as_ref()
            }
            (DfValue::BitVector(bits_a), DfValue::BitVector(bits_b)) => {
                bits_a.as_ref() == bits_b.as_ref()
            }
            (DfValue::Array(vs_a), DfValue::Array(vs_b)) => vs_a == vs_b,
            (&DfValue::None, &DfValue::None) => true,
            (&DfValue::Max, &DfValue::Max) => true,
            _ => false,
        }
    }
}

impl PartialOrd for DfValue {
    fn partial_cmp(&self, other: &DfValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DfValue {
    fn cmp(&self, other: &DfValue) -> Ordering {
        match (self, other) {
            (&DfValue::Text(..), &DfValue::Text(..))
            | (&DfValue::TinyText(..), &DfValue::TinyText(..))
            | (&DfValue::Text(..), &DfValue::TinyText(..))
            | (&DfValue::TinyText(..), &DfValue::Text(..)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let b: &str = <&str>::try_from(other).unwrap();
                #[allow(clippy::unwrap_used)] // All text values have a collation
                let collation = self.collation().unwrap();

                // See [note: heterogenous-collations]
                collation.compare_strs(a, b)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), DfValue::TimestampTz(other_dt)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                let dt: Result<TimestampTz, _> = a.parse();
                dt.map(|dt| dt.cmp(other_dt)).unwrap_or(Ordering::Greater)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), DfValue::Time(other_t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap().parse();
                a.map(|t: MySqlTime| t.cmp(other_t))
                    .unwrap_or(Ordering::Greater)
            }
            (
                &DfValue::Time(_) | &DfValue::TimestampTz(_),
                &DfValue::Text(..) | &DfValue::TinyText(..),
            ) => other.cmp(self).reverse(),
            (&DfValue::Int(a), &DfValue::Int(b)) => a.cmp(&b),
            (&DfValue::UnsignedInt(a), &DfValue::UnsignedInt(b)) => a.cmp(&b),
            (&DfValue::UnsignedInt(..), &DfValue::Int(..))
            | (&DfValue::Int(..), &DfValue::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for i128 (&i128 on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();
                a.cmp(&b)
            }
            (&DfValue::Float(fa), &DfValue::Float(fb)) => fa.total_cmp(&fb),
            (&DfValue::Double(fa), &DfValue::Double(fb)) => fa.total_cmp(&fb),
            (DfValue::Numeric(da), DfValue::Numeric(db)) => da.cmp(db),
            (&DfValue::Float(fa), &DfValue::Double(fb)) => fa.total_cmp(&(fb as f32)),
            (&DfValue::Double(fa), &DfValue::Float(fb)) => fb.total_cmp(&(fa as f32)).reverse(),
            (&DfValue::Float(fa), DfValue::Numeric(d)) => {
                if let Some(da) = Decimal::from_f32_retain(fa) {
                    da.cmp(d)
                } else {
                    d.to_f32()
                        .as_ref()
                        .map(|fb| fa.total_cmp(fb))
                        .unwrap_or(Ordering::Greater)
                }
            }
            (&DfValue::Double(fa), DfValue::Numeric(d)) => {
                if let Some(da) = Decimal::from_f64_retain(fa) {
                    da.cmp(d)
                } else {
                    d.to_f64()
                        .as_ref()
                        .map(|fb| fa.total_cmp(fb))
                        .unwrap_or(Ordering::Greater)
                }
            }
            (&DfValue::Numeric(_), &DfValue::Float(_) | &DfValue::Double(_)) => {
                other.cmp(self).reverse()
            }
            (DfValue::TimestampTz(tsa), DfValue::TimestampTz(tsb)) => tsa.cmp(tsb),
            (DfValue::Time(ta), DfValue::Time(tb)) => ta.cmp(tb),

            // Convert ints to f32 and cmp against Float.
            (&DfValue::Int(..), &DfValue::Float(b, ..))
            | (&DfValue::UnsignedInt(..), &DfValue::Float(b, ..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a = <i128>::try_from(self).unwrap();

                (a as f32).total_cmp(&b)
            }
            // Convert ints to double and cmp against Real.
            (&DfValue::Int(..), &DfValue::Double(b, ..))
            | (&DfValue::UnsignedInt(..), &DfValue::Double(b, ..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();

                (a as f64).total_cmp(&b)
            }
            // Convert ints to f32 and cmp against Float.
            (&DfValue::Int(..), &DfValue::Numeric(ref b))
            | (&DfValue::UnsignedInt(..), &DfValue::Numeric(ref b)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();

                Decimal::from(a).cmp(b)
            }
            (&DfValue::Float(a, ..), &DfValue::Int(..))
            | (&DfValue::Float(a, ..), &DfValue::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128)  on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();

                a.total_cmp(&(b as f32))
            }
            (&DfValue::Double(a, ..), &DfValue::Int(..))
            | (&DfValue::Double(a, ..), &DfValue::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();

                a.total_cmp(&(b as f64))
            }
            (&DfValue::Numeric(_), &DfValue::Int(..))
            | (&DfValue::Numeric(_), &DfValue::UnsignedInt(..)) => other.cmp(self).reverse(),
            (DfValue::ByteArray(array_a), DfValue::ByteArray(array_b)) => array_a.cmp(array_b),
            (DfValue::BitVector(bits_a), DfValue::BitVector(bits_b)) => bits_a.cmp(bits_b),
            (DfValue::Array(vs_a), DfValue::Array(vs_b)) => vs_a.cmp(vs_b),

            // for all other kinds of data types, just compare the variants in order
            (_, _) => DfValueKind::from(self).cmp(&DfValueKind::from(other)),
        }
    }
}

impl Hash for DfValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The default derived hash function also hashes the variant tag, which turns out to be
        // rather expensive. This version could (but probably won't) have a higher rate of
        // collisions, but the decreased overhead is worth it.
        match *self {
            DfValue::None => {}
            DfValue::Max => 1i64.hash(state),
            DfValue::Int(n) => n.hash(state),
            DfValue::UnsignedInt(n) => n.hash(state),
            DfValue::Float(f) => (f as f64).to_bits().hash(state),
            DfValue::Double(f) => f.to_bits().hash(state),
            DfValue::Text(..) | DfValue::TinyText(..) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let t: &str = <&str>::try_from(self).unwrap();
                #[allow(clippy::unwrap_used)] // All text values have a collation
                let collation = self.collation().unwrap();
                collation.hash_str(t, state)
            }
            DfValue::TimestampTz(ts) => ts.hash(state),
            DfValue::Time(ref t) => t.hash(state),
            DfValue::ByteArray(ref array) => array.hash(state),
            DfValue::Numeric(ref d) => d.hash(state),
            DfValue::BitVector(ref bits) => bits.hash(state),
            DfValue::Array(ref vs) => vs.hash(state),
            DfValue::PassThrough(ref p) => p.hash(state),
        }
    }
}

impl<T> From<Option<T>> for DfValue
where
    DfValue: From<T>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(t) => DfValue::from(t),
            None => DfValue::None,
        }
    }
}

impl TryFrom<i128> for DfValue {
    type Error = ReadySetError;

    fn try_from(i: i128) -> Result<Self, Self::Error> {
        if let Ok(i) = i64::try_from(i) {
            Ok(i.into())
        } else if let Ok(i) = u64::try_from(i) {
            Ok(i.into())
        } else {
            Err(ReadySetError::DfValueConversionError {
                src_type: "i128".to_string(),
                target_type: "DfValue".to_string(),
                details: "unsupported".to_string(),
            })
        }
    }
}

macro_rules! signed_integer_into_value {
    ($int:ty) => {
        impl From<$int> for DfValue {
            #[inline]
            fn from(i: $int) -> Self {
                Self::Int(i as i64)
            }
        }
    };
    ($($int:ty),+) => {
        $(signed_integer_into_value!($int);)+
    };
}

macro_rules! unsigned_integer_into_value {
    ($int:ty) => {
        impl From<$int> for DfValue {
            #[inline]
            fn from(i: $int) -> Self {
                Self::UnsignedInt(i as u64)
            }
        }
    };
    ($($int:ty),+) => {
        $(unsigned_integer_into_value!($int);)+
    };
}

signed_integer_into_value!(isize, i64, i32, i16, i8);
unsigned_integer_into_value!(usize, u64, u32, u16, u8);

impl TryFrom<f32> for DfValue {
    type Error = ReadySetError;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        if !f.is_finite() {
            return Err(Self::Error::DfValueConversionError {
                src_type: "f32".to_string(),
                target_type: "DfValue".to_string(),
                details: "".to_string(),
            });
        }

        Ok(DfValue::Float(f))
    }
}

impl TryFrom<f64> for DfValue {
    type Error = ReadySetError;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        if !f.is_finite() {
            return Err(Self::Error::DfValueConversionError {
                src_type: "f64".to_string(),
                target_type: "DfValue".to_string(),
                details: "".to_string(),
            });
        }

        Ok(DfValue::Double(f))
    }
}

impl From<Decimal> for DfValue {
    fn from(d: Decimal) -> Self {
        DfValue::Numeric(Arc::new(d))
    }
}

impl<'a> TryFrom<&'a DfValue> for Decimal {
    type Error = ReadySetError;

    fn try_from(dt: &'a DfValue) -> Result<Self, Self::Error> {
        match dt {
            DfValue::Int(i) => Ok(Decimal::from(*i)),
            DfValue::UnsignedInt(i) => Ok(Decimal::from(*i)),
            DfValue::Float(value) => {
                Decimal::from_f32(*value).ok_or_else(|| Self::Error::DfValueConversionError {
                    src_type: "DfValue".to_string(),
                    target_type: "Decimal".to_string(),
                    details: "".to_string(),
                })
            }
            DfValue::Double(value) => {
                Decimal::from_f64(*value).ok_or_else(|| Self::Error::DfValueConversionError {
                    src_type: "DfValue".to_string(),
                    target_type: "Decimal".to_string(),
                    details: "".to_string(),
                })
            }
            DfValue::Numeric(d) => Ok(*d.as_ref()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "Decimal".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

/// Bit vectors are represented as [`BitVec`].
impl From<BitVec> for DfValue {
    fn from(b: BitVec) -> Self {
        DfValue::BitVector(Arc::new(b))
    }
}

impl<'a> TryFrom<&'a DfValue> for BitVec {
    type Error = ReadySetError;

    fn try_from(dt: &'a DfValue) -> Result<Self, Self::Error> {
        match dt {
            DfValue::BitVector(ref bits) => Ok(bits.as_ref().clone()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "Decimal".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

/// Booleans are represented as `u32`s which are equal to either 0 or 1
impl From<bool> for DfValue {
    fn from(b: bool) -> Self {
        DfValue::from(b as u32)
    }
}

impl<'a> From<&'a DfValue> for DfValue {
    fn from(dt: &'a DfValue) -> Self {
        dt.clone()
    }
}

impl<'a> TryFrom<&'a Literal> for DfValue {
    type Error = ReadySetError;

    fn try_from(l: &'a Literal) -> Result<Self, Self::Error> {
        match l {
            Literal::Null => Ok(DfValue::None),
            Literal::Boolean(b) => Ok(DfValue::from(*b)),
            Literal::Integer(i) => Ok((*i).into()),
            Literal::UnsignedInteger(i) => Ok((*i).into()),
            Literal::String(s) => Ok(s.as_str().into()),
            Literal::Float(ref float) => Ok(DfValue::Float(float.value)),
            Literal::Double(ref double) => Ok(DfValue::Double(double.value)),
            Literal::Numeric(i, s) => Decimal::try_from_i128_with_scale(*i, *s)
                .map_err(|e| ReadySetError::DfValueConversionError {
                    src_type: "Literal".to_string(),
                    target_type: "DfValue".to_string(),
                    details: format!("Values out-of-bounds for Numeric type. Error: {}", e),
                })
                .map(|d| DfValue::Numeric(Arc::new(d))),
            Literal::Blob(b) => Ok(DfValue::from(b.as_slice())),
            Literal::ByteArray(b) => Ok(DfValue::ByteArray(Arc::new(b.clone()))),
            Literal::BitVector(b) => Ok(DfValue::from(BitVec::from_bytes(b.as_slice()))),
            Literal::Placeholder(_) => {
                internal!("Tried to convert a Placeholder literal to a DfValue")
            }
        }
    }
}

impl TryFrom<Literal> for DfValue {
    type Error = ReadySetError;

    fn try_from(l: Literal) -> Result<Self, Self::Error> {
        (&l).try_into()
    }
}

impl TryFrom<DfValue> for Literal {
    type Error = ReadySetError;

    fn try_from(value: DfValue) -> Result<Self, Self::Error> {
        match value {
            DfValue::None => Ok(Literal::Null),
            DfValue::Int(i) => Ok(Literal::Integer(i)),
            DfValue::UnsignedInt(i) => Ok(Literal::Integer(i as _)),
            DfValue::Float(value) => Ok(Literal::Float(Float {
                value,
                precision: u8::MAX,
            })),
            DfValue::Double(value) => Ok(Literal::Double(Double {
                value,
                precision: u8::MAX,
            })),
            DfValue::Text(_) => Ok(Literal::String(String::try_from(value)?)),
            DfValue::TinyText(_) => Ok(Literal::String(String::try_from(value)?)),
            DfValue::TimestampTz(_) => Ok(Literal::String(String::try_from(
                value.coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)?,
            )?)),
            DfValue::Time(_) => Ok(Literal::String(String::try_from(
                value.coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)?,
            )?)),
            DfValue::ByteArray(ref array) => Ok(Literal::ByteArray(array.as_ref().clone())),
            DfValue::Numeric(ref d) => Ok(Literal::Numeric(d.mantissa(), d.scale())),
            DfValue::BitVector(ref bits) => Ok(Literal::BitVector(bits.as_ref().to_bytes())),
            DfValue::Array(_) => unsupported!("Arrays not implemented yet"),
            DfValue::PassThrough(_) => internal!("PassThrough has no representation as a literal"),
            DfValue::Max => internal!("MAX has no representation as a literal"),
        }
    }
}

impl From<NaiveTime> for DfValue {
    fn from(t: NaiveTime) -> Self {
        DfValue::Time(t.into())
    }
}

impl From<MySqlTime> for DfValue {
    fn from(t: MySqlTime) -> Self {
        DfValue::Time(t)
    }
}

impl From<Vec<u8>> for DfValue {
    fn from(t: Vec<u8>) -> Self {
        DfValue::ByteArray(Arc::new(t))
    }
}

impl From<NaiveDate> for DfValue {
    fn from(dt: NaiveDate) -> Self {
        DfValue::TimestampTz(dt.into())
    }
}

impl From<NaiveDateTime> for DfValue {
    fn from(dt: NaiveDateTime) -> Self {
        DfValue::TimestampTz(dt.into())
    }
}

impl From<DateTime<FixedOffset>> for DfValue {
    fn from(dt: DateTime<FixedOffset>) -> Self {
        DfValue::TimestampTz(dt.into())
    }
}

impl<'a> TryFrom<&'a DfValue> for NaiveDateTime {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::TimestampTz(ref dt) => Ok(dt.to_chrono().naive_utc()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "NaiveDateTime".to_string(),
                details: "".to_string(),
            }),
        }
    }
}
impl<'a> TryFrom<&'a DfValue> for DateTime<FixedOffset> {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::TimestampTz(dt) => Ok(dt.to_chrono()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "DateTime<FixedOffset>".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DfValue> for NaiveDate {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::TimestampTz(ref dt) => Ok(dt.to_chrono().naive_local().date()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "NaiveDate".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DfValue> for MySqlTime {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Time(ref mysql_time) => Ok(*mysql_time),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "MySqlTime".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DfValue> for Vec<u8> {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Text(ref t) => Ok(t.as_bytes().to_vec()),
            DfValue::TinyText(ref tt) => Ok(tt.as_str().as_bytes().to_vec()),
            DfValue::ByteArray(ref array) => Ok(array.as_ref().clone()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "Vec<u8>".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DfValue> for &'a str {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        data.as_str()
            .ok_or_else(|| ReadySetError::DfValueConversionError {
                src_type: match data.sql_type() {
                    Some(ty) => ty.display(nom_sql::Dialect::MySQL).to_string(),
                    None => "Null".to_string(),
                },
                target_type: "&str".to_string(),
                details: "".to_string(),
            })
    }
}

impl TryFrom<DfValue> for Vec<u8> {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        match data {
            DfValue::Text(t) => Ok(t.as_bytes().to_vec()),
            DfValue::TinyText(tt) => Ok(tt.as_str().as_bytes().to_vec()),
            DfValue::ByteArray(bytes) => Ok(bytes.as_ref().clone()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: match data.sql_type() {
                    Some(ty) => ty.display(nom_sql::Dialect::MySQL).to_string(),
                    None => "Null".to_string(),
                },
                target_type: "&str".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

/// Implements `TryFrom<&DfValue>` and `TryFrom<DfValue>` for an integer type using its
/// `TryFrom<i128>` implementation.
macro_rules! integer_try_from {
    ($int:ty) => {
        impl TryFrom<&DfValue> for $int {
            type Error = ReadySetError;

            fn try_from(data: &DfValue) -> Result<Self, Self::Error> {
                let error = |details: String| ReadySetError::DfValueConversionError {
                    src_type: "DfValue".to_owned(),
                    target_type: stringify!($int).to_owned(),
                    details,
                };

                data.as_int()
                    .ok_or_else(|| error("unsupported".to_owned()))
                    .and_then(|i| {
                        Self::try_from(i)
                            .map_err(|_| error(format!("out of bounds {}", Sensitive(&i))))
                    })
            }
        }

        impl TryFrom<DfValue> for $int {
            type Error = ReadySetError;

            #[inline]
            fn try_from(data: DfValue) -> Result<Self, Self::Error> {
                Self::try_from(&data)
            }
        }
    };
    ($($int:ty),+) => {
        $(integer_try_from!($int);)+
    };
}

integer_try_from!(usize, isize, u128, i128, u64, i64, u32, i32, u16, i16, u8, i8);

impl TryFrom<DfValue> for bool {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        (&data).try_into()
    }
}

impl TryFrom<&DfValue> for bool {
    type Error = ReadySetError;

    fn try_from(data: &DfValue) -> Result<Self, Self::Error> {
        data.as_int()
            .map(|i| i != 0)
            .ok_or_else(|| ReadySetError::DfValueConversionError {
                src_type: "DfValue".to_owned(),
                target_type: "bool".to_owned(),
                details: "".to_owned(),
            })
    }
}

impl TryFrom<DfValue> for f32 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <f32>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for f32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Float(f) => Ok(f),
            DfValue::Double(f) => Ok(f as f32),
            DfValue::Numeric(ref d) => {
                d.to_f32()
                    .ok_or_else(|| Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "f32".to_string(),
                        details: "".to_string(),
                    })
            }
            DfValue::UnsignedInt(i) => Ok(i as f32),
            DfValue::Int(i) => Ok(i as f32),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DfValue> for f64 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <f64>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for f64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Float(f) => Ok(f as f64),
            DfValue::Double(f) => Ok(f),
            DfValue::Numeric(ref d) => {
                d.to_f64()
                    .ok_or_else(|| Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "f32".to_string(),
                        details: "".to_string(),
                    })
            }
            DfValue::UnsignedInt(i) => Ok(i as f64),
            DfValue::Int(i) => Ok(i as f64),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "f64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl From<String> for DfValue {
    fn from(s: String) -> Self {
        DfValue::from(s.as_str())
    }
}

impl TryFrom<&'_ DfValue> for String {
    type Error = ReadySetError;

    fn try_from(dt: &DfValue) -> Result<Self, Self::Error> {
        let s: &str = <&str>::try_from(dt)?;
        Ok(s.into())
    }
}

impl TryFrom<DfValue> for String {
    type Error = ReadySetError;

    fn try_from(dt: DfValue) -> Result<Self, Self::Error> {
        String::try_from(&dt)
    }
}

impl<'a> From<&'a str> for DfValue {
    fn from(s: &'a str) -> Self {
        Self::from_str_and_collation(s, Default::default())
    }
}

impl From<&[u8]> for DfValue {
    fn from(b: &[u8]) -> Self {
        // NOTE: should we *really* be converting to Text here?
        if let Ok(s) = str::from_utf8(b) {
            s.into()
        } else {
            DfValue::ByteArray(b.to_vec().into())
        }
    }
}

impl From<Array> for DfValue {
    fn from(arr: Array) -> Self {
        Self::Array(Arc::new(arr))
    }
}

impl From<Vec<DfValue>> for DfValue {
    fn from(vs: Vec<DfValue>) -> Self {
        Self::from(Array::from(vs))
    }
}

impl From<JsonValue> for DfValue {
    fn from(value: JsonValue) -> Self {
        (&value).into()
    }
}

impl From<&JsonValue> for DfValue {
    fn from(value: &JsonValue) -> Self {
        value.to_string().into()
    }
}

impl From<JsonObject> for DfValue {
    fn from(value: JsonObject) -> Self {
        (&value).into()
    }
}

impl From<&JsonObject> for DfValue {
    fn from(value: &JsonObject) -> Self {
        // This should not fail because `JsonValue` (of which `JsonObject` is a variant) should also
        // not fail conversion to `String`.
        #[allow(clippy::unwrap_used)]
        serde_json::to_string(value).unwrap().into()
    }
}

impl From<Vec<JsonValue>> for DfValue {
    fn from(value: Vec<JsonValue>) -> Self {
        value.as_slice().into()
    }
}

impl From<&[JsonValue]> for DfValue {
    fn from(value: &[JsonValue]) -> Self {
        // This should not fail because `JsonValue` (of which `Vec<JsonValue>` is a variant) should
        // also not fail conversion to `String`.
        #[allow(clippy::unwrap_used)]
        serde_json::to_string(value).unwrap().into()
    }
}

impl TryFrom<(Type, String)> for DfValue {
    type Error = ReadySetError;

    fn try_from((pgtype, s): (Type, String)) -> Result<Self, Self::Error> {
        DfValue::from_sql(&pgtype, s.as_bytes()).map_err(|e| {
            ReadySetError::DfValueConversionError {
                src_type: "(tokio_postgres::types::Type, String)".into(),
                target_type: "DfValue".into(),
                details: e.to_string(),
            }
        })
    }
}

impl TryFrom<mysql_common::value::Value> for DfValue {
    type Error = ReadySetError;

    fn try_from(v: mysql_common::value::Value) -> Result<Self, Self::Error> {
        DfValue::try_from(&v)
    }
}

impl TryFrom<&mysql_common::value::Value> for DfValue {
    type Error = ReadySetError;

    fn try_from(v: &mysql_common::value::Value) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match v {
            Value::NULL => Ok(DfValue::None),
            Value::Bytes(v) => Ok(DfValue::from(&v[..])),
            Value::Int(v) => Ok(DfValue::from(*v)),
            Value::UInt(v) => Ok(DfValue::from(*v)),
            Value::Float(v) => DfValue::try_from(*v),
            Value::Double(v) => DfValue::try_from(*v),
            Value::Date(year, month, day, hour, minutes, seconds, micros) => {
                if let Some(dt) =
                    NaiveDate::from_ymd_opt((*year).into(), (*month).into(), (*day).into())
                {
                    if let Some(dt) = dt.and_hms_micro_opt(
                        (*hour).into(),
                        (*minutes).into(),
                        (*seconds).into(),
                        *micros,
                    ) {
                        return Ok(DfValue::TimestampTz(dt.into()));
                    }
                }
                Ok(DfValue::None)
            }
            Value::Time(neg, days, hours, minutes, seconds, microseconds) => {
                Ok(DfValue::Time(MySqlTime::from_hmsus(
                    !neg,
                    <u16>::try_from(*hours as u32 + days * 24u32).unwrap_or(u16::MAX),
                    *minutes,
                    *seconds,
                    (*microseconds).into(),
                )))
            }
        }
    }
}

impl ToSql for DfValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Sync + Send>> {
        match (self, ty) {
            (Self::None | Self::Max, _) => None::<i8>.to_sql(ty, out),
            (Self::Int(x), &Type::CHAR) => i8::try_from(*x)?.to_sql(ty, out),
            (Self::UnsignedInt(x), &Type::CHAR) => i8::try_from(*x)?.to_sql(ty, out),
            (Self::Int(x), &Type::INT2) => (*x as i16).to_sql(ty, out),
            (Self::Int(x), &Type::INT4) => (*x as i32).to_sql(ty, out),
            (Self::Int(x), _) => x.to_sql(ty, out),
            (Self::UnsignedInt(x), &Type::BOOL) => (*x != 0).to_sql(ty, out),
            (
                Self::UnsignedInt(x),
                &Type::OID
                | &Type::REGCLASS
                | &Type::REGCOLLATION
                | &Type::REGCONFIG
                | &Type::REGDICTIONARY
                | &Type::REGNAMESPACE
                | &Type::REGOPER
                | &Type::REGOPERATOR
                | &Type::REGPROC
                | &Type::REGPROCEDURE
                | &Type::REGROLE
                | &Type::REGTYPE,
            ) => u32::try_from(*x)?.to_sql(ty, out),
            (Self::UnsignedInt(x), _) => (*x as i64).to_sql(ty, out),
            (Self::Float(x), _) => x.to_sql(ty, out),
            (Self::Double(x), _) => x.to_sql(ty, out),
            (Self::Numeric(d), _) => d.to_sql(ty, out),
            (Self::Text(_) | Self::TinyText(_), &Type::MACADDR) => {
                MacAddress::parse_str(<&str>::try_from(self).unwrap())
                    .map_err(|e| {
                        Box::<dyn Error + Send + Sync>::from(format!(
                            "Could not convert Text into a Mac Address: {}",
                            e
                        ))
                    })
                    .and_then(|m| m.to_sql(ty, out))
            }
            (Self::Text(_) | Self::TinyText(_), &Type::INET) => <&str>::try_from(self)
                .unwrap()
                .parse::<IpInet>()
                .map_err(|e| {
                    Box::<dyn Error + Send + Sync>::from(format!(
                        "Could not convert Text into an IP Address: {}",
                        e
                    ))
                })
                .and_then(|ip| ip.to_sql(ty, out)),
            (Self::Text(_) | Self::TinyText(_), &Type::UUID) => {
                Uuid::parse_str(<&str>::try_from(self).unwrap())
                    .map_err(|e| {
                        Box::<dyn Error + Send + Sync>::from(format!(
                            "Could not convert Text into a UUID: {}",
                            e
                        ))
                    })
                    .and_then(|m| m.to_sql(ty, out))
            }
            (Self::Text(_) | Self::TinyText(_), &Type::JSON | &Type::JSONB) => {
                serde_json::from_str::<serde_json::Value>(<&str>::try_from(self).unwrap())
                    .map_err(|e| {
                        Box::<dyn Error + Send + Sync>::from(format!(
                            "Could not convert Text into a JSON: {}",
                            e
                        ))
                    })
                    .and_then(|v| v.to_sql(ty, out))
            }
            (Self::Text(_) | Self::TinyText(_), _) => {
                <&str>::try_from(self).unwrap().to_sql(ty, out)
            }
            (Self::TimestampTz(x), &Type::DATE) => {
                x.to_chrono().naive_local().date().to_sql(ty, out)
            }
            (Self::TimestampTz(x), &Type::TIMESTAMP) => x.to_chrono().naive_local().to_sql(ty, out),
            (Self::TimestampTz(ref ts), _) => ts.to_chrono().to_sql(ty, out),
            (Self::Time(x), _) => NaiveTime::from(*x).to_sql(ty, out),
            (Self::ByteArray(ref array), _) => array.as_ref().to_sql(ty, out),
            (Self::BitVector(ref bits), _) => bits.as_ref().to_sql(ty, out),
            (Self::Array(ref array), _) => array.as_ref().to_sql(ty, out),
            (Self::PassThrough(p), _) => p.data.as_ref().to_sql(&p.ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for DfValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        macro_rules! mk_from_sql {
            ($target:ty) => {
                DfValue::try_from(<$target>::from_sql(ty, raw)?).map_err(|e| {
                    format!(
                        "Could not convert Postgres type {} into a DfValue. Error: {}",
                        ty, e
                    )
                    .into()
                })
            };
        }
        match ty.kind() {
            Kind::Array(_) => mk_from_sql!(Array),
            Kind::Enum(_) => mk_from_sql!(&str),
            _ => match *ty {
                Type::BOOL => mk_from_sql!(bool),
                Type::CHAR => mk_from_sql!(i8),
                Type::INT2 => mk_from_sql!(i16),
                Type::INT4 => mk_from_sql!(i32),
                Type::INT8 => mk_from_sql!(i64),
                Type::OID
                | Type::REGCLASS
                | Type::REGCOLLATION
                | Type::REGCONFIG
                | Type::REGDICTIONARY
                | Type::REGNAMESPACE
                | Type::REGOPER
                | Type::REGOPERATOR
                | Type::REGPROC
                | Type::REGPROCEDURE
                | Type::REGROLE
                | Type::REGTYPE => mk_from_sql!(u32),
                Type::BPCHAR | Type::TEXT | Type::VARCHAR | Type::NAME => mk_from_sql!(&str),
                Type::FLOAT4 => mk_from_sql!(f32),
                Type::FLOAT8 => mk_from_sql!(f64),
                Type::DATE => mk_from_sql!(NaiveDate),
                Type::TIME => mk_from_sql!(NaiveTime),
                Type::BYTEA => mk_from_sql!(Vec<u8>),
                Type::NUMERIC => {
                    // rust-decimal has a bug whereby it will successfully deserialize from the
                    // Postgres binary format NUMERIC values with scales in [0, 255], but it will
                    // panic when serializing them to bincode if they are outside [0, 28].
                    let d = Decimal::from_sql(ty, raw)?;
                    if d.scale() > 28 {
                        Err(format!("Could not convert Postgres type {ty} into a DfValue. Error: scale > 28").into())
                    } else {
                        Ok(DfValue::from(d))
                    }
                }
                Type::TIMESTAMP => mk_from_sql!(NaiveDateTime),
                Type::TIMESTAMPTZ => mk_from_sql!(chrono::DateTime<chrono::FixedOffset>),
                Type::MACADDR => Ok(DfValue::from(
                    MacAddress::from_sql(ty, raw)?.to_string(MacAddressFormat::HexString),
                )),
                Type::INET => Ok(DfValue::from(IpInet::from_sql(ty, raw)?.to_string())),
                Type::UUID => Ok(DfValue::from(Uuid::from_sql(ty, raw)?.to_string())),
                Type::JSON | Type::JSONB => {
                    let raw = match (ty, raw) {
                        (&Type::JSONB, []) => {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "Missing JSONB encoding version",
                            )
                            .into())
                        }
                        (&Type::JSONB, [1, ..]) => &raw[1..],
                        (&Type::JSONB, [..]) => {
                            return Err("Unsupported JSONB encoding version".into())
                        }
                        _ => raw,
                    };

                    // Deserializing/serializing deeply nested JSON data can hit serde_json's
                    // recursion limit, to protect against stack overflows. To circumvent this,
                    // serde_stacker is used instead of the recursion limit, to dynamically grow
                    // the stack as the limit is approached.
                    let value = {
                        let mut deserializer = serde_json::Deserializer::from_slice(raw);
                        deserializer.disable_recursion_limit();
                        let deserializer = serde_stacker::Deserializer::new(&mut deserializer);
                        serde_json::Value::deserialize(deserializer)?
                    };

                    let out = {
                        let mut out = Vec::new();
                        let mut serializer = serde_json::Serializer::new(&mut out);
                        let serializer = serde_stacker::Serializer::new(&mut serializer);
                        value.serialize(serializer)?;
                        String::from_utf8(out).unwrap()
                    };

                    // Iteratively drop the serde_json::Value's contents, to avoid stack
                    // overflows from recursion.
                    {
                        let mut stack = vec![value];
                        while let Some(value) = stack.pop() {
                            match value {
                                serde_json::Value::Array(array) => stack.extend(array),
                                serde_json::Value::Object(obj) => {
                                    stack.extend(obj.into_iter().map(|(_, value)| value))
                                }
                                _ => {}
                            };
                        }
                    }
                    Ok(DfValue::from(out))
                }
                Type::BIT | Type::VARBIT => mk_from_sql!(BitVec),
                ref ty if ty.name() == "citext" => Ok(DfValue::from_str_and_collation(
                    <&str>::from_sql(ty, raw)?,
                    Collation::Citext,
                )),
                ref ty => Ok(DfValue::PassThrough(Arc::new(PassThrough {
                    ty: ty.clone(),
                    data: Box::from(raw),
                }))),
            },
        }
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(DfValue::None)
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

impl TryFrom<DfValue> for mysql_common::value::Value {
    type Error = ReadySetError;

    fn try_from(dt: DfValue) -> Result<Self, Self::Error> {
        Self::try_from(&dt)
    }
}

impl TryFrom<&DfValue> for mysql_common::value::Value {
    type Error = ReadySetError;
    fn try_from(dt: &DfValue) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match dt {
            DfValue::None | DfValue::Max => Ok(Value::NULL),
            DfValue::Int(val) => Ok(Value::Int(*val)),
            DfValue::UnsignedInt(val) => Ok(Value::UInt(*val)),
            DfValue::Float(val) => Ok(Value::Float(*val)),
            DfValue::Double(val) => Ok(Value::Double(*val)),
            DfValue::Numeric(d) => Ok(Value::from(**d)),
            DfValue::Text(_) | DfValue::TinyText(_) => Ok(Value::Bytes(Vec::<u8>::try_from(dt)?)),
            DfValue::TimestampTz(val) => Ok(val.to_chrono().naive_utc().into()),
            DfValue::Time(val) => Ok(Value::Time(
                !val.is_positive(),
                (val.hour() / 24).into(),
                (val.hour() % 24) as _,
                val.minutes(),
                val.seconds(),
                val.microseconds(),
            )),
            DfValue::ByteArray(array) => Ok(Value::Bytes(array.as_ref().clone())),
            DfValue::PassThrough(_) => {
                internal!("DfValue::PassThrough to MySQL Value type is not implemented")
            }
            DfValue::BitVector(_) => internal!("MySQL does not support bit vector types"),
            DfValue::Array(_) => internal!("MySQL does not support array types"),
        }
    }
}

// Performs an arithmetic operation on two numeric DfValues,
// returning a new DfValue as the result.
//
// Note that Rust's builtin floating-point types do not panic on overflow or division by zero, so
// there are no checked_add/sub/mul/div methods for f32/f64, hence the need to pass in both $op and
// $checked_op to this macro.
macro_rules! arithmetic_operation (
    ($op:tt, $checked_op:ident, $first:ident, $second:ident) => (
        match ($first, $second) {
            (&DfValue::None, _) | (_, &DfValue::None) => DfValue::None,
            (&DfValue::Int(a), &DfValue::Int(b)) => a.$checked_op(b).into(),
            (&DfValue::UnsignedInt(a), &DfValue::UnsignedInt(b)) => a.$checked_op(b).into(),

            (&DfValue::UnsignedInt(a), &DfValue::Int(b)) => i128::from(a).$checked_op(i128::from(b)).map_or(Ok(DfValue::None), DfValue::try_from)?,
            (&DfValue::Int(a), &DfValue::UnsignedInt(b)) => i128::from(a).$checked_op(i128::from(b)).map_or(Ok(DfValue::None), DfValue::try_from)?,

            (first @ &DfValue::Int(..), second @ &DfValue::Float(..)) |
            (first @ &DfValue::UnsignedInt(..), second @ &DfValue::Float(..)) |
            (first @ &DfValue::Float(..), second @ &DfValue::Int(..)) |
            (first @ &DfValue::Float(..), second @ &DfValue::UnsignedInt(..)) |
            (first @ &DfValue::Float(..), second @ &DfValue::Float(..)) |
            (first @ &DfValue::Float(..), second @ &DfValue::Double(..)) |
            (first @ &DfValue::Float(..), second @ &DfValue::Numeric(..)) => {
                let a: f32 = f32::try_from(first)?;
                let b: f32 = f32::try_from(second)?;
                DfValue::try_from(a $op b)?
            }

            (first @ &DfValue::Int(..), second @ &DfValue::Double(..)) |
            (first @ &DfValue::UnsignedInt(..), second @ &DfValue::Double(..)) |
            (first @ &DfValue::Double(..), second @ &DfValue::Int(..)) |
            (first @ &DfValue::Double(..), second @ &DfValue::UnsignedInt(..)) |
            (first @ &DfValue::Double(..), second @ &DfValue::Double(..)) |
            (first @ &DfValue::Double(..), second @ &DfValue::Float(..)) |
            (first @ &DfValue::Double(..), second @ &DfValue::Numeric(..)) => {
                let a: f64 = f64::try_from(first)?;
                let b: f64 = f64::try_from(second)?;
                DfValue::try_from(a $op b)?
            }

            (first @ &DfValue::Int(..), second @ &DfValue::Numeric(..)) |
            (first @ &DfValue::UnsignedInt(..), second @ &DfValue::Numeric(..)) |
            (first @ &DfValue::Numeric(..), second @ &DfValue::Int(..)) |
            (first @ &DfValue::Numeric(..), second @ &DfValue::UnsignedInt(..)) |
            (first @ &DfValue::Numeric(..), second @ &DfValue::Numeric(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = Decimal::try_from(second)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                DfValue::from(a.$checked_op(b))
            }
            (first @ &DfValue::Numeric(..), second @ &DfValue::Float(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f32::try_from(second).and_then(|f| Decimal::from_f32(f)
                    .ok_or_else(|| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DfValue::from(a.$checked_op(b))
            }
            (first @ &DfValue::Numeric(..), second @ &DfValue::Double(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f64::try_from(second).and_then(|f| Decimal::from_f64(f)
                    .ok_or_else(|| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DfValue::from(a.$checked_op(b))
            }


            (first, second) => return Err(invalid_err!(
                "can't {} a {:?} and {:?}",
                stringify!($op),
                DfValueKind::from(first),
                DfValueKind::from(second),
            )),
        }
    );
);

impl<'a, 'b> Add<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn add(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(+, checked_add, self, other))
    }
}

impl<'a, 'b> Sub<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn sub(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(-, checked_sub, self, other))
    }
}

impl<'a, 'b> Mul<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn mul(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(*, checked_mul, self, other))
    }
}

impl<'a, 'b> Div<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn div(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(/, checked_div, self, other))
    }
}

impl Arbitrary for DfValue {
    type Parameters = Option<DfValueKind>;
    type Strategy = proptest::strategy::BoxedStrategy<DfValue>;

    fn arbitrary_with(opt_kind: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        match opt_kind {
            Some(DfValueKind::None) => Just(DfValue::None).boxed(),
            Some(DfValueKind::Max) => Just(DfValue::Max).boxed(),
            Some(DfValueKind::Int) => any::<i64>().prop_map(DfValue::Int).boxed(),
            Some(DfValueKind::UnsignedInt) => any::<u64>().prop_map(DfValue::UnsignedInt).boxed(),
            Some(DfValueKind::Float) => any::<f32>().prop_map(DfValue::Float).boxed(),
            Some(DfValueKind::Double) => any::<f64>().prop_map(DfValue::Double).boxed(),
            Some(DfValueKind::Text | DfValueKind::TinyText) => any::<String>()
                .prop_map(|s| DfValue::from(s.replace('\0', "")))
                .boxed(),
            Some(DfValueKind::TimestampTz) => any::<crate::TimestampTz>()
                .prop_map(DfValue::TimestampTz)
                .boxed(),
            Some(DfValueKind::Time) => arbitrary_duration()
                .prop_map(MySqlTime::new)
                .prop_map(DfValue::Time)
                .boxed(),
            Some(DfValueKind::ByteArray) => any::<Vec<u8>>()
                .prop_map(|b| DfValue::ByteArray(Arc::new(b)))
                .boxed(),
            Some(DfValueKind::Numeric) => arbitrary_decimal().prop_map(DfValue::from).boxed(),
            Some(DfValueKind::BitVector) => any::<Vec<u8>>()
                .prop_map(|bs| DfValue::BitVector(Arc::new(BitVec::from_bytes(&bs))))
                .boxed(),
            Some(DfValueKind::Array) => any::<Array>().prop_map(DfValue::from).boxed(),
            Some(DfValueKind::PassThrough) => any::<(u32, Vec<u8>)>()
                .prop_map(|(oid, data)| {
                    DfValue::PassThrough(Arc::new(PassThrough {
                        ty: Type::new(
                            "Test Type".to_string(),
                            oid,
                            Kind::Simple,
                            "Test Schema".to_string(),
                        ),
                        data: data.into_boxed_slice(),
                    }))
                })
                .boxed(),
            None => prop_oneof![
                Just(DfValue::None),
                Just(DfValue::Max),
                any::<i64>().prop_map(DfValue::Int),
                any::<u64>().prop_map(DfValue::UnsignedInt),
                any::<f32>().prop_map(DfValue::Float),
                any::<f64>().prop_map(DfValue::Double),
                any::<String>().prop_map(|s| DfValue::from(s.replace('\0', ""))),
                any::<crate::TimestampTz>().prop_map(DfValue::TimestampTz),
                arbitrary_duration()
                    .prop_map(MySqlTime::new)
                    .prop_map(DfValue::Time),
                any::<Vec<u8>>().prop_map(|b| DfValue::ByteArray(Arc::new(b))),
                arbitrary_decimal().prop_map(DfValue::from),
                any::<Array>().prop_map(DfValue::from)
            ]
            .boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use derive_more::{From, Into};
    use proptest::prelude::*;
    use readyset_util::{eq_laws, hash_laws, ord_laws};
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn test_size_and_alignment() {
        assert_eq!(std::mem::size_of::<DfValue>(), 16);

        let timestamp_tz = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );

        match &timestamp_tz {
            DfValue::TimestampTz(ts) => {
                // Make sure datetime is properly aligned to 4 bytes within embedded timestamptz
                assert!(std::mem::align_of::<chrono::NaiveDateTime>() <= 4);
                let ts_ptr = std::ptr::addr_of!(ts.datetime) as usize;
                assert_eq!(ts_ptr & 0x3, 0);
            }
            _ => panic!(),
        }
    }

    fn non_numeric() -> impl Strategy<Value = DfValue> {
        any::<DfValue>().prop_filter("Numeric DfValue", |dt| !matches!(dt, DfValue::Numeric(_)))
    }

    eq_laws!(DfValue);
    hash_laws!(DfValue);
    ord_laws!(
        // [note: mixed-type-comparisons]
        // The comparison semantics between Numeric and the other numeric types (floats, integers)
        // isn't well-defined, so we skip that in our ord tests.
        //
        // This would be a problem, except that in the main place where having a proper ordering
        // relation actually matters (persisted base table state) we only store homogeneously-typed
        // values. If that becomes *not* the case for whatever reason, we need to figure out how to
        // fix this.
        #[strategy(non_numeric())]
        DfValue
    );

    #[derive(Debug, From, Into)]
    struct MySqlValue(mysql_common::value::Value);

    impl Arbitrary for MySqlValue {
        type Parameters = ();
        type Strategy = proptest::strategy::BoxedStrategy<MySqlValue>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            use mysql_common::value::Value;
            use proptest::prelude::*;

            //TODO(DAN): cleaner way of avoiding invalid dates/times
            prop_oneof![
                Just(Value::NULL),
                any::<i64>().prop_map(Value::Int),
                any::<u64>().prop_map(Value::UInt),
                any::<f32>().prop_map(Value::Float),
                any::<f64>().prop_map(Value::Double),
                (
                    1001u16..9999,
                    1u8..13,
                    1u8..29, //TODO(DAN): Allow up to 31 and check for invalid dates in the test
                    0u8..24,
                    0u8..60,
                    0u8..60,
                    0u32..100
                )
                    .prop_map(|(y, m, d, h, min, s, ms)| Value::Date(y, m, d, h, min, s, ms)),
                (
                    any::<bool>(),
                    0u32..34, // DfValue cannot accept time hihger than 838:59:59
                    0u8..24,
                    0u8..60,
                    0u8..60,
                    0u32..100
                )
                    .prop_map(|(neg, d, h, m, s, ms)| Value::Time(neg, d, h, m, s, ms)),
            ]
            .prop_map(MySqlValue)
            .boxed()
        }
    }

    #[proptest]
    fn max_greater_than_all(dt: DfValue) {
        prop_assume!(dt != DfValue::Max, "MAX");
        assert!(DfValue::Max > dt);
    }

    #[proptest]
    #[allow(clippy::float_cmp)]
    fn dt_to_mysql_value_roundtrip_prop(v: MySqlValue) {
        use mysql_common::value::Value;
        let MySqlValue(v) = v;
        match (
            Value::try_from(DfValue::try_from(v.clone()).unwrap()).unwrap(),
            v,
        ) {
            (Value::Float(f1), Value::Float(f2)) => assert_eq!(f1, f2),
            (Value::Double(d1), Value::Double(d2)) => assert_eq!(d1, d2),
            (v1, v2) => assert_eq!(v1, v2),
        }
    }

    #[proptest]
    #[allow(clippy::float_cmp)]
    fn mysql_value_to_dt_roundtrip_prop(dt: DfValue) {
        use chrono::Datelike;
        use mysql_common::value::Value;

        prop_assume!(match dt {
            DfValue::TimestampTz(t)
                if t.to_chrono().naive_local().date().year() < 1000
                    || t.to_chrono().naive_local().date().year() > 9999 =>
                false,
            DfValue::ByteArray(_) | DfValue::BitVector(_) | DfValue::Array(_) | DfValue::Max =>
                false,
            _ => true,
        });

        match (
            DfValue::try_from(Value::try_from(dt.clone()).unwrap()).unwrap(),
            dt,
        ) {
            (DfValue::Float(f1), DfValue::Float(f2)) => assert_eq!(f1, f2),
            (DfValue::Double(f1), DfValue::Double(f2)) => assert_eq!(f1, f2),
            // Mysql returns Numeric values as Text :(
            (v @ (DfValue::Text(_) | DfValue::TinyText(_)), DfValue::Numeric(n)) => {
                assert_eq!(<&str>::try_from(&v).unwrap(), n.to_string())
            }
            (dt1, dt2) => assert_eq!(dt1, dt2),
        }
    }

    #[test]
    fn mysql_value_to_dataflow_value_roundtrip() {
        use mysql_common::value::Value;

        assert_eq!(
            Value::Bytes(vec![1, 2, 3]),
            Value::try_from(DfValue::try_from(Value::Bytes(vec![1, 2, 3])).unwrap()).unwrap()
        );

        assert_eq!(
            Value::Int(1),
            Value::try_from(DfValue::try_from(Value::Int(1)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::UInt(1),
            Value::try_from(DfValue::try_from(Value::UInt(1)).unwrap()).unwrap()
        );
        // round trip results in conversion from float to double
        assert_eq!(
            Value::Float(8.99),
            Value::try_from(DfValue::try_from(Value::Float(8.99)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::Double(8.99),
            Value::try_from(DfValue::try_from(Value::Double(8.99)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::Date(2021, 1, 1, 1, 1, 1, 1),
            Value::try_from(DfValue::try_from(Value::Date(2021, 1, 1, 1, 1, 1, 1)).unwrap())
                .unwrap()
        );
        assert_eq!(
            Value::Time(false, 1, 1, 1, 1, 1),
            Value::try_from(DfValue::try_from(Value::Time(false, 1, 1, 1, 1, 1)).unwrap()).unwrap()
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn mysql_value_to_dataflow_value() {
        use mysql_common::value::Value;

        // Test Value::NULL.
        let a = Value::NULL;
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DfValue::None);

        // Test Value::Bytes.

        // Can't build a String from non-utf8 chars, but now it can be mapped
        // to a ByteArray.
        let a = Value::Bytes(vec![0xff; 30]);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DfValue::ByteArray(Arc::new(vec![0xff; 30])));

        let s = "abcdef";
        let a = Value::Bytes(s.as_bytes().to_vec());
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DfValue::TinyText(TinyText::from_arr(b"abcdef"))
        );

        // Test Value::Int.
        let a = Value::Int(-5);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DfValue::Int(-5));

        // Test Value::Float.
        let a = Value::UInt(5);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DfValue::UnsignedInt(5));

        // Test Value::Float.
        let initial_float: f32 = 8.99;
        let a = Value::Float(initial_float);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f32 = <f32>::try_from(a_dt.unwrap()).unwrap();
        assert_eq!(converted_float, initial_float);

        // Test Value::Double.
        let initial_float: f64 = 8.99;
        let a = Value::Double(initial_float);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f64 = <f64>::try_from(a_dt.unwrap()).unwrap();
        assert_eq!(converted_float, initial_float);

        // Test Value::Date.
        let ts = NaiveDate::from_ymd(1111, 1, 11).and_hms_micro(2, 3, 4, 5);
        let a = Value::from(ts);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DfValue::TimestampTz(ts.into()));

        // Test Value::Time.
        // noria_data::DfValue has no `Time` representation.
        let a = Value::Time(true, 0, 0, 0, 0, 0);
        let a_dt = DfValue::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DfValue::Time(MySqlTime::from_microseconds(0))
        )
    }

    #[test]
    fn real_to_string() {
        let a_float: DfValue = DfValue::try_from(8.99_f32).unwrap();
        let b_float: DfValue = DfValue::try_from(-8.099_f32).unwrap();
        let c_float: DfValue = DfValue::try_from(-0.012_345_678_f32).unwrap();

        assert_eq!(a_float.to_string(), "8.99");
        assert_eq!(b_float.to_string(), "-8.099");
        assert_eq!(c_float.to_string(), "-0.012345678");

        let a_double: DfValue = DfValue::try_from(8.99_f64).unwrap();
        let b_double: DfValue = DfValue::try_from(-8.099_f64).unwrap();
        let c_double: DfValue = DfValue::try_from(-0.012_345_678_f64).unwrap();
        assert_eq!(a_double.to_string(), "8.99");
        assert_eq!(b_double.to_string(), "-8.099");
        assert_eq!(c_double.to_string(), "-0.012345678");

        let d_float: DfValue = DfValue::from(Decimal::new(3141, 3));
        assert_eq!(d_float.to_string(), "3.141");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_float() {
        let original: f32 = 8.99;
        let data_type: DfValue = DfValue::try_from(original).unwrap();
        let converted: f32 = <f32>::try_from(&data_type).unwrap();
        assert_eq!(DfValue::Float(8.99), data_type);
        assert_eq!(original, converted);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_double() {
        let original: f64 = 8.99;
        let data_type: DfValue = DfValue::try_from(original).unwrap();
        let converted: f64 = <f64>::try_from(&data_type).unwrap();
        assert_eq!(DfValue::Double(8.99), data_type);
        assert_eq!(original, converted);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_numeric() {
        let original: Decimal = Decimal::new(3141, 3);
        let data_type: DfValue = DfValue::try_from(original).unwrap();
        let converted: Decimal = Decimal::try_from(&data_type).unwrap();
        assert_eq!(DfValue::Numeric(Arc::new(Decimal::new(3141, 3))), data_type);
        assert_eq!(original, converted);
    }

    macro_rules! assert_arithmetic {
        ($op:tt, $left:expr, $right:expr, $expected:expr) => {
            assert_eq!(
                (&DfValue::try_from($left).unwrap() $op &DfValue::try_from($right).unwrap()).unwrap(),
                DfValue::try_from($expected).unwrap()
            );
        }
    }

    #[test]
    fn add_data_types() {
        assert_arithmetic!(+, 1, 2, 3);
        assert_arithmetic!(+, 1.5_f32, 2, 3.5_f32);
        assert_arithmetic!(+, 1.5_f64, 2, 3.5_f64);
        assert_arithmetic!(+, Decimal::new(15, 1), 2, Decimal::new(35, 1));
        assert_arithmetic!(+, 2, 1.5_f32, 3.5_f32);
        assert_arithmetic!(+, 2, 1.5_f64, 3.5_f64);
        assert_arithmetic!(+, 2, Decimal::new(15, 1), Decimal::new(35, 1));
        assert_arithmetic!(+, 1.5_f32, 2.5_f32, 4.0_f32);
        assert_arithmetic!(+, 1.5_f32, 2.5_f64, 4.0_f64);
        assert_arithmetic!(+, 1.5_f32, Decimal::new(25, 1), Decimal::new(40, 1));
        assert_arithmetic!(+, 1.5_f64, 2.5_f32, 4.0_f64);
        assert_arithmetic!(+, 1.5_f64, 2.5_f64, 4.0_f64);
        assert_arithmetic!(+, 1.5_f64, Decimal::new(25, 1), Decimal::new(40, 1));
        assert_arithmetic!(+, Decimal::new(15, 1), Decimal::new(25, 1), Decimal::new(40, 1));
        assert_arithmetic!(+, Decimal::new(15, 1), 2.5_f32, Decimal::new(40, 1));
        assert_arithmetic!(+, Decimal::new(15, 1), 2.5_f64, Decimal::new(40, 1));
        assert_arithmetic!(+, i64::MAX, 1, None::<i64>);
        assert_arithmetic!(+, Decimal::MAX, Decimal::MAX, None::<Decimal>);
        assert_eq!((&DfValue::Int(1) + &DfValue::Int(2)).unwrap(), 3.into());
        assert_eq!((&DfValue::from(1) + &DfValue::Int(2)).unwrap(), 3.into());
        assert_eq!((&DfValue::Int(2) + &DfValue::from(1)).unwrap(), 3.into());
    }

    #[test]
    fn subtract_data_types() {
        assert_arithmetic!(-, 2, 1, 1);
        assert_arithmetic!(-, 3.5_f32, 2, 1.5_f32);
        assert_arithmetic!(-, 3.5_f64, 2, 1.5_f64);
        assert_arithmetic!(-, Decimal::new(35, 1), 2, Decimal::new(15, 1));
        assert_arithmetic!(-, 2, 1.5_f32, 0.5_f32);
        assert_arithmetic!(-, 2, 1.5_f64, 0.5_f64);
        assert_arithmetic!(-, 2, Decimal::new(15, 1), Decimal::new(5, 1));
        assert_arithmetic!(-, 3.5_f32, 2.0_f32, 1.5_f32);
        assert_arithmetic!(-, 3.5_f32, 2.0_f64, 1.5_f64);
        assert_arithmetic!(-, 3.5_f32, Decimal::new(20, 1), Decimal::new(15, 1));
        assert_arithmetic!(-, 3.5_f64, 2.0_f32, 1.5_f64);
        assert_arithmetic!(-, 3.5_f64, 2.0_f64, 1.5_f64);
        assert_arithmetic!(-, 3.5_f64, Decimal::new(20, 1), Decimal::new(15, 1));
        assert_arithmetic!(-, Decimal::new(35, 1), 2.0_f32, Decimal::new(15, 1));
        assert_arithmetic!(-, Decimal::new(35, 1), 2.0_f64, Decimal::new(15, 1));
        assert_arithmetic!(-, Decimal::new(35, 1), Decimal::new(20, 1), Decimal::new(15, 1));
        assert_arithmetic!(-, 1_u64, 2_u64, None::<u64>);
        assert_arithmetic!(-, Decimal::MIN, Decimal::MAX, None::<Decimal>);
        assert_eq!((&DfValue::Int(1) - &DfValue::Int(2)).unwrap(), (-1).into());
        assert_eq!((&DfValue::from(1) - &DfValue::Int(2)).unwrap(), (-1).into());
        assert_eq!((&DfValue::Int(2) - &DfValue::from(1)).unwrap(), 1.into());
    }

    #[test]
    fn multiply_data_types() {
        assert_arithmetic!(*, 2, 1, 2);
        assert_arithmetic!(*, 3.5_f32, 2, 7.0_f32);
        assert_arithmetic!(*, 3.5_f64, 2, 7.0_f64);
        assert_arithmetic!(*, Decimal::new(35, 1), 2, Decimal::new(70, 1));
        assert_arithmetic!(*, 2, 1.5_f32, 3.0_f32);
        assert_arithmetic!(*, 2, 1.5_f64, 3.0_f64);
        assert_arithmetic!(*, 2, Decimal::new(15, 1), Decimal::new(30, 1));
        assert_arithmetic!(*, 3.5_f32, 2.0_f32, 7.0_f32);
        assert_arithmetic!(*, 3.5_f32, 2.0_f64, 7.0_f64);
        assert_arithmetic!(*, 3.5_f32, Decimal::new(20, 1), Decimal::new(70, 1));
        assert_arithmetic!(*, 3.5_f64, 2.0_f32, 7.0_f64);
        assert_arithmetic!(*, 3.5_f64, 2.0_f64, 7.0_f64);
        assert_arithmetic!(*, 3.5_f64, Decimal::new(20, 1), Decimal::new(70, 1));
        assert_arithmetic!(*, i64::MAX, 2, None::<i64>);
        assert_arithmetic!(*, Decimal::MAX, Decimal::MAX, None::<Decimal>);
        assert_eq!((&DfValue::Int(1) * &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::from(1) * &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::Int(2) * &DfValue::from(1)).unwrap(), 2.into());
    }

    #[test]
    fn divide_data_types() {
        assert_arithmetic!(/, 2, 1, 2);
        assert_arithmetic!(/, 7.5_f32, 2, 3.75_f32);
        assert_arithmetic!(/, 7.5_f64, 2, 3.75_f64);
        assert_arithmetic!(/, Decimal::new(75, 1), 2, Decimal::new(375, 2));
        assert_arithmetic!(/, 7, 2.5_f32, 2.8_f32);
        assert_arithmetic!(/, 7, 2.5_f64, 2.8_f64);
        assert_arithmetic!(/, 7, Decimal::new(25, 1), Decimal::new(28, 1));
        assert_arithmetic!(/, 3.5_f32, 2.0_f32, 1.75_f32);
        assert_arithmetic!(/, 3.5_f32, 2.0_f64, 1.75_f64);
        assert_arithmetic!(/, 3.5_f32, Decimal::new(20, 1), Decimal::new(175, 2));
        assert_arithmetic!(/, 3.5_f64, 2.0_f32, 1.75_f64);
        assert_arithmetic!(/, 3.5_f64, 2.0_f64, 1.75_f64);
        assert_arithmetic!(/, 3.5_f64, Decimal::new(20, 1), Decimal::new(175, 2));
        assert_arithmetic!(/, 1, 0, None::<i64>);
        assert_arithmetic!(/, Decimal::ONE, Decimal::ZERO, None::<Decimal>);
        assert_eq!((&DfValue::Int(4) / &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::from(4) / &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::Int(4) / &DfValue::from(2)).unwrap(), 2.into());
    }

    #[test]
    fn invalid_arithmetic_returns_error() {
        (&DfValue::from(0) + &DfValue::from("abc")).unwrap_err();
    }

    #[test]
    fn data_type_display() {
        let tiny_text: DfValue = "hi".try_into().unwrap();
        let text: DfValue = "this is a very long text indeed".try_into().unwrap();
        let float_from_real: DfValue = DfValue::try_from(-8.99_f32).unwrap();
        let float = DfValue::Float(-8.99);
        let double_from_real: DfValue = DfValue::try_from(-8.99_f64).unwrap();
        let double = DfValue::Double(-8.99);
        let numeric = DfValue::from(Decimal::new(-899, 2)); // -8.99
        let timestamp = DfValue::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let timestamp_tz = DfValue::from(
            FixedOffset::west(19_800)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let int = DfValue::Int(5);
        let bytes = DfValue::ByteArray(Arc::new(vec![0, 8, 39, 92, 100, 128]));
        // bits = 000000000000100000100111010111000110010010000000
        let bits = DfValue::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));
        assert_eq!(tiny_text.to_string(), "hi");
        assert_eq!(text.to_string(), "this is a very long text indeed");
        assert_eq!(float_from_real.to_string(), "-8.99");
        assert_eq!(float.to_string(), "-8.99");
        assert_eq!(double_from_real.to_string(), "-8.99");
        assert_eq!(double.to_string(), "-8.99");
        assert_eq!(numeric.to_string(), "-8.99");
        assert_eq!(timestamp.to_string(), "1970-01-01 00:00:00");
        assert_eq!(timestamp_tz.to_string(), "1969-12-31 18:30:00-05:30");
        assert_eq!(int.to_string(), "5");
        assert_eq!(bytes.to_string(), "E'\\x0008275c6480'");
        assert_eq!(
            bits.to_string(),
            "000000000000100000100111010111000110010010000000"
        );
    }

    fn _data_type_fungibility_test_eq<T>(f: &dyn for<'a> Fn(&'a DfValue) -> T)
    where
        T: PartialEq + fmt::Debug,
    {
        let txt1: DfValue = "hi".try_into().unwrap();
        let txt12: DfValue = "no".try_into().unwrap();
        let txt2: DfValue = DfValue::Text("hi".into());
        let text: DfValue = "this is a very long text indeed".try_into().unwrap();
        let text2: DfValue = "this is another long text".try_into().unwrap();
        let float = DfValue::Float(-8.99);
        let float2 = DfValue::Float(-8.98);
        let float_from_real: DfValue = DfValue::try_from(-8.99_f32).unwrap();
        let float_from_real2: DfValue = DfValue::try_from(-8.98_f32).unwrap();
        let double = DfValue::Double(-8.99);
        let double2 = DfValue::Double(-8.98);
        let double_from_real: DfValue = DfValue::try_from(-8.99_f64).unwrap();
        let double_from_real2: DfValue = DfValue::try_from(-8.98_f64).unwrap();
        let numeric = DfValue::from(Decimal::new(-899, 2)); // -8.99
        let numeric2 = DfValue::from(Decimal::new(-898, 2)); // -8.99
        let time = DfValue::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let time2 = DfValue::TimestampTz(NaiveDateTime::from_timestamp(1, 42_000_000).into());
        let timestamp_tz = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let timestamp_tz2 = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(1, 42_000_000)),
        );
        let shrt = DfValue::Int(5);
        let shrt6 = DfValue::Int(6);
        let ushrt = DfValue::UnsignedInt(5);
        let ushrt6 = DfValue::UnsignedInt(6);
        let bytes = DfValue::ByteArray(Arc::new("hi".as_bytes().to_vec()));
        let bytes2 = DfValue::ByteArray(Arc::new(vec![0, 8, 39, 92, 101, 128]));
        let bits = DfValue::BitVector(Arc::new(BitVec::from_bytes("hi".as_bytes())));
        let bits2 = DfValue::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));

        assert_eq!(f(&txt1), f(&txt1));
        assert_eq!(f(&txt2), f(&txt2));
        assert_eq!(f(&text), f(&text));
        assert_eq!(f(&shrt), f(&shrt));
        assert_eq!(f(&ushrt), f(&ushrt));
        assert_eq!(f(&float), f(&float));
        assert_eq!(f(&float_from_real), f(&float_from_real));
        assert_eq!(f(&double), f(&double));
        assert_eq!(f(&numeric), f(&numeric));
        assert_eq!(f(&double_from_real), f(&double_from_real));
        assert_eq!(f(&time), f(&time));
        assert_eq!(f(&timestamp_tz), f(&timestamp_tz));
        assert_eq!(f(&bytes), f(&bytes));
        assert_eq!(f(&bits), f(&bits));

        // coercion
        assert_eq!(f(&txt1), f(&txt2));
        assert_eq!(f(&txt2), f(&txt1));
        assert_eq!(f(&ushrt), f(&shrt));
        assert_eq!(f(&shrt), f(&ushrt));
        assert_eq!(f(&time), f(&timestamp_tz));
        assert_eq!(f(&timestamp_tz), f(&time));

        // negation
        assert_ne!(f(&txt1), f(&txt12));
        assert_ne!(f(&txt1), f(&text));
        assert_ne!(f(&txt1), f(&float));
        assert_ne!(f(&txt1), f(&float_from_real));
        assert_ne!(f(&txt1), f(&double));
        assert_ne!(f(&txt1), f(&double_from_real));
        assert_ne!(f(&txt1), f(&numeric));
        assert_ne!(f(&txt1), f(&time));
        assert_ne!(f(&txt1), f(&shrt));
        assert_ne!(f(&txt1), f(&ushrt));
        assert_ne!(f(&txt1), f(&bytes));
        assert_ne!(f(&txt1), f(&bits));
        assert_ne!(f(&txt1), f(&timestamp_tz));

        assert_ne!(f(&txt2), f(&txt12));
        assert_ne!(f(&txt2), f(&text));
        assert_ne!(f(&txt2), f(&float));
        assert_ne!(f(&txt2), f(&float_from_real));
        assert_ne!(f(&txt2), f(&double));
        assert_ne!(f(&txt2), f(&double_from_real));
        assert_ne!(f(&txt2), f(&numeric));
        assert_ne!(f(&txt2), f(&time));
        assert_ne!(f(&txt2), f(&shrt));
        assert_ne!(f(&txt2), f(&ushrt));
        assert_ne!(f(&txt2), f(&bytes));
        assert_ne!(f(&txt2), f(&bits));
        assert_ne!(f(&txt2), f(&timestamp_tz));

        assert_ne!(f(&text), f(&text2));
        assert_ne!(f(&text), f(&txt1));
        assert_ne!(f(&text), f(&txt2));
        assert_ne!(f(&text), f(&float));
        assert_ne!(f(&text), f(&float_from_real));
        assert_ne!(f(&text), f(&double));
        assert_ne!(f(&text), f(&double_from_real));
        assert_ne!(f(&text), f(&numeric));
        assert_ne!(f(&text), f(&time));
        assert_ne!(f(&text), f(&shrt));
        assert_ne!(f(&text), f(&ushrt));
        assert_ne!(f(&text), f(&bytes));
        assert_ne!(f(&text), f(&bits));
        assert_ne!(f(&text), f(&timestamp_tz));

        assert_ne!(f(&float), f(&float2));
        assert_ne!(f(&float_from_real), f(&float_from_real2));
        assert_ne!(f(&double), f(&double2));
        assert_ne!(f(&double_from_real), f(&double_from_real2));
        assert_ne!(f(&numeric), f(&numeric2));
        assert_ne!(f(&float), f(&txt1));
        assert_ne!(f(&float_from_real), f(&txt1));
        assert_ne!(f(&double), f(&txt1));
        assert_ne!(f(&double_from_real), f(&txt1));
        assert_ne!(f(&numeric), f(&txt1));
        assert_ne!(f(&float), f(&txt2));
        assert_ne!(f(&float_from_real), f(&txt2));
        assert_ne!(f(&double), f(&txt2));
        assert_ne!(f(&double_from_real), f(&txt2));
        assert_ne!(f(&numeric), f(&txt2));
        assert_ne!(f(&float), f(&text));
        assert_ne!(f(&float_from_real), f(&text));
        assert_ne!(f(&double), f(&text));
        assert_ne!(f(&double_from_real), f(&text));
        assert_ne!(f(&numeric), f(&text));
        assert_ne!(f(&float), f(&time));
        assert_ne!(f(&float_from_real), f(&time));
        assert_ne!(f(&double), f(&time));
        assert_ne!(f(&double_from_real), f(&time));
        assert_ne!(f(&numeric), f(&time));
        assert_ne!(f(&float), f(&shrt));
        assert_ne!(f(&float_from_real), f(&shrt));
        assert_ne!(f(&double), f(&shrt));
        assert_ne!(f(&double_from_real), f(&shrt));
        assert_ne!(f(&numeric), f(&shrt));
        assert_ne!(f(&float), f(&ushrt));
        assert_ne!(f(&float_from_real), f(&ushrt));
        assert_ne!(f(&double), f(&ushrt));
        assert_ne!(f(&double_from_real), f(&ushrt));
        assert_ne!(f(&numeric), f(&ushrt));

        assert_ne!(f(&time), f(&time2));
        assert_ne!(f(&time), f(&txt1));
        assert_ne!(f(&time), f(&txt2));
        assert_ne!(f(&time), f(&text));
        assert_ne!(f(&time), f(&float_from_real));
        assert_ne!(f(&time), f(&shrt));
        assert_ne!(f(&time), f(&ushrt));
        assert_ne!(f(&time), f(&bytes));
        assert_ne!(f(&time), f(&bits));

        assert_ne!(f(&shrt), f(&shrt6));
        assert_ne!(f(&shrt), f(&txt1));
        assert_ne!(f(&shrt), f(&txt2));
        assert_ne!(f(&shrt), f(&text));
        assert_ne!(f(&shrt), f(&float_from_real));
        assert_ne!(f(&shrt), f(&time));
        assert_ne!(f(&shrt), f(&bytes));
        assert_ne!(f(&shrt), f(&bits));

        assert_ne!(f(&ushrt), f(&ushrt6));
        assert_ne!(f(&ushrt), f(&txt1));
        assert_ne!(f(&ushrt), f(&txt2));
        assert_ne!(f(&ushrt), f(&text));
        assert_ne!(f(&ushrt), f(&float_from_real));
        assert_ne!(f(&ushrt), f(&time));
        assert_ne!(f(&ushrt), f(&shrt6));
        assert_ne!(f(&ushrt), f(&bytes));
        assert_ne!(f(&ushrt), f(&bits));
        assert_ne!(f(&ushrt), f(&timestamp_tz));

        assert_ne!(f(&bytes), f(&txt1));
        assert_ne!(f(&bytes), f(&txt2));
        assert_ne!(f(&bytes), f(&text));
        assert_ne!(f(&bytes), f(&float_from_real));
        assert_ne!(f(&bytes), f(&time));
        assert_ne!(f(&bytes), f(&ushrt6));
        assert_ne!(f(&bytes), f(&shrt6));
        assert_ne!(f(&bytes), f(&bits));
        assert_ne!(f(&bytes), f(&timestamp_tz));
        assert_ne!(f(&bytes), f(&bytes2));

        assert_ne!(f(&bits), f(&txt1));
        assert_ne!(f(&bits), f(&txt2));
        assert_ne!(f(&bits), f(&text));
        assert_ne!(f(&bits), f(&float_from_real));
        assert_ne!(f(&bits), f(&time));
        assert_ne!(f(&bits), f(&ushrt6));
        assert_ne!(f(&bits), f(&shrt6));
        assert_ne!(f(&bits), f(&timestamp_tz));
        assert_ne!(f(&bits), f(&bits2));

        assert_ne!(f(&timestamp_tz), f(&txt1));
        assert_ne!(f(&timestamp_tz), f(&txt2));
        assert_ne!(f(&timestamp_tz), f(&text));
        assert_ne!(f(&timestamp_tz), f(&float_from_real));
        assert_ne!(f(&timestamp_tz), f(&ushrt6));
        assert_ne!(f(&timestamp_tz), f(&shrt6));
        assert_ne!(f(&timestamp_tz), f(&bits));
        assert_ne!(f(&timestamp_tz), f(&bytes));
        assert_ne!(f(&timestamp_tz), f(&timestamp_tz2));
    }

    #[test]
    fn data_type_conversion() {
        let bigint_i64_min = DfValue::Int(std::i64::MIN);
        let bigint_i32_min = DfValue::Int(std::i32::MIN as i64);
        let bigint_u32_min = DfValue::Int(std::u32::MIN as i64);
        let bigint_i32_max = DfValue::Int(std::i32::MAX as i64);
        let bigint_u32_max = DfValue::Int(std::u32::MAX as i64);
        let bigint_i64_max = DfValue::Int(std::i64::MAX);
        let ubigint_u32_min = DfValue::UnsignedInt(std::u32::MIN as u64);
        let ubigint_i32_max = DfValue::UnsignedInt(std::i32::MAX as u64);
        let ubigint_u32_max = DfValue::UnsignedInt(std::u32::MAX as u64);
        let ubigint_i64_max = DfValue::UnsignedInt(std::i64::MAX as u64);
        let ubigint_u64_max = DfValue::UnsignedInt(std::u64::MAX);

        fn _data_type_conversion_test_eq_i32(d: &DfValue) {
            assert_eq!(
                i32::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i32_panic(d: &DfValue) {
            i32::try_from(d).unwrap_err();
        }
        fn _data_type_conversion_test_eq_i64(d: &DfValue) {
            assert_eq!(
                i64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i64_panic(d: &DfValue) {
            i64::try_from(d).unwrap_err();
        }
        fn _data_type_conversion_test_eq_u32(d: &DfValue) {
            assert_eq!(
                u32::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u32_panic(d: &DfValue) {
            u32::try_from(d).unwrap_err();
        }
        fn _data_type_conversion_test_eq_u64(d: &DfValue) {
            assert_eq!(
                u64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u64_panic(d: &DfValue) {
            u64::try_from(d).unwrap_err();
        }
        fn _data_type_conversion_test_eq_i128(d: &DfValue) {
            assert_eq!({ i128::try_from(d).unwrap() }, i128::try_from(d).unwrap())
        }
        fn _data_type_conversion_test_eq_i128_panic(d: &DfValue) {
            i128::try_from(d).unwrap_err();
        }

        _data_type_conversion_test_eq_i32_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_u32_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_u64_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_i64(&bigint_i64_min);
        _data_type_conversion_test_eq_i128(&bigint_i64_min);

        _data_type_conversion_test_eq_u32_panic(&bigint_i32_min);
        _data_type_conversion_test_eq_u64_panic(&bigint_i32_min);
        _data_type_conversion_test_eq_i32(&bigint_i32_min);
        _data_type_conversion_test_eq_i64(&bigint_i32_min);
        _data_type_conversion_test_eq_i128(&bigint_i32_min);

        _data_type_conversion_test_eq_i32(&bigint_u32_min);
        _data_type_conversion_test_eq_i64(&bigint_u32_min);
        _data_type_conversion_test_eq_u32(&bigint_u32_min);
        _data_type_conversion_test_eq_u64(&bigint_u32_min);
        _data_type_conversion_test_eq_i128(&bigint_u32_min);
        _data_type_conversion_test_eq_i32(&ubigint_u32_min);
        _data_type_conversion_test_eq_i64(&ubigint_u32_min);
        _data_type_conversion_test_eq_u32(&ubigint_u32_min);
        _data_type_conversion_test_eq_u64(&ubigint_u32_min);
        _data_type_conversion_test_eq_i128(&ubigint_u32_min);

        _data_type_conversion_test_eq_i32(&bigint_i32_max);
        _data_type_conversion_test_eq_i64(&bigint_i32_max);
        _data_type_conversion_test_eq_u32(&bigint_i32_max);
        _data_type_conversion_test_eq_u64(&bigint_i32_max);
        _data_type_conversion_test_eq_i128(&bigint_i32_max);
        _data_type_conversion_test_eq_i32(&ubigint_i32_max);
        _data_type_conversion_test_eq_i64(&ubigint_i32_max);
        _data_type_conversion_test_eq_u32(&ubigint_i32_max);
        _data_type_conversion_test_eq_u64(&ubigint_i32_max);
        _data_type_conversion_test_eq_i128(&ubigint_i32_max);

        _data_type_conversion_test_eq_i32_panic(&bigint_u32_max);
        _data_type_conversion_test_eq_i64(&bigint_u32_max);
        _data_type_conversion_test_eq_u32(&bigint_u32_max);
        _data_type_conversion_test_eq_u64(&bigint_u32_max);
        _data_type_conversion_test_eq_i128(&bigint_u32_max);
        _data_type_conversion_test_eq_i32_panic(&ubigint_u32_max);
        _data_type_conversion_test_eq_i64(&ubigint_u32_max);
        _data_type_conversion_test_eq_u32(&ubigint_u32_max);
        _data_type_conversion_test_eq_u64(&ubigint_u32_max);
        _data_type_conversion_test_eq_i128(&ubigint_u32_max);

        _data_type_conversion_test_eq_i32_panic(&bigint_i64_max);
        _data_type_conversion_test_eq_u32_panic(&bigint_i64_max);
        _data_type_conversion_test_eq_i64(&bigint_i64_max);
        _data_type_conversion_test_eq_u64(&bigint_i64_max);
        _data_type_conversion_test_eq_i128(&bigint_i64_max);
        _data_type_conversion_test_eq_i32_panic(&ubigint_i64_max);
        _data_type_conversion_test_eq_u32_panic(&ubigint_i64_max);
        _data_type_conversion_test_eq_i64(&ubigint_i64_max);
        _data_type_conversion_test_eq_u64(&ubigint_i64_max);
        _data_type_conversion_test_eq_i128(&ubigint_i64_max);

        _data_type_conversion_test_eq_i32_panic(&ubigint_u64_max);
        _data_type_conversion_test_eq_u32_panic(&ubigint_u64_max);
        _data_type_conversion_test_eq_i64_panic(&ubigint_u64_max);
        _data_type_conversion_test_eq_u64(&ubigint_u64_max);
        _data_type_conversion_test_eq_i128(&ubigint_u64_max);
    }

    #[proptest]
    fn data_type_string_conversion_roundtrip(s: String) {
        assert_eq!(
            String::try_from(&DfValue::try_from(s.clone()).unwrap()).unwrap(),
            s
        )
    }

    #[test]
    fn data_type_passthrough_same_type_roundtrip() -> Result<(), Box<dyn Error + Sync + Send>> {
        let original_type = Type::new(
            "type".into(),
            100,
            tokio_postgres::types::Kind::Pseudo,
            "schema".into(),
        );
        let original_data = vec![0u8, 1, 2, 3, 4, 5];

        let data_type = DfValue::from_sql(&original_type, &original_data)?;
        assert!(matches!(data_type, DfValue::PassThrough(_)));
        let decode_type = original_type;
        let mut received_data = BytesMut::new();
        data_type.to_sql(&decode_type, &mut received_data)?;
        assert_eq!(
            original_data, received_data,
            "Failed to round-trip when supplying same type to to_sql"
        );
        Ok(())
    }

    #[test]
    fn data_type_passthrough_different_type_roundtrip() -> Result<(), Box<dyn Error + Sync + Send>>
    {
        let original_type = Type::new(
            "type".into(),
            100,
            tokio_postgres::types::Kind::Pseudo,
            "schema".into(),
        );
        let original_data = vec![0u8, 1, 2, 3, 4, 5];

        let data_type = DfValue::from_sql(&original_type, &original_data)?;
        assert!(matches!(data_type, DfValue::PassThrough(_)));
        let decode_type = Type::ANY;
        let mut received_data = BytesMut::new();
        data_type.to_sql(&decode_type, &mut received_data)?;
        assert_eq!(
            original_data, received_data,
            "Failed to round-trip when supplying different type to to_sql"
        );
        Ok(())
    }

    #[test]
    fn deeply_nested_json_roundtrip() -> Result<(), Box<dyn Error + Sync + Send>> {
        let nesting_level = 1_000_000;
        let original_value: String = (0..nesting_level)
            .map(|_| "[")
            .chain(["5"])
            .chain((0..nesting_level).map(|_| "]"))
            .collect();
        let value = DfValue::from_sql(&Type::JSON, original_value.as_bytes())?;
        assert_eq!(
            original_value,
            value.to_string(),
            "JSON data malformed during roundtrip"
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn data_type_fungibility() {
        let hash = |dt: &DfValue| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut s = DefaultHasher::new();
            dt.hash(&mut s);
            s.finish()
        };
        let json_serialize = |dt: &DfValue| -> DfValue {
            serde_json::from_str(&serde_json::to_string(dt).unwrap()).unwrap()
        };
        let bincode_serialize = |dt: &DfValue| -> DfValue {
            bincode::deserialize(&bincode::serialize(dt).unwrap()).unwrap()
        };

        _data_type_fungibility_test_eq(&|x: &DfValue| x.clone());
        _data_type_fungibility_test_eq(&hash);
        _data_type_fungibility_test_eq(&json_serialize);
        _data_type_fungibility_test_eq(&bincode_serialize);

        use std::convert::TryFrom;

        let txt1: DfValue = "hi".try_into().unwrap();
        let txt12: DfValue = "no".try_into().unwrap();
        let txt2: DfValue = DfValue::Text("hi".into());
        let text: DfValue = "this is a very long text indeed".try_into().unwrap();
        let text2: DfValue = "this is another long text".try_into().unwrap();
        let float = DfValue::Float(-8.99);
        let float2 = DfValue::Float(-8.98);
        let float_from_real: DfValue = DfValue::try_from(-8.99_f32).unwrap();
        let float_from_real2: DfValue = DfValue::try_from(-8.98_f32).unwrap();
        let double = DfValue::Double(-8.99);
        let double2 = DfValue::Double(-8.98);
        let double_from_real: DfValue = DfValue::try_from(-8.99_f64).unwrap();
        let double_from_real2: DfValue = DfValue::try_from(-8.98_f64).unwrap();
        let numeric = DfValue::from(Decimal::new(-899, 2)); // -8.99
        let numeric2 = DfValue::from(Decimal::new(-898, 2)); // -8.99
        let time = DfValue::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let time2 = DfValue::TimestampTz(NaiveDateTime::from_timestamp(1, 42_000_000).into());
        let timestamp_tz = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let timestamp_tz2 = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(1, 42_000_000)),
        );
        let shrt = DfValue::Int(5);
        let shrt6 = DfValue::Int(6);
        let ushrt = DfValue::UnsignedInt(5);
        let ushrt6 = DfValue::UnsignedInt(6);
        let bytes = DfValue::ByteArray(Arc::new("hi".as_bytes().to_vec()));
        let bytes2 = DfValue::ByteArray(Arc::new(vec![0, 8, 39, 92, 101, 128]));
        let bits = DfValue::BitVector(Arc::new(BitVec::from_bytes("hi".as_bytes())));
        let bits2 = DfValue::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));

        use std::cmp::Ordering;
        assert_eq!(txt1.cmp(&txt1), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt2), Ordering::Equal);
        assert_eq!(text.cmp(&text), Ordering::Equal);
        assert_eq!(shrt.cmp(&shrt), Ordering::Equal);
        assert_eq!(ushrt.cmp(&ushrt), Ordering::Equal);
        assert_eq!(float.cmp(&float), Ordering::Equal);
        assert_eq!(float_from_real.cmp(&float_from_real), Ordering::Equal);
        assert_eq!(double.cmp(&double), Ordering::Equal);
        assert_eq!(double_from_real.cmp(&double_from_real), Ordering::Equal);
        assert_eq!(numeric.cmp(&numeric), Ordering::Equal);
        assert_eq!(time.cmp(&time), Ordering::Equal);
        assert_eq!(bytes.cmp(&bytes), Ordering::Equal);
        assert_eq!(bits.cmp(&bits), Ordering::Equal);
        assert_eq!(timestamp_tz.cmp(&timestamp_tz), Ordering::Equal);

        // coercion
        assert_eq!(txt1.cmp(&txt2), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt1), Ordering::Equal);
        // SELECT TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54+00' = TIMESTAMP '2004-10-19
        // 10:23:54';
        // ?column?
        // ----------
        // t
        assert_eq!(time.cmp(&timestamp_tz), Ordering::Equal);
        assert_eq!(timestamp_tz.cmp(&time), Ordering::Equal);

        // negation
        assert_ne!(txt1.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt1.cmp(&text), Ordering::Equal);
        assert_ne!(txt1.cmp(&float), Ordering::Equal);
        assert_ne!(txt1.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(txt1.cmp(&double), Ordering::Equal);
        assert_ne!(txt1.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(txt1.cmp(&numeric), Ordering::Equal);
        assert_ne!(txt1.cmp(&time), Ordering::Equal);
        assert_ne!(txt1.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt1.cmp(&ushrt), Ordering::Equal);
        assert_ne!(txt1.cmp(&bytes), Ordering::Equal);
        assert_ne!(txt1.cmp(&bits), Ordering::Equal);
        assert_ne!(txt1.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(txt2.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt2.cmp(&text), Ordering::Equal);
        assert_ne!(txt2.cmp(&float), Ordering::Equal);
        assert_ne!(txt2.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(txt2.cmp(&double), Ordering::Equal);
        assert_ne!(txt2.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(txt2.cmp(&numeric), Ordering::Equal);
        assert_ne!(txt2.cmp(&time), Ordering::Equal);
        assert_ne!(txt2.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt2.cmp(&ushrt), Ordering::Equal);
        assert_ne!(txt2.cmp(&bytes), Ordering::Equal);
        assert_ne!(txt2.cmp(&bits), Ordering::Equal);
        assert_ne!(txt2.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(text.cmp(&text2), Ordering::Equal);
        assert_ne!(text.cmp(&txt1), Ordering::Equal);
        assert_ne!(text.cmp(&txt2), Ordering::Equal);
        assert_ne!(text.cmp(&float), Ordering::Equal);
        assert_ne!(text.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(text.cmp(&double), Ordering::Equal);
        assert_ne!(text.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(text.cmp(&numeric), Ordering::Equal);
        assert_ne!(text.cmp(&time), Ordering::Equal);
        assert_ne!(text.cmp(&shrt), Ordering::Equal);
        assert_ne!(text.cmp(&ushrt), Ordering::Equal);
        assert_ne!(text.cmp(&bytes), Ordering::Equal);
        assert_ne!(text.cmp(&bits), Ordering::Equal);
        assert_ne!(text.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(float.cmp(&float2), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&float_from_real2), Ordering::Equal);
        assert_ne!(double.cmp(&double2), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&double_from_real2), Ordering::Equal);
        assert_ne!(numeric.cmp(&numeric2), Ordering::Equal);
        assert_ne!(float.cmp(&txt1), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&txt1), Ordering::Equal);
        assert_ne!(double.cmp(&txt1), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&txt1), Ordering::Equal);
        assert_ne!(numeric.cmp(&txt1), Ordering::Equal);
        assert_ne!(float.cmp(&txt2), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&txt2), Ordering::Equal);
        assert_ne!(double.cmp(&txt2), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&txt2), Ordering::Equal);
        assert_ne!(numeric.cmp(&txt2), Ordering::Equal);
        assert_ne!(float.cmp(&text), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&text), Ordering::Equal);
        assert_ne!(double.cmp(&text), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&text), Ordering::Equal);
        assert_ne!(numeric.cmp(&text), Ordering::Equal);
        assert_ne!(float.cmp(&time), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&time), Ordering::Equal);
        assert_ne!(double.cmp(&time), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&time), Ordering::Equal);
        assert_ne!(numeric.cmp(&time), Ordering::Equal);
        assert_ne!(float.cmp(&shrt), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&shrt), Ordering::Equal);
        assert_ne!(double.cmp(&shrt), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&shrt), Ordering::Equal);
        assert_ne!(numeric.cmp(&shrt), Ordering::Equal);
        assert_ne!(float.cmp(&shrt), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&shrt), Ordering::Equal);
        assert_ne!(double.cmp(&shrt), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&shrt), Ordering::Equal);
        assert_ne!(float.cmp(&ushrt), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&ushrt), Ordering::Equal);
        assert_ne!(double.cmp(&ushrt), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&ushrt), Ordering::Equal);
        assert_ne!(numeric.cmp(&ushrt), Ordering::Equal);
        assert_ne!(float.cmp(&bytes), Ordering::Equal);
        assert_ne!(float_from_real.cmp(&bytes), Ordering::Equal);
        assert_ne!(double.cmp(&bytes), Ordering::Equal);
        assert_ne!(double_from_real.cmp(&bytes), Ordering::Equal);

        assert_ne!(time.cmp(&time2), Ordering::Equal);
        assert_ne!(time.cmp(&txt1), Ordering::Equal);
        assert_ne!(time.cmp(&txt2), Ordering::Equal);
        assert_ne!(time.cmp(&text), Ordering::Equal);
        assert_ne!(time.cmp(&float), Ordering::Equal);
        assert_ne!(time.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(time.cmp(&double), Ordering::Equal);
        assert_ne!(time.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(time.cmp(&numeric), Ordering::Equal);
        assert_ne!(time.cmp(&shrt), Ordering::Equal);
        assert_ne!(time.cmp(&ushrt), Ordering::Equal);
        assert_ne!(time.cmp(&bytes), Ordering::Equal);
        assert_ne!(time.cmp(&bits), Ordering::Equal);

        assert_ne!(shrt.cmp(&shrt6), Ordering::Equal);
        assert_ne!(shrt.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt1), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt2), Ordering::Equal);
        assert_ne!(shrt.cmp(&text), Ordering::Equal);
        assert_ne!(shrt.cmp(&float), Ordering::Equal);
        assert_ne!(shrt.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(shrt.cmp(&double), Ordering::Equal);
        assert_ne!(shrt.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(shrt.cmp(&numeric), Ordering::Equal);
        assert_ne!(shrt.cmp(&time), Ordering::Equal);
        assert_ne!(shrt.cmp(&bytes), Ordering::Equal);
        assert_ne!(shrt.cmp(&bits), Ordering::Equal);
        assert_ne!(shrt.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(ushrt.cmp(&shrt6), Ordering::Equal);
        assert_ne!(ushrt.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(ushrt.cmp(&txt1), Ordering::Equal);
        assert_ne!(ushrt.cmp(&txt2), Ordering::Equal);
        assert_ne!(ushrt.cmp(&text), Ordering::Equal);
        assert_ne!(ushrt.cmp(&float), Ordering::Equal);
        assert_ne!(ushrt.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(ushrt.cmp(&double), Ordering::Equal);
        assert_ne!(ushrt.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(ushrt.cmp(&numeric), Ordering::Equal);
        assert_ne!(ushrt.cmp(&time), Ordering::Equal);
        assert_ne!(ushrt.cmp(&bytes), Ordering::Equal);
        assert_ne!(ushrt.cmp(&bits), Ordering::Equal);
        assert_ne!(ushrt.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(bytes.cmp(&txt1), Ordering::Equal);
        assert_ne!(bytes.cmp(&txt2), Ordering::Equal);
        assert_ne!(bytes.cmp(&text), Ordering::Equal);
        assert_ne!(bytes.cmp(&float), Ordering::Equal);
        assert_ne!(bytes.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(bytes.cmp(&double), Ordering::Equal);
        assert_ne!(bytes.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(bytes.cmp(&time), Ordering::Equal);
        assert_ne!(bytes.cmp(&shrt6), Ordering::Equal);
        assert_ne!(bytes.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(bytes.cmp(&bytes2), Ordering::Equal);
        assert_ne!(bytes.cmp(&bits), Ordering::Equal);
        assert_ne!(bytes.cmp(&timestamp_tz), Ordering::Equal);

        assert_ne!(bits.cmp(&txt1), Ordering::Equal);
        assert_ne!(bits.cmp(&txt2), Ordering::Equal);
        assert_ne!(bits.cmp(&text), Ordering::Equal);
        assert_ne!(bits.cmp(&float), Ordering::Equal);
        assert_ne!(bits.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(bits.cmp(&double), Ordering::Equal);
        assert_ne!(bits.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(bits.cmp(&time), Ordering::Equal);
        assert_ne!(bits.cmp(&shrt6), Ordering::Equal);
        assert_ne!(bits.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(bits.cmp(&bytes), Ordering::Equal);
        assert_ne!(bits.cmp(&timestamp_tz), Ordering::Equal);
        assert_ne!(bits.cmp(&bits2), Ordering::Equal);

        assert_ne!(timestamp_tz.cmp(&txt1), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&txt2), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&text), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&float), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&float_from_real), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&double), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&double_from_real), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&shrt6), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&bytes), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&bits), Ordering::Equal);
        assert_ne!(timestamp_tz.cmp(&timestamp_tz2), Ordering::Equal);

        // Test invariants
        // 1. Text types always > everything else
        // 2. Double & float comparable directly with int
        let int1 = DfValue::Int(1);
        let float1 = DfValue::Float(1.2);
        let float2 = DfValue::Float(0.8);
        let double1 = DfValue::Double(1.2);
        let double2 = DfValue::Double(0.8);
        let numeric1 = DfValue::from(Decimal::new(12, 1));
        let numeric2 = DfValue::from(Decimal::new(8, 1));
        assert_eq!(txt1.cmp(&int1), Ordering::Greater);
        assert_eq!(int1.cmp(&txt1), Ordering::Less);
        assert_eq!(txt1.cmp(&double1), Ordering::Greater);
        assert_eq!(float1.cmp(&txt1), Ordering::Less);
        assert_eq!(float1.cmp(&int1), Ordering::Greater);
        assert_eq!(float2.cmp(&int1), Ordering::Less);
        assert_eq!(double1.cmp(&txt1), Ordering::Less);
        assert_eq!(double1.cmp(&int1), Ordering::Greater);
        assert_eq!(double2.cmp(&int1), Ordering::Less);
        assert_eq!(numeric.cmp(&txt1), Ordering::Greater);
        assert_eq!(numeric1.cmp(&int1), Ordering::Greater);
        assert_eq!(numeric2.cmp(&int1), Ordering::Less);
    }

    #[test]
    fn array_sql_type() {
        let arr = DfValue::from(vec![DfValue::None, DfValue::from(1)]);
        assert_eq!(
            arr.sql_type(),
            Some(SqlType::Array(Box::new(SqlType::BigInt(None))))
        );
    }

    mod coerce_to {
        use readyset_util::arbitrary::{
            arbitrary_naive_date, arbitrary_naive_date_time, arbitrary_naive_time,
        };
        use rust_decimal::Decimal;
        use test_strategy::proptest;

        use super::*;

        #[proptest]
        fn same_type_is_identity(dt: DfValue) {
            prop_assume!(dt.is_homogeneous());

            let ty = dt.infer_dataflow_type();
            prop_assume!(ty.is_strictly_known());

            assert_eq!(
                dt.coerce_to(&ty, &DfType::Unknown),
                Ok(dt),
                "coerce to {ty:?}"
            );
        }

        #[proptest]
        fn unknown_to_unknown(value: DfValue) {
            assert_eq!(
                value.coerce_to(&DfType::Unknown, &DfType::Unknown),
                Ok(value)
            );
        }

        #[proptest]
        fn parse_timestamps(#[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime) {
            let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();
            let expected = DfValue::from(ndt);
            let input = DfValue::from(ndt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string());

            let result = input
                .coerce_to(&DfType::Timestamp { subsecond_digits }, &DfType::Unknown)
                .unwrap();

            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_times(#[strategy(arbitrary_naive_time())] nt: NaiveTime) {
            let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();
            let expected = DfValue::from(nt);
            let input = DfValue::from(nt.format(TIME_FORMAT).to_string());
            let result = input
                .coerce_to(&DfType::Time { subsecond_digits }, &DfType::Unknown)
                .unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_datetimes(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();
            let dt = NaiveDateTime::new(nd, NaiveTime::from_hms(12, 0, 0));
            let expected = DfValue::from(dt);
            let input = DfValue::from(dt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string());

            let result = input
                .coerce_to(&DfType::DateTime { subsecond_digits }, &DfType::Unknown)
                .unwrap();

            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_dates(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let expected = DfValue::from(NaiveDateTime::new(nd, NaiveTime::from_hms(0, 0, 0)));
            let input = DfValue::from(nd.format(crate::timestamp::DATE_FORMAT).to_string());
            let result = input.coerce_to(&DfType::Date, &DfType::Unknown).unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn timestamp_surjections() {
            let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();
            let input = DfValue::from(NaiveDate::from_ymd(2021, 3, 17).and_hms(11, 34, 56));
            assert_eq!(
                input.coerce_to(&DfType::Date, &DfType::Unknown).unwrap(),
                NaiveDate::from_ymd(2021, 3, 17).into()
            );
            assert_eq!(
                input
                    .coerce_to(&DfType::Time { subsecond_digits }, &DfType::Unknown)
                    .unwrap(),
                NaiveTime::from_hms(11, 34, 56).into()
            );
        }

        #[proptest]
        fn timestamp_to_datetime(
            #[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime,
            subsecond_digits: Option<u16>,
        ) {
            let subsecond_digits = subsecond_digits
                .unwrap_or_else(|| Dialect::DEFAULT_MYSQL.default_subsecond_digits());
            let input = DfValue::from(ndt);
            assert_eq!(
                input
                    .clone()
                    .coerce_to(&DfType::DateTime { subsecond_digits }, &DfType::Unknown)
                    .unwrap(),
                input
            );
        }

        macro_rules! int_conversion {
            ($name: ident, $from: ty, $to: ty, $df_type: expr) => {
                #[proptest]
                fn $name(source: $to) {
                    let input = <$from>::try_from(source);
                    prop_assume!(input.is_ok());
                    assert_eq!(
                        DfValue::from(input.unwrap())
                            .coerce_to(&$df_type, &DfType::Unknown)
                            .unwrap(),
                        DfValue::from(source)
                    );
                }
            };
        }

        int_conversion!(int_to_tinyint, i32, i8, DfType::TinyInt);
        int_conversion!(int_to_unsigned_tinyint, i32, u8, DfType::UnsignedTinyInt);
        int_conversion!(int_to_smallint, i32, i16, DfType::SmallInt);
        int_conversion!(int_to_unsigned_smallint, i32, u16, DfType::UnsignedSmallInt);
        int_conversion!(bigint_to_tinyint, i64, i8, DfType::TinyInt);
        int_conversion!(bigint_to_unsigned_tinyint, i64, u8, DfType::UnsignedTinyInt);
        int_conversion!(bigint_to_smallint, i64, i16, DfType::SmallInt);
        int_conversion!(
            bigint_to_unsigned_smallint,
            i64,
            u16,
            DfType::UnsignedSmallInt
        );
        int_conversion!(bigint_to_int, i64, i32, DfType::Int);
        int_conversion!(bigint_to_unsigned_bigint, i64, u64, DfType::UnsignedBigInt);

        macro_rules! real_conversion {
            ($name: ident, $from: ty, $to: ty, $df_type: expr) => {
                #[proptest]
                fn $name(source: $from) {
                    if (source as $to).is_finite() {
                        assert_eq!(
                            DfValue::try_from(source)
                                .unwrap()
                                .coerce_to(&$df_type, &DfType::Unknown)
                                .unwrap(),
                            DfValue::try_from(source as $to).unwrap()
                        );
                    } else {
                        assert!(DfValue::try_from(source)
                            .unwrap()
                            .coerce_to(&$df_type, &DfType::Unknown)
                            .is_err());
                        assert!(DfValue::try_from(source as $to).is_err());
                    }
                }
            };
        }

        real_conversion!(float_to_double, f32, f64, DfType::Double);

        real_conversion!(double_to_float, f64, f32, DfType::Float);

        #[proptest]
        fn char_equal_length(#[strategy("a{1,30}")] text: String) {
            let input = DfValue::from(text.as_str());
            let intermediate =
                DfType::Char(u16::try_from(text.len()).unwrap(), Collation::default());
            let result = input.coerce_to(&intermediate, &DfType::Unknown).unwrap();
            assert_eq!(String::try_from(&result).unwrap().as_str(), text.as_str());
        }

        #[test]
        fn text_to_json() {
            let input = DfValue::from("{\"name\": \"John Doe\", \"age\": 43, \"phones\": [\"+44 1234567\", \"+44 2345678\"] }");
            let result = input.coerce_to(&DfType::Json, &DfType::Unknown).unwrap();
            assert_eq!(input, result);

            let result = input.coerce_to(&DfType::Jsonb, &DfType::Unknown).unwrap();
            assert_eq!(input, result);

            let input = DfValue::from("not a json");
            let result = input.coerce_to(&DfType::Json, &DfType::Unknown);
            result.unwrap_err();

            let result = input.coerce_to(&DfType::Jsonb, &DfType::Unknown);
            result.unwrap_err();
        }

        #[test]
        fn text_to_macaddr() {
            let input = DfValue::from("12:34:56:ab:cd:ef");
            let result = input.coerce_to(&DfType::MacAddr, &DfType::Unknown).unwrap();
            assert_eq!(input, result);
        }

        #[test]

        fn int_to_text() {
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20070523"),
            );
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&DfType::VarChar(2, Collation::default()), &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20"),
            );
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&DfType::Char(10, Collation::default()), &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20070523  "),
            );
        }

        #[test]
        fn int_to_date_time() {
            let subsecond_digits = Dialect::DEFAULT_MYSQL.default_subsecond_digits();

            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&DfType::Date, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(2007, 5, 23))
            );

            assert_eq!(
                DfValue::from(70523u64)
                    .coerce_to(&DfType::Date, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(2007, 5, 23))
            );

            assert_eq!(
                DfValue::from(19830905132800i64)
                    .coerce_to(&DfType::Timestamp { subsecond_digits }, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(1983, 9, 5).and_hms(13, 28, 00))
            );

            assert_eq!(
                DfValue::from(830905132800u64)
                    .coerce_to(&DfType::DateTime { subsecond_digits }, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(1983, 9, 5).and_hms(13, 28, 00))
            );

            assert_eq!(
                DfValue::from(101112i64)
                    .coerce_to(&DfType::Time { subsecond_digits }, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(MySqlTime::from_hmsus(true, 10, 11, 12, 0))
            );
        }

        #[test]
        fn text_to_uuid() {
            let input = DfValue::from(uuid::Uuid::new_v4().to_string());
            let result = input.coerce_to(&DfType::Uuid, &DfType::Unknown).unwrap();
            assert_eq!(input, result);
        }

        macro_rules! bool_conversion {
            ($name: ident, $ty: ty) => {
                #[proptest]
                fn $name(input: $ty) {
                    let input_dt = DfValue::from(input);
                    let result = input_dt.coerce_to(&DfType::Bool, &DfType::Unknown).unwrap();
                    let expected = DfValue::from(input != 0);
                    assert_eq!(result, expected);
                }
            };
        }

        bool_conversion!(i8_to_bool, i8);
        bool_conversion!(u8_to_bool, u8);
        bool_conversion!(i16_to_bool, i16);
        bool_conversion!(u16_to_bool, u16);
        bool_conversion!(i32_to_bool, i32);
        bool_conversion!(u32_to_bool, u32);
        bool_conversion!(i64_to_bool, i64);
        bool_conversion!(u64_to_bool, u64);

        #[test]
        fn string_to_array() {
            let input = DfValue::from(r#"{"a", "b", "c"}"#);
            let res = input
                .coerce_to(
                    &DfType::Array(Box::new(DfType::DEFAULT_TEXT)),
                    &DfType::Unknown,
                )
                .unwrap();
            assert_eq!(
                res,
                DfValue::from(vec![
                    DfValue::from("a"),
                    DfValue::from("b"),
                    DfValue::from("c"),
                ])
            )
        }

        #[test]
        fn array_coercing_values() {
            let input = DfValue::from(r#"{1, 2, 3}"#);
            let res = input
                .coerce_to(
                    &DfType::Array(Box::new(DfType::DEFAULT_TEXT)),
                    &DfType::Unknown,
                )
                .unwrap();
            assert_eq!(
                res,
                DfValue::from(vec![
                    DfValue::from("1"),
                    DfValue::from("2"),
                    DfValue::from("3"),
                ])
            )
        }

        #[test]
        fn two_d_array_coercing_values() {
            let input = DfValue::from(r#"[0:1][0:1]={{1, 2}, {3, 4}}"#);
            let res = input
                .coerce_to(
                    &DfType::Array(Box::new(DfType::DEFAULT_TEXT)),
                    &DfType::Unknown,
                )
                .unwrap();
            assert_eq!(
                res,
                DfValue::from(
                    crate::Array::from_lower_bounds_and_contents(
                        vec![0, 0],
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 2]),
                            vec![
                                DfValue::from("1"),
                                DfValue::from("2"),
                                DfValue::from("3"),
                                DfValue::from("4"),
                            ]
                        )
                        .unwrap()
                    )
                    .unwrap()
                )
            )
        }

        #[test]
        fn enum_coercions() {
            let variants = ["red", "yellow", "green"];
            let enum_ty = DfType::from_enum_variants(variants.into_iter().map(Into::into), None);

            // Test conversions from enums to strings
            for dv in [DfValue::Int(2), DfValue::UnsignedInt(2)] {
                assert_eq!(
                    "yellow",
                    dv.coerce_to(&DfType::DEFAULT_TEXT, &enum_ty)
                        .unwrap()
                        .to_string()
                )
            }

            // Test conversion of invalid enum indices to the empty string
            for i in [-1, 0, 4] {
                assert_eq!(
                    "",
                    DfValue::Int(i)
                        .coerce_to(&DfType::DEFAULT_TEXT, &enum_ty)
                        .unwrap()
                        .to_string()
                );
            }

            // Test coercion of out-of-range integers to enum
            for i in [-1, 0, 4] {
                let dv = DfValue::Int(i)
                    .coerce_to(&enum_ty, &DfType::Unknown)
                    .unwrap();
                assert_eq!(0, <u32>::try_from(dv).unwrap());
            }

            // Test valid coercions from other number types to enums
            let from_vals = [
                DfValue::Int(3),
                DfValue::UnsignedInt(3),
                DfValue::Float(3.0),
                DfValue::Float(3.1),
                DfValue::Float(3.9),
                DfValue::Double(3.0),
                DfValue::Double(3.1),
                DfValue::Double(3.9),
                DfValue::Numeric(Arc::new(Decimal::new(3, 0))),
                DfValue::Numeric(Arc::new(Decimal::new(31, 1))),
                DfValue::Numeric(Arc::new(Decimal::new(39, 1))),
            ];
            for dv in from_vals {
                assert_eq!(
                    DfValue::Int(3),
                    dv.coerce_to(&enum_ty, &DfType::Unknown).unwrap()
                );
            }

            // Test out-of-range coercion for non-integer types
            let from_vals = [
                DfValue::Float(4.0),
                DfValue::Float(4.1),
                DfValue::Float(0.0),
                DfValue::Float(0.1),
                DfValue::Float(0.9),
                DfValue::Float(-0.1),
                DfValue::Float(-1.0),
                DfValue::Float(f32::MAX),
                DfValue::Double(4.0),
                DfValue::Double(4.1),
                DfValue::Double(0.0),
                DfValue::Double(0.1),
                DfValue::Double(0.9),
                DfValue::Double(-0.1),
                DfValue::Double(-1.0),
                DfValue::Double(f64::MAX),
                DfValue::Numeric(Arc::new(Decimal::new(4, 0))),
                DfValue::Numeric(Arc::new(Decimal::new(41, 1))),
                DfValue::Numeric(Arc::new(Decimal::new(0, 0))),
                DfValue::Numeric(Arc::new(Decimal::new(1, 1))),
                DfValue::Numeric(Arc::new(Decimal::new(9, 1))),
                DfValue::Numeric(Arc::new(Decimal::new(-1, 1))),
                DfValue::Numeric(Arc::new(Decimal::new(-1, 0))),
                DfValue::Numeric(Arc::new(Decimal::MAX)),
            ];
            for dv in from_vals {
                let result = dv.coerce_to(&enum_ty, &DfType::Unknown).unwrap();
                assert_eq!(0, <u32>::try_from(result).unwrap());
            }

            // Test coercion from enum to text types with length limits

            let result = DfValue::Int(2)
                .coerce_to(&DfType::Char(3, Collation::default()), &enum_ty)
                .unwrap();
            assert_eq!("yel", result.to_string());

            let result = DfValue::Int(2)
                .coerce_to(&DfType::VarChar(3, Collation::default()), &enum_ty)
                .unwrap();
            assert_eq!("yel", result.to_string());

            let result = DfValue::Int(2)
                .coerce_to(&DfType::Char(10, Collation::default()), &enum_ty)
                .unwrap();
            assert_eq!("yellow    ", result.to_string());

            let no_change_tys = [
                DfType::VarChar(10, Collation::default()),
                // FIXME(ENG-1839)
                // DfType::Char(None, Collation::default(), Dialect::DEFAULT_MYSQL),
                DfType::DEFAULT_TEXT,
            ];

            for to_ty in no_change_tys {
                let result = DfValue::Int(2).coerce_to(&to_ty, &enum_ty).unwrap();
                assert_eq!("yellow", result.to_string());
            }
        }

        #[test]
        fn int_to_unknown() {
            assert_eq!(
                DfValue::from(1u64)
                    .coerce_to(&DfType::Unknown, &DfType::UnsignedBigInt)
                    .unwrap(),
                DfValue::from(1u64)
            );
            assert_eq!(
                DfValue::from(vec![DfValue::from(1u64)])
                    .coerce_to(
                        &DfType::Array(Box::new(DfType::Unknown)),
                        &DfType::Array(Box::new(DfType::UnsignedBigInt))
                    )
                    .unwrap(),
                DfValue::from(vec![DfValue::from(1u64)])
            );
        }
    }
}
