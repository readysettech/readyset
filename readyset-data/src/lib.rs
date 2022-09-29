#![feature(box_patterns)]

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;

use bit_vec::BitVec;
use bytes::BytesMut;
use chrono::{self, DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use enum_kinds::EnumKind;
use eui48::{MacAddress, MacAddressFormat};
use itertools::Itertools;
use launchpad::arbitrary::{arbitrary_decimal, arbitrary_duration};
use mysql_time::MysqlTime;
use ndarray::{ArrayD, IxDyn};
use nom_sql::{Dialect, Double, Float, Literal, SqlType};
use proptest::prelude::{prop_oneof, Arbitrary};
use readyset_errors::{internal, invalid_err, unsupported, ReadySetError, ReadySetResult};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use test_strategy::Arbitrary;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};
use uuid::Uuid;

mod array;
pub mod collation;
mod r#enum;
mod float;
mod integer;
mod serde;
mod text;
mod timestamp;
mod r#type;

pub use crate::array::Array;
pub use crate::collation::Collation;
pub use crate::r#type::DfType;
pub use crate::text::{Text, TinyText};
pub use crate::timestamp::{TimestampTz, TIMESTAMP_FORMAT, TIMESTAMP_PARSE_FORMAT};

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
pub enum DfValue {
    /// An empty value.
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
    /// NOTE: [`MysqlTime`] is from -838:59:59 to 838:59:59 whereas Postgres time is from 00:00:00
    /// to 24:00:00
    Time(MysqlTime),
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
    // NOTE: when adding new DfValue variants, make sure to always keep Max last - we use the
    // order of the variants to compare
    Max,
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
            DfValue::Time(_) => DfValue::Time(MysqlTime::min_value()),
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
            DfValue::Time(_) => DfValue::Time(MysqlTime::max_value()),
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

    /// Clone the value contained within this `DfValue`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        match *self {
            DfValue::Text(ref text) => DfValue::Text(text.as_str().into()),
            DfValue::ByteArray(ref bytes) => DfValue::ByteArray(Arc::new(bytes.as_ref().clone())),
            DfValue::BitVector(ref bits) => DfValue::from(bits.as_ref().clone()),
            ref dt => dt.clone(),
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
            DfValue::Time(ref t) => *t != MysqlTime::from_microseconds(0),
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
    pub fn infer_dataflow_type(&self, dialect: Dialect) -> DfType {
        use DfType::*;

        match self {
            Self::None | Self::PassThrough(_) | Self::Max => Unknown,
            Self::Int(_) => BigInt,
            Self::UnsignedInt(_) => UnsignedBigInt,
            Self::Float(_) => Float(dialect),
            Self::Double(_) => Double,
            Self::Text(_) | Self::TinyText(_) => Text {
                collation: Default::default(), /* TODO */
            },
            Self::TimestampTz(ts) => {
                let fsp = u16::from(ts.get_microsecond_precision());
                if ts.has_timezone() {
                    TimestampTz(fsp)
                } else {
                    Timestamp(fsp)
                }
            }
            // TODO(ENG-1833): Make this based off of the time value.
            Self::Time(_) => Time(dialect.default_datetime_precision()),
            Self::ByteArray(_) => Blob(dialect),
            Self::Numeric(_) => DfType::DEFAULT_NUMERIC,
            Self::BitVector(_) => VarBit(None),
            Self::Array(array) => Array(Box::new(
                array
                    .values()
                    .map(|v| v.infer_dataflow_type(dialect))
                    .find(DfType::is_known)
                    .unwrap_or_default(),
            )),
        }
    }

    /// Attempt to coerce the given DfValue to a value of the given `SqlType`.
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
    /// use nom_sql::SqlType;
    /// use readyset_data::{DfType, DfValue};
    ///
    /// let real = DfValue::Double(123.0);
    /// let int = real
    ///     .coerce_to(&SqlType::Int(None), &DfType::Unknown)
    ///     .unwrap();
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
    /// use nom_sql::SqlType;
    /// use readyset_data::{DfType, DfValue};
    ///
    /// let text = DfValue::from("2021-01-26 10:20:37");
    /// let timestamp = text
    ///     .coerce_to(&SqlType::Timestamp, &DfType::Unknown)
    ///     .unwrap();
    /// assert_eq!(
    ///     timestamp,
    ///     DfValue::TimestampTz(NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 20, 37).into())
    /// );
    /// ```
    pub fn coerce_to(&self, to_ty: &SqlType, from_ty: &DfType) -> ReadySetResult<DfValue> {
        use crate::text::TextCoerce;

        let mk_err = || ReadySetError::DfValueConversionError {
            src_type: from_ty.to_string(),
            target_type: to_ty.to_string(),
            details: "unsupported".into(),
        };

        // This lets us avoid repeating this logic in multiple match arms for different int types.
        // Maybe there's a nicer way to do this with a closure instead of a macro, but I haven't
        // been able to find any way to define a closure that takes generic parameters, which is
        // necessary if we want to support both u64 and i64 for both UnsignedInt and Int.
        macro_rules! handle_enum_or_coerce_int {
            ($v:expr, $to_ty:expr, $from_ty:expr) => {
                if let DfType::Enum(enum_elements, _) = $from_ty {
                    // This will either be u64 or i64, and if it's i64 then negative values will be
                    // converted to 0 anyway, so unwrap_or_default gets us what we want here:
                    let enum_val = u64::try_from(*$v).unwrap_or_default();
                    r#enum::coerce_enum(enum_val, enum_elements, $to_ty, $from_ty)
                } else {
                    integer::coerce_integer(*$v, $to_ty, $from_ty)
                }
            };
        }

        match self {
            DfValue::None => Ok(DfValue::None),
            DfValue::Array(arr) => match to_ty {
                SqlType::Array(t) => Ok(DfValue::from(arr.coerce_to(t, from_ty)?)),
                SqlType::Text => Ok(DfValue::from(arr.to_string())),
                _ => Err(mk_err()),
            },
            dt if dt.sql_type().as_ref() == Some(to_ty) => Ok(self.clone()),
            DfValue::Text(t) => t.coerce_to(to_ty, from_ty),
            DfValue::TinyText(tt) => tt.coerce_to(to_ty, from_ty),
            DfValue::TimestampTz(tz) => tz.coerce_to(to_ty),
            DfValue::Int(v) => handle_enum_or_coerce_int!(v, to_ty, from_ty),
            DfValue::UnsignedInt(v) => handle_enum_or_coerce_int!(v, to_ty, from_ty),
            DfValue::Float(f) => float::coerce_f64(f64::from(*f), to_ty, from_ty),
            DfValue::Double(f) => float::coerce_f64(*f, to_ty, from_ty),
            DfValue::Numeric(d) => float::coerce_decimal(d.as_ref(), to_ty, from_ty),
            DfValue::Time(ts) if to_ty.is_any_text() => Ok(ts.to_string().into()),
            DfValue::BitVector(vec) => match to_ty {
                SqlType::VarBit(None) => Ok(self.clone()),
                SqlType::VarBit(max_size_opt) => match max_size_opt {
                    Some(max_size) if vec.len() > *max_size as usize => Err(mk_err()),
                    _ => Ok(self.clone()),
                },
                _ => Err(mk_err()),
            },
            DfValue::Time(_) | DfValue::ByteArray(_) | DfValue::Max => Err(mk_err()),
            DfValue::PassThrough(ref p) => Err(ReadySetError::DfValueConversionError {
                src_type: format!("PassThrough[{}]", p.ty),
                target_type: to_ty.to_string(),
                details: "PassThrough items cannot be coerced".into(),
            }),
        }
    }

    /// Mutates the given DfType value to match its underlying database representation for the
    /// given column schema.
    pub fn maybe_coerce_for_table_op(&mut self, col_ty: &DfType) {
        if let DfType::Enum(enum_ty, _) = col_ty {
            // PERF: Cloning enum types is O(1).
            let enum_ty = SqlType::Enum(enum_ty.clone());

            // There shouldn't be any cases where this coerce_to call returns an error, but if
            // it does then a value of 0 is generally a safe bet for enum values that don't
            // trigger the happy path:
            *self = self
                .coerce_to(&enum_ty, &DfType::Unknown)
                .unwrap_or(DfValue::Int(0));
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
}

impl PartialEq for DfValue {
    fn eq(&self, other: &DfValue) -> bool {
        match (self, other) {
            (&DfValue::Text(ref a), &DfValue::Text(ref b)) => a == b,
            (&DfValue::TinyText(ref a), &DfValue::TinyText(ref b)) => a == b,
            (&DfValue::Text(..), &DfValue::TinyText(..))
            | (&DfValue::TinyText(..), &DfValue::Text(..)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let b: &str = <&str>::try_from(other).unwrap();
                a == b
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), &DfValue::TimestampTz(ref dt)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                let other_dt: Result<TimestampTz, _> = a.parse();
                other_dt.map(|other_dt| other_dt.eq(dt)).unwrap_or(false)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), &DfValue::Time(ref t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                a.parse()
                    .map(|other_t: MysqlTime| t.eq(&other_t))
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
            (&DfValue::Float(fa), &DfValue::Numeric(ref d)) => {
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
            (&DfValue::Double(fa), &DfValue::Numeric(ref d)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                d.to_f64()
                    .map(|df| fa.to_bits() == df.to_bits())
                    .unwrap_or(false)
            }
            (&DfValue::Numeric(ref da), &DfValue::Numeric(ref db)) => da == db,
            (&DfValue::Numeric(_), &DfValue::Float(_) | &DfValue::Double(_)) => other == self,
            (
                &DfValue::Time(_) | &DfValue::TimestampTz(_),
                &DfValue::Text(..) | &DfValue::TinyText(..),
            ) => other == self,
            (&DfValue::TimestampTz(tsa), &DfValue::TimestampTz(tsb)) => tsa == tsb,
            (&DfValue::Time(ref ta), &DfValue::Time(ref tb)) => ta == tb,
            (&DfValue::ByteArray(ref array_a), &DfValue::ByteArray(ref array_b)) => {
                array_a.as_ref() == array_b.as_ref()
            }
            (&DfValue::BitVector(ref bits_a), &DfValue::BitVector(ref bits_b)) => {
                bits_a.as_ref() == bits_b.as_ref()
            }
            (&DfValue::Array(ref vs_a), &DfValue::Array(ref vs_b)) => vs_a == vs_b,
            (&DfValue::None, &DfValue::None) => true,
            (&DfValue::Max, &DfValue::Max) => true,
            _ => false,
        }
    }
}

impl Default for DfValue {
    fn default() -> Self {
        DfValue::None
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
            (&DfValue::Text(ref a), &DfValue::Text(ref b)) => a.cmp(b),
            (&DfValue::TinyText(ref a), &DfValue::TinyText(ref b)) => a.as_str().cmp(b.as_str()),
            (&DfValue::Text(..), &DfValue::TinyText(..))
            | (&DfValue::TinyText(..), &DfValue::Text(..)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let b: &str = <&str>::try_from(other).unwrap();
                a.cmp(b)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), &DfValue::TimestampTz(ref other_dt)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                let dt: Result<TimestampTz, _> = a.parse();
                dt.map(|dt| dt.cmp(other_dt)).unwrap_or(Ordering::Greater)
            }
            (&DfValue::Text(..) | &DfValue::TinyText(..), &DfValue::Time(ref other_t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap().parse();
                a.map(|t: MysqlTime| t.cmp(other_t))
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
            (&DfValue::Numeric(ref da), &DfValue::Numeric(ref db)) => da.cmp(db),
            (&DfValue::Float(fa), &DfValue::Double(fb)) => fa.total_cmp(&(fb as f32)),
            (&DfValue::Double(fa), &DfValue::Float(fb)) => fb.total_cmp(&(fa as f32)).reverse(),
            (&DfValue::Float(fa), &DfValue::Numeric(ref d)) => {
                if let Some(da) = Decimal::from_f32_retain(fa) {
                    da.cmp(d)
                } else {
                    d.to_f32()
                        .as_ref()
                        .map(|fb| fa.total_cmp(fb))
                        .unwrap_or(Ordering::Greater)
                }
            }
            (&DfValue::Double(fa), &DfValue::Numeric(ref d)) => {
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
            (&DfValue::TimestampTz(ref tsa), &DfValue::TimestampTz(ref tsb)) => tsa.cmp(tsb),
            (&DfValue::Time(ref ta), &DfValue::Time(ref tb)) => ta.cmp(tb),

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
            (&DfValue::ByteArray(ref array_a), &DfValue::ByteArray(ref array_b)) => {
                array_a.cmp(array_b)
            }
            (&DfValue::BitVector(ref bits_a), &DfValue::BitVector(ref bits_b)) => {
                bits_a.cmp(bits_b)
            }
            (&DfValue::Array(ref vs_a), &DfValue::Array(ref vs_b)) => vs_a.cmp(vs_b),

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
                t.hash(state)
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

    fn try_from(s: i128) -> Result<Self, Self::Error> {
        if s >= std::i64::MIN.into() && s <= std::i64::MAX.into() {
            Ok(DfValue::Int(s as i64))
        } else if s >= std::u64::MIN.into() && s <= std::u64::MAX.into() {
            Ok(DfValue::UnsignedInt(s as u64))
        } else {
            Err(Self::Error::DfValueConversionError {
                src_type: "i128".to_string(),
                target_type: "DfValue".to_string(),
                details: "".to_string(),
            })
        }
    }
}

impl From<i64> for DfValue {
    fn from(s: i64) -> Self {
        DfValue::Int(s)
    }
}

impl From<u64> for DfValue {
    fn from(s: u64) -> Self {
        DfValue::UnsignedInt(s)
    }
}

impl From<i8> for DfValue {
    fn from(s: i8) -> Self {
        DfValue::Int(s.into())
    }
}

impl From<u8> for DfValue {
    fn from(s: u8) -> Self {
        DfValue::UnsignedInt(s.into())
    }
}

impl From<i16> for DfValue {
    fn from(s: i16) -> Self {
        DfValue::Int(s.into())
    }
}

impl From<u16> for DfValue {
    fn from(s: u16) -> Self {
        DfValue::UnsignedInt(s.into())
    }
}

impl From<i32> for DfValue {
    fn from(s: i32) -> Self {
        DfValue::Int(s as _)
    }
}

impl From<u32> for DfValue {
    fn from(s: u32) -> Self {
        DfValue::UnsignedInt(s as _)
    }
}

impl From<usize> for DfValue {
    fn from(s: usize) -> Self {
        DfValue::UnsignedInt(s as u64)
    }
}

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
                Decimal::from_f32(*value).ok_or(Self::Error::DfValueConversionError {
                    src_type: "DfValue".to_string(),
                    target_type: "Decimal".to_string(),
                    details: "".to_string(),
                })
            }
            DfValue::Double(value) => {
                Decimal::from_f64(*value).ok_or(Self::Error::DfValueConversionError {
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
            Literal::Array(_) => {
                fn find_shape(lit: &Literal, out: &mut Vec<usize>) -> ReadySetResult<()> {
                    if let Literal::Array(elems) = lit {
                        out.push(elems.len());
                        if let Some(elem) = elems.first() {
                            find_shape(elem, out)?
                        }
                    }
                    Ok(())
                }

                fn flatten(lit: &Literal, out: &mut Vec<DfValue>) -> ReadySetResult<()> {
                    match lit {
                        Literal::Array(elems) => {
                            for elem in elems {
                                flatten(elem, out)?;
                            }
                        }
                        _ => out.push(lit.try_into()?),
                    }
                    Ok(())
                }

                let mut shape = vec![];
                let mut elems = vec![];
                find_shape(l, &mut shape)?;
                flatten(l, &mut elems)?;

                Ok(DfValue::from(Array::from(
                    ArrayD::from_shape_vec(IxDyn(&shape), elems).map_err(|_| {
                        invalid_err!(
                            "Multidimensional arrays must have array expressions with matching \
                             dimensions",
                        )
                    })?,
                )))
            }
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

    fn try_from(dt: DfValue) -> Result<Self, Self::Error> {
        match dt {
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
            DfValue::Text(_) => Ok(Literal::String(String::try_from(dt)?)),
            DfValue::TinyText(_) => Ok(Literal::String(String::try_from(dt)?)),
            DfValue::TimestampTz(_) => Ok(Literal::String(String::try_from(
                dt.coerce_to(&SqlType::Text, &DfType::Unknown)?,
            )?)),
            DfValue::Time(_) => Ok(Literal::String(String::try_from(
                dt.coerce_to(&SqlType::Text, &DfType::Unknown)?,
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

impl From<MysqlTime> for DfValue {
    fn from(t: MysqlTime) -> Self {
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

impl<'a> TryFrom<&'a DfValue> for MysqlTime {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Time(ref mysql_time) => Ok(*mysql_time),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "MysqlTime".to_string(),
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

// This conversion has many unwraps, but all of them are expected to be safe,
// because DfValue variants (i.e. `Text` and `TinyText`) constructors are all
// generated from valid UTF-8 strings, or the constructor fails (e.g. TryFrom &[u8]).
// Thus, we can safely generate a &str from a DfValue.
impl<'a> TryFrom<&'a DfValue> for &'a str {
    type Error = ReadySetError;

    fn try_from(data: &'a DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Text(ref t) => Ok(t.as_str()),
            DfValue::TinyText(ref tt) => Ok(tt.as_str()),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: match data.sql_type() {
                    Some(ty) => ty.to_string(),
                    None => "Null".to_string(),
                },
                target_type: "&str".to_string(),
                details: "".to_string(),
            }),
        }
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
                    Some(ty) => ty.to_string(),
                    None => "Null".to_string(),
                },
                target_type: "&str".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DfValue> for i128 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <i128>::try_from(&data)
    }
}

impl TryFrom<DfValue> for i64 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<i64, Self::Error> {
        <i64>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for i128 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::Int(s) => Ok(i128::from(s)),
            DfValue::UnsignedInt(s) => Ok(i128::from(s)),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "i128".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<&'_ DfValue> for i64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::UnsignedInt(s) => {
                if s as i128 >= std::i64::MIN.into() && s as i128 <= std::i64::MAX.into() {
                    Ok(s as i64)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "i64".to_string(),
                        details: format!("Out of bounds {}", s),
                    })
                }
            }
            DfValue::Int(s) => Ok(s),
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "i64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DfValue> for u64 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <u64>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for u64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::UnsignedInt(s) => Ok(s),
            DfValue::Int(s) => {
                if s as i128 >= std::u64::MIN.into() && s as i128 <= std::u64::MAX.into() {
                    Ok(s as u64)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "u64".to_string(),
                        details: "Out of bounds".to_string(),
                    })
                }
            }

            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "u64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DfValue> for i32 {
    type Error = ReadySetError;

    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <i32>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for i32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::UnsignedInt(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    Ok(s as i32)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "i32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            DfValue::Int(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    Ok(s as i32)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "i32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }

            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "i32".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DfValue> for u32 {
    type Error = ReadySetError;
    fn try_from(data: DfValue) -> Result<Self, Self::Error> {
        <u32>::try_from(&data)
    }
}

impl TryFrom<&'_ DfValue> for u32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DfValue) -> Result<Self, Self::Error> {
        match *data {
            DfValue::UnsignedInt(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    Ok(s as u32)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "u32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            DfValue::Int(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    Ok(s as u32)
                } else {
                    Err(Self::Error::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "u32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            _ => Err(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "u32".to_string(),
                details: "".to_string(),
            }),
        }
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
            DfValue::Numeric(ref d) => d.to_f32().ok_or(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
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
            DfValue::Numeric(ref d) => d.to_f64().ok_or(Self::Error::DfValueConversionError {
                src_type: "DfValue".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
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
        if let Ok(tt) = TinyText::try_from(s) {
            DfValue::TinyText(tt)
        } else {
            DfValue::Text(s.into())
        }
    }
}

impl From<&[u8]> for DfValue {
    fn from(b: &[u8]) -> Self {
        // NOTE: should we *really* be converting to Text here?
        if let Ok(s) = std::str::from_utf8(b) {
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
                Ok(DfValue::Time(MysqlTime::from_hmsus(
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
                .parse::<IpAddr>()
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
                Type::TEXT | Type::VARCHAR | Type::NAME => mk_from_sql!(&str),
                Type::FLOAT4 => mk_from_sql!(f32),
                Type::FLOAT8 => mk_from_sql!(f64),
                Type::DATE => mk_from_sql!(NaiveDate),
                Type::TIME => mk_from_sql!(NaiveTime),
                Type::BYTEA => mk_from_sql!(Vec<u8>),
                Type::NUMERIC => mk_from_sql!(Decimal),
                Type::TIMESTAMP => mk_from_sql!(NaiveDateTime),
                Type::TIMESTAMPTZ => mk_from_sql!(chrono::DateTime<chrono::FixedOffset>),
                Type::MACADDR => Ok(DfValue::from(
                    MacAddress::from_sql(ty, raw)?.to_string(MacAddressFormat::HexString),
                )),
                Type::INET => Ok(DfValue::from(IpAddr::from_sql(ty, raw)?.to_string())),
                Type::UUID => Ok(DfValue::from(Uuid::from_sql(ty, raw)?.to_string())),
                Type::JSON | Type::JSONB => Ok(DfValue::from(
                    serde_json::Value::from_sql(ty, raw)?.to_string(),
                )),
                Type::BIT | Type::VARBIT => mk_from_sql!(BitVec),
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
            DfValue::Numeric(_) => {
                internal!("DfValue::Numeric to MySQL DECIMAL is not implemented")
            }
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
macro_rules! arithmetic_operation (
    ($op:tt, $first:ident, $second:ident) => (
        match ($first, $second) {
            (&DfValue::None, _) | (_, &DfValue::None) => DfValue::None,
            (&DfValue::Int(a), &DfValue::Int(b)) => (a $op b).into(),
            (&DfValue::UnsignedInt(a), &DfValue::UnsignedInt(b)) => (a $op b).into(),

            (&DfValue::UnsignedInt(a), &DfValue::Int(b)) => DfValue::try_from(i128::from(a) $op i128::from(b))?,
            (&DfValue::Int(a), &DfValue::UnsignedInt(b)) => DfValue::try_from(i128::from(a) $op i128::from(b))?,

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
                DfValue::from(a $op b)
            }
            (first @ &DfValue::Numeric(..), second @ &DfValue::Float(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f32::try_from(second).and_then(|f| Decimal::from_f32(f)
                    .ok_or(ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DfValue::from(a $op b)
            }
            (first @ &DfValue::Numeric(..), second @ &DfValue::Double(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f64::try_from(second).and_then(|f| Decimal::from_f64(f)
                    .ok_or(ReadySetError::DfValueConversionError {
                        src_type: "DfValue".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DfValue::from(a $op b)
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
        Ok(arithmetic_operation!(+, self, other))
    }
}

impl<'a, 'b> Sub<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn sub(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(-, self, other))
    }
}

impl<'a, 'b> Mul<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn mul(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(*, self, other))
    }
}

impl<'a, 'b> Div<&'b DfValue> for &'a DfValue {
    type Output = ReadySetResult<DfValue>;

    fn div(self, other: &'b DfValue) -> Self::Output {
        Ok(arithmetic_operation!(/, self, other))
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
                .prop_map(MysqlTime::new)
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
                    .prop_map(MysqlTime::new)
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
    use launchpad::{eq_laws, hash_laws, ord_laws};
    use ndarray::{ArrayD, IxDyn};
    use proptest::prelude::*;
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
        // relation actually matters (persisted base table state) we only store homogenously-typed
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
            DfValue::ByteArray(_)
            | DfValue::Numeric(_)
            | DfValue::BitVector(_)
            | DfValue::Array(_)
            | DfValue::Max => false,
            _ => true,
        });

        match (
            DfValue::try_from(Value::try_from(dt.clone()).unwrap()).unwrap(),
            dt,
        ) {
            (DfValue::Float(f1), DfValue::Float(f2)) => assert_eq!(f1, f2),
            (DfValue::Double(f1), DfValue::Double(f2)) => assert_eq!(f1, f2),
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
            DfValue::Time(MysqlTime::from_microseconds(0))
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
        assert_eq!((&DfValue::Int(4) / &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::from(4) / &DfValue::Int(2)).unwrap(), 2.into());
        assert_eq!((&DfValue::Int(4) / &DfValue::from(2)).unwrap(), 2.into());
    }

    #[test]
    fn data_type_debug() {
        let tiny_text: DfValue = "hi".try_into().unwrap();
        let text: DfValue = "I contain ' and \"".try_into().unwrap();
        let float_from_real: DfValue = DfValue::try_from(-0.05_f32).unwrap();
        let float = DfValue::Float(-0.05);
        let double_from_real: DfValue = DfValue::try_from(-0.05_f64).unwrap();
        let double = DfValue::Double(-0.05);
        let numeric = DfValue::from(Decimal::new(-5, 2)); // -0.05
        let timestamp_tz = DfValue::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let int = DfValue::Int(5);
        let bytes = DfValue::ByteArray(Arc::new(vec![0, 8, 39, 92, 100, 128]));
        // bits = 000000000000100000100111010111000110010010000000
        let bits = DfValue::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));
        assert_eq!(format!("{:?}", tiny_text), "TinyText(\"hi\")");
        assert_eq!(format!("{:?}", text), "Text(\"I contain ' and \\\"\")");
        assert_eq!(format!("{:?}", float_from_real), "Float(-0.05)");
        assert_eq!(format!("{:?}", float), "Float(-0.05)");
        assert_eq!(format!("{:?}", double_from_real), "Double(-0.05)");
        assert_eq!(format!("{:?}", double), "Double(-0.05)");
        assert_eq!(format!("{:?}", numeric), "Numeric(-0.05)");
        assert_eq!(
            format!("{:?}", timestamp_tz),
            "TimestampTz(1969-12-31T19:00:00.042-05:00)"
        );
        assert_eq!(format!("{:?}", int), "Int(5)");
        assert_eq!(
            format!("{:?}", bytes),
            "ByteArray([0, 8, 39, 92, 100, 128])"
        );
        assert_eq!(
            format!("{:?}", bits),
            "BitVector(000000000000100000100111010111000110010010000000)"
        );
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
        assert_eq!(format!("{}", tiny_text), "hi");
        assert_eq!(format!("{}", text), "this is a very long text indeed");
        assert_eq!(format!("{}", float_from_real), "-8.99");
        assert_eq!(format!("{}", float), "-8.99");
        assert_eq!(format!("{}", double_from_real), "-8.99");
        assert_eq!(format!("{}", double), "-8.99");
        assert_eq!(format!("{}", numeric), "-8.99");
        assert_eq!(format!("{}", timestamp), "1970-01-01 00:00:00");
        assert_eq!(format!("{}", timestamp_tz), "1969-12-31 18:30:00-05:30");
        assert_eq!(format!("{}", int), "5");
        assert_eq!(format!("{}", bytes), "E'\\x0008275c6480'");
        assert_eq!(
            format!("{}", bits),
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
            assert!(std::panic::catch_unwind(|| i32::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_i64(d: &DfValue) {
            assert_eq!(
                i64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i64_panic(d: &DfValue) {
            assert!(std::panic::catch_unwind(|| i64::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_u32(d: &DfValue) {
            assert_eq!(
                u32::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u32_panic(d: &DfValue) {
            assert!(std::panic::catch_unwind(|| u32::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_u64(d: &DfValue) {
            assert_eq!(
                u64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u64_panic(d: &DfValue) {
            assert!(std::panic::catch_unwind(|| u64::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_i128(d: &DfValue) {
            assert_eq!(
                i128::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i128_panic(d: &DfValue) {
            assert!(std::panic::catch_unwind(|| i128::try_from(d).unwrap()).is_err())
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
        let decode_type = original_type.clone();
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
        // 1. Text types always > everythign else
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

    #[test]
    fn array_from_literal() {
        let literal = Literal::Array(vec![
            Literal::Array(vec![1.into(), 2.into(), 3.into()]),
            Literal::Array(vec![4.into(), 5.into(), 6.into()]),
        ]);
        let dt = DfValue::try_from(literal).unwrap();
        assert_eq!(
            dt,
            DfValue::from(Array::from(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 3]),
                    vec![
                        DfValue::from(1),
                        DfValue::from(2),
                        DfValue::from(3),
                        DfValue::from(4),
                        DfValue::from(5),
                        DfValue::from(6),
                    ]
                )
                .unwrap()
            ))
        );
    }

    #[test]
    fn array_from_invalid_literal() {
        let literal = Literal::Array(vec![
            Literal::Array(vec![1.into(), 2.into()]),
            Literal::Array(vec![3.into()]),
        ]);
        let res = DfValue::try_from(literal);
        assert!(res.is_err());
        assert!(res.err().unwrap().is_invalid_query())
    }

    mod coerce_to {
        use launchpad::arbitrary::{
            arbitrary_naive_date, arbitrary_naive_date_time, arbitrary_naive_time,
        };
        use rust_decimal::Decimal;
        use test_strategy::proptest;
        use SqlType::*;

        use super::*;

        #[proptest]
        fn same_type_is_identity(dt: DfValue) {
            if let Some(ty) = dt.sql_type() {
                assert_eq!(dt.coerce_to(&ty, &DfType::Unknown).unwrap(), dt);
            }
        }

        #[proptest]
        fn parse_timestamps(#[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime) {
            let expected = DfValue::from(ndt);
            let input = DfValue::from(ndt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string());
            let result = input.coerce_to(&Timestamp, &DfType::Unknown).unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_times(#[strategy(arbitrary_naive_time())] nt: NaiveTime) {
            let expected = DfValue::from(nt);
            let input = DfValue::from(nt.format(TIME_FORMAT).to_string());
            let result = input.coerce_to(&Time, &DfType::Unknown).unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_datetimes(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let dt = NaiveDateTime::new(nd, NaiveTime::from_hms(12, 0, 0));
            let expected = DfValue::from(dt);
            let input = DfValue::from(dt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string());
            let result = input
                .coerce_to(&SqlType::DateTime(None), &DfType::Unknown)
                .unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_dates(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let expected = DfValue::from(NaiveDateTime::new(nd, NaiveTime::from_hms(0, 0, 0)));
            let input = DfValue::from(nd.format(crate::timestamp::DATE_FORMAT).to_string());
            let result = input.coerce_to(&Date, &DfType::Unknown).unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn timestamp_surjections() {
            let input = DfValue::from(NaiveDate::from_ymd(2021, 3, 17).and_hms(11, 34, 56));
            assert_eq!(
                input.coerce_to(&Date, &DfType::Unknown).unwrap(),
                NaiveDate::from_ymd(2021, 3, 17).into()
            );
            assert_eq!(
                input.coerce_to(&Time, &DfType::Unknown).unwrap(),
                NaiveTime::from_hms(11, 34, 56).into()
            );
        }

        #[proptest]
        fn timestamp_to_datetime(
            #[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime,
            prec: Option<u16>,
        ) {
            let input = DfValue::from(ndt);
            assert_eq!(
                input
                    .clone()
                    .coerce_to(&SqlType::DateTime(prec), &DfType::Unknown)
                    .unwrap(),
                input
            );
        }

        macro_rules! int_conversion {
            ($name: ident, $from: ty, $to: ty, $sql_type: expr) => {
                #[proptest]
                fn $name(source: $to) {
                    let input = <$from>::try_from(source);
                    prop_assume!(input.is_ok());
                    assert_eq!(
                        DfValue::from(input.unwrap())
                            .coerce_to(&$sql_type, &DfType::Unknown)
                            .unwrap(),
                        DfValue::from(source)
                    );
                }
            };
        }

        int_conversion!(int_to_tinyint, i32, i8, TinyInt(None));
        int_conversion!(int_to_unsigned_tinyint, i32, u8, UnsignedTinyInt(None));
        int_conversion!(int_to_smallint, i32, i16, SmallInt(None));
        int_conversion!(int_to_unsigned_smallint, i32, u16, UnsignedSmallInt(None));
        int_conversion!(bigint_to_tinyint, i64, i8, TinyInt(None));
        int_conversion!(bigint_to_unsigned_tinyint, i64, u8, UnsignedTinyInt(None));
        int_conversion!(bigint_to_smallint, i64, i16, SmallInt(None));
        int_conversion!(
            bigint_to_unsigned_smallint,
            i64,
            u16,
            UnsignedSmallInt(None)
        );
        int_conversion!(bigint_to_int, i64, i32, Int(None));
        int_conversion!(bigint_to_serial, i64, i32, Serial);
        int_conversion!(bigint_to_unsigned_bigint, i64, u64, UnsignedBigInt(None));

        macro_rules! real_conversion {
            ($name: ident, $from: ty, $to: ty, $sql_type: expr) => {
                #[proptest]
                fn $name(source: $from) {
                    if (source as $to).is_finite() {
                        assert_eq!(
                            DfValue::try_from(source)
                                .unwrap()
                                .coerce_to(&$sql_type, &DfType::Unknown)
                                .unwrap(),
                            DfValue::try_from(source as $to).unwrap()
                        );
                    } else {
                        assert!(DfValue::try_from(source)
                            .unwrap()
                            .coerce_to(&$sql_type, &DfType::Unknown)
                            .is_err());
                        assert!(DfValue::try_from(source as $to).is_err());
                    }
                }
            };
        }

        real_conversion!(float_to_double, f32, f64, Double);

        real_conversion!(double_to_float, f64, f32, Float);

        #[proptest]
        fn char_equal_length(#[strategy("a{1,30}")] text: String) {
            use SqlType::*;
            let input = DfValue::from(text.as_str());
            let intermediate = Char(Some(u16::try_from(text.len()).unwrap()));
            let result = input.coerce_to(&intermediate, &DfType::Unknown).unwrap();
            assert_eq!(String::try_from(&result).unwrap().as_str(), text.as_str());
        }

        #[test]
        fn text_to_json() {
            let input = DfValue::from("{\"name\": \"John Doe\", \"age\": 43, \"phones\": [\"+44 1234567\", \"+44 2345678\"] }");
            let result = input.coerce_to(&SqlType::Json, &DfType::Unknown).unwrap();
            assert_eq!(input, result);

            let result = input.coerce_to(&SqlType::Jsonb, &DfType::Unknown).unwrap();
            assert_eq!(input, result);

            let input = DfValue::from("not a json");
            let result = input.coerce_to(&SqlType::Json, &DfType::Unknown);
            assert!(result.is_err());

            let result = input.coerce_to(&SqlType::Jsonb, &DfType::Unknown);
            assert!(result.is_err());
        }

        #[test]
        fn text_to_macaddr() {
            let input = DfValue::from("12:34:56:ab:cd:ef");
            let result = input
                .coerce_to(&SqlType::MacAddr, &DfType::Unknown)
                .unwrap();
            assert_eq!(input, result);
        }

        #[test]

        fn int_to_text() {
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&SqlType::Text, &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20070523"),
            );
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&SqlType::VarChar(Some(2)), &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20"),
            );
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&SqlType::Char(Some(10)), &DfType::Unknown)
                    .unwrap(),
                DfValue::from("20070523  "),
            );
        }

        #[test]
        fn int_to_date_time() {
            assert_eq!(
                DfValue::from(20070523i64)
                    .coerce_to(&SqlType::Date, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(2007, 05, 23))
            );

            assert_eq!(
                DfValue::from(70523u64)
                    .coerce_to(&SqlType::Date, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(2007, 05, 23))
            );

            assert_eq!(
                DfValue::from(19830905132800i64)
                    .coerce_to(&SqlType::Timestamp, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(1983, 09, 05).and_hms(13, 28, 00))
            );

            assert_eq!(
                DfValue::from(830905132800u64)
                    .coerce_to(&SqlType::DateTime(None), &DfType::Unknown)
                    .unwrap(),
                DfValue::from(NaiveDate::from_ymd(1983, 09, 05).and_hms(13, 28, 00))
            );

            assert_eq!(
                DfValue::from(101112i64)
                    .coerce_to(&SqlType::Time, &DfType::Unknown)
                    .unwrap(),
                DfValue::from(MysqlTime::from_hmsus(true, 10, 11, 12, 0))
            );
        }

        #[test]
        fn text_to_uuid() {
            let input = DfValue::from(uuid::Uuid::new_v4().to_string());
            let result = input.coerce_to(&SqlType::Uuid, &DfType::Unknown).unwrap();
            assert_eq!(input, result);
        }

        macro_rules! bool_conversion {
            ($name: ident, $ty: ty) => {
                #[proptest]
                fn $name(input: $ty) {
                    let input_dt = DfValue::from(input);
                    let result = input_dt
                        .coerce_to(&SqlType::Bool, &DfType::Unknown)
                        .unwrap();
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
                .coerce_to(&SqlType::Array(Box::new(SqlType::Text)), &DfType::Unknown)
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
                .coerce_to(&SqlType::Array(Box::new(SqlType::Text)), &DfType::Unknown)
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
                .coerce_to(&SqlType::Array(Box::new(SqlType::Text)), &DfType::Unknown)
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
            let enum_ty = SqlType::from_enum_variants(
                variants.iter().map(|s| Literal::String(s.to_string())),
            );
            let from_ty = DfType::from_sql_type(&enum_ty, Dialect::MySQL);

            // Test conversions from enums to strings
            for dv in [DfValue::Int(2), DfValue::UnsignedInt(2)] {
                assert_eq!(
                    "yellow",
                    dv.coerce_to(&SqlType::Text, &from_ty).unwrap().to_string()
                )
            }

            // Test conversion of invalid enum indices to the empty string
            for i in [-1, 0, 4] {
                assert_eq!(
                    "",
                    DfValue::Int(i)
                        .coerce_to(&SqlType::Text, &from_ty)
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
                .coerce_to(&SqlType::Char(Some(3)), &from_ty)
                .unwrap();
            assert_eq!("yel", result.to_string());

            let result = DfValue::Int(2)
                .coerce_to(&SqlType::VarChar(Some(3)), &from_ty)
                .unwrap();
            assert_eq!("yel", result.to_string());

            let result = DfValue::Int(2)
                .coerce_to(&SqlType::Char(Some(10)), &from_ty)
                .unwrap();
            assert_eq!("yellow    ", result.to_string());

            let no_change_tys = [
                SqlType::VarChar(Some(10)),
                SqlType::Char(None),
                SqlType::VarChar(None),
                SqlType::Text,
                SqlType::TinyText,
                SqlType::MediumText,
                SqlType::LongText,
            ];

            for to_ty in no_change_tys {
                let result = DfValue::Int(2).coerce_to(&to_ty, &from_ty).unwrap();
                assert_eq!("yellow", result.to_string());
            }
        }
    }
}
