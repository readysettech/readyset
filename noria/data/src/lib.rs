use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::ops::{Add, Div, Mul, Sub};
use std::{fmt, mem};

use bytes::BytesMut;
use chrono::{self, DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use enum_kinds::EnumKind;
use itertools::Itertools;
use launchpad::arbitrary::arbitrary_duration;
use nom_sql::{Double, Float, Literal, SqlType};
use noria_errors::{internal, unsupported, ReadySetError, ReadySetResult};
use proptest::prelude::{prop_oneof, Arbitrary};
use test_strategy::Arbitrary;
use tokio_postgres::types::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

mod array;
mod float;
mod integer;
pub mod noria_type;
mod serde;
mod text;
mod timestamp;

pub use crate::array::Array;
pub use crate::text::{Text, TinyText};
pub use crate::timestamp::{TimestampTz, TIMESTAMP_FORMAT};

/// DateTime offsets must be bigger than -86_000 seconds and smaller than 86_000 (not inclusive in
/// either case), and we don't care about seconds, so our maximum offset is gonna be
/// 86_000 - 60 = 85_940.
const MAX_SECONDS_DATETIME_OFFSET: i32 = 85_940;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DataType` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DataType::deep_clone` to
/// clone the *value* of a `DataType` without danger of contention.
#[warn(variant_size_differences)]
#[derive(Clone, Debug, EnumKind)]
#[enum_kind(DataTypeKind, derive(Ord, PartialOrd, Hash, Arbitrary))]
pub enum DataType {
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
    //NOTE(Fran): Using an `Arc` to keep the `DataType` type 16 bytes long
    /// A byte array
    ByteArray(Arc<Vec<u8>>),
    /// A fixed-point fractional representation.
    Numeric(Arc<Decimal>),
    /// A bit or varbit value.
    BitVector(Arc<BitVec>),
    /// An array of [`DataType`]s.
    Array(Arc<Array>),
    /// A sentinel maximal value.
    ///
    /// This value is always greater than all other [`DataType`]s, except itself.
    ///
    /// This is a special value, as it cannot ever be constructed by user supplied values, and
    /// always returns an error when encountered during expression evaluation. Its only use is as
    /// the upper bound in a range query
    // NOTE: when adding new DataType variants, make sure to always keep Max last - we use the
    // order of the variants to compare
    Max,
}

impl Eq for DataType {}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "NULL"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: &str = <&str>::try_from(self).map_err(|_| fmt::Error)?;
                write!(f, "{}", text)
            }
            DataType::Int(n) => write!(f, "{}", n),
            DataType::UnsignedInt(n) => write!(f, "{}", n),
            DataType::Float(n) => write!(f, "{}", n),
            DataType::Double(n) => write!(f, "{}", n),
            DataType::TimestampTz(ref ts) => write!(f, "{}", ts),
            DataType::Time(ref t) => write!(f, "{}", t),
            DataType::ByteArray(ref array) => {
                write!(
                    f,
                    "E'\\x{}'",
                    array.iter().map(|byte| format!("{:02x}", byte)).join("")
                )
            }
            DataType::Numeric(ref d) => write!(f, "{}", d),
            DataType::BitVector(ref b) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|bit| if bit { "1" } else { "0" }).join("")
                )
            }
            DataType::Array(ref arr) => write!(f, "{}", arr),
            DataType::Max => f.write_str("MAX"),
        }
    }
}

/// The format for times when parsed as text
pub const TIME_FORMAT: &str = "%H:%M:%S";

impl DataType {
    /// Construct a new [`DataType::Array`] containing an empty array
    pub fn empty_array() -> Self {
        // TODO: static singleton empty array?
        DataType::from(Vec::<DataType>::new())
    }

    /// Generates the minimum DataType corresponding to the type of a given DataType.
    pub fn min_value(other: &Self) -> Self {
        match other {
            DataType::None => DataType::None,
            DataType::Text(_) | DataType::TinyText(_) => DataType::TinyText("".try_into().unwrap()), /* Safe because fits in length */
            DataType::TimestampTz(_) => DataType::from(
                FixedOffset::west(-MAX_SECONDS_DATETIME_OFFSET).from_utc_datetime(
                    &NaiveDateTime::new(chrono::naive::MIN_DATE, NaiveTime::from_hms(0, 0, 0)),
                ),
            ),
            DataType::Float(..) => DataType::Float(f32::MIN),
            DataType::Double(..) => DataType::Double(f64::MIN),
            DataType::Int(_) => DataType::Int(i64::min_value()),
            DataType::UnsignedInt(_) => DataType::UnsignedInt(0),
            DataType::Time(_) => DataType::Time(MysqlTime::min_value()),
            DataType::ByteArray(_) => DataType::ByteArray(Arc::new(Vec::new())),
            DataType::Numeric(_) => DataType::from(Decimal::MIN),
            DataType::BitVector(_) => DataType::from(BitVec::new()),
            DataType::Array(_) => DataType::empty_array(),
            DataType::Max => DataType::None,
        }
    }

    /// Generates the maximum DataType corresponding to the type of a given DataType.
    pub fn max_value(other: &Self) -> Self {
        match other {
            DataType::None => DataType::None,
            DataType::TimestampTz(_) => DataType::from(
                FixedOffset::east(MAX_SECONDS_DATETIME_OFFSET).from_utc_datetime(
                    &NaiveDateTime::new(chrono::naive::MAX_DATE, NaiveTime::from_hms(23, 59, 59)),
                ),
            ),
            DataType::Float(..) => DataType::Float(f32::MAX),
            DataType::Double(..) => DataType::Double(f64::MIN),
            DataType::Int(_) => DataType::Int(i64::max_value()),
            DataType::UnsignedInt(_) => DataType::UnsignedInt(u64::max_value()),
            DataType::Time(_) => DataType::Time(MysqlTime::max_value()),
            DataType::Numeric(_) => DataType::from(Decimal::MAX),
            DataType::TinyText(_)
            | DataType::Text(_)
            | DataType::ByteArray(_)
            | DataType::BitVector(_)
            | DataType::Array(_)
            | DataType::Max => DataType::Max,
        }
    }

    /// Clone the value contained within this `DataType`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        match *self {
            DataType::Text(ref text) => DataType::Text(text.as_str().into()),
            DataType::ByteArray(ref bytes) => DataType::ByteArray(Arc::new(bytes.as_ref().clone())),
            DataType::BitVector(ref bits) => DataType::from(bits.as_ref().clone()),
            ref dt => dt.clone(),
        }
    }

    /// Checks if this value is `DataType::None`.
    pub fn is_none(&self) -> bool {
        matches!(*self, DataType::None)
    }

    /// Checks if this value is of an integral data type (i.e., can be converted into integral
    /// types).
    pub fn is_integer(&self) -> bool {
        matches!(*self, DataType::Int(_) | DataType::UnsignedInt(_))
    }

    /// Checks if this value is of a real data type (i.e., can be converted into `f32` or `f64`).
    pub fn is_real(&self) -> bool {
        matches!(*self, DataType::Float(_) | DataType::Double(_))
    }

    /// Checks if this value is of a string data type (i.e., can be converted into `String` and
    /// `&str`).
    pub fn is_string(&self) -> bool {
        matches!(*self, DataType::Text(_) | DataType::TinyText(_))
    }

    /// Checks if this value is of a timestamp data type.
    pub fn is_datetime(&self) -> bool {
        matches!(*self, DataType::TimestampTz(_))
    }

    /// Checks if this value is of a time data type.
    pub fn is_time(&self) -> bool {
        matches!(*self, DataType::Time(_))
    }

    /// Checks if this value is of a byte array data type.
    pub fn is_byte_array(&self) -> bool {
        matches!(*self, DataType::ByteArray(_))
    }

    /// Returns true if this datatype is truthy (is not 0, 0.0, '', or NULL)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use noria_data::DataType;
    ///
    /// assert!(!DataType::None.is_truthy());
    /// assert!(!DataType::Int(0).is_truthy());
    /// assert!(DataType::Int(1).is_truthy());
    /// ```
    pub fn is_truthy(&self) -> bool {
        match *self {
            DataType::None | DataType::Max => false,
            DataType::Int(x) => x != 0,
            DataType::UnsignedInt(x) => x != 0,
            DataType::Float(f) => f != 0.0,
            DataType::Double(f) => f != 0.0,
            DataType::Text(ref t) => !t.as_str().is_empty(),
            DataType::TinyText(ref tt) => !tt.as_bytes().is_empty(),
            DataType::TimestampTz(ref dt) => {
                dt.to_chrono().naive_local() != NaiveDate::from_ymd(0, 0, 0).and_hms(0, 0, 0)
            }
            DataType::Time(ref t) => *t != MysqlTime::from_microseconds(0),
            DataType::ByteArray(ref array) => !array.is_empty(),
            DataType::Numeric(ref d) => !d.is_zero(),
            DataType::BitVector(ref bits) => !bits.is_empty(),
            // Truthiness only matters for mysql, and mysql doesn't have arrays, so we can kind of
            // pick whatever we want here - but it makes the most sense to try to limit falsiness to
            // only the things that mysql considers falsey
            DataType::Array(_) => true,
        }
    }

    /// Checks if the given DataType::Double or DataType::Float is equal to another DataType::Double
    /// or DataType::Float (respectively) under an acceptable error margin. If None is supplied,
    /// we use f32::EPSILON or f64::EPSILON, accordingly.
    pub fn equal_under_error_margin(&self, other: &DataType, error_margin: Option<f64>) -> bool {
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
            DataType::Float(self_float) => match other {
                DataType::Float(other_float) => {
                    let other_epsilon = error_margin.map(|f| f as f32);
                    numeric_comparison!(self_float, other_float, other_epsilon, f32::EPSILON)
                }
                _ => false,
            },
            DataType::Double(self_double) => match other {
                DataType::Double(other_double) => {
                    numeric_comparison!(self_double, other_double, error_margin, f64::EPSILON)
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Returns the SqlType for this DataType, or None if [`DataType::None`] (which is valid for any
    /// type)
    pub fn sql_type(&self) -> Option<SqlType> {
        use SqlType::*;
        match self {
            Self::None | Self::Max => None,
            Self::Int(_) => Some(Bigint(None)),
            Self::UnsignedInt(_) => Some(UnsignedBigint(None)),
            Self::Float(_) => Some(Float),
            Self::Double(_) => Some(Real),
            Self::Text(_) => Some(Text),
            Self::TinyText(_) => Some(Tinytext),
            Self::TimestampTz(_) => Some(TimestampTz), // TODO: Timestamp if no tz
            Self::Time(_) => Some(Time),
            Self::ByteArray(_) => Some(ByteArray),
            Self::Numeric(_) => Some(Numeric(None)),
            Self::BitVector(_) => Some(Varbit(None)),
            // TODO: Once this returns NoriaType instead of SqlType, an empty array and an array of
            // null should be Array(Unknown) not Unknown
            Self::Array(vs) => Some(SqlType::Array(Box::new(
                vs.values().find_map(|v| v.sql_type())?,
            ))),
        }
    }

    /// Attempt to coerce the given DataType to a value of the given `SqlType`.
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
    /// use noria_data::DataType;
    ///
    /// let real = DataType::Double(123.0);
    /// let int = real.coerce_to(&SqlType::Int(None)).unwrap();
    /// assert_eq!(int, DataType::Int(123));
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
    /// use noria_data::DataType;
    ///
    /// let text = DataType::try_from("2021-01-26 10:20:37").unwrap();
    /// let timestamp = text.coerce_to(&SqlType::Timestamp).unwrap();
    /// assert_eq!(
    ///     timestamp,
    ///     DataType::TimestampTz(NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 20, 37).into())
    /// );
    /// ```
    pub fn coerce_to(&self, ty: &SqlType) -> ReadySetResult<DataType> {
        use crate::text::TextCoerce;

        let mk_err = || ReadySetError::DataTypeConversionError {
            src_type: format!("{:?}", DataTypeKind::from(self)),
            target_type: ty.to_string(),
            details: "unsupported".into(),
        };

        match self {
            DataType::None => Ok(DataType::None),
            dt if dt.sql_type().as_ref() == Some(ty) => Ok(self.clone()),
            DataType::Text(t) => t.coerce_to(ty),
            DataType::TinyText(tt) => tt.coerce_to(ty),
            DataType::TimestampTz(tz) => tz.coerce_to(ty),
            DataType::Int(v) => integer::coerce_integer(*v, "Int", ty),
            DataType::UnsignedInt(v) => integer::coerce_integer(*v, "UnsignedInt", ty),
            // We can coerce f32 as f64, because casting to the latter doesn't increase precision
            // and therfore is equivalent
            DataType::Float(f) => float::coerce_f64(*f as f64, ty),
            DataType::Double(f) => float::coerce_f64(*f, ty),
            DataType::Numeric(d) => float::coerce_decimal(d.as_ref(), ty),
            DataType::Time(ts)
                if matches!(
                    ty,
                    SqlType::Text | SqlType::Tinytext | SqlType::Mediumtext | SqlType::Varchar(_)
                ) =>
            {
                Ok(ts.to_string().into())
            }
            DataType::BitVector(vec) => match ty {
                SqlType::Varbit(None) => Ok(self.clone()),
                SqlType::Varbit(max_size_opt) => match max_size_opt {
                    Some(max_size) if vec.len() > *max_size as usize => Err(mk_err()),
                    _ => Ok(self.clone()),
                },
                _ => Err(mk_err()),
            },
            DataType::Time(_) | DataType::ByteArray(_) | DataType::Array(_) | DataType::Max => {
                Err(mk_err())
            }
        }
    }

    /// Returns Some(&self) if self is not [`DataType::None`]
    ///
    /// # Examples
    ///
    /// ```
    /// use noria_data::DataType;
    ///
    /// assert_eq!(DataType::Int(12).non_null(), Some(&DataType::Int(12)));
    /// assert_eq!(DataType::None.non_null(), None);
    /// ```
    pub fn non_null(&self) -> Option<&Self> {
        if self.is_none() {
            None
        } else {
            Some(self)
        }
    }

    /// Returns Some(self) if self is not [`DataType::None`]
    ///
    /// # Examples
    ///
    /// ```
    /// use noria_data::DataType;
    ///
    /// assert_eq!(DataType::Int(12).into_non_null(), Some(DataType::Int(12)));
    /// assert_eq!(DataType::None.into_non_null(), None);
    /// ```
    pub fn into_non_null(self) -> Option<Self> {
        if self.is_none() {
            None
        } else {
            Some(self)
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        unsafe {
            use std::slice;
            // if the two datatypes are byte-for-byte identical, they're the same.
            let a: &[u8] =
                slice::from_raw_parts(&*self as *const _ as *const u8, mem::size_of::<Self>());
            let b: &[u8] =
                slice::from_raw_parts(&*other as *const _ as *const u8, mem::size_of::<Self>());
            if a == b {
                return true;
            }
        }

        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a == b,
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
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
            (&DataType::Text(..) | &DataType::TinyText(..), &DataType::TimestampTz(ref dt)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                let other_dt: Result<TimestampTz, _> = a.parse();
                other_dt.map(|other_dt| other_dt.eq(dt)).unwrap_or(false)
            }
            (&DataType::Text(..) | &DataType::TinyText(..), &DataType::Time(ref t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap();
                a.parse()
                    .map(|other_t: MysqlTime| t.eq(&other_t))
                    .unwrap_or(false)
            }
            (&DataType::Int(a), &DataType::Int(b)) => a == b,
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a == b,
            (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..)) => {
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
            (&DataType::Float(fa), &DataType::Float(fb)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == fb.to_bits()
            }
            (&DataType::Float(fa), &DataType::Numeric(ref d)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                d.to_f32()
                    .map(|df| fa.to_bits() == df.to_bits())
                    .unwrap_or(false)
            }
            (&DataType::Double(fa), &DataType::Double(fb)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == fb.to_bits()
            }
            (&DataType::Double(fa), &DataType::Float(fb))
            | (&DataType::Float(fb), &DataType::Double(fa)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                fa.to_bits() == (fb as f64).to_bits()
            }
            (&DataType::Double(fa), &DataType::Numeric(ref d)) => {
                // We need to compare the *bit patterns* of the floats so that our Hash matches our
                // Eq
                d.to_f64()
                    .map(|df| fa.to_bits() == df.to_bits())
                    .unwrap_or(false)
            }
            (&DataType::Numeric(ref da), &DataType::Numeric(ref db)) => da == db,
            (&DataType::Numeric(_), &DataType::Float(_) | &DataType::Double(_)) => other == self,
            (
                &DataType::Time(_) | &DataType::TimestampTz(_),
                &DataType::Text(..) | &DataType::TinyText(..),
            ) => other == self,
            (&DataType::TimestampTz(tsa), &DataType::TimestampTz(tsb)) => tsa == tsb,
            (&DataType::Time(ref ta), &DataType::Time(ref tb)) => ta == tb,
            (&DataType::ByteArray(ref array_a), &DataType::ByteArray(ref array_b)) => {
                array_a.as_ref() == array_b.as_ref()
            }
            (&DataType::BitVector(ref bits_a), &DataType::BitVector(ref bits_b)) => {
                bits_a.as_ref() == bits_b.as_ref()
            }
            (&DataType::Array(ref vs_a), &DataType::Array(ref vs_b)) => vs_a == vs_b,
            (&DataType::None, &DataType::None) => true,
            (&DataType::Max, &DataType::Max) => true,
            _ => false,
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::None
    }
}

use std::cmp::Ordering;
use std::sync::Arc;

use bit_vec::BitVec;
use eui48::{MacAddress, MacAddressFormat};
use launchpad::arbitrary::arbitrary_decimal;
use mysql_time::MysqlTime;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use uuid::Uuid;

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &DataType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataType {
    fn cmp(&self, other: &DataType) -> Ordering {
        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a.cmp(b),
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a.as_str().cmp(b.as_str()),
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
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
            (
                &DataType::Text(..) | &DataType::TinyText(..),
                &DataType::TimestampTz(ref other_dt),
            ) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a: &str = <&str>::try_from(self).unwrap();
                let dt: Result<TimestampTz, _> = a.parse();
                dt.map(|dt| dt.cmp(other_dt)).unwrap_or(Ordering::Greater)
            }
            (&DataType::Text(..) | &DataType::TinyText(..), &DataType::Time(ref other_t)) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let a = <&str>::try_from(self).unwrap().parse();
                a.map(|t: MysqlTime| t.cmp(other_t))
                    .unwrap_or(Ordering::Greater)
            }
            (
                &DataType::Time(_) | &DataType::TimestampTz(_),
                &DataType::Text(..) | &DataType::TinyText(..),
            ) => other.cmp(self).reverse(),
            (&DataType::Int(a), &DataType::Int(b)) => a.cmp(&b),
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a.cmp(&b),
            (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..)) => {
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
            (&DataType::Float(fa), &DataType::Float(fb)) => fa.total_cmp(&fb),
            (&DataType::Double(fa), &DataType::Double(fb)) => fa.total_cmp(&fb),
            (&DataType::Numeric(ref da), &DataType::Numeric(ref db)) => da.cmp(db),
            (&DataType::Float(fa), &DataType::Double(fb)) => fa.total_cmp(&(fb as f32)),
            (&DataType::Double(fa), &DataType::Float(fb)) => fb.total_cmp(&(fa as f32)).reverse(),
            (&DataType::Float(fa), &DataType::Numeric(ref d)) => {
                if let Some(da) = Decimal::from_f32_retain(fa) {
                    da.cmp(d)
                } else {
                    d.to_f32()
                        .as_ref()
                        .map(|fb| fa.total_cmp(fb))
                        .unwrap_or(Ordering::Greater)
                }
            }
            (&DataType::Double(fa), &DataType::Numeric(ref d)) => {
                if let Some(da) = Decimal::from_f64_retain(fa) {
                    da.cmp(d)
                } else {
                    d.to_f64()
                        .as_ref()
                        .map(|fb| fa.total_cmp(fb))
                        .unwrap_or(Ordering::Greater)
                }
            }
            (&DataType::Numeric(_), &DataType::Float(_) | &DataType::Double(_)) => {
                other.cmp(self).reverse()
            }
            (&DataType::TimestampTz(ref tsa), &DataType::TimestampTz(ref tsb)) => tsa.cmp(tsb),
            (&DataType::Time(ref ta), &DataType::Time(ref tb)) => ta.cmp(tb),

            // Convert ints to f32 and cmp against Float.
            (&DataType::Int(..), &DataType::Float(b, ..))
            | (&DataType::UnsignedInt(..), &DataType::Float(b, ..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a = <i128>::try_from(self).unwrap();

                (a as f32).total_cmp(&b)
            }
            // Convert ints to double and cmp against Real.
            (&DataType::Int(..), &DataType::Double(b, ..))
            | (&DataType::UnsignedInt(..), &DataType::Double(b, ..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();

                (a as f64).total_cmp(&b)
            }
            // Convert ints to f32 and cmp against Float.
            (&DataType::Int(..), &DataType::Numeric(ref b))
            | (&DataType::UnsignedInt(..), &DataType::Numeric(ref b)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let a: i128 = <i128>::try_from(self).unwrap();

                Decimal::from(a).cmp(b)
            }
            (&DataType::Float(a, ..), &DataType::Int(..))
            | (&DataType::Float(a, ..), &DataType::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128)  on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();

                a.total_cmp(&(b as f32))
            }
            (&DataType::Double(a, ..), &DataType::Int(..))
            | (&DataType::Double(a, ..), &DataType::UnsignedInt(..)) => {
                // this unwrap should be safe because no error path in try_from for i128 (&i128) on
                // Int and UnsignedInt
                #[allow(clippy::unwrap_used)]
                let b: i128 = <i128>::try_from(other).unwrap();

                a.total_cmp(&(b as f64))
            }
            (&DataType::Numeric(_), &DataType::Int(..))
            | (&DataType::Numeric(_), &DataType::UnsignedInt(..)) => other.cmp(self).reverse(),
            (&DataType::ByteArray(ref array_a), &DataType::ByteArray(ref array_b)) => {
                array_a.cmp(array_b)
            }
            (&DataType::BitVector(ref bits_a), &DataType::BitVector(ref bits_b)) => {
                bits_a.cmp(bits_b)
            }
            (&DataType::Array(ref vs_a), &DataType::Array(ref vs_b)) => vs_a.cmp(vs_b),

            // for all other kinds of data types, just compare the variants in order
            (_, _) => DataTypeKind::from(self).cmp(&DataTypeKind::from(other)),
        }
    }
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The default derived hash function also hashes the variant tag, which turns out to be
        // rather expensive. This version could (but probably won't) have a higher rate of
        // collisions, but the decreased overhead is worth it.
        match *self {
            DataType::None => {}
            DataType::Max => 1i64.hash(state),
            DataType::Int(n) => n.hash(state),
            DataType::UnsignedInt(n) => n.hash(state),
            DataType::Float(f) => (f as f64).to_bits().hash(state),
            DataType::Double(f) => f.to_bits().hash(state),
            DataType::Text(..) | DataType::TinyText(..) => {
                // this unwrap should be safe because no error path in try_from for &str on Text or
                // TinyText
                #[allow(clippy::unwrap_used)]
                let t: &str = <&str>::try_from(self).unwrap();
                t.hash(state)
            }
            DataType::TimestampTz(ts) => ts.hash(state),
            DataType::Time(ref t) => t.hash(state),
            DataType::ByteArray(ref array) => array.hash(state),
            DataType::Numeric(ref d) => d.hash(state),
            DataType::BitVector(ref bits) => bits.hash(state),
            DataType::Array(ref vs) => vs.hash(state),
        }
    }
}

impl<T> From<Option<T>> for DataType
where
    DataType: From<T>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(t) => DataType::from(t),
            None => DataType::None,
        }
    }
}

impl TryFrom<i128> for DataType {
    type Error = ReadySetError;

    fn try_from(s: i128) -> Result<Self, Self::Error> {
        if s >= std::i64::MIN.into() && s <= std::i64::MAX.into() {
            Ok(DataType::Int(s as i64))
        } else if s >= std::u64::MIN.into() && s <= std::u64::MAX.into() {
            Ok(DataType::UnsignedInt(s as u64))
        } else {
            Err(Self::Error::DataTypeConversionError {
                src_type: "i128".to_string(),
                target_type: "DataType".to_string(),
                details: "".to_string(),
            })
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::Int(s)
    }
}

impl From<u64> for DataType {
    fn from(s: u64) -> Self {
        DataType::UnsignedInt(s)
    }
}

impl From<i8> for DataType {
    fn from(s: i8) -> Self {
        DataType::Int(s.into())
    }
}

impl From<u8> for DataType {
    fn from(s: u8) -> Self {
        DataType::UnsignedInt(s.into())
    }
}

impl From<i16> for DataType {
    fn from(s: i16) -> Self {
        DataType::Int(s.into())
    }
}

impl From<u16> for DataType {
    fn from(s: u16) -> Self {
        DataType::UnsignedInt(s.into())
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Int(s as _)
    }
}

impl From<u32> for DataType {
    fn from(s: u32) -> Self {
        DataType::UnsignedInt(s as _)
    }
}

impl From<usize> for DataType {
    fn from(s: usize) -> Self {
        DataType::UnsignedInt(s as u64)
    }
}

impl TryFrom<f32> for DataType {
    type Error = ReadySetError;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        if !f.is_finite() {
            return Err(Self::Error::DataTypeConversionError {
                src_type: "f32".to_string(),
                target_type: "DataType".to_string(),
                details: "".to_string(),
            });
        }

        Ok(DataType::Float(f))
    }
}

impl TryFrom<f64> for DataType {
    type Error = ReadySetError;

    fn try_from(f: f64) -> Result<Self, Self::Error> {
        if !f.is_finite() {
            return Err(Self::Error::DataTypeConversionError {
                src_type: "f64".to_string(),
                target_type: "DataType".to_string(),
                details: "".to_string(),
            });
        }

        Ok(DataType::Double(f))
    }
}

impl From<Decimal> for DataType {
    fn from(d: Decimal) -> Self {
        DataType::Numeric(Arc::new(d))
    }
}

impl<'a> TryFrom<&'a DataType> for Decimal {
    type Error = ReadySetError;

    fn try_from(dt: &'a DataType) -> Result<Self, Self::Error> {
        match dt {
            DataType::Int(i) => Ok(Decimal::from(*i)),
            DataType::UnsignedInt(i) => Ok(Decimal::from(*i)),
            DataType::Float(value) => {
                Decimal::from_f32(*value).ok_or(Self::Error::DataTypeConversionError {
                    src_type: "DataType".to_string(),
                    target_type: "Decimal".to_string(),
                    details: "".to_string(),
                })
            }
            DataType::Double(value) => {
                Decimal::from_f64(*value).ok_or(Self::Error::DataTypeConversionError {
                    src_type: "DataType".to_string(),
                    target_type: "Decimal".to_string(),
                    details: "".to_string(),
                })
            }
            DataType::Numeric(d) => Ok(*d.as_ref()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "Decimal".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

/// Bit vectors are represented as [`BitVec`].
impl From<BitVec> for DataType {
    fn from(b: BitVec) -> Self {
        DataType::BitVector(Arc::new(b))
    }
}

impl<'a> TryFrom<&'a DataType> for BitVec {
    type Error = ReadySetError;

    fn try_from(dt: &'a DataType) -> Result<Self, Self::Error> {
        match dt {
            DataType::BitVector(ref bits) => Ok(bits.as_ref().clone()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "Decimal".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

/// Booleans are represented as `u32`s which are equal to either 0 or 1
impl From<bool> for DataType {
    fn from(b: bool) -> Self {
        DataType::from(b as u32)
    }
}

impl<'a> From<&'a DataType> for DataType {
    fn from(dt: &'a DataType) -> Self {
        dt.clone()
    }
}

impl<'a> TryFrom<&'a Literal> for DataType {
    type Error = ReadySetError;

    fn try_from(l: &'a Literal) -> Result<Self, Self::Error> {
        match l {
            Literal::Null => Ok(DataType::None),
            Literal::Boolean(b) => Ok(DataType::from(*b)),
            Literal::Integer(i) => Ok((*i as i64).into()),
            Literal::String(s) => Ok(s.as_str().into()),
            Literal::CurrentTimestamp | Literal::CurrentTime => {
                let ts = time::OffsetDateTime::now_utc();
                if let Some(dt) =
                    NaiveDate::from_ymd_opt(ts.year(), ts.month() as u32, ts.day() as u32)
                {
                    Ok(DataType::TimestampTz(
                        dt.and_hms_nano(
                            ts.hour() as u32,
                            ts.minute() as u32,
                            ts.second() as u32,
                            ts.nanosecond(),
                        )
                        .into(),
                    ))
                } else {
                    Ok(DataType::None)
                }
            }
            Literal::CurrentDate => {
                let ts = time::OffsetDateTime::now_utc();
                let nd = NaiveDate::from_ymd(ts.year(), ts.month() as u32, ts.day() as u32)
                    .and_hms(0, 0, 0);
                Ok(DataType::TimestampTz(nd.into()))
            }
            Literal::Float(ref float) => Ok(DataType::Float(float.value)),
            Literal::Double(ref double) => Ok(DataType::Double(double.value)),
            Literal::Numeric(i, s) => Decimal::try_from_i128_with_scale(*i, *s)
                .map_err(|e| ReadySetError::DataTypeConversionError {
                    src_type: "Literal".to_string(),
                    target_type: "DataType".to_string(),
                    details: format!("Values out-of-bounds for Numeric type. Error: {}", e),
                })
                .map(|d| DataType::Numeric(Arc::new(d))),
            Literal::Blob(b) => Ok(DataType::from(b.as_slice())),
            Literal::ByteArray(b) => Ok(DataType::ByteArray(Arc::new(b.clone()))),
            Literal::BitVector(b) => Ok(DataType::from(BitVec::from_bytes(b.as_slice()))),
            Literal::Placeholder(_) => {
                internal!("Tried to convert a Placeholder literal to a DataType")
            }
        }
    }
}

impl TryFrom<Literal> for DataType {
    type Error = ReadySetError;

    fn try_from(l: Literal) -> Result<Self, Self::Error> {
        (&l).try_into()
    }
}

impl TryFrom<DataType> for Literal {
    type Error = ReadySetError;

    fn try_from(dt: DataType) -> Result<Self, Self::Error> {
        match dt {
            DataType::None => Ok(Literal::Null),
            DataType::Int(i) => Ok(Literal::Integer(i)),
            DataType::UnsignedInt(i) => Ok(Literal::Integer(i as _)),
            DataType::Float(value) => Ok(Literal::Float(Float {
                value,
                precision: u8::MAX,
            })),
            DataType::Double(value) => Ok(Literal::Double(Double {
                value,
                precision: u8::MAX,
            })),
            DataType::Text(_) => Ok(Literal::String(String::try_from(dt)?)),
            DataType::TinyText(_) => Ok(Literal::String(String::try_from(dt)?)),
            DataType::TimestampTz(_) => Ok(Literal::String(String::try_from(
                dt.coerce_to(&SqlType::Text)?,
            )?)),
            DataType::Time(_) => Ok(Literal::String(String::try_from(
                dt.coerce_to(&SqlType::Text)?,
            )?)),
            DataType::ByteArray(ref array) => Ok(Literal::ByteArray(array.as_ref().clone())),
            DataType::Numeric(ref d) => Ok(Literal::Numeric(d.mantissa(), d.scale())),
            DataType::BitVector(ref bits) => Ok(Literal::BitVector(bits.as_ref().to_bytes())),
            DataType::Array(_) => unsupported!("Arrays not implemented yet"),
            DataType::Max => internal!("MAX has no representation as a literal"),
        }
    }
}

impl From<NaiveTime> for DataType {
    fn from(t: NaiveTime) -> Self {
        DataType::Time(t.into())
    }
}

impl From<MysqlTime> for DataType {
    fn from(t: MysqlTime) -> Self {
        DataType::Time(t)
    }
}

impl From<Vec<u8>> for DataType {
    fn from(t: Vec<u8>) -> Self {
        DataType::ByteArray(Arc::new(t))
    }
}

impl From<NaiveDate> for DataType {
    fn from(dt: NaiveDate) -> Self {
        DataType::TimestampTz(dt.into())
    }
}

impl From<NaiveDateTime> for DataType {
    fn from(dt: NaiveDateTime) -> Self {
        DataType::TimestampTz(dt.into())
    }
}

impl From<DateTime<FixedOffset>> for DataType {
    fn from(dt: DateTime<FixedOffset>) -> Self {
        DataType::TimestampTz(dt.into())
    }
}

impl<'a> TryFrom<&'a DataType> for NaiveDateTime {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::TimestampTz(ref dt) => Ok(dt.to_chrono().naive_utc()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "NaiveDateTime".to_string(),
                details: "".to_string(),
            }),
        }
    }
}
impl<'a> TryFrom<&'a DataType> for DateTime<FixedOffset> {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::TimestampTz(dt) => Ok(dt.to_chrono()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "DateTime<FixedOffset>".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DataType> for NaiveDate {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::TimestampTz(ref dt) => Ok(dt.to_chrono().naive_local().date()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "NaiveDate".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DataType> for MysqlTime {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Time(ref mysql_time) => Ok(*mysql_time),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "MysqlTime".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl<'a> TryFrom<&'a DataType> for Vec<u8> {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Text(ref t) => Ok(t.as_bytes().to_vec()),
            DataType::TinyText(ref tt) => Ok(tt.as_str().as_bytes().to_vec()),
            DataType::ByteArray(ref array) => Ok(array.as_ref().clone()),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "Vec<u8>".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

// This conversion has many unwraps, but all of them are expected to be safe,
// because DataType variants (i.e. `Text` and `TinyText`) constructors are all
// generated from valid UTF-8 strings, or the constructor fails (e.g. TryFrom &[u8]).
// Thus, we can safely generate a &str from a DataType.
impl<'a> TryFrom<&'a DataType> for &'a str {
    type Error = ReadySetError;

    fn try_from(data: &'a DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Text(ref t) => Ok(t.as_str()),
            DataType::TinyText(ref tt) => Ok(tt.as_str()),
            _ => Err(Self::Error::DataTypeConversionError {
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

impl TryFrom<DataType> for Vec<u8> {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        match data {
            DataType::Text(t) => Ok(t.as_bytes().to_vec()),
            DataType::TinyText(tt) => Ok(tt.as_str().as_bytes().to_vec()),
            DataType::ByteArray(bytes) => Ok(bytes.as_ref().clone()),
            _ => Err(Self::Error::DataTypeConversionError {
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

impl TryFrom<DataType> for i128 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <i128>::try_from(&data)
    }
}

impl TryFrom<DataType> for i64 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<i64, Self::Error> {
        <i64>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for i128 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Int(s) => Ok(i128::from(s)),
            DataType::UnsignedInt(s) => Ok(i128::from(s)),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "i128".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<&'_ DataType> for i64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::UnsignedInt(s) => {
                if s as i128 >= std::i64::MIN.into() && s as i128 <= std::i64::MAX.into() {
                    Ok(s as i64)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "i64".to_string(),
                        details: format!("Out of bounds {}", s),
                    })
                }
            }
            DataType::Int(s) => Ok(s),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "i64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DataType> for u64 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <u64>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for u64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::UnsignedInt(s) => Ok(s),
            DataType::Int(s) => {
                if s as i128 >= std::u64::MIN.into() && s as i128 <= std::u64::MAX.into() {
                    Ok(s as u64)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "u64".to_string(),
                        details: "Out of bounds".to_string(),
                    })
                }
            }

            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "u64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DataType> for i32 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <i32>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for i32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::UnsignedInt(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    Ok(s as i32)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "i32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            DataType::Int(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    Ok(s as i32)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "i32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }

            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "i32".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DataType> for u32 {
    type Error = ReadySetError;
    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <u32>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for u32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::UnsignedInt(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    Ok(s as u32)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "u32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            DataType::Int(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    Ok(s as u32)
                } else {
                    Err(Self::Error::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "u32".to_string(),
                        details: "out of bounds".to_string(),
                    })
                }
            }
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "u32".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DataType> for f32 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <f32>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for f32 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Float(f) => Ok(f),
            DataType::Double(f) => Ok(f as f32),
            DataType::Numeric(ref d) => d.to_f32().ok_or(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
            DataType::UnsignedInt(i) => Ok(i as f32),
            DataType::Int(i) => Ok(i as f32),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl TryFrom<DataType> for f64 {
    type Error = ReadySetError;

    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        <f64>::try_from(&data)
    }
}

impl TryFrom<&'_ DataType> for f64 {
    type Error = ReadySetError;

    fn try_from(data: &'_ DataType) -> Result<Self, Self::Error> {
        match *data {
            DataType::Float(f) => Ok(f as f64),
            DataType::Double(f) => Ok(f),
            DataType::Numeric(ref d) => d.to_f64().ok_or(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "f32".to_string(),
                details: "".to_string(),
            }),
            DataType::UnsignedInt(i) => Ok(i as f64),
            DataType::Int(i) => Ok(i as f64),
            _ => Err(Self::Error::DataTypeConversionError {
                src_type: "DataType".to_string(),
                target_type: "f64".to_string(),
                details: "".to_string(),
            }),
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType::from(s.as_str())
    }
}

impl TryFrom<&'_ DataType> for String {
    type Error = ReadySetError;

    fn try_from(dt: &DataType) -> Result<Self, Self::Error> {
        let s: &str = <&str>::try_from(dt)?;
        Ok(s.into())
    }
}

impl TryFrom<DataType> for String {
    type Error = ReadySetError;

    fn try_from(dt: DataType) -> Result<Self, Self::Error> {
        String::try_from(&dt)
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        if let Ok(tt) = TinyText::try_from(s) {
            DataType::TinyText(tt)
        } else {
            DataType::Text(s.into())
        }
    }
}

impl From<&[u8]> for DataType {
    fn from(b: &[u8]) -> Self {
        // NOTE: should we *really* be converting to Text here?
        if let Ok(s) = std::str::from_utf8(b) {
            s.into()
        } else {
            DataType::ByteArray(b.to_vec().into())
        }
    }
}

impl From<Array> for DataType {
    fn from(arr: Array) -> Self {
        Self::Array(Arc::new(arr))
    }
}

impl From<Vec<DataType>> for DataType {
    fn from(vs: Vec<DataType>) -> Self {
        Self::from(Array::from(vs))
    }
}

impl TryFrom<mysql_common::value::Value> for DataType {
    type Error = ReadySetError;

    fn try_from(v: mysql_common::value::Value) -> Result<Self, Self::Error> {
        DataType::try_from(&v)
    }
}

impl TryFrom<&mysql_common::value::Value> for DataType {
    type Error = ReadySetError;

    fn try_from(v: &mysql_common::value::Value) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match v {
            Value::NULL => Ok(DataType::None),
            Value::Bytes(v) => Ok(DataType::from(&v[..])),
            Value::Int(v) => Ok(DataType::from(*v)),
            Value::UInt(v) => Ok(DataType::from(*v)),
            Value::Float(v) => DataType::try_from(*v),
            Value::Double(v) => DataType::try_from(*v),
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
                        return Ok(DataType::TimestampTz(dt.into()));
                    }
                }
                Ok(DataType::None)
            }
            Value::Time(neg, days, hours, minutes, seconds, microseconds) => {
                Ok(DataType::Time(MysqlTime::from_hmsus(
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

impl ToSql for DataType {
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
            (Self::Array(_), _) => Err(Box::<dyn Error + Send + Sync>::from(
                "Array to SQL not implemented yet",
            )),
        }
    }

    accepts!(
        BOOL,
        BYTEA,
        CHAR,
        NAME,
        INT2,
        INT4,
        INT8,
        OID,
        REGCLASS,
        REGCOLLATION,
        REGCONFIG,
        REGDICTIONARY,
        REGNAMESPACE,
        REGOPER,
        REGOPERATOR,
        REGPROC,
        REGPROCEDURE,
        REGROLE,
        REGTYPE,
        FLOAT4,
        FLOAT8,
        NUMERIC,
        TEXT,
        VARCHAR,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMPTZ,
        MACADDR,
        INET,
        UUID,
        JSON,
        JSONB,
        BIT,
        VARBIT
    );

    to_sql_checked!();
}

impl<'a> FromSql<'a> for DataType {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        macro_rules! mk_from_sql {
            ($target:ty) => {
                DataType::try_from(<$target>::from_sql(ty, raw)?).map_err(|e| {
                    format!(
                        "Could not convert Postgres type {} into a DataType. Error: {}",
                        ty, e
                    )
                    .into()
                })
            };
        }
        match *ty {
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
            Type::MACADDR => Ok(DataType::from(
                MacAddress::from_sql(ty, raw)?.to_string(MacAddressFormat::HexString),
            )),
            Type::INET => Ok(DataType::from(IpAddr::from_sql(ty, raw)?.to_string())),
            Type::UUID => Ok(DataType::from(Uuid::from_sql(ty, raw)?.to_string())),
            Type::JSON | Type::JSONB => Ok(DataType::from(
                serde_json::Value::from_sql(ty, raw)?.to_string(),
            )),
            Type::BIT | Type::VARBIT => mk_from_sql!(BitVec),
            _ => Err(format!(
                "Conversion from Postgres type '{}' to DataType is not implemented.",
                ty
            )
            .into()),
        }
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(DataType::None)
    }

    accepts!(
        BOOL,
        BYTEA,
        CHAR,
        NAME,
        INT2,
        INT4,
        INT8,
        OID,
        REGCLASS,
        REGCOLLATION,
        REGCONFIG,
        REGDICTIONARY,
        REGNAMESPACE,
        REGOPER,
        REGOPERATOR,
        REGPROC,
        REGPROCEDURE,
        REGROLE,
        REGTYPE,
        FLOAT4,
        FLOAT8,
        NUMERIC,
        TEXT,
        UUID,
        VARCHAR,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMPTZ,
        MACADDR,
        INET,
        JSON,
        JSONB,
        BIT,
        VARBIT
    );
}

impl TryFrom<DataType> for mysql_common::value::Value {
    type Error = ReadySetError;

    fn try_from(dt: DataType) -> Result<Self, Self::Error> {
        Self::try_from(&dt)
    }
}

impl TryFrom<&DataType> for mysql_common::value::Value {
    type Error = ReadySetError;
    fn try_from(dt: &DataType) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match dt {
            DataType::None | DataType::Max => Ok(Value::NULL),
            DataType::Int(val) => Ok(Value::Int(*val)),
            DataType::UnsignedInt(val) => Ok(Value::UInt(*val)),
            DataType::Float(val) => Ok(Value::Float(*val)),
            DataType::Double(val) => Ok(Value::Double(*val)),
            DataType::Numeric(_) => {
                internal!("DataType::Numeric to MySQL DECIMAL is not implemented")
            }
            DataType::Text(_) | DataType::TinyText(_) => Ok(Value::Bytes(Vec::<u8>::try_from(dt)?)),
            DataType::TimestampTz(val) => Ok(val.to_chrono().naive_utc().into()),
            DataType::Time(val) => Ok(Value::Time(
                !val.is_positive(),
                (val.hour() / 24).into(),
                (val.hour() % 24) as _,
                val.minutes(),
                val.seconds(),
                val.microseconds(),
            )),
            DataType::ByteArray(array) => Ok(Value::Bytes(array.as_ref().clone())),
            DataType::BitVector(_) => internal!("MySQL does not support bit vector types"),
            DataType::Array(_) => internal!("MySQL does not support array types"),
        }
    }
}

// Performs an arithmetic operation on two numeric DataTypes,
// returning a new DataType as the result.
macro_rules! arithmetic_operation (
    ($op:tt, $first:ident, $second:ident) => (
        match ($first, $second) {
            (&DataType::None, _) | (_, &DataType::None) => DataType::None,
            (&DataType::Int(a), &DataType::Int(b)) => (a $op b).into(),
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => (a $op b).into(),

            (&DataType::UnsignedInt(a), &DataType::Int(b)) => DataType::try_from(i128::from(a) $op i128::from(b))?,
            (&DataType::Int(a), &DataType::UnsignedInt(b)) => DataType::try_from(i128::from(a) $op i128::from(b))?,

            (first @ &DataType::Int(..), second @ &DataType::Float(..)) |
            (first @ &DataType::UnsignedInt(..), second @ &DataType::Float(..)) |
            (first @ &DataType::Float(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Float(..), second @ &DataType::UnsignedInt(..)) |
            (first @ &DataType::Float(..), second @ &DataType::Float(..)) |
            (first @ &DataType::Float(..), second @ &DataType::Double(..)) |
            (first @ &DataType::Float(..), second @ &DataType::Numeric(..)) => {
                let a: f32 = f32::try_from(first)?;
                let b: f32 = f32::try_from(second)?;
                DataType::try_from(a $op b)?
            }

            (first @ &DataType::Int(..), second @ &DataType::Double(..)) |
            (first @ &DataType::UnsignedInt(..), second @ &DataType::Double(..)) |
            (first @ &DataType::Double(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Double(..), second @ &DataType::UnsignedInt(..)) |
            (first @ &DataType::Double(..), second @ &DataType::Double(..)) |
            (first @ &DataType::Double(..), second @ &DataType::Float(..)) |
            (first @ &DataType::Double(..), second @ &DataType::Numeric(..)) => {
                let a: f64 = f64::try_from(first)?;
                let b: f64 = f64::try_from(second)?;
                DataType::try_from(a $op b)?
            }

            (first @ &DataType::Int(..), second @ &DataType::Numeric(..)) |
            (first @ &DataType::UnsignedInt(..), second @ &DataType::Numeric(..)) |
            (first @ &DataType::Numeric(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Numeric(..), second @ &DataType::UnsignedInt(..)) |
            (first @ &DataType::Numeric(..), second @ &DataType::Numeric(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = Decimal::try_from(second)
                    .map_err(|e| ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                DataType::from(a $op b)
            }
            (first @ &DataType::Numeric(..), second @ &DataType::Float(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f32::try_from(second).and_then(|f| Decimal::from_f32(f)
                    .ok_or(ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DataType::from(a $op b)
            }
            (first @ &DataType::Numeric(..), second @ &DataType::Double(..)) => {
                let a: Decimal = Decimal::try_from(first)
                    .map_err(|e| ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: e.to_string(),
                    })?;
                let b: Decimal = f64::try_from(second).and_then(|f| Decimal::from_f64(f)
                    .ok_or(ReadySetError::DataTypeConversionError {
                        src_type: "DataType".to_string(),
                        target_type: "Decimal".to_string(),
                        details: "".to_string(),
                    }))?;
                DataType::from(a $op b)
            }


            (first, second) => panic!(
                "can't {} a {:?} and {:?}",
                stringify!($op),
                first,
                second,
            ),
        }
    );
);

impl<'a, 'b> Add<&'b DataType> for &'a DataType {
    type Output = ReadySetResult<DataType>;

    fn add(self, other: &'b DataType) -> Self::Output {
        Ok(arithmetic_operation!(+, self, other))
    }
}

impl<'a, 'b> Sub<&'b DataType> for &'a DataType {
    type Output = ReadySetResult<DataType>;

    fn sub(self, other: &'b DataType) -> Self::Output {
        Ok(arithmetic_operation!(-, self, other))
    }
}

impl<'a, 'b> Mul<&'b DataType> for &'a DataType {
    type Output = ReadySetResult<DataType>;

    fn mul(self, other: &'b DataType) -> Self::Output {
        Ok(arithmetic_operation!(*, self, other))
    }
}

impl<'a, 'b> Div<&'b DataType> for &'a DataType {
    type Output = ReadySetResult<DataType>;

    fn div(self, other: &'b DataType) -> Self::Output {
        Ok(arithmetic_operation!(/, self, other))
    }
}

impl Arbitrary for DataType {
    type Parameters = Option<DataTypeKind>;
    type Strategy = proptest::strategy::BoxedStrategy<DataType>;

    fn arbitrary_with(opt_kind: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        match opt_kind {
            Some(DataTypeKind::None) => Just(DataType::None).boxed(),
            Some(DataTypeKind::Max) => Just(DataType::Max).boxed(),
            Some(DataTypeKind::Int) => any::<i64>().prop_map(DataType::Int).boxed(),
            Some(DataTypeKind::UnsignedInt) => any::<u64>().prop_map(DataType::UnsignedInt).boxed(),
            Some(DataTypeKind::Float) => any::<f32>().prop_map(DataType::Float).boxed(),
            Some(DataTypeKind::Double) => any::<f64>().prop_map(DataType::Double).boxed(),
            Some(DataTypeKind::Text | DataTypeKind::TinyText) => any::<String>()
                .prop_map(|s| DataType::from(s.replace('\0', "")))
                .boxed(),
            Some(DataTypeKind::TimestampTz) => any::<crate::TimestampTz>()
                .prop_map(DataType::TimestampTz)
                .boxed(),
            Some(DataTypeKind::Time) => arbitrary_duration()
                .prop_map(MysqlTime::new)
                .prop_map(DataType::Time)
                .boxed(),
            Some(DataTypeKind::ByteArray) => any::<Vec<u8>>()
                .prop_map(|b| DataType::ByteArray(Arc::new(b)))
                .boxed(),
            Some(DataTypeKind::Numeric) => arbitrary_decimal().prop_map(DataType::from).boxed(),
            Some(DataTypeKind::BitVector) => any::<Vec<u8>>()
                .prop_map(|bs| DataType::BitVector(Arc::new(BitVec::from_bytes(&bs))))
                .boxed(),
            Some(DataTypeKind::Array) => any::<Array>().prop_map(DataType::from).boxed(),
            None => prop_oneof![
                Just(DataType::None),
                Just(DataType::Max),
                any::<i64>().prop_map(DataType::Int),
                any::<u64>().prop_map(DataType::UnsignedInt),
                any::<f32>().prop_map(DataType::Float),
                any::<f64>().prop_map(DataType::Double),
                any::<String>().prop_map(|s| DataType::from(s.replace('\0', ""))),
                any::<crate::TimestampTz>().prop_map(DataType::TimestampTz),
                arbitrary_duration()
                    .prop_map(MysqlTime::new)
                    .prop_map(DataType::Time),
                any::<Vec<u8>>().prop_map(|b| DataType::ByteArray(Arc::new(b))),
                arbitrary_decimal().prop_map(DataType::from),
                any::<Array>().prop_map(DataType::from)
            ]
            .boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use derive_more::{From, Into};
    use launchpad::{eq_laws, hash_laws, ord_laws};
    use proptest::prelude::*;
    use test_strategy::proptest;

    use super::*;

    #[test]
    fn test_size_and_alignment() {
        assert_eq!(std::mem::size_of::<DataType>(), 16);

        let timestamp_tz = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );

        match &timestamp_tz {
            DataType::TimestampTz(ts) => {
                // Make sure datetime is properly aligned to 4 bytes within embedded timestamptz
                assert!(std::mem::align_of::<chrono::NaiveDateTime>() <= 4);
                let ts_ptr = std::ptr::addr_of!(ts.datetime) as usize;
                assert_eq!(ts_ptr & 0x3, 0);
            }
            _ => panic!(),
        }
    }

    fn non_numeric() -> impl Strategy<Value = DataType> {
        any::<DataType>().prop_filter("Numeric DataType", |dt| !matches!(dt, DataType::Numeric(_)))
    }

    eq_laws!(DataType);
    hash_laws!(DataType);
    ord_laws!(
        // The comparison semantics between Numeric and the other numeric types (floats, integers)
        // isn't well-defined, so we skip that in our ord tests.
        //
        // This would be a problem, except that in the main place where having a proper ordering
        // relation actually matters (persisted base table state) we only store homogenously-typed
        // values. If that becomes *not* the case for whatever reason, we need to figure out how to
        // fix this.
        #[strategy(non_numeric())]
        DataType
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
                    0u32..34, // DataType cannot accept time hihger than 838:59:59
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
    fn max_greater_than_all(dt: DataType) {
        prop_assume!(dt != DataType::Max, "MAX");
        assert!(DataType::Max > dt);
    }

    #[proptest]
    #[allow(clippy::float_cmp)]
    fn dt_to_mysql_value_roundtrip_prop(v: MySqlValue) {
        use mysql_common::value::Value;
        let MySqlValue(v) = v;
        match (
            Value::try_from(DataType::try_from(v.clone()).unwrap()).unwrap(),
            v,
        ) {
            (Value::Float(f1), Value::Float(f2)) => assert_eq!(f1, f2),
            (Value::Double(d1), Value::Double(d2)) => assert_eq!(d1, d2),
            (v1, v2) => assert_eq!(v1, v2),
        }
    }

    #[proptest]
    #[allow(clippy::float_cmp)]
    fn mysql_value_to_dt_roundtrip_prop(dt: DataType) {
        use chrono::Datelike;
        use mysql_common::value::Value;

        prop_assume!(match dt {
            DataType::TimestampTz(t)
                if t.to_chrono().naive_local().date().year() < 1000
                    || t.to_chrono().naive_local().date().year() > 9999 =>
                false,
            DataType::ByteArray(_)
            | DataType::Numeric(_)
            | DataType::BitVector(_)
            | DataType::Array(_)
            | DataType::Max => false,
            _ => true,
        });

        match (
            DataType::try_from(Value::try_from(dt.clone()).unwrap()).unwrap(),
            dt,
        ) {
            (DataType::Float(f1), DataType::Float(f2)) => assert_eq!(f1, f2),
            (DataType::Double(f1), DataType::Double(f2)) => assert_eq!(f1, f2),
            (dt1, dt2) => assert_eq!(dt1, dt2),
        }
    }

    #[test]
    fn mysql_value_to_datatype_roundtrip() {
        use mysql_common::value::Value;

        assert_eq!(
            Value::Bytes(vec![1, 2, 3]),
            Value::try_from(DataType::try_from(Value::Bytes(vec![1, 2, 3])).unwrap()).unwrap()
        );

        assert_eq!(
            Value::Int(1),
            Value::try_from(DataType::try_from(Value::Int(1)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::UInt(1),
            Value::try_from(DataType::try_from(Value::UInt(1)).unwrap()).unwrap()
        );
        // round trip results in conversion from float to double
        assert_eq!(
            Value::Float(8.99),
            Value::try_from(DataType::try_from(Value::Float(8.99)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::Double(8.99),
            Value::try_from(DataType::try_from(Value::Double(8.99)).unwrap()).unwrap()
        );
        assert_eq!(
            Value::Date(2021, 1, 1, 1, 1, 1, 1),
            Value::try_from(DataType::try_from(Value::Date(2021, 1, 1, 1, 1, 1, 1)).unwrap())
                .unwrap()
        );
        assert_eq!(
            Value::Time(false, 1, 1, 1, 1, 1),
            Value::try_from(DataType::try_from(Value::Time(false, 1, 1, 1, 1, 1)).unwrap())
                .unwrap()
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn mysql_value_to_datatype() {
        use mysql_common::value::Value;

        // Test Value::NULL.
        let a = Value::NULL;
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::None);

        // Test Value::Bytes.

        // Can't build a String from non-utf8 chars, but now it can be mapped
        // to a ByteArray.
        let a = Value::Bytes(vec![0xff; 30]);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::ByteArray(Arc::new(vec![0xff; 30])));

        let s = "abcdef";
        let a = Value::Bytes(s.as_bytes().to_vec());
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DataType::TinyText(TinyText::from_arr(b"abcdef"))
        );

        // Test Value::Int.
        let a = Value::Int(-5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::Int(-5));

        // Test Value::Float.
        let a = Value::UInt(5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::UnsignedInt(5));

        // Test Value::Float.
        let initial_float: f32 = 8.99;
        let a = Value::Float(initial_float);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f32 = <f32>::try_from(a_dt.unwrap()).unwrap();
        assert_eq!(converted_float, initial_float);

        // Test Value::Double.
        let initial_float: f64 = 8.99;
        let a = Value::Double(initial_float);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f64 = <f64>::try_from(a_dt.unwrap()).unwrap();
        assert_eq!(converted_float, initial_float);

        // Test Value::Date.
        let ts = NaiveDate::from_ymd(1111, 1, 11).and_hms_micro(2, 3, 4, 5);
        let a = Value::from(ts);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::TimestampTz(ts.into()));

        // Test Value::Time.
        // noria_data::DataType has no `Time` representation.
        let a = Value::Time(true, 0, 0, 0, 0, 0);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DataType::Time(MysqlTime::from_microseconds(0))
        )
    }

    #[test]
    fn real_to_string() {
        let a_float: DataType = DataType::try_from(8.99_f32).unwrap();
        let b_float: DataType = DataType::try_from(-8.099_f32).unwrap();
        let c_float: DataType = DataType::try_from(-0.012_345_678_f32).unwrap();

        assert_eq!(a_float.to_string(), "8.99");
        assert_eq!(b_float.to_string(), "-8.099");
        assert_eq!(c_float.to_string(), "-0.012345678");

        let a_double: DataType = DataType::try_from(8.99_f64).unwrap();
        let b_double: DataType = DataType::try_from(-8.099_f64).unwrap();
        let c_double: DataType = DataType::try_from(-0.012_345_678_f64).unwrap();
        assert_eq!(a_double.to_string(), "8.99");
        assert_eq!(b_double.to_string(), "-8.099");
        assert_eq!(c_double.to_string(), "-0.012345678");

        let d_float: DataType = DataType::from(Decimal::new(3141, 3));
        assert_eq!(d_float.to_string(), "3.141");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_float() {
        let original: f32 = 8.99;
        let data_type: DataType = DataType::try_from(original).unwrap();
        let converted: f32 = <f32>::try_from(&data_type).unwrap();
        assert_eq!(DataType::Float(8.99), data_type);
        assert_eq!(original, converted);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_double() {
        let original: f64 = 8.99;
        let data_type: DataType = DataType::try_from(original).unwrap();
        let converted: f64 = <f64>::try_from(&data_type).unwrap();
        assert_eq!(DataType::Double(8.99), data_type);
        assert_eq!(original, converted);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_numeric() {
        let original: Decimal = Decimal::new(3141, 3);
        let data_type: DataType = DataType::try_from(original).unwrap();
        let converted: Decimal = Decimal::try_from(&data_type).unwrap();
        assert_eq!(
            DataType::Numeric(Arc::new(Decimal::new(3141, 3))),
            data_type
        );
        assert_eq!(original, converted);
    }

    macro_rules! assert_arithmetic {
        ($op:tt, $left:expr, $right:expr, $expected:expr) => {
            assert_eq!(
                (&DataType::try_from($left).unwrap() $op &DataType::try_from($right).unwrap()).unwrap(),
                DataType::try_from($expected).unwrap()
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
        assert_eq!((&DataType::Int(1) + &DataType::Int(2)).unwrap(), 3.into());
        assert_eq!((&DataType::from(1) + &DataType::Int(2)).unwrap(), 3.into());
        assert_eq!(
            (&DataType::Int(2) + &DataType::try_from(1).unwrap()).unwrap(),
            3.into()
        );
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
        assert_eq!(
            (&DataType::Int(1) - &DataType::Int(2)).unwrap(),
            (-1).into()
        );
        assert_eq!(
            (&DataType::from(1) - &DataType::Int(2)).unwrap(),
            (-1).into()
        );
        assert_eq!((&DataType::Int(2) - &DataType::from(1)).unwrap(), 1.into());
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
        assert_eq!((&DataType::Int(1) * &DataType::Int(2)).unwrap(), 2.into());
        assert_eq!((&DataType::from(1) * &DataType::Int(2)).unwrap(), 2.into());
        assert_eq!((&DataType::Int(2) * &DataType::from(1)).unwrap(), 2.into());
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
        assert_eq!((&DataType::Int(4) / &DataType::Int(2)).unwrap(), 2.into());
        assert_eq!((&DataType::from(4) / &DataType::Int(2)).unwrap(), 2.into());
        assert_eq!((&DataType::Int(4) / &DataType::from(2)).unwrap(), 2.into());
    }

    #[test]
    #[should_panic(expected = "can't + a TinyText(\"hi\") and Int(5)")]
    fn add_invalid_types() {
        let a: DataType = "hi".try_into().unwrap();
        let b: DataType = 5.into();
        let _ = &a + &b;
    }

    #[test]
    fn data_type_debug() {
        let tiny_text: DataType = "hi".try_into().unwrap();
        let text: DataType = "I contain ' and \"".try_into().unwrap();
        let float_from_real: DataType = DataType::try_from(-0.05_f32).unwrap();
        let float = DataType::Float(-0.05);
        let double_from_real: DataType = DataType::try_from(-0.05_f64).unwrap();
        let double = DataType::Double(-0.05);
        let numeric = DataType::from(Decimal::new(-5, 2)); // -0.05
        let timestamp_tz = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let int = DataType::Int(5);
        let bytes = DataType::ByteArray(Arc::new(vec![0, 8, 39, 92, 100, 128]));
        // bits = 000000000000100000100111010111000110010010000000
        let bits = DataType::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));
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
    fn data_type_display() {
        let tiny_text: DataType = "hi".try_into().unwrap();
        let text: DataType = "this is a very long text indeed".try_into().unwrap();
        let float_from_real: DataType = DataType::try_from(-8.99_f32).unwrap();
        let float = DataType::Float(-8.99);
        let double_from_real: DataType = DataType::try_from(-8.99_f64).unwrap();
        let double = DataType::Double(-8.99);
        let numeric = DataType::from(Decimal::new(-899, 2)); // -8.99
        let timestamp = DataType::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let timestamp_tz = DataType::from(
            FixedOffset::west(19_800)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let int = DataType::Int(5);
        let bytes = DataType::ByteArray(Arc::new(vec![0, 8, 39, 92, 100, 128]));
        // bits = 000000000000100000100111010111000110010010000000
        let bits = DataType::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));
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

    fn _data_type_fungibility_test_eq<T>(f: &dyn for<'a> Fn(&'a DataType) -> T)
    where
        T: PartialEq + fmt::Debug,
    {
        let txt1: DataType = "hi".try_into().unwrap();
        let txt12: DataType = "no".try_into().unwrap();
        let txt2: DataType = DataType::Text("hi".into());
        let text: DataType = "this is a very long text indeed".try_into().unwrap();
        let text2: DataType = "this is another long text".try_into().unwrap();
        let float = DataType::Float(-8.99);
        let float2 = DataType::Float(-8.98);
        let float_from_real: DataType = DataType::try_from(-8.99_f32).unwrap();
        let float_from_real2: DataType = DataType::try_from(-8.98_f32).unwrap();
        let double = DataType::Double(-8.99);
        let double2 = DataType::Double(-8.98);
        let double_from_real: DataType = DataType::try_from(-8.99_f64).unwrap();
        let double_from_real2: DataType = DataType::try_from(-8.98_f64).unwrap();
        let numeric = DataType::from(Decimal::new(-899, 2)); // -8.99
        let numeric2 = DataType::from(Decimal::new(-898, 2)); // -8.99
        let time = DataType::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let time2 = DataType::TimestampTz(NaiveDateTime::from_timestamp(1, 42_000_000).into());
        let timestamp_tz = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let timestamp_tz2 = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(1, 42_000_000)),
        );
        let shrt = DataType::Int(5);
        let shrt6 = DataType::Int(6);
        let ushrt = DataType::UnsignedInt(5);
        let ushrt6 = DataType::UnsignedInt(6);
        let bytes = DataType::ByteArray(Arc::new("hi".as_bytes().to_vec()));
        let bytes2 = DataType::ByteArray(Arc::new(vec![0, 8, 39, 92, 101, 128]));
        let bits = DataType::BitVector(Arc::new(BitVec::from_bytes("hi".as_bytes())));
        let bits2 = DataType::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));

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
        let bigint_i64_min = DataType::Int(std::i64::MIN);
        let bigint_i32_min = DataType::Int(std::i32::MIN as i64);
        let bigint_u32_min = DataType::Int(std::u32::MIN as i64);
        let bigint_i32_max = DataType::Int(std::i32::MAX as i64);
        let bigint_u32_max = DataType::Int(std::u32::MAX as i64);
        let bigint_i64_max = DataType::Int(std::i64::MAX);
        let ubigint_u32_min = DataType::UnsignedInt(std::u32::MIN as u64);
        let ubigint_i32_max = DataType::UnsignedInt(std::i32::MAX as u64);
        let ubigint_u32_max = DataType::UnsignedInt(std::u32::MAX as u64);
        let ubigint_i64_max = DataType::UnsignedInt(std::i64::MAX as u64);
        let ubigint_u64_max = DataType::UnsignedInt(std::u64::MAX);

        fn _data_type_conversion_test_eq_i32(d: &DataType) {
            assert_eq!(
                i32::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i32_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| i32::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_i64(d: &DataType) {
            assert_eq!(
                i64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i64_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| i64::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_u32(d: &DataType) {
            assert_eq!(
                u32::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u32_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| u32::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_u64(d: &DataType) {
            assert_eq!(
                u64::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_u64_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| u64::try_from(d).unwrap()).is_err())
        }
        fn _data_type_conversion_test_eq_i128(d: &DataType) {
            assert_eq!(
                i128::try_from(d).unwrap() as i128,
                i128::try_from(d).unwrap()
            )
        }
        fn _data_type_conversion_test_eq_i128_panic(d: &DataType) {
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
            String::try_from(&DataType::try_from(s.clone()).unwrap()).unwrap(),
            s
        )
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn data_type_fungibility() {
        let hash = |dt: &DataType| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut s = DefaultHasher::new();
            dt.hash(&mut s);
            s.finish()
        };
        let json_serialize = |dt: &DataType| -> DataType {
            serde_json::from_str(&serde_json::to_string(dt).unwrap()).unwrap()
        };
        let bincode_serialize = |dt: &DataType| -> DataType {
            bincode::deserialize(&bincode::serialize(dt).unwrap()).unwrap()
        };

        _data_type_fungibility_test_eq(&|x: &DataType| x.clone());
        _data_type_fungibility_test_eq(&hash);
        _data_type_fungibility_test_eq(&json_serialize);
        _data_type_fungibility_test_eq(&bincode_serialize);

        use std::convert::TryFrom;

        let txt1: DataType = "hi".try_into().unwrap();
        let txt12: DataType = "no".try_into().unwrap();
        let txt2: DataType = DataType::Text("hi".into());
        let text: DataType = "this is a very long text indeed".try_into().unwrap();
        let text2: DataType = "this is another long text".try_into().unwrap();
        let float = DataType::Float(-8.99);
        let float2 = DataType::Float(-8.98);
        let float_from_real: DataType = DataType::try_from(-8.99_f32).unwrap();
        let float_from_real2: DataType = DataType::try_from(-8.98_f32).unwrap();
        let double = DataType::Double(-8.99);
        let double2 = DataType::Double(-8.98);
        let double_from_real: DataType = DataType::try_from(-8.99_f64).unwrap();
        let double_from_real2: DataType = DataType::try_from(-8.98_f64).unwrap();
        let numeric = DataType::from(Decimal::new(-899, 2)); // -8.99
        let numeric2 = DataType::from(Decimal::new(-898, 2)); // -8.99
        let time = DataType::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());
        let time2 = DataType::TimestampTz(NaiveDateTime::from_timestamp(1, 42_000_000).into());
        let timestamp_tz = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(0, 42_000_000)),
        );
        let timestamp_tz2 = DataType::from(
            FixedOffset::west(18_000)
                .from_utc_datetime(&NaiveDateTime::from_timestamp(1, 42_000_000)),
        );
        let shrt = DataType::Int(5);
        let shrt6 = DataType::Int(6);
        let ushrt = DataType::UnsignedInt(5);
        let ushrt6 = DataType::UnsignedInt(6);
        let bytes = DataType::ByteArray(Arc::new("hi".as_bytes().to_vec()));
        let bytes2 = DataType::ByteArray(Arc::new(vec![0, 8, 39, 92, 101, 128]));
        let bits = DataType::BitVector(Arc::new(BitVec::from_bytes("hi".as_bytes())));
        let bits2 = DataType::BitVector(Arc::new(BitVec::from_bytes(&[0, 8, 39, 92, 100, 128])));

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
        let int1 = DataType::Int(1);
        let float1 = DataType::Float(1.2);
        let float2 = DataType::Float(0.8);
        let double1 = DataType::Double(1.2);
        let double2 = DataType::Double(0.8);
        let numeric1 = DataType::from(Decimal::new(12, 1));
        let numeric2 = DataType::from(Decimal::new(8, 1));
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
        let arr = DataType::from(vec![DataType::None, DataType::from(1)]);
        assert_eq!(
            arr.sql_type(),
            Some(SqlType::Array(Box::new(SqlType::Bigint(None))))
        );
    }

    mod coerce_to {
        use launchpad::arbitrary::{
            arbitrary_naive_date, arbitrary_naive_date_time, arbitrary_naive_time,
        };
        use test_strategy::proptest;
        use SqlType::*;

        use super::*;

        #[proptest]
        fn same_type_is_identity(dt: DataType) {
            if let Some(ty) = dt.sql_type() {
                assert_eq!(dt.coerce_to(&ty).unwrap(), dt);
            }
        }

        #[proptest]
        fn parse_timestamps(#[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime) {
            let expected = DataType::from(ndt);
            let input =
                DataType::try_from(ndt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string())
                    .unwrap();
            let result = input.coerce_to(&Timestamp).unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_times(#[strategy(arbitrary_naive_time())] nt: NaiveTime) {
            let expected = DataType::from(nt);
            let input = DataType::try_from(nt.format(TIME_FORMAT).to_string()).unwrap();
            let result = input.coerce_to(&Time).unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_datetimes(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let dt = NaiveDateTime::new(nd, NaiveTime::from_hms(12, 0, 0));
            let expected = DataType::from(dt);
            let input =
                DataType::try_from(dt.format(crate::timestamp::TIMESTAMP_FORMAT).to_string())
                    .unwrap();
            let result = input.coerce_to(&SqlType::DateTime(None)).unwrap();
            assert_eq!(result, expected);
        }

        #[proptest]
        fn parse_dates(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let expected = DataType::from(NaiveDateTime::new(nd, NaiveTime::from_hms(0, 0, 0)));
            let input =
                DataType::try_from(nd.format(crate::timestamp::DATE_FORMAT).to_string()).unwrap();
            let result = input.coerce_to(&Date).unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn timestamp_surjections() {
            let input = DataType::from(NaiveDate::from_ymd(2021, 3, 17).and_hms(11, 34, 56));
            assert_eq!(
                input.coerce_to(&Date).unwrap(),
                NaiveDate::from_ymd(2021, 3, 17).into()
            );
            assert_eq!(
                input.coerce_to(&Time).unwrap(),
                NaiveTime::from_hms(11, 34, 56).into()
            );
        }

        #[proptest]
        fn timestamp_to_datetime(
            #[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime,
            prec: Option<u16>,
        ) {
            let input = DataType::from(ndt);
            assert_eq!(
                input.clone().coerce_to(&SqlType::DateTime(prec)).unwrap(),
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
                        DataType::from(input.unwrap())
                            .coerce_to(&$sql_type)
                            .unwrap(),
                        DataType::from(source)
                    );
                }
            };
        }

        int_conversion!(int_to_tinyint, i32, i8, Tinyint(None));
        int_conversion!(int_to_unsigned_tinyint, i32, u8, UnsignedTinyint(None));
        int_conversion!(int_to_smallint, i32, i16, Smallint(None));
        int_conversion!(int_to_unsigned_smallint, i32, u16, UnsignedSmallint(None));
        int_conversion!(bigint_to_tinyint, i64, i8, Tinyint(None));
        int_conversion!(bigint_to_unsigned_tinyint, i64, u8, UnsignedTinyint(None));
        int_conversion!(bigint_to_smallint, i64, i16, Smallint(None));
        int_conversion!(
            bigint_to_unsigned_smallint,
            i64,
            u16,
            UnsignedSmallint(None)
        );
        int_conversion!(bigint_to_int, i64, i32, Int(None));
        int_conversion!(bigint_to_serial, i64, i32, Serial);
        int_conversion!(bigint_to_unsigned_bigint, i64, u64, UnsignedBigint(None));

        macro_rules! real_conversion {
            ($name: ident, $from: ty, $to: ty, $sql_type: expr) => {
                #[proptest]
                fn $name(source: $from) {
                    if (source as $to).is_finite() {
                        assert_eq!(
                            DataType::try_from(source)
                                .unwrap()
                                .coerce_to(&$sql_type)
                                .unwrap(),
                            DataType::try_from(source as $to).unwrap()
                        );
                    } else {
                        assert!(DataType::try_from(source)
                            .unwrap()
                            .coerce_to(&$sql_type)
                            .is_err());
                        assert!(DataType::try_from(source as $to).is_err());
                    }
                }
            };
        }

        real_conversion!(float_to_double, f32, f64, Double);

        real_conversion!(double_to_float, f64, f32, Float);

        #[proptest]
        fn char_equal_length(#[strategy("a{1,30}")] text: String) {
            use SqlType::*;
            let input = DataType::try_from(text.as_str()).unwrap();
            let intermediate = Char(Some(u16::try_from(text.len()).unwrap()));
            let result = input.coerce_to(&intermediate).unwrap();
            assert_eq!(String::try_from(&result).unwrap().as_str(), text.as_str());
        }

        #[test]
        fn text_to_json() {
            let input = DataType::from("{\"name\": \"John Doe\", \"age\": 43, \"phones\": [\"+44 1234567\", \"+44 2345678\"] }");
            let result = input.coerce_to(&SqlType::Json).unwrap();
            assert_eq!(input, result);

            let result = input.coerce_to(&SqlType::Jsonb).unwrap();
            assert_eq!(input, result);

            let input = DataType::from("not a json");
            let result = input.coerce_to(&SqlType::Json);
            assert!(result.is_err());

            let result = input.coerce_to(&SqlType::Jsonb);
            assert!(result.is_err());
        }

        #[test]
        fn text_to_macaddr() {
            let input = DataType::from("12:34:56:ab:cd:ef");
            let result = input.coerce_to(&SqlType::MacAddr).unwrap();
            assert_eq!(input, result);
        }

        #[test]

        fn int_to_text() {
            assert_eq!(
                DataType::from(20070523i64)
                    .coerce_to(&SqlType::Text)
                    .unwrap(),
                DataType::from("20070523"),
            );
            assert_eq!(
                DataType::from(20070523i64)
                    .coerce_to(&SqlType::Varchar(Some(2)))
                    .unwrap(),
                DataType::from("20"),
            );
            assert_eq!(
                DataType::from(20070523i64)
                    .coerce_to(&SqlType::Char(Some(10)))
                    .unwrap(),
                DataType::from("20070523  "),
            );
        }

        #[test]
        fn int_to_date_time() {
            assert_eq!(
                DataType::from(20070523i64)
                    .coerce_to(&SqlType::Date)
                    .unwrap(),
                DataType::from(NaiveDate::from_ymd(2007, 05, 23))
            );

            assert_eq!(
                DataType::from(70523u64).coerce_to(&SqlType::Date).unwrap(),
                DataType::from(NaiveDate::from_ymd(2007, 05, 23))
            );

            assert_eq!(
                DataType::from(19830905132800i64)
                    .coerce_to(&SqlType::Timestamp)
                    .unwrap(),
                DataType::from(NaiveDate::from_ymd(1983, 09, 05).and_hms(13, 28, 00))
            );

            assert_eq!(
                DataType::from(830905132800u64)
                    .coerce_to(&SqlType::DateTime(None))
                    .unwrap(),
                DataType::from(NaiveDate::from_ymd(1983, 09, 05).and_hms(13, 28, 00))
            );

            assert_eq!(
                DataType::from(101112i64).coerce_to(&SqlType::Time).unwrap(),
                DataType::from(MysqlTime::from_hmsus(true, 10, 11, 12, 0))
            );
        }

        #[test]
        fn text_to_uuid() {
            let input = DataType::from(uuid::Uuid::new_v4().to_string());
            let result = input.coerce_to(&SqlType::Uuid).unwrap();
            assert_eq!(input, result);
        }

        macro_rules! bool_conversion {
            ($name: ident, $ty: ty) => {
                #[proptest]
                fn $name(input: $ty) {
                    let input_dt = DataType::from(input);
                    let result = input_dt.coerce_to(&SqlType::Bool).unwrap();
                    let expected = DataType::from(input != 0);
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
    }
}
