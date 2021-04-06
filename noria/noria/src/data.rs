use arccstr::ArcCStr;

use chrono::{self, NaiveDate, NaiveDateTime, NaiveTime};

use nom_sql::{Literal, Real, SqlType};

use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div, Mul, Sub};

use proptest::prelude::{prop_oneof, Arbitrary};
use thiserror::Error;

const FLOAT_PRECISION: f64 = 1_000_000_000.0;
const TINYTEXT_WIDTH: usize = 15;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DataType` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DataType::deep_clone` to
/// clone the *value* of a `DataType` without danger of contention.
///
/// Also note that DataType uses a custom implementation of the Serialize trait, which must be
/// manually updated if the DataType enum changes:
/// https://www.notion.so/Text-TinyText-Serialization-Deserialization-9dff56b6974b4bcdae28f236882783a8
#[derive(Eq, Clone)]
#[warn(variant_size_differences)]
pub enum DataType {
    /// An empty value.
    None,
    /// A signed 32-bit numeric value.
    Int(i32),
    /// An unsigned 32-bit numeric value.
    UnsignedInt(u32),
    /// A signed 64-bit numeric value.
    BigInt(i64),
    /// An unsigned signed 64-bit numeric value.
    UnsignedBigInt(u64),
    /// A fixed point real value. The first field is the integer part, while the second is the
    /// fractional and must be between -999999999 and 999999999.
    Real(i64, i32),
    /// A reference-counted string-like value.
    Text(ArcCStr),
    /// A tiny string that fits in a pointer
    TinyText([u8; TINYTEXT_WIDTH]),
    /// A timestamp for date/time types.
    Timestamp(NaiveDateTime),
    /// A time duration
    Time(Arc<MysqlTime>), //NOTE(Fran): Using an `Arc` to keep the `DataType` type 16 bytes long
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "NULL"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: &str = self.into();
                // TODO: do we really want to produce quoted strings?
                write!(f, "\"{}\"", text)
            }
            DataType::Int(n) => write!(f, "{}", n),
            DataType::UnsignedInt(n) => write!(f, "{}", n),
            DataType::BigInt(n) => write!(f, "{}", n),
            DataType::UnsignedBigInt(n) => write!(f, "{}", n),
            DataType::Real(i, frac) => {
                if i == 0 && frac < 0 {
                    // We have to insert the negative sign ourselves.
                    write!(f, "-0.{:09}", frac.abs())
                } else {
                    write!(f, "{}.{:09}", i, frac.abs())
                }
            }
            DataType::Timestamp(ts) => write!(f, "{}", ts.format("%c")),
            DataType::Time(ref t) => {
                write!(f, "{}", t.to_string())
            }
        }
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "None"),
            DataType::Text(..) => {
                let text: &str = self.into();
                write!(f, "Text({:?})", text)
            }
            DataType::TinyText(..) => {
                let text: &str = self.into();
                write!(f, "TinyText({:?})", text)
            }
            DataType::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            DataType::Real(..) => write!(f, "Real({})", self),
            DataType::Int(n) => write!(f, "Int({})", n),
            DataType::UnsignedInt(n) => write!(f, "UnsignedInt({})", n),
            DataType::BigInt(n) => write!(f, "BigInt({})", n),
            DataType::UnsignedBigInt(n) => write!(f, "UnsignedBigInt({})", n),
            DataType::Time(ref t) => f.debug_tuple("Time").field(t.as_ref()).finish(),
        }
    }
}

/// The format for timestamps when parsed as text
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The format for times when parsed as text
pub const TIME_FORMAT: &str = "%H:%M:%S";

/// The format for dates when parsed as text
pub const DATE_FORMAT: &str = "%Y-%m-%d";

impl DataType {
    /// Generates the minimum DataType corresponding to the type of a given DataType.
    pub fn min_value(other: &Self) -> Self {
        match other {
            DataType::None => DataType::None,
            DataType::Text(_) | DataType::TinyText(_) => DataType::TinyText([0; 15]),
            DataType::Timestamp(_) => DataType::Timestamp(NaiveDateTime::new(
                chrono::naive::MIN_DATE,
                NaiveTime::from_hms(0, 0, 0),
            )),
            DataType::Real(..) => DataType::Real(i64::min_value(), i32::max_value()),
            DataType::Int(_) => DataType::Int(i32::min_value()),
            DataType::UnsignedInt(_) => DataType::UnsignedInt(0),
            DataType::BigInt(_) => DataType::BigInt(i64::min_value()),
            DataType::UnsignedBigInt(_) => DataType::UnsignedInt(0),
            DataType::Time(_) => DataType::Time(Arc::new(MysqlTime::min_value())),
        }
    }

    /// Generates the maximum DataType corresponding to the type of a given DataType.
    /// Note that there is no possible maximum for the `Text` variant, hence it is not implemented.
    pub fn max_value(other: &Self) -> Self {
        match other {
            DataType::None => DataType::None,
            DataType::Text(_) => unimplemented!(),
            DataType::TinyText(_) => DataType::TinyText([u8::max_value(); 15]),
            DataType::Timestamp(_) => DataType::Timestamp(NaiveDateTime::new(
                chrono::naive::MAX_DATE,
                NaiveTime::from_hms(23, 59, 59),
            )),
            DataType::Real(..) => DataType::Real(i64::max_value(), i32::max_value()),
            DataType::Int(_) => DataType::Int(i32::max_value()),
            DataType::UnsignedInt(_) => DataType::UnsignedInt(u32::max_value()),
            DataType::BigInt(_) => DataType::BigInt(i64::max_value()),
            DataType::UnsignedBigInt(_) => DataType::UnsignedBigInt(u64::max_value()),
            DataType::Time(_) => DataType::Time(Arc::new(MysqlTime::max_value())),
        }
    }

    /// Clone the value contained within this `DataType`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    pub fn deep_clone(&self) -> Self {
        match *self {
            DataType::Text(ref cstr) => DataType::Text(ArcCStr::from(&**cstr)),
            ref dt => dt.clone(),
        }
    }

    /// Checks if this value is `DataType::None`.
    pub fn is_none(&self) -> bool {
        matches!(*self, DataType::None)
    }

    /// Checks if this value is of an integral data type (i.e., can be converted into integral types).
    pub fn is_integer(&self) -> bool {
        matches!(*self, DataType::Int(_) | DataType::BigInt(_))
    }

    /// Checks if this value is of a real data type (i.e., can be converted into `f64`).
    pub fn is_real(&self) -> bool {
        matches!(*self, DataType::Real(_, _))
    }

    /// Checks if this value is of a string data type (i.e., can be converted into `String` and
    /// `&str`).
    pub fn is_string(&self) -> bool {
        matches!(*self, DataType::Text(_) | DataType::TinyText(_))
    }

    /// Checks if this value is of a timestamp data type.
    pub fn is_datetime(&self) -> bool {
        matches!(*self, DataType::Timestamp(_))
    }

    /// Checks if this value is of a time data type.
    pub fn is_time(&self) -> bool {
        matches!(*self, DataType::Time(_))
    }

    /// Returns the SqlType for this DataType, or None if [`DataType::None`] (which is valid for any
    /// type)
    pub fn sql_type(&self) -> Option<SqlType> {
        use SqlType::*;
        match self {
            Self::None => None,
            Self::Int(_) => Some(Int(32)),
            Self::UnsignedInt(_) => Some(UnsignedInt(32)),
            Self::BigInt(_) => Some(Bigint(64)),
            Self::UnsignedBigInt(_) => Some(UnsignedBigint(64)),
            Self::Real(_, _) => Some(Real),
            Self::Text(_) => Some(Text),
            Self::TinyText(_) => Some(Tinytext),
            Self::Timestamp(_) => Some(Timestamp),
            Self::Time(_) => Some(Time),
        }
    }

    /// Attempt to coerce the given DataType to a value of the given `SqlType`.
    ///
    /// Currently, this entails:
    ///
    /// * Coercing values to the type they already are
    /// * Parsing strings ([`Text`], [`Tinytext`], [`Mediumtext`]) as integers
    /// * Parsing strings ([`Text`], [`Tinytext`], [`Mediumtext`]) as timestamps
    /// * Changing numeric type sizes (bigint -> int, int -> bigint, etc)
    /// * Converting [`Real`]s with a zero fractional part to an integer
    /// * Removing information from date/time data types (timestamps to dates or times)
    ///
    /// More coercions will likely need to be added in the future
    ///
    /// # Examples
    ///
    /// Reals can be converted to integers
    ///
    /// ```rust
    /// use noria::DataType;
    /// use nom_sql::SqlType;
    ///
    /// let real = DataType::Real(123, 0);
    /// let int = real.coerce_to(&SqlType::Int(32)).unwrap();
    /// assert_eq!(int.into_owned(), DataType::Int(123));
    /// ```
    ///
    /// Text can be parsed as a timestamp using the SQL `%Y-%m-%d %H:%M:%S` format:
    ///
    /// ```rust
    /// use noria::DataType;
    /// use nom_sql::SqlType;
    /// use chrono::NaiveDate;
    /// use std::borrow::Borrow;
    ///
    /// let text = DataType::from("2021-01-26 10:20:37");
    /// let timestamp = text.coerce_to(&SqlType::Timestamp).unwrap();
    /// assert_eq!(
    ///   timestamp.into_owned(),
    ///   DataType::Timestamp(NaiveDate::from_ymd(2021, 01, 26).and_hms(10, 20, 37))
    /// );
    /// ```
    pub fn coerce_to<'a>(&'a self, ty: &SqlType) -> Result<Cow<'a, Self>, ValueCoerceError> {
        let mk_err = |message: String, source: Option<anyhow::Error>| ValueCoerceError {
            value: self.clone(),
            expected_type: ty.clone(),
            // FIXME(eta): this source thing is less than ideal
            message: format!(
                "{}: {}",
                message,
                source
                    .map(|x| x.to_string())
                    .unwrap_or_else(|| "<empty>".into())
            ),
        };

        use SqlType::*;
        match (self, self.sql_type(), ty) {
            (_, None, _) => Ok(Cow::Borrowed(self)),
            (_, Some(src_type), tgt_type) if src_type == *tgt_type => Ok(Cow::Borrowed(self)),
            (_, Some(Text | Tinytext | Mediumtext), Text | Tinytext | Mediumtext) => {
                Ok(Cow::Borrowed(self))
            }
            (_, Some(Real), Float | Double) => Ok(Cow::Borrowed(self)),
            (_, Some(Text | Tinytext | Mediumtext), Varchar(max_len)) => {
                let actual_len = <&str>::from(self).len();
                if actual_len <= (*max_len).into() {
                    Ok(Cow::Borrowed(self))
                } else {
                    Err(mk_err(
                        format!(
                            "Value ({} characters long) longer than maximum length of {} characters",
                            actual_len,
                            max_len
                        ),
                        None
                    ))
                }
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Char(len)) => {
                let actual_len = <&str>::from(self).len();
                if actual_len <= usize::from(*len) {
                    Ok(Cow::Borrowed(self))
                } else {
                    Err(mk_err(
                        format!(
                            "Value ({} characters long) is not required length of {} characters",
                            actual_len, len
                        ),
                        None,
                    ))
                }
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Timestamp) => {
                NaiveDateTime::parse_from_str(self.into(), TIMESTAMP_FORMAT)
                    .map_err(|e| {
                        mk_err(
                            "Could not parse value as timestamp".to_owned(),
                            Some(e.into()),
                        )
                    })
                    .map(Self::Timestamp)
                    .map(Cow::Owned)
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Date) => {
                let text: &str = self.into();
                NaiveDate::parse_from_str(text, DATE_FORMAT)
                    .or_else(|_e| {
                        NaiveDateTime::parse_from_str(text, TIMESTAMP_FORMAT).map(|dt| dt.date())
                    })
                    .map_err(|e| mk_err("Could not parse value as date".to_owned(), Some(e.into())))
                    .map(|date| {
                        Self::Timestamp(NaiveDateTime::new(date, NaiveTime::from_hms(0, 0, 0)))
                    })
                    .map(Cow::Owned)
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Time) => <&str>::from(self)
                .parse()
                .map_err(|e: String| {
                    mk_err(
                        "Could not parse value as time".to_owned(),
                        Some(anyhow::Error::msg(e)),
                    )
                })
                .map(Arc::new)
                .map(Self::Time)
                .map(Cow::Owned),
            (_, Some(Int(_) | Bigint(_) | Real | Float), Time) => {
                MysqlTime::try_from(<f64>::from(self))
                    .map_err(|e: String| {
                        mk_err(
                            "Could not parse value as time".to_owned(),
                            Some(anyhow::Error::msg(e)),
                        )
                    })
                    .map(Arc::new)
                    .map(Self::Time)
                    .map(Cow::Owned)
            }
            (Self::Timestamp(ts), Some(Timestamp), Text | Tinytext | Mediumtext | Varchar(_)) => {
                Ok(Cow::Owned(ts.format(TIMESTAMP_FORMAT).to_string().into()))
            }
            (Self::Time(ts), Some(Time), Text | Tinytext | Mediumtext | Varchar(_)) => {
                Ok(Cow::Owned(ts.to_string().into()))
            }
            (Self::Timestamp(ts), Some(Timestamp), Date) => {
                Ok(Cow::Owned(Self::Timestamp(ts.date().and_hms(0, 0, 0))))
            }
            (Self::Timestamp(ts), Some(Timestamp), Time) => {
                Ok(Cow::Owned(Self::Time(Arc::new(ts.time().into()))))
            }
            (_, Some(Bigint(_)), Int(_)) => {
                Ok(Cow::Owned(DataType::Int(i32::try_from(self).map_err(
                    |e| mk_err("Could not convert numeric types".to_owned(), Some(e.into())),
                )?)))
            }
            (Self::BigInt(n), Some(Bigint(_)), Tinyint(_)) => Ok(Cow::Owned(
                i8::try_from(*n)
                    .map_err(|e| {
                        mk_err("Could not convert numeric types".to_owned(), Some(e.into()))
                    })?
                    .into(),
            )),
            (Self::BigInt(n), Some(Bigint(_)), UnsignedSmallint(_)) => Ok(Cow::Owned(
                u16::try_from(*n)
                    .map_err(|e| {
                        mk_err("Could not convert numeric types".to_owned(), Some(e.into()))
                    })?
                    .into(),
            )),
            (_, Some(Int(_)), Bigint(_)) => Ok(Cow::Owned(DataType::BigInt(i64::from(self)))),
            (Self::Real(n, 0), Some(Real), Tinyint(_) | Smallint(_) | Int(_)) => {
                Ok(Cow::Owned(DataType::Int(i32::try_from(*n).map_err(
                    |e| mk_err("Could not convert numeric types".to_owned(), Some(e.into())),
                )?)))
            }
            (Self::Real(n, 0), Some(Real), Bigint(_)) => Ok(Cow::Owned(DataType::BigInt(*n))),
            (
                Self::Real(n, 0),
                Some(Real),
                UnsignedTinyint(_) | UnsignedSmallint(_) | UnsignedInt(_),
            ) => Ok(Cow::Owned(DataType::UnsignedInt(
                u32::try_from(*n).map_err(|e| {
                    mk_err("Could not convert numeric types".to_owned(), Some(e.into()))
                })?,
            ))),
            (Self::Real(n, 0), Some(Real), UnsignedBigint(_)) => Ok(Cow::Owned(
                DataType::UnsignedBigInt(u64::try_from(*n).map_err(|e| {
                    mk_err("Could not convert numeric types".to_owned(), Some(e.into()))
                })?),
            )),
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Tinyint(_)) => <&str>::from(self)
                .parse::<i8>()
                .map(|x| (Cow::Owned(DataType::from(x))))
                .map_err(|e| mk_err("Could not parse value as number".to_owned(), Some(e.into()))),
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Smallint(_)) => <&str>::from(self)
                .parse::<i16>()
                .map(|x| (Cow::Owned(DataType::from(x))))
                .map_err(|e| mk_err("Could not parse value as number".to_owned(), Some(e.into()))),
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Int(_)) => <&str>::from(self)
                .parse::<i32>()
                .map(|x| (Cow::Owned(DataType::from(x))))
                .map_err(|e| mk_err("Could not parse value as number".to_owned(), Some(e.into()))),
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), Bigint(_)) => <&str>::from(self)
                .parse::<i64>()
                .map(|x| (Cow::Owned(DataType::from(x))))
                .map_err(|e| mk_err("Could not parse value as number".to_owned(), Some(e.into()))),
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), UnsignedTinyint(_)) => {
                <&str>::from(self)
                    .parse::<u8>()
                    .map(|x| (Cow::Owned(DataType::from(x))))
                    .map_err(|e| {
                        mk_err("Could not parse value as number".to_owned(), Some(e.into()))
                    })
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), UnsignedSmallint(_)) => {
                <&str>::from(self)
                    .parse::<u16>()
                    .map(|x| (Cow::Owned(DataType::from(x))))
                    .map_err(|e| {
                        mk_err("Could not parse value as number".to_owned(), Some(e.into()))
                    })
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), UnsignedInt(_)) => {
                <&str>::from(self)
                    .parse::<u32>()
                    .map(|x| (Cow::Owned(DataType::from(x))))
                    .map_err(|e| {
                        mk_err("Could not parse value as number".to_owned(), Some(e.into()))
                    })
            }
            (_, Some(Text | Tinytext | Mediumtext | Varchar(_)), UnsignedBigint(_)) => {
                <&str>::from(self)
                    .parse::<u64>()
                    .map(|x| (Cow::Owned(DataType::from(x))))
                    .map_err(|e| {
                        mk_err("Could not parse value as number".to_owned(), Some(e.into()))
                    })
            }
            (_, Some(src_type), tgt_type) => Err(ValueCoerceError {
                value: self.clone(),
                expected_type: ty.clone(),
                message: format!("Cannot coerce {:?} to {:?}", src_type, tgt_type),
            }),
        }
    }

    /// Returns Some(&self) if self is not [`DataType::None`]
    ///
    /// # Examples
    ///
    /// ```
    /// use noria::DataType;
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
    /// use noria::DataType;
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
            use std::{mem, slice};
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
                let a: &str = self.into();
                let b: &str = other.into();
                a == b
            }
            (&DataType::BigInt(a), &DataType::BigInt(b)) => a == b,
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(b)) => a == b,
            (&DataType::Int(a), &DataType::Int(b)) => a == b,
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a == b,
            (&DataType::UnsignedBigInt(..), &DataType::Int(..))
            | (&DataType::UnsignedBigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::UnsignedInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::BigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..))
            | (&DataType::Int(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::BigInt(..)) => {
                let a: i128 = self.into();
                let b: i128 = other.into();
                a == b
            }
            (&DataType::Real(ai, af), &DataType::Real(bi, bf)) => ai == bi && af == bf,
            (&DataType::Timestamp(tsa), &DataType::Timestamp(tsb)) => tsa == tsb,
            (&DataType::Time(ref ta), &DataType::Time(ref tb)) => ta.as_ref() == tb.as_ref(),
            (&DataType::None, &DataType::None) => true,

            _ => false,
        }
    }
}

use msql_srv::MysqlTime;
use std::cmp::Ordering;
use std::sync::Arc;

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &DataType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataType {
    fn cmp(&self, other: &DataType) -> Ordering {
        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a.cmp(b),
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a.cmp(b),
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
                let a: &str = self.into();
                let b: &str = other.into();
                a.cmp(&b)
            }
            (&DataType::BigInt(a), &DataType::BigInt(ref b)) => a.cmp(b),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(ref b)) => a.cmp(b),
            (&DataType::Int(a), &DataType::Int(b)) => a.cmp(&b),
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a.cmp(&b),
            (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::BigInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedInt(..), &DataType::BigInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::UnsignedBigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::Int(..))
            | (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..)) => {
                let a: i128 = self.into();
                let b: i128 = other.into();
                a.cmp(&b)
            }
            (&DataType::Real(ai, af), &DataType::Real(ref bi, ref bf)) => {
                ai.cmp(bi).then_with(|| af.cmp(bf))
            }
            (&DataType::Timestamp(tsa), &DataType::Timestamp(ref tsb)) => tsa.cmp(tsb),
            (&DataType::Time(ref ta), &DataType::Time(ref tb)) => ta.cmp(tb),
            (&DataType::None, &DataType::None) => Ordering::Equal,

            // order Ints, Reals, Text, Timestamps, None
            (&DataType::Int(..), _)
            | (&DataType::UnsignedInt(..), _)
            | (&DataType::BigInt(..), _)
            | (&DataType::UnsignedBigInt(..), _) => Ordering::Greater,
            (&DataType::Real(..), _) => Ordering::Greater,
            (&DataType::Text(..), _) | (&DataType::TinyText(..), _) => Ordering::Greater,
            (&DataType::Timestamp(..) | DataType::Time(_), _) => Ordering::Greater,
            (&DataType::None, _) => Ordering::Greater,
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
            DataType::Int(..) | DataType::BigInt(..) => {
                let n: i64 = self.into();
                n.hash(state)
            }
            DataType::UnsignedInt(..) | DataType::UnsignedBigInt(..) => {
                let n: u64 = self.into();
                n.hash(state)
            }
            DataType::Real(i, f) => {
                i.hash(state);
                f.hash(state);
            }
            DataType::Text(..) | DataType::TinyText(..) => {
                let t: &str = self.into();
                t.hash(state)
            }
            DataType::Timestamp(ts) => ts.hash(state),
            DataType::Time(ref t) => t.hash(state),
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

impl From<i128> for DataType {
    fn from(s: i128) -> Self {
        if s >= std::i32::MIN.into() && s <= std::i32::MAX.into() {
            DataType::Int(s as i32)
        } else if s >= std::u32::MIN.into() && s <= std::u32::MAX.into() {
            DataType::UnsignedInt(s as u32)
        } else if s >= std::i64::MIN.into() && s <= std::i64::MAX.into() {
            DataType::BigInt(s as i64)
        } else if s >= std::u64::MIN.into() && s <= std::u64::MAX.into() {
            DataType::UnsignedBigInt(s as u64)
        } else {
            panic!("can't fit {} in a DataType", s)
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::BigInt(s)
    }
}

impl From<u64> for DataType {
    fn from(s: u64) -> Self {
        DataType::UnsignedBigInt(s)
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
        DataType::Int(s)
    }
}

impl From<u32> for DataType {
    fn from(s: u32) -> Self {
        DataType::UnsignedInt(s)
    }
}

impl From<usize> for DataType {
    fn from(s: usize) -> Self {
        DataType::UnsignedBigInt(s as u64)
    }
}

impl From<f32> for DataType {
    fn from(f: f32) -> Self {
        Self::from(f as f64)
    }
}

impl From<f64> for DataType {
    fn from(f: f64) -> Self {
        if !f.is_finite() {
            panic!();
        }

        let mut i = f.trunc() as i64;
        let mut frac = (f.fract() * FLOAT_PRECISION).round() as i32;
        if frac == 1_000_000_000 {
            i += 1;
            frac = 0;
        } else if frac == -1_000_000_000 {
            i -= 1;
            frac = 0;
        }

        DataType::Real(i, frac)
    }
}

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

impl<'a> From<&'a Literal> for DataType {
    fn from(l: &'a Literal) -> Self {
        match *l {
            Literal::Null => DataType::None,
            Literal::Integer(i) => (i as i64).into(),
            Literal::String(ref s) => s.as_str().into(),
            Literal::CurrentTimestamp => {
                let ts = chrono::Local::now().naive_local();
                DataType::Timestamp(ts)
            }
            Literal::FixedPoint(ref r) => {
                DataType::Real(i64::from(r.integral), r.fractional as i32)
            }
            _ => unimplemented!(),
        }
    }
}

impl From<Literal> for DataType {
    fn from(l: Literal) -> Self {
        (&l).into()
    }
}

impl From<NaiveTime> for DataType {
    fn from(t: NaiveTime) -> Self {
        DataType::Time(Arc::new(t.into()))
    }
}

impl From<MysqlTime> for DataType {
    fn from(t: MysqlTime) -> Self {
        DataType::Time(Arc::new(t))
    }
}

impl From<NaiveDate> for DataType {
    fn from(dt: NaiveDate) -> Self {
        DataType::Timestamp(dt.and_hms(0, 0, 0))
    }
}

impl From<NaiveDateTime> for DataType {
    fn from(dt: NaiveDateTime) -> Self {
        DataType::Timestamp(dt)
    }
}

impl<'a> From<&'a DataType> for NaiveDateTime {
    fn from(data: &'a DataType) -> Self {
        match *data {
            DataType::Timestamp(ref dt) => *dt,
            _ => panic!("attempted to convert a {:?} to a NaiveDateTime", data),
        }
    }
}

impl<'a> From<&'a DataType> for NaiveDate {
    fn from(data: &'a DataType) -> Self {
        match *data {
            DataType::Timestamp(ref dt) => dt.date(),
            _ => panic!("attempted to convert a {:?} to a NaiveDate", data),
        }
    }
}

impl<'a> From<&'a DataType> for MysqlTime {
    fn from(data: &'a DataType) -> Self {
        match *data {
            DataType::Time(ref mysql_time) => mysql_time.as_ref().clone(),
            _ => panic!("attempted to convert a {:?} to a MysqlTime", data),
        }
    }
}

// This conversion has many unwraps, but all of them are expected to be safe,
// because DataType variants (i.e. `Text` and `TinyText`) constructors are all
// generated from valid UTF-8 strings, or the constructor fails (e.g. TryFrom &[u8]).
// Thus, we can safely generate a &str from a DataType.
impl<'a> From<&'a DataType> for &'a str {
    fn from(data: &'a DataType) -> Self {
        match *data {
            DataType::Text(ref s) => s.to_str().unwrap(),
            DataType::TinyText(ref bts) => {
                if bts[TINYTEXT_WIDTH - 1] == 0 {
                    // NULL terminated CStr
                    use std::ffi::CStr;
                    let null = bts.iter().position(|&i| i == 0).unwrap() + 1;
                    CStr::from_bytes_with_nul(&bts[0..null])
                        .unwrap()
                        .to_str()
                        .unwrap()
                } else {
                    // String is exactly eight bytes
                    std::str::from_utf8(bts).unwrap()
                }
            }
            _ => panic!("attempted to convert a {:?} to a string", data),
        }
    }
}

impl From<DataType> for i128 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<DataType> for i64 {
    fn from(data: DataType) -> i64 {
        (&data).into()
    }
}

impl From<&'_ DataType> for i128 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::BigInt(s) => i128::from(s),
            DataType::UnsignedBigInt(s) => i128::from(s),
            DataType::Int(s) => i128::from(s),
            DataType::UnsignedInt(s) => i128::from(s),
            _ => panic!("attempted to convert a {:?} to an i128", data),
        }
    }
}

impl From<&'_ DataType> for i64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::UnsignedBigInt(s) => {
                if s as i128 >= std::i64::MIN.into() && s as i128 <= std::i64::MAX.into() {
                    s as i64
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an i64", data)
                }
            }
            DataType::BigInt(s) => s,
            DataType::Int(s) => i64::from(s),
            DataType::UnsignedInt(s) => i64::from(s),
            _ => panic!("attempted to convert a {:?} to an i64", data),
        }
    }
}

impl From<DataType> for u64 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for u64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::UnsignedBigInt(s) => s,
            DataType::BigInt(s) => {
                if s as i128 >= std::u64::MIN.into() && s as i128 <= std::u64::MAX.into() {
                    s as u64
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to a u64", data)
                }
            }
            DataType::UnsignedInt(s) => u64::from(s),
            DataType::Int(s) => {
                if s as i128 >= std::u64::MIN.into() && s as i128 <= std::u64::MAX.into() {
                    s as u64
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to a u64", data)
                }
            }
            _ => panic!("attempted to convert a {:?} to a u64", data),
        }
    }
}

impl From<DataType> for i32 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for i32 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::UnsignedBigInt(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    s as i32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an i32", data)
                }
            }
            DataType::BigInt(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    s as i32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an i32", data)
                }
            }
            DataType::UnsignedInt(s) => {
                if s as i128 >= std::i32::MIN.into() && s as i128 <= std::i32::MAX.into() {
                    s as i32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an i32", data)
                }
            }
            DataType::Int(s) => s,
            _ => panic!("attempted to convert a {:?} to a i32", data),
        }
    }
}

impl From<DataType> for u32 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for u32 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::UnsignedBigInt(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    s as u32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an u32", data)
                }
            }
            DataType::BigInt(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    s as u32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an u32", data)
                }
            }
            DataType::UnsignedInt(s) => s,
            DataType::Int(s) => {
                if s as i128 >= std::u32::MIN.into() && s as i128 <= std::u32::MAX.into() {
                    s as u32
                } else {
                    panic!("attempted to convert an out-of-bounds {:?} to an u32", data)
                }
            }
            _ => panic!("attempted to convert a {:?} to a i32", data),
        }
    }
}

impl From<DataType> for f64 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for f64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::Real(i, f) => i as f64 + f64::from(f) / FLOAT_PRECISION,
            DataType::UnsignedInt(i) => f64::from(i),
            DataType::Int(i) => f64::from(i),
            DataType::UnsignedBigInt(i) => i as f64,
            DataType::BigInt(i) => i as f64,
            _ => panic!("attempted to convert a {:?} to an f64", data),
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType::try_from(s.as_bytes()).unwrap()
    }
}

impl From<&'_ DataType> for String {
    fn from(dt: &DataType) -> Self {
        let s: &str = dt.into();
        s.into()
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::try_from(s.as_bytes()).unwrap()
    }
}

impl<'a> TryFrom<&'a [u8]> for DataType {
    type Error = &'static str;

    fn try_from(b: &[u8]) -> Result<Self, Self::Error> {
        let len = b.len();
        if len <= TINYTEXT_WIDTH {
            let mut bytes = [0; TINYTEXT_WIDTH];
            if len != 0 {
                let bts = &mut bytes[0..len];
                bts.copy_from_slice(b);
            }

            Ok(DataType::TinyText(bytes))
        } else {
            match ArcCStr::try_from(b) {
                Ok(arc_c_str) => Ok(DataType::Text(arc_c_str)),
                Err(_) => Err("Invalid utf-8 string"),
            }
        }
    }
}

impl TryFrom<mysql_common::value::Value> for DataType {
    type Error = &'static str;

    fn try_from(v: mysql_common::value::Value) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match v {
            Value::NULL => Ok(DataType::None),
            Value::Bytes(v) => DataType::try_from(&v[..]),
            Value::Int(v) => Ok(v.into()),
            Value::UInt(v) => Ok(v.into()),
            Value::Float(v) => Ok(v.into()),
            Value::Double(v) => Ok(v.into()),
            Value::Date(year, month, day, hour, minutes, seconds, micros) => {
                Ok(DataType::Timestamp(
                    NaiveDate::from_ymd(year.into(), month.into(), day.into()).and_hms_micro(
                        hour.into(),
                        minutes.into(),
                        seconds.into(),
                        micros.into(),
                    ),
                ))
            }
            Value::Time(..) => Err("`mysql_common::value::Value::time` is not supported in Noria"),
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
            (&DataType::BigInt(a), &DataType::BigInt(b)) => (a $op b).into(),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(b)) => (a $op b).into(),

            (&DataType::Int(a), &DataType::BigInt(b)) => (i64::from(a) $op b).into(),
            (&DataType::BigInt(a), &DataType::Int(b)) => (a $op i64::from(b)).into(),
            (&DataType::Int(a), &DataType::UnsignedBigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::Int(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::BigInt(a), &DataType::UnsignedBigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::BigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedInt(b)) => (a $op u64::from(b)).into(),
            (&DataType::UnsignedInt(a), &DataType::UnsignedBigInt(b)) => (u64::from(a) $op b).into(),

            (first @ &DataType::Int(..), second @ &DataType::Real(..)) |
            (first @ &DataType::BigInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::UnsignedInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::UnsignedBigInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Real(..), second @ &DataType::BigInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::UnsignedInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::UnsignedBigInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Real(..)) => {
                let a: f64 = first.into();
                let b: f64 = second.into();
                (a $op b).into()
            }
            (first, second) => panic!(
                format!(
                    "can't {} a {:?} and {:?}",
                    stringify!($op),
                    first,
                    second,
                )
            ),
        }
    );
);

impl<'a, 'b> Add<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn add(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(+, self, other)
    }
}

impl<'a, 'b> Sub<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn sub(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(-, self, other)
    }
}

impl<'a, 'b> Mul<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn mul(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(*, self, other)
    }
}

impl<'a, 'b> Div<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn div(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(/, self, other)
    }
}

/// A modification to make to an existing value.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Operation {
    /// Add the given value to the existing one.
    Add,
    /// Subtract the given value from the existing value.
    Sub,
}

/// A modification to make to a column in an existing row.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Modification {
    /// Set the cell to this value.
    Set(DataType),
    /// Use the given [`Operation`] to combine the existing value and this one.
    Apply(Operation, DataType),
    /// Leave the existing value as-is.
    None,
}

impl<T> From<T> for Modification
where
    T: Into<DataType>,
{
    fn from(t: T) -> Modification {
        Modification::Set(t.into())
    }
}

/// An operation to apply to a base table.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TableOperation {
    /// Insert the contained row.
    Insert(Vec<DataType>),
    /// Delete a row with the contained key.
    Delete {
        /// The key.
        key: Vec<DataType>,
    },
    /// If a row exists with the same key as the contained row, update it using `update`, otherwise
    /// insert `row`.
    InsertOrUpdate {
        /// This row will be inserted if no existing row is found.
        row: Vec<DataType>,
        /// These modifications will be applied to the columns of an existing row.
        update: Vec<Modification>,
    },
    /// Update an existing row with the given `key`.
    Update {
        /// The modifications to make to each column of the existing row.
        set: Vec<Modification>,
        /// The key used to identify the row to update.
        key: Vec<DataType>,
    },
}

impl TableOperation {
    #[doc(hidden)]
    pub fn row(&self) -> Option<&[DataType]> {
        match *self {
            TableOperation::Insert(ref r) => Some(r),
            TableOperation::InsertOrUpdate { ref row, .. } => Some(row),
            _ => None,
        }
    }
}

impl From<Vec<DataType>> for TableOperation {
    fn from(other: Vec<DataType>) -> Self {
        TableOperation::Insert(other)
    }
}

impl Arbitrary for DataType {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<DataType>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use launchpad::arbitrary::{arbitrary_duration, arbitrary_naive_date_time};
        use proptest::arbitrary::any;
        use proptest::prelude::*;
        use DataType::*;

        prop_oneof![
            Just(None),
            any::<i32>().prop_map(Int),
            any::<u32>().prop_map(UnsignedInt),
            any::<i64>().prop_map(BigInt),
            any::<u64>().prop_map(UnsignedBigInt),
            any::<(i64, i32)>().prop_map(|(i, f)| Real(i, f)),
            any::<String>().prop_map(DataType::from),
            arbitrary_naive_date_time().prop_map(Timestamp),
            arbitrary_duration()
                .prop_map(MysqlTime::new)
                .prop_map(Arc::new)
                .prop_map(Time),
        ]
        .boxed()
    }
}

/// Errors that can occur when coercing a [`DataType`] to a different [`SqlType`] with
/// [`DataType::coerce_to`]
#[derive(Serialize, Deserialize, Debug, Error)]
#[error("error coercing value {value:?} to {expected_type:?}: {message}")]
pub struct ValueCoerceError {
    /// The value that was being coerced
    pub value: DataType,
    /// The type that we were trying to coerce to
    pub expected_type: SqlType,
    /// A human-readable message for the error
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    #[test]
    fn mysql_value_to_datatype() {
        use assert_approx_eq::assert_approx_eq;
        use mysql_common::value::Value;

        // Test Value::NULL.
        let a = Value::NULL;
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::None);

        // Test Value::Bytes.
        // Can't build a CString with interior nul-terminated chars.
        let a = Value::Bytes(vec![0; 30]);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_err());

        let a = Value::Bytes(vec![1; 30]);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DataType::Text(ArcCStr::try_from(&vec![1; 30][..]).unwrap())
        );

        let s = [1; 15];
        let a = Value::Bytes(s.to_vec());
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::TinyText(s));

        // Test Value::Int.
        let a = Value::Int(-5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::BigInt(-5));

        // Test Value::Float.
        let a = Value::UInt(5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::UnsignedBigInt(5));

        // Test Value::Float.
        let initial_float: f32 = 4.2;
        let a = Value::Float(initial_float);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f64 = a_dt.unwrap().into();
        assert_approx_eq!(converted_float, initial_float as f64);

        // Test Value::Date.
        let ts = NaiveDate::from_ymd(1111, 1, 11).and_hms_micro(2, 3, 4, 5);
        let a = Value::from(ts.clone());
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::Timestamp(ts));

        // Test Value::Time.
        // noria::DataType has no `Time` representation.
        let a = Value::Time(true, 0, 0, 0, 0, 0);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_err());
    }

    #[test]
    fn real_to_string() {
        let a: DataType = (2.5).into();
        let b: DataType = (-2.01).into();
        let c: DataType = (-0.012_345_678).into();
        assert_eq!(a.to_string(), "2.500000000");
        assert_eq!(b.to_string(), "-2.010000000");
        assert_eq!(c.to_string(), "-0.012345678");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_float() {
        let original = 2.5;
        let data_type: DataType = original.into();
        let converted: f64 = (&data_type).into();
        assert_eq!(original, converted);
    }

    #[test]
    fn add_data_types() {
        assert_eq!(&DataType::from(1) + &DataType::from(2), 3.into());
        assert_eq!(&DataType::from(1.5) + &DataType::from(2), (3.5).into());
        assert_eq!(&DataType::from(2) + &DataType::from(1.5), (3.5).into());
        assert_eq!(&DataType::from(1.5) + &DataType::from(2.5), (4.0).into());
        assert_eq!(&DataType::BigInt(1) + &DataType::BigInt(2), 3.into());
        assert_eq!(&DataType::from(1) + &DataType::BigInt(2), 3.into());
        assert_eq!(&DataType::BigInt(2) + &DataType::from(1), 3.into());
    }

    #[test]
    fn subtract_data_types() {
        assert_eq!(&DataType::from(2) - &DataType::from(1), 1.into());
        assert_eq!(&DataType::from(3.5) - &DataType::from(2), (1.5).into());
        assert_eq!(&DataType::from(2) - &DataType::from(1.5), (0.5).into());
        assert_eq!(&DataType::from(3.5) - &DataType::from(2.0), (1.5).into());
        assert_eq!(&DataType::BigInt(1) - &DataType::BigInt(2), (-1).into());
        assert_eq!(&DataType::from(1) - &DataType::BigInt(2), (-1).into());
        assert_eq!(&DataType::BigInt(2) - &DataType::from(1), 1.into());
    }

    #[test]
    fn multiply_data_types() {
        assert_eq!(&DataType::from(2) * &DataType::from(1), 2.into());
        assert_eq!(&DataType::from(3.5) * &DataType::from(2), (7.0).into());
        assert_eq!(&DataType::from(2) * &DataType::from(1.5), (3.0).into());
        assert_eq!(&DataType::from(3.5) * &DataType::from(2.0), (7.0).into());
        assert_eq!(&DataType::BigInt(1) * &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::from(1) * &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::BigInt(2) * &DataType::from(1), 2.into());
    }

    #[test]
    fn divide_data_types() {
        assert_eq!(&DataType::from(2) / &DataType::from(1), 2.into());
        assert_eq!(&DataType::from(7.5) / &DataType::from(2), (3.75).into());
        assert_eq!(&DataType::from(7) / &DataType::from(2.5), (2.8).into());
        assert_eq!(&DataType::from(3.5) / &DataType::from(2.0), (1.75).into());
        assert_eq!(&DataType::BigInt(4) / &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::from(4) / &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::BigInt(4) / &DataType::from(2), 2.into());
    }

    #[test]
    #[should_panic(expected = "can't + a TinyText(\"hi\") and Int(5)")]
    fn add_invalid_types() {
        let a: DataType = "hi".into();
        let b: DataType = 5.into();
        let _ = &a + &b;
    }

    #[test]
    fn data_type_debug() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "I contain ' and \"".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{:?}", tiny_text), "TinyText(\"hi\")");
        assert_eq!(format!("{:?}", text), "Text(\"I contain \\' and \\\"\")");
        assert_eq!(format!("{:?}", real), "Real(-0.050000000)");
        assert_eq!(
            format!("{:?}", timestamp),
            "Timestamp(1970-01-01T00:00:00.042)"
        );
        assert_eq!(format!("{:?}", int), "Int(5)");
        assert_eq!(format!("{:?}", big_int), "BigInt(5)");
    }

    #[test]
    fn data_type_display() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "this is a very long text indeed".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{}", tiny_text), "\"hi\"");
        assert_eq!(format!("{}", text), "\"this is a very long text indeed\"");
        assert_eq!(format!("{}", real), "-0.050000000");
        assert_eq!(format!("{}", timestamp), "Thu Jan  1 00:00:00 1970");
        assert_eq!(format!("{}", int), "5");
        assert_eq!(format!("{}", big_int), "5");
    }

    fn _data_type_fungibility_test_eq<T>(f: &dyn for<'a> Fn(&'a DataType) -> T)
    where
        T: PartialEq + fmt::Debug,
    {
        let txt1: DataType = "hi".into();
        let txt12: DataType = "no".into();
        let txt2: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
        let text: DataType = "this is a very long text indeed".into();
        let text2: DataType = "this is another long text".into();
        let real: DataType = (-0.05).into();
        let real2: DataType = (-0.06).into();
        let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let time2 = DataType::Timestamp(NaiveDateTime::from_timestamp(1, 42_000_000));
        let shrt = DataType::Int(5);
        let shrt6 = DataType::Int(6);
        let long = DataType::BigInt(5);
        let long6 = DataType::BigInt(6);
        let ushrt = DataType::UnsignedInt(5);
        let ushrt6 = DataType::UnsignedInt(6);
        let ulong = DataType::UnsignedBigInt(5);
        let ulong6 = DataType::UnsignedBigInt(6);

        assert_eq!(f(&txt1), f(&txt1));
        assert_eq!(f(&txt2), f(&txt2));
        assert_eq!(f(&text), f(&text));
        assert_eq!(f(&shrt), f(&shrt));
        assert_eq!(f(&long), f(&long));
        assert_eq!(f(&ushrt), f(&ushrt));
        assert_eq!(f(&ulong), f(&ulong));
        assert_eq!(f(&real), f(&real));
        assert_eq!(f(&time), f(&time));

        // coercion
        assert_eq!(f(&txt1), f(&txt2));
        assert_eq!(f(&txt2), f(&txt1));
        assert_eq!(f(&shrt), f(&long));
        assert_eq!(f(&long), f(&shrt));
        assert_eq!(f(&ushrt), f(&shrt));
        assert_eq!(f(&shrt), f(&ushrt));
        assert_eq!(f(&ulong), f(&long));
        assert_eq!(f(&long), f(&ulong));
        assert_eq!(f(&shrt), f(&ulong));
        assert_eq!(f(&ulong), f(&shrt));
        assert_eq!(f(&ushrt), f(&long));
        assert_eq!(f(&long), f(&ushrt));
        assert_eq!(f(&ushrt), f(&ulong));
        assert_eq!(f(&ulong), f(&ushrt));

        // negation
        assert_ne!(f(&txt1), f(&txt12));
        assert_ne!(f(&txt1), f(&text));
        assert_ne!(f(&txt1), f(&real));
        assert_ne!(f(&txt1), f(&time));
        assert_ne!(f(&txt1), f(&shrt));
        assert_ne!(f(&txt1), f(&long));
        assert_ne!(f(&txt1), f(&ushrt));
        assert_ne!(f(&txt1), f(&ulong));

        assert_ne!(f(&txt2), f(&txt12));
        assert_ne!(f(&txt2), f(&text));
        assert_ne!(f(&txt2), f(&real));
        assert_ne!(f(&txt2), f(&time));
        assert_ne!(f(&txt2), f(&shrt));
        assert_ne!(f(&txt2), f(&long));
        assert_ne!(f(&txt2), f(&ushrt));
        assert_ne!(f(&txt2), f(&ulong));

        assert_ne!(f(&text), f(&text2));
        assert_ne!(f(&text), f(&txt1));
        assert_ne!(f(&text), f(&txt2));
        assert_ne!(f(&text), f(&real));
        assert_ne!(f(&text), f(&time));
        assert_ne!(f(&text), f(&shrt));
        assert_ne!(f(&text), f(&long));
        assert_ne!(f(&text), f(&ushrt));
        assert_ne!(f(&text), f(&ulong));

        assert_ne!(f(&real), f(&real2));
        assert_ne!(f(&real), f(&txt1));
        assert_ne!(f(&real), f(&txt2));
        assert_ne!(f(&real), f(&text));
        assert_ne!(f(&real), f(&time));
        assert_ne!(f(&real), f(&shrt));
        assert_ne!(f(&real), f(&long));
        assert_ne!(f(&real), f(&ushrt));
        assert_ne!(f(&real), f(&ulong));

        assert_ne!(f(&time), f(&time2));
        assert_ne!(f(&time), f(&txt1));
        assert_ne!(f(&time), f(&txt2));
        assert_ne!(f(&time), f(&text));
        assert_ne!(f(&time), f(&real));
        assert_ne!(f(&time), f(&shrt));
        assert_ne!(f(&time), f(&long));
        assert_ne!(f(&time), f(&ushrt));
        assert_ne!(f(&time), f(&ulong));

        assert_ne!(f(&shrt), f(&shrt6));
        assert_ne!(f(&shrt), f(&txt1));
        assert_ne!(f(&shrt), f(&txt2));
        assert_ne!(f(&shrt), f(&text));
        assert_ne!(f(&shrt), f(&real));
        assert_ne!(f(&shrt), f(&time));
        assert_ne!(f(&shrt), f(&long6));

        assert_ne!(f(&long), f(&long6));
        assert_ne!(f(&long), f(&txt1));
        assert_ne!(f(&long), f(&txt2));
        assert_ne!(f(&long), f(&text));
        assert_ne!(f(&long), f(&real));
        assert_ne!(f(&long), f(&time));
        assert_ne!(f(&long), f(&shrt6));

        assert_ne!(f(&ushrt), f(&ushrt6));
        assert_ne!(f(&ushrt), f(&txt1));
        assert_ne!(f(&ushrt), f(&txt2));
        assert_ne!(f(&ushrt), f(&text));
        assert_ne!(f(&ushrt), f(&real));
        assert_ne!(f(&ushrt), f(&time));
        assert_ne!(f(&ushrt), f(&ulong6));
        assert_ne!(f(&ushrt), f(&shrt6));
        assert_ne!(f(&ushrt), f(&long6));

        assert_ne!(f(&ulong), f(&ulong6));
        assert_ne!(f(&ulong), f(&txt1));
        assert_ne!(f(&ulong), f(&txt2));
        assert_ne!(f(&ulong), f(&text));
        assert_ne!(f(&ulong), f(&real));
        assert_ne!(f(&ulong), f(&time));
        assert_ne!(f(&ulong), f(&ushrt6));
        assert_ne!(f(&ulong), f(&shrt6));
        assert_ne!(f(&ulong), f(&long6));
    }

    #[test]
    fn data_type_conversion() {
        let int_i32_min = DataType::Int(std::i32::MIN);
        let int_u32_min = DataType::Int(std::u32::MIN as i32);
        let int_i32_max = DataType::Int(std::i32::MAX);
        let uint_u32_min = DataType::UnsignedInt(std::u32::MIN);
        let uint_i32_max = DataType::UnsignedInt(std::i32::MAX as u32);
        let uint_u32_max = DataType::UnsignedInt(std::u32::MAX);
        let bigint_i64_min = DataType::BigInt(std::i64::MIN);
        let bigint_i32_min = DataType::BigInt(std::i32::MIN as i64);
        let bigint_u32_min = DataType::BigInt(std::u32::MIN as i64);
        let bigint_i32_max = DataType::BigInt(std::i32::MAX as i64);
        let bigint_u32_max = DataType::BigInt(std::u32::MAX as i64);
        let bigint_i64_max = DataType::BigInt(std::i64::MAX);
        let ubigint_u32_min = DataType::UnsignedBigInt(std::u32::MIN as u64);
        let ubigint_i32_max = DataType::UnsignedBigInt(std::i32::MAX as u64);
        let ubigint_u32_max = DataType::UnsignedBigInt(std::u32::MAX as u64);
        let ubigint_i64_max = DataType::UnsignedBigInt(std::i64::MAX as u64);
        let ubigint_u64_max = DataType::UnsignedBigInt(std::u64::MAX);

        fn _data_type_conversion_test_eq_i32(d: &DataType) {
            assert_eq!(i32::from(d) as i128, i128::from(d))
        }
        fn _data_type_conversion_test_eq_i32_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| i32::from(d)).is_err())
        }
        fn _data_type_conversion_test_eq_i64(d: &DataType) {
            assert_eq!(i64::from(d) as i128, i128::from(d))
        }
        fn _data_type_conversion_test_eq_i64_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| i64::from(d)).is_err())
        }
        fn _data_type_conversion_test_eq_u32(d: &DataType) {
            assert_eq!(u32::from(d) as i128, i128::from(d))
        }
        fn _data_type_conversion_test_eq_u32_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| u32::from(d)).is_err())
        }
        fn _data_type_conversion_test_eq_u64(d: &DataType) {
            assert_eq!(u64::from(d) as i128, i128::from(d))
        }
        fn _data_type_conversion_test_eq_u64_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| u64::from(d)).is_err())
        }
        fn _data_type_conversion_test_eq_i128(d: &DataType) {
            assert_eq!(i128::from(d) as i128, i128::from(d))
        }
        fn _data_type_conversion_test_eq_i128_panic(d: &DataType) {
            assert!(std::panic::catch_unwind(|| i128::from(d)).is_err())
        }

        _data_type_conversion_test_eq_i32_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_u32_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_u64_panic(&bigint_i64_min);
        _data_type_conversion_test_eq_i64(&bigint_i64_min);
        _data_type_conversion_test_eq_i128(&bigint_i64_min);

        _data_type_conversion_test_eq_u32_panic(&int_i32_min);
        _data_type_conversion_test_eq_u64_panic(&int_i32_min);
        _data_type_conversion_test_eq_i32(&int_i32_min);
        _data_type_conversion_test_eq_i64(&int_i32_min);
        _data_type_conversion_test_eq_i128(&int_i32_min);
        _data_type_conversion_test_eq_u32_panic(&bigint_i32_min);
        _data_type_conversion_test_eq_u64_panic(&bigint_i32_min);
        _data_type_conversion_test_eq_i32(&bigint_i32_min);
        _data_type_conversion_test_eq_i64(&bigint_i32_min);
        _data_type_conversion_test_eq_i128(&bigint_i32_min);

        _data_type_conversion_test_eq_i32(&int_u32_min);
        _data_type_conversion_test_eq_i64(&int_u32_min);
        _data_type_conversion_test_eq_u32(&int_u32_min);
        _data_type_conversion_test_eq_u64(&int_u32_min);
        _data_type_conversion_test_eq_i128(&int_u32_min);
        _data_type_conversion_test_eq_i32(&uint_u32_min);
        _data_type_conversion_test_eq_i64(&uint_u32_min);
        _data_type_conversion_test_eq_u32(&uint_u32_min);
        _data_type_conversion_test_eq_u64(&uint_u32_min);
        _data_type_conversion_test_eq_i128(&uint_u32_min);
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

        _data_type_conversion_test_eq_i32(&int_i32_max);
        _data_type_conversion_test_eq_i64(&int_i32_max);
        _data_type_conversion_test_eq_u32(&int_i32_max);
        _data_type_conversion_test_eq_u64(&int_i32_max);
        _data_type_conversion_test_eq_i128(&int_i32_max);
        _data_type_conversion_test_eq_i32(&uint_i32_max);
        _data_type_conversion_test_eq_i64(&uint_i32_max);
        _data_type_conversion_test_eq_u32(&uint_i32_max);
        _data_type_conversion_test_eq_u64(&uint_i32_max);
        _data_type_conversion_test_eq_i128(&uint_i32_max);
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

        _data_type_conversion_test_eq_i32_panic(&uint_u32_max);
        _data_type_conversion_test_eq_i64(&uint_u32_max);
        _data_type_conversion_test_eq_u32(&uint_u32_max);
        _data_type_conversion_test_eq_u64(&uint_u32_max);
        _data_type_conversion_test_eq_i128(&uint_u32_max);
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
        assert_eq!(String::try_from(&DataType::from(s.clone())), Ok(s))
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
        let json_serialize = |dt: &DataType| serde_json::to_string(dt).unwrap();
        let bincode_serialize = |dt: &DataType| bincode::serialize(dt).unwrap();
        _data_type_fungibility_test_eq(&|x: &DataType| x.clone());
        _data_type_fungibility_test_eq(&hash);
        _data_type_fungibility_test_eq(&json_serialize);
        _data_type_fungibility_test_eq(&bincode_serialize);

        use std::convert::TryFrom;

        let txt1: DataType = "hi".into();
        let txt12: DataType = "no".into();
        let txt2: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
        let text: DataType = "this is a very long text indeed".into();
        let text2: DataType = "this is another long text".into();
        let real: DataType = (-0.05).into();
        let real2: DataType = (-0.06).into();
        let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let time2 = DataType::Timestamp(NaiveDateTime::from_timestamp(1, 42_000_000));
        let shrt = DataType::Int(5);
        let shrt6 = DataType::Int(6);
        let long = DataType::BigInt(5);
        let long6 = DataType::BigInt(6);
        let ushrt = DataType::UnsignedInt(5);
        let ushrt6 = DataType::UnsignedInt(6);
        let ulong = DataType::UnsignedBigInt(5);
        let ulong6 = DataType::UnsignedBigInt(6);

        use std::cmp::Ordering;
        assert_eq!(txt1.cmp(&txt1), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt2), Ordering::Equal);
        assert_eq!(text.cmp(&text), Ordering::Equal);
        assert_eq!(shrt.cmp(&shrt), Ordering::Equal);
        assert_eq!(ushrt.cmp(&ushrt), Ordering::Equal);
        assert_eq!(long.cmp(&long), Ordering::Equal);
        assert_eq!(ulong.cmp(&ulong), Ordering::Equal);
        assert_eq!(real.cmp(&real), Ordering::Equal);
        assert_eq!(time.cmp(&time), Ordering::Equal);

        // coercion
        assert_eq!(txt1.cmp(&txt2), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt1), Ordering::Equal);
        assert_eq!(shrt.cmp(&long), Ordering::Equal);
        assert_eq!(shrt.cmp(&ulong), Ordering::Equal);
        assert_eq!(ushrt.cmp(&long), Ordering::Equal);
        assert_eq!(ushrt.cmp(&ulong), Ordering::Equal);
        assert_eq!(long.cmp(&shrt), Ordering::Equal);
        assert_eq!(long.cmp(&ushrt), Ordering::Equal);
        assert_eq!(ulong.cmp(&shrt), Ordering::Equal);
        assert_eq!(ulong.cmp(&ushrt), Ordering::Equal);

        // negation
        assert_ne!(txt1.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt1.cmp(&text), Ordering::Equal);
        assert_ne!(txt1.cmp(&real), Ordering::Equal);
        assert_ne!(txt1.cmp(&time), Ordering::Equal);
        assert_ne!(txt1.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt1.cmp(&ushrt), Ordering::Equal);
        assert_ne!(txt1.cmp(&long), Ordering::Equal);
        assert_ne!(txt1.cmp(&ulong), Ordering::Equal);

        assert_ne!(txt2.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt2.cmp(&text), Ordering::Equal);
        assert_ne!(txt2.cmp(&real), Ordering::Equal);
        assert_ne!(txt2.cmp(&time), Ordering::Equal);
        assert_ne!(txt2.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt2.cmp(&ushrt), Ordering::Equal);
        assert_ne!(txt2.cmp(&long), Ordering::Equal);
        assert_ne!(txt2.cmp(&ulong), Ordering::Equal);

        assert_ne!(text.cmp(&text2), Ordering::Equal);
        assert_ne!(text.cmp(&txt1), Ordering::Equal);
        assert_ne!(text.cmp(&txt2), Ordering::Equal);
        assert_ne!(text.cmp(&real), Ordering::Equal);
        assert_ne!(text.cmp(&time), Ordering::Equal);
        assert_ne!(text.cmp(&shrt), Ordering::Equal);
        assert_ne!(text.cmp(&ushrt), Ordering::Equal);
        assert_ne!(text.cmp(&long), Ordering::Equal);
        assert_ne!(text.cmp(&ulong), Ordering::Equal);

        assert_ne!(real.cmp(&real2), Ordering::Equal);
        assert_ne!(real.cmp(&txt1), Ordering::Equal);
        assert_ne!(real.cmp(&txt2), Ordering::Equal);
        assert_ne!(real.cmp(&text), Ordering::Equal);
        assert_ne!(real.cmp(&time), Ordering::Equal);
        assert_ne!(real.cmp(&shrt), Ordering::Equal);
        assert_ne!(real.cmp(&ushrt), Ordering::Equal);
        assert_ne!(real.cmp(&long), Ordering::Equal);
        assert_ne!(real.cmp(&ulong), Ordering::Equal);

        assert_ne!(time.cmp(&time2), Ordering::Equal);
        assert_ne!(time.cmp(&txt1), Ordering::Equal);
        assert_ne!(time.cmp(&txt2), Ordering::Equal);
        assert_ne!(time.cmp(&text), Ordering::Equal);
        assert_ne!(time.cmp(&real), Ordering::Equal);
        assert_ne!(time.cmp(&shrt), Ordering::Equal);
        assert_ne!(time.cmp(&ushrt), Ordering::Equal);
        assert_ne!(time.cmp(&long), Ordering::Equal);
        assert_ne!(time.cmp(&ulong), Ordering::Equal);

        assert_ne!(shrt.cmp(&shrt6), Ordering::Equal);
        assert_ne!(shrt.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt1), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt2), Ordering::Equal);
        assert_ne!(shrt.cmp(&text), Ordering::Equal);
        assert_ne!(shrt.cmp(&real), Ordering::Equal);
        assert_ne!(shrt.cmp(&time), Ordering::Equal);
        assert_ne!(shrt.cmp(&long6), Ordering::Equal);
        assert_ne!(shrt.cmp(&ulong6), Ordering::Equal);

        assert_ne!(ushrt.cmp(&shrt6), Ordering::Equal);
        assert_ne!(ushrt.cmp(&ushrt6), Ordering::Equal);
        assert_ne!(ushrt.cmp(&txt1), Ordering::Equal);
        assert_ne!(ushrt.cmp(&txt2), Ordering::Equal);
        assert_ne!(ushrt.cmp(&text), Ordering::Equal);
        assert_ne!(ushrt.cmp(&real), Ordering::Equal);
        assert_ne!(ushrt.cmp(&time), Ordering::Equal);
        assert_ne!(ushrt.cmp(&long6), Ordering::Equal);
        assert_ne!(ushrt.cmp(&ulong6), Ordering::Equal);

        assert_ne!(long.cmp(&long6), Ordering::Equal);
        assert_ne!(long.cmp(&ulong6), Ordering::Equal);
        assert_ne!(long.cmp(&txt1), Ordering::Equal);
        assert_ne!(long.cmp(&txt2), Ordering::Equal);
        assert_ne!(long.cmp(&text), Ordering::Equal);
        assert_ne!(long.cmp(&real), Ordering::Equal);
        assert_ne!(long.cmp(&time), Ordering::Equal);
        assert_ne!(long.cmp(&shrt6), Ordering::Equal);
        assert_ne!(long.cmp(&ushrt6), Ordering::Equal);

        assert_ne!(ulong.cmp(&long6), Ordering::Equal);
        assert_ne!(ulong.cmp(&ulong6), Ordering::Equal);
        assert_ne!(ulong.cmp(&txt1), Ordering::Equal);
        assert_ne!(ulong.cmp(&txt2), Ordering::Equal);
        assert_ne!(ulong.cmp(&text), Ordering::Equal);
        assert_ne!(ulong.cmp(&real), Ordering::Equal);
        assert_ne!(ulong.cmp(&time), Ordering::Equal);
        assert_ne!(ulong.cmp(&shrt6), Ordering::Equal);
        assert_ne!(ulong.cmp(&ushrt6), Ordering::Equal);
    }

    #[allow(clippy::eq_op)]
    mod eq {
        use super::*;
        use test_strategy::proptest;

        #[proptest]
        fn reflexive(dt: DataType) {
            assert!(dt == dt);
        }

        #[proptest]
        fn symmetric(dt1: DataType, dt2: DataType) {
            assert_eq!(dt1 == dt2, dt2 == dt1);
        }

        #[proptest]
        fn transitive(dt1: DataType, dt2: DataType, dt3: DataType) {
            if dt1 == dt2 && dt2 == dt3 {
                assert!(dt1 == dt3)
            }
        }
    }

    mod coerce_to {
        use super::*;
        use launchpad::arbitrary::{
            arbitrary_naive_date, arbitrary_naive_date_time, arbitrary_naive_time,
        };
        use proptest::sample::select;
        use proptest::strategy::Strategy;
        use test_strategy::proptest;
        use SqlType::*;

        #[proptest]
        fn same_type_is_identity(dt: DataType) {
            if let Some(ty) = dt.sql_type() {
                assert_eq!(dt.coerce_to(&ty).as_deref().unwrap(), &dt);
            }
        }

        #[proptest]
        fn parse_timestamps(#[strategy(arbitrary_naive_date_time())] ndt: NaiveDateTime) {
            let expected = DataType::from(ndt);
            let input = DataType::from(ndt.format(TIMESTAMP_FORMAT).to_string());
            let result = input.coerce_to(&Timestamp).unwrap();
            assert_eq!(*result, expected);
        }

        #[proptest]
        fn parse_times(#[strategy(arbitrary_naive_time())] nt: NaiveTime) {
            let expected = DataType::from(nt);
            let input = DataType::from(nt.format(TIME_FORMAT).to_string());
            let result = input.coerce_to(&Time).unwrap();
            assert_eq!(*result, expected);
        }

        #[proptest]
        fn parse_dates(#[strategy(arbitrary_naive_date())] nd: NaiveDate) {
            let expected = DataType::from(NaiveDateTime::new(nd, NaiveTime::from_hms(0, 0, 0)));
            let input = DataType::from(nd.format(DATE_FORMAT).to_string());
            let result = input.coerce_to(&Date).unwrap();
            assert_eq!(*result, expected);
        }

        #[test]
        fn timestamp_surjections() {
            let input = DataType::from(NaiveDate::from_ymd(2021, 3, 17).and_hms(11, 34, 56));
            assert_eq!(
                *input.coerce_to(&Date).unwrap(),
                NaiveDate::from_ymd(2021, 3, 17).into()
            );
            assert_eq!(
                *input.coerce_to(&Time).unwrap(),
                NaiveTime::from_hms(11, 34, 56).into()
            );
        }

        #[proptest]
        fn bigint_to_int(int: i32) {
            assert_eq!(
                *DataType::from(int as i64).coerce_to(&Int(32)).unwrap(),
                DataType::from(int as i32)
            );
        }

        fn int_type() -> impl Strategy<Value = SqlType> {
            use SqlType::*;
            select(vec![Tinyint(8), Smallint(16), Int(32), Bigint(64)])
        }

        #[proptest]
        fn real_to_int(whole_part: i32, #[strategy(int_type())] int_type: SqlType) {
            let real = DataType::Real(whole_part.into(), 0);
            let result = real.coerce_to(&int_type).unwrap();
            assert_eq!(i32::from(result.into_owned()), whole_part);
        }

        fn unsigned_type() -> impl Strategy<Value = SqlType> {
            use SqlType::*;
            select(vec![
                UnsignedTinyint(8),
                UnsignedSmallint(16),
                UnsignedInt(32),
                UnsignedBigint(64),
            ])
        }

        #[proptest]
        fn real_to_unsigned(whole_part: u32, #[strategy(unsigned_type())] unsigned_type: SqlType) {
            let real = DataType::Real(whole_part as i64, 0);
            let result = real.coerce_to(&unsigned_type).unwrap();
            assert_eq!(u32::from(result.into_owned()), whole_part);
        }

        #[proptest]
        fn char_unequal_length(
            #[strategy(1..30u16)] chlen: u16,
            #[strategy("a{1,30}")]
            #[filter(#chlen != u16::try_from(#text.len()).unwrap())]
            text: String,
        ) {
            use SqlType::*;
            let input = DataType::from(text.as_str());
            assert!(input.coerce_to(&Char(chlen)).is_err());
        }

        #[proptest]
        fn char_equal_length(#[strategy("a{1,30}")] text: String) {
            use SqlType::*;
            let input = DataType::from(text.as_str());
            let intermediate = Char(u16::try_from(text.len()).unwrap());
            let result = input.coerce_to(&intermediate).unwrap();
            assert_eq!(String::from(&result.into_owned()).as_str(), text.as_str());
        }
    }
}
