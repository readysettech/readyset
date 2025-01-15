use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use chrono::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike,
};
use proptest::arbitrary::Arbitrary;
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::{DfType, DfValue};

/// The format for timestamps when parsed as text
pub const TIMESTAMP_PARSE_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// The format for timestamps when presented as text
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The format for timestamps with time zone when presented as text
pub const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%:z";

/// The format for dates when parsed as text
pub const DATE_FORMAT: &str = "%Y-%m-%d";

/// An optimized storage for date and datetime SQL formats. The possible inner data
/// may be:
///
/// DATE
/// DATETIME(n) with or without tz
/// TIMESTAMP with or without tz
///
/// Externally this type behaves like a [`chrono::DateTime<Tz>`], since it casts
/// itself to that type except for storage. The only difference is in how [`fmt::Display`]
/// behaves. It knows to properly format to whatever inner representation is appropriate,
/// i.e. date only, timestamp with timezone, timestamp with subsecond digits etc.
///
/// Sadly the way chrono implements `DateTime<Tz>` occupies at least 16 bytes, and therefore
/// overflows DfValue. So this type internally stores a [`NaiveDateTime`] with a 3 byte
/// of extra data. Since 3 bytes allow us to store 24 bits, this is how we use them:
///
/// 17 bits for the timezone offset (0 to 86_400)
/// 1 bit to signify negative offset
/// 3 bits for the subsecond digit count required
/// 1 bit to signify this is DATE only
/// 1 bit to signify timezone offset is present (since 0 is a valid offset)
/// 1 bit to signify the invalid MySQL zero date (0000-00-00)
#[derive(Clone, Copy, Serialize, Deserialize)]
#[repr(C, packed)] // So we can actually fit into the 15 bytes
pub struct TimestampTz {
    // extra is 3 bytes as described above
    pub(crate) extra: [u8; 3],
    // datetime is 12 bytes for a total of 15 bytes
    pub(crate) datetime: NaiveDateTime,
}

impl TimestampTz {
    const ZERO_FLAG: u8 = 0b_1000_0000;
    const TIMEZONE_FLAG: u8 = 0b_0100_0000;
    const DATE_ONLY_FLAG: u8 = 0b_0010_0000;
    const SUBSECOND_DIGITS_BITS: u8 = 0b_0001_1100;
    const NEGATIVE_FLAG: u8 = 0b_0000_0010;
    const TOP_OFFSET_BIT: u8 = 0b_0000_0001;

    #[inline(always)]
    /// Convert self to local timezone while keeping the same extra
    pub fn to_local(&self) -> Self {
        let mut ts = *self;
        ts.datetime = self.to_chrono().with_timezone(&chrono::Local).naive_local();
        ts
    }
    #[inline(always)]
    /// Constructs a [`TimestampTz`] when provided with the number of ms since the unix epoch.
    pub fn from_unix_ms(time_ms: u64) -> Self {
        let (secs, ns) = (
            (time_ms / 1000) as i64,
            ((time_ms % 1000) * 1_000 * 1_000) as u32,
        );
        Self::from(DateTime::from_timestamp(secs, ns).unwrap().naive_utc())
    }

    /// Returns true if the contained offset should be negated
    #[inline(always)]
    fn has_negative_offset(&self) -> bool {
        self.extra[2] & 0b_10 != 0
    }

    /// Returns true if timezone should be displayed
    #[inline(always)]
    pub fn has_timezone(&self) -> bool {
        self.extra[2] & TimestampTz::TIMEZONE_FLAG != 0
    }

    /// Returns true if should be displayed as date only
    #[inline(always)]
    pub fn has_date_only(&self) -> bool {
        self.extra[2] & TimestampTz::DATE_ONLY_FLAG != 0
    }

    /// Mark this timestamp as only containing a date value
    #[inline(always)]
    pub fn set_date_only(&mut self) {
        self.extra[2] |= TimestampTz::DATE_ONLY_FLAG
    }

    /// Return the timezone offset from UTC in seconds
    #[inline(always)]
    fn get_offset(&self) -> i32 {
        // First load the 17 bits of offset
        let e = &self.extra;
        let offset = i32::from_le_bytes([e[0], e[1], e[2] & TimestampTz::TOP_OFFSET_BIT, 0]);
        // Then check the sign bit
        if !self.has_negative_offset() {
            offset
        } else {
            -offset
        }
    }

    /// Set the timezone offset from UTC in seconds
    #[inline(always)]
    fn set_offset(&mut self, offset: i32) {
        // This assertion is always true so long as we use chrono, so no reason to return an error
        // here
        assert!(offset > -86_400 && offset < 86_400);

        let sign = offset.is_negative() as u8;
        let offset = offset.abs();

        let offset = offset.to_le_bytes();
        self.extra[0] = offset[0];
        self.extra[1] = offset[1];
        self.extra[2] =
            offset[2] & TimestampTz::TOP_OFFSET_BIT | self.extra[2] & !TimestampTz::TOP_OFFSET_BIT;
        self.extra[2] &= !TimestampTz::NEGATIVE_FLAG;
        self.extra[2] |=
            (sign << TimestampTz::NEGATIVE_FLAG.trailing_zeros()) & TimestampTz::NEGATIVE_FLAG;
        self.extra[2] |= TimestampTz::TIMEZONE_FLAG;
    }

    /// Return the desired precision when displaying subseconds.
    #[inline(always)]
    pub fn subsecond_digits(&self) -> u8 {
        (self.extra[2] & TimestampTz::SUBSECOND_DIGITS_BITS)
            >> TimestampTz::SUBSECOND_DIGITS_BITS.trailing_zeros()
    }

    /// Set the desired precision when displaying subseconds.
    #[inline(always)]
    pub fn set_subsecond_digits(&mut self, count: u8) {
        self.extra[2] = ((count << TimestampTz::SUBSECOND_DIGITS_BITS.trailing_zeros())
            & TimestampTz::SUBSECOND_DIGITS_BITS)
            | (self.extra[2] & !TimestampTz::SUBSECOND_DIGITS_BITS);
    }

    /// Construct an invalid timestamp representing the invalid MySQL zero date (0000-00-00).
    pub fn zero() -> Self {
        let mut extra = [0; 3];
        extra[2] |= TimestampTz::ZERO_FLAG;
        Self {
            extra,
            datetime: NaiveDateTime::MIN,
        }
    }

    /// Check for the flag indicating that this is an invalid MySQL zero date (0000-00-00).
    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        self.extra[2] & TimestampTz::ZERO_FLAG != 0
    }

    /// From individual components (as found in [`mysql_common::value::Value::Date`]). Basically a
    /// convenience for constructing zero dates if the date is not valid.
    pub fn from_components(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    ) -> Self {
        if let Some(dt) =
            NaiveDate::from_ymd_opt(year.into(), month.into(), day.into()).and_then(|dt| {
                dt.and_hms_micro_opt(hour.into(), minutes.into(), seconds.into(), micros)
            })
        {
            return dt.into();
        }
        TimestampTz::zero()
    }
}

impl From<&TimestampTz> for DateTime<FixedOffset> {
    fn from(ts: &TimestampTz) -> Self {
        // need to make a local copy of the datetime as it's at an unaligned
        // position in TimestampTz struct, and we need to pass a reference to
        // the FixedOffset functions.
        let dt = ts.datetime;
        FixedOffset::east_opt(ts.get_offset())
            .unwrap()
            .from_utc_datetime(&dt)
    }
}

impl fmt::Debug for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampTz")
            .field("is_zero", &self.is_zero())
            .field("has_negative_offset", &self.has_negative_offset())
            .field("has_timezone", &self.has_timezone())
            .field("has_date_only", &self.has_date_only())
            .field("offset", &self.get_offset())
            .field("subsecond_digits", &self.subsecond_digits())
            .field("ts", &self.to_chrono())
            .finish()
    }
}

impl fmt::Display for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ts = self.to_chrono();

        if self.has_date_only() {
            if self.is_zero() {
                return write!(f, "0000-00-00");
            } else {
                return write!(f, "{}", ts.format(DATE_FORMAT));
            }
        }

        if self.has_timezone() {
            assert!(!self.is_zero(), "Zero date should not have timezone");
            write!(f, "{}", ts.format(TIMESTAMP_TZ_FORMAT))?;
        } else if self.is_zero() {
            write!(f, "0000-00-00 00:00:00")?;
        } else {
            write!(f, "{}", ts.format(TIMESTAMP_FORMAT))?;
        }

        if self.subsecond_digits() > 0 {
            let micros = if self.is_zero() {
                0
            } else {
                ts.time().nanosecond() / 1000
            };
            let micros_str = format!(
                "{1:.0$}",
                self.subsecond_digits() as usize,
                micros as f64 * 0.000001
            );
            write!(f, "{}", &micros_str[1..])?;
        }

        Ok(())
    }
}

impl From<DateTime<FixedOffset>> for TimestampTz {
    fn from(dt: DateTime<FixedOffset>) -> Self {
        let mut ts = TimestampTz {
            datetime: dt.naive_utc(),
            extra: Default::default(),
        };

        ts.set_offset(dt.offset().local_minus_utc());

        ts
    }
}

impl From<NaiveDate> for TimestampTz {
    fn from(dt: NaiveDate) -> Self {
        let mut ts = TimestampTz {
            datetime: dt.and_hms_opt(0, 0, 0).unwrap(),
            extra: Default::default(),
        };

        ts.set_date_only();

        ts
    }
}

impl From<NaiveDateTime> for TimestampTz {
    fn from(dt: NaiveDateTime) -> Self {
        TimestampTz {
            datetime: dt,
            extra: [0u8; 3],
        }
    }
}

impl FromStr for TimestampTz {
    type Err = anyhow::Error;

    /// Attempts to parse a TIMESTAMP a TIMESTAMP WITH TIMEZONE or a DATE
    fn from_str(ts: &str) -> anyhow::Result<TimestampTz> {
        match ts.strip_suffix(" BC") {
            Some(str_without_epoch) => {
                // This is a negative year coming from Postgres. Postgres uses the BC suffix, but
                // chrono only supports non-positive proleptic Gregorian calendar years to
                // represent years before 1. However, we can't just strip the suffix and parse
                // before correcting for this, because BC leap years are all offset by 1 from their
                // "absolute value" AD counterparts, due to there not being a year
                // 0 in BC/AD-notated years; we used to do this but it failed to parse on leap days
                // before the year 1. However, since we know the format used by Postgres, the
                // simplest thing to do is just manually parse off the year and convert it to the
                // corresponding negative year, then reassemble the date string and parse using
                // the normal chrono routines.
                let (year_str, rest) = str_without_epoch
                    .split_once('-')
                    .ok_or(internal_err!("Invalid date format"))?;

                let year = -(year_str.parse::<i32>()? - 1);
                let neg_year_str = format!("{year}-{rest}");

                Self::from_str_impl(&neg_year_str)
            }
            None => Self::from_str_no_bc(ts),
        }
    }
}

impl TimestampTz {
    pub fn to_chrono(&self) -> DateTime<FixedOffset> {
        self.into()
    }

    // MySQL can cast a timestamp into a signed/unsigned integer
    // where the fields up to seconds are decimal digits. i.e.
    // +--------------------------------------------------------------+
    // | CAST(CAST('2004-10-19 10:23:54.15' as DATETIME) AS unsigned) |
    // +--------------------------------------------------------------+
    // |                                               20041019102354 |
    // +--------------------------------------------------------------+
    fn datetime_as_int(&self) -> i64 {
        if self.has_date_only() {
            return self.date_as_int();
        }

        if self.is_zero() {
            return 0;
        }

        let naive = self.to_chrono().naive_local();
        let date = naive.date();
        let time = naive.time();

        let year = date.year() as i64;
        let month = date.month() as i64;
        let day = date.day() as i64;
        let hh = time.hour() as i64;
        let mm = time.minute() as i64;
        let ss = time.second() as i64;

        year * 10_000_000_000 + month * 100_000_000 + day * 1_000_000 + hh * 10_000 + mm * 100 + ss
    }

    // MySQL can cast a timestamp containing a date into a signed/unsigned
    // integer where the fields up to seconds are decimal digits. i.e.
    // +----------------------------------------------------------+
    // | CAST(CAST('2004-10-19 10:23:54.15' as DATE) AS unsigned) |
    // +----------------------------------------------------------+
    // |                                                200410191 |
    // +----------------------------------------------------------+
    // TODO: actually differentiate between date and datetime
    fn date_as_int(&self) -> i64 {
        if self.is_zero() {
            return 0;
        }

        let date = self.to_chrono().naive_local().date();

        let year = date.year() as i64;
        let month = date.month() as i64;
        let day = date.day() as i64;

        year * 10_000 + month * 100 + day
    }

    fn from_str_impl(ts: &str) -> anyhow::Result<TimestampTz> {
        // If there is a dot, there is a microseconds field attached
        Ok(
            if let Ok((naive_date_time, offset_tag)) =
                NaiveDateTime::parse_and_remainder(ts, TIMESTAMP_PARSE_FORMAT)
            {
                if let Some(offset) = parse_timestamp_tag(offset_tag) {
                    offset?
                        .from_local_datetime(&naive_date_time)
                        .single()
                        .ok_or(internal_err!("Invalid date format"))?
                        .into()
                } else {
                    naive_date_time.into()
                }
            } else if let Ok(dt) = NaiveDateTime::parse_from_str(ts, TIMESTAMP_PARSE_FORMAT) {
                dt.into()
            } else {
                // Make TimestampTz object with time portion 00:00:00
                NaiveDate::parse_from_str(ts, DATE_FORMAT)?
                    .and_hms_opt(0, 0, 0)
                    .ok_or(internal_err!("Invalid date format"))?
                    .into()
            },
        )
    }

    fn from_str_no_bc(ts: &str) -> anyhow::Result<TimestampTz> {
        let ts = ts.trim();
        let ts = if ts.starts_with(['-', '+']) {
            Cow::Borrowed(ts)
        } else {
            Cow::Owned(format!("+{ts}"))
        };
        Self::from_str_impl(&ts)
    }

    /// Attempt to coerce this timestamp to a specific [`DfType`].
    pub(crate) fn coerce_to(&self, to_ty: &DfType) -> ReadySetResult<DfValue> {
        match *to_ty {
            DfType::Timestamp { subsecond_digits } => {
                // Conversion into timestamp without tz.
                let mut ts: TimestampTz = if self.is_zero() {
                    Self::zero()
                } else {
                    self.to_chrono().naive_local().into()
                };
                ts.set_subsecond_digits(subsecond_digits as u8);
                Ok(DfValue::TimestampTz(ts))
            }
            DfType::TimestampTz { subsecond_digits } => {
                // TODO: when converting into a timestamp with tz on postgres should apply
                // local tz, but what is local for noria?
                let mut ts_tz = *self;
                ts_tz.set_offset(0);
                ts_tz.set_subsecond_digits(subsecond_digits as u8);
                Ok(DfValue::TimestampTz(ts_tz))
            }
            DfType::DateTime { subsecond_digits } => {
                let mut ts = *self;
                ts.set_subsecond_digits(subsecond_digits as u8);
                Ok(DfValue::TimestampTz(ts))
            }
            DfType::Date => {
                let mut ts = if self.is_zero() {
                    Self::zero()
                } else if self.has_timezone() {
                    let datetime = self.to_chrono();
                    let dd = datetime.timezone().from_utc_datetime(&NaiveDateTime::new(
                        datetime.date_naive(),
                        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ));
                    dd.into()
                } else {
                    self.to_chrono().date_naive().into()
                };
                ts.set_date_only();
                Ok(DfValue::TimestampTz(ts))
            }

            // TODO(ENG-1833): Use `subsecond_digits` value.
            DfType::Time { .. } => Ok(if self.is_zero() {
                NaiveTime::from_hms_opt(0, 0, 0)
                    .ok_or_else(|| ReadySetError::DfValueConversionError {
                        src_type: "DfValue::TimestampTz".to_string(),
                        target_type: format!("{:?}", to_ty),
                        details: "Unexpected error constructing 00:00:00 time".to_string(),
                    })?
                    .into()
            } else {
                self.to_chrono().naive_local().time().into()
            }),

            DfType::BigInt => Ok(DfValue::Int(self.datetime_as_int())),
            DfType::UnsignedBigInt => Ok(DfValue::UnsignedInt(self.datetime_as_int() as _)),

            DfType::Int if self.has_date_only() => Ok(DfValue::Int(self.date_as_int())),
            DfType::UnsignedInt if self.has_date_only() => {
                Ok(DfValue::UnsignedInt(self.date_as_int() as _))
            }

            DfType::Int
            | DfType::UnsignedInt
            | DfType::MediumInt
            | DfType::UnsignedMediumInt
            | DfType::SmallInt
            | DfType::UnsignedSmallInt
            | DfType::TinyInt
            | DfType::UnsignedTinyInt => Err(ReadySetError::DfValueConversionError {
                src_type: "DfValue::TimestampTz".to_string(),
                target_type: format!("{:?}", to_ty),
                details: "Out of range".to_string(),
            }),

            DfType::Double => Ok(DfValue::Double(self.datetime_as_int() as _)),
            DfType::Float => Ok(DfValue::Float(self.datetime_as_int() as _)),

            DfType::Numeric { .. } => Ok(DfValue::Numeric(std::sync::Arc::new(
                self.datetime_as_int().into(),
            ))),

            DfType::Bool => Ok(DfValue::from(
                // TODO(mvzink): This will never work as chrono does not support zero dates.
                self.to_chrono().naive_local()
                    != NaiveDate::from_ymd_opt(0, 0, 0)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
            )),

            DfType::Text(collation) => Ok(DfValue::from_str_and_collation(
                &self.to_string(),
                collation,
            )),
            DfType::Char(l, ..) | DfType::VarChar(l, ..) => {
                let mut string = self.to_string();
                string.truncate(l as usize);
                Ok(string.into())
            }

            DfType::Blob => Ok(DfValue::ByteArray(std::sync::Arc::new(
                self.to_string().as_bytes().into(),
            ))),

            DfType::Binary(l) | DfType::VarBinary(l) => {
                let mut string = self.to_string();
                string.truncate(l as usize);
                Ok(DfValue::ByteArray(std::sync::Arc::new(
                    string.as_bytes().into(),
                )))
            }

            DfType::Json => {
                let mut ts = *self;
                ts.set_subsecond_digits(6); // Set max precision before json conversion
                Ok(DfValue::from(format!("\"{}\"", ts).as_str()))
            }

            DfType::Unknown
            | DfType::Enum { .. }
            | DfType::Jsonb
            | DfType::MacAddr
            | DfType::Inet
            | DfType::Uuid
            | DfType::Bit(_)
            | DfType::VarBit(_)
            | DfType::Array(_) => Err(ReadySetError::DfValueConversionError {
                src_type: "DfValue::TimestampTz".to_string(),
                target_type: format!("{:?}", to_ty),
                details: "Not allowed".to_string(),
            }),
        }
    }
}

impl PartialEq for TimestampTz {
    fn eq(&self, other: &Self) -> bool {
        (self.is_zero() && other.is_zero()) || self.to_chrono() == other.to_chrono()
    }
}

impl Eq for TimestampTz {}

impl PartialOrd for TimestampTz {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.to_chrono().cmp(&other.to_chrono()))
    }
}

impl Ord for TimestampTz {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_chrono().cmp(&other.to_chrono())
    }
}

impl Hash for TimestampTz {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.is_zero().hash(state);
        self.to_chrono().hash(state)
    }
}

impl Arbitrary for TimestampTz {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<TimestampTz>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use proptest::prop_oneof;
        use proptest::strategy::Strategy;
        use readyset_util::arbitrary::{
            arbitrary_date, arbitrary_date_time, arbitrary_naive_date_time,
            arbitrary_timestamp_naive_date_time,
        };

        prop_oneof![
            arbitrary_naive_date_time().prop_map(|n| n.into()),
            arbitrary_date().prop_map(|n| n.into()),
            arbitrary_date_time().prop_map(|n| n.into()),
            arbitrary_timestamp_naive_date_time().prop_map(|n| n.into()),
        ]
        .boxed()
    }
}

pub fn parse_timestamp_tag(tag: &str) -> Option<ReadySetResult<FixedOffset>> {
    let tag = tag.trim();
    if tag.is_empty() {
        None
    } else {
        let err = || ReadySetError::Internal("Failed to parse timestamptz offset".into());

        // An offset tag should be at least 3 characters long
        if tag.len() < 3 {
            return Some(Err(err()));
        }

        let is_positive = match tag.chars().next() {
            Some('+') => true,
            Some('-') => false,
            _ => return Some(Err(err())),
        };

        let mut offset = &tag[1..3].parse::<i32>().unwrap() * 60 * 60;

        if tag.len() >= 6 {
            offset += &tag[4..6].parse::<i32>().unwrap_or_default() * 60;

            if tag.len() == 9 {
                offset += &tag[7..9].parse::<i32>().unwrap_or_default();
            }
        }

        if !is_positive {
            offset = -offset;
        }

        Some(FixedOffset::east_opt(offset).ok_or_else(err))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveTime, TimeZone};

    use super::*;
    use crate::{Collation, DfType};

    #[test]
    fn timestamp_coercion() {
        let ts = DfValue::from(
            chrono::NaiveDate::from_ymd_opt(2022, 2, 9)
                .unwrap()
                .and_hms_milli_opt(13, 14, 15, 169)
                .unwrap(),
        );

        assert_eq!(
            ts.coerce_to(&DfType::BigInt, &DfType::Unknown).unwrap(),
            DfValue::from(20220209131415i64)
        );

        assert_eq!(
            ts.coerce_to(&DfType::Date, &DfType::Unknown)
                .unwrap()
                .coerce_to(&DfType::BigInt, &DfType::Unknown)
                .unwrap(),
            DfValue::from(20220209i64)
        );

        assert_eq!(
            ts.coerce_to(&DfType::Double, &DfType::Unknown).unwrap(),
            DfValue::Double(20220209131415.0f64)
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                    .unwrap()
            ),
            "2022-02-09 13:14:15"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(&DfType::VarChar(6, Collation::default()), &DfType::Unknown)
                    .unwrap()
            ),
            "2022-0"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(
                    &DfType::DateTime {
                        subsecond_digits: 6
                    },
                    &DfType::Unknown
                )
                .unwrap()
                .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                .unwrap()
            ),
            "2022-02-09 13:14:15.169000"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(
                    &DfType::DateTime {
                        subsecond_digits: 2
                    },
                    &DfType::Unknown
                )
                .unwrap()
                .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                .unwrap()
            ),
            "2022-02-09 13:14:15.17"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(
                    &DfType::DateTime {
                        subsecond_digits: 1
                    },
                    &DfType::Unknown
                )
                .unwrap()
                .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                .unwrap()
            ),
            "2022-02-09 13:14:15.2"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(&DfType::Date, &DfType::Unknown)
                    .unwrap()
                    .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                    .unwrap()
            ),
            "2022-02-09"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(
                    &DfType::DateTime {
                        subsecond_digits: 1
                    },
                    &DfType::Unknown
                )
                .unwrap()
                .coerce_to(&DfType::Json, &DfType::Unknown)
                .unwrap()
            ),
            "\"2022-02-09 13:14:15.169000\""
        );
    }

    #[test]
    fn timestamp_from_str() {
        assert_eq!(
            TimestampTz::from_str("1000-01-01 00:00:00.000000")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd_opt(1000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("9999-12-31 23:59:59.999999")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
                .unwrap()
                .and_hms_micro_opt(23, 59, 59, 999999)
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("9999-12-31 23:59:59.99")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
                .unwrap()
                .and_hms_micro_opt(23, 59, 59, 990000)
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("2012-02-09 12:12:12")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd_opt(2012, 2, 9)
                .unwrap()
                .and_hms_opt(12, 12, 12)
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54+02")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(2 * 60 * 60)
                .unwrap()
                .with_ymd_and_hms(2004, 10, 19, 10, 23, 54)
                .single()
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54.1234+02")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(2 * 60 * 60)
                .unwrap()
                .from_local_datetime(&NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2004, 10, 19).unwrap(),
                    NaiveTime::from_hms_micro_opt(10, 23, 54, 123400).unwrap(),
                ))
                .single()
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54+02 BC")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(2 * 60 * 60)
                .unwrap()
                .with_ymd_and_hms(-2003, 10, 19, 10, 23, 54)
                .single()
                .unwrap()
        );

        assert_eq!(
            TimestampTz::from_str("10000-10-19 10:23:54+02")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(2 * 60 * 60)
                .unwrap()
                .with_ymd_and_hms(10000, 10, 19, 10, 23, 54)
                .single()
                .unwrap()
        );

        #[allow(clippy::zero_prefixed_literal)]
        let year_5_bce = -0004;
        // BC date with time zone
        assert_eq!(
            TimestampTz::from_str("0005-02-29 12:34:56+00 BC")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(year_5_bce, 2, 29, 12, 34, 56)
                .single()
                .unwrap()
        );
        // BC date without time zone
        assert_eq!(
            TimestampTz::from_str("0005-02-21 12:34:56 BC")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(year_5_bce, 2, 21, 12, 34, 56)
                .single()
                .unwrap()
        );
    }
}
