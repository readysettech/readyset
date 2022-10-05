use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use chrono::{Date, DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike};
use proptest::arbitrary::Arbitrary;
use readyset_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::{DfType, DfValue};

/// The format for timestamps when parsed as text
pub const TIMESTAMP_PARSE_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// The format for timestamps when presented as text
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The format for timestamps with time zone when parsed as text
pub const TIMESTAMP_TZ_PARSE_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f%#z";

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
/// of extra data. Since 3 bytes allow us to store 24 bytes, this is how we use them:
///
/// 17 bits for the timezone offset (0 to 86_400)
/// 1 bit to signify negative offset
/// 3 bits for the subsecond digit count required
/// 1 bit to signify this is DATE only
/// 1 bit to signify timezone offset is present (since 0 is a valid offset)
/// 1 bit unused - available for future use
#[derive(Clone, Copy, Serialize, Deserialize)]
#[repr(C, packed)] // So we can actually fit into the 15 bytes
pub struct TimestampTz {
    // extra is 3 bytes as described above
    pub(crate) extra: [u8; 3],
    // datetime is 12 bytes for a total of 15 bytes
    pub(crate) datetime: NaiveDateTime,
}

impl TimestampTz {
    const TIMEZONE_FLAG: u8 = 0b_0100_0000;
    const DATE_FLAG: u8 = 0b_0010_0000;
    const SUBSECOND_DIGITS_BITS: u8 = 0b_0001_1100;
    const NEGATIVE_FLAG: u8 = 0b_0000_0010;
    const TOP_OFFSET_BIT: u8 = 0b_0000_0001;

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
    fn has_date_only(&self) -> bool {
        self.extra[2] & TimestampTz::DATE_FLAG != 0
    }

    /// Mark this timestamp as only containing a date value
    #[inline(always)]
    fn set_date_only(&mut self) {
        self.extra[2] |= TimestampTz::DATE_FLAG
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
            sign << TimestampTz::NEGATIVE_FLAG.trailing_zeros() & TimestampTz::NEGATIVE_FLAG;
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
    fn set_subsecond_digits(&mut self, count: u8) {
        self.extra[2] = ((count << TimestampTz::SUBSECOND_DIGITS_BITS.trailing_zeros())
            & TimestampTz::SUBSECOND_DIGITS_BITS)
            | (self.extra[2] & !TimestampTz::SUBSECOND_DIGITS_BITS);
    }
}

impl From<&TimestampTz> for DateTime<FixedOffset> {
    fn from(ts: &TimestampTz) -> Self {
        DateTime::from_utc(ts.datetime, FixedOffset::east(ts.get_offset()))
    }
}

impl fmt::Debug for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_chrono().fmt(f)
    }
}

impl fmt::Display for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ts = self.to_chrono();

        if self.has_date_only() {
            return write!(f, "{}", ts.format(DATE_FORMAT));
        }

        if self.has_timezone() {
            write!(f, "{}", ts.format(TIMESTAMP_TZ_FORMAT))?;
        } else {
            write!(f, "{}", ts.format(TIMESTAMP_FORMAT))?;
        }

        if self.subsecond_digits() > 0 {
            let micros = ts.time().nanosecond() / 1000;
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

impl From<Date<FixedOffset>> for TimestampTz {
    fn from(dt: Date<FixedOffset>) -> Self {
        let mut ts = TimestampTz {
            datetime: dt.and_hms(0, 0, 0).naive_utc(),
            extra: Default::default(),
        };

        ts.set_offset(dt.offset().local_minus_utc());
        ts.set_date_only();

        ts
    }
}

impl From<NaiveDate> for TimestampTz {
    fn from(dt: NaiveDate) -> Self {
        let mut ts = TimestampTz {
            datetime: dt.and_hms(0, 0, 0),
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
                // This is a negative year coming from Postgres
                if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(
                    str_without_epoch,
                    TIMESTAMP_TZ_PARSE_FORMAT,
                ) {
                    let year = dt.year();
                    Ok(dt.with_year(-year + 1).unwrap_or(dt).into())
                } else {
                    let d = NaiveDate::parse_from_str(str_without_epoch, DATE_FORMAT)?;
                    let year = d.year();
                    Ok(d.with_year(-year + 1).unwrap_or(d).into())
                }
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
    #[allow(dead_code)]
    fn date_as_int(&self) -> i64 {
        let date = self.to_chrono().naive_local().date();

        let year = date.year() as i64;
        let month = date.month() as i64;
        let day = date.day() as i64;

        year * 10_000 + month * 100 + day
    }

    fn from_str_no_bc(ts: &str) -> anyhow::Result<TimestampTz> {
        let ts = ts.trim();
        // If there is a dot, there is a microseconds field attached
        Ok(
            if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(ts, TIMESTAMP_TZ_PARSE_FORMAT) {
                dt.into()
            } else if let Ok(dt) = NaiveDateTime::parse_from_str(ts, TIMESTAMP_PARSE_FORMAT) {
                dt.into()
            } else {
                NaiveDate::parse_from_str(ts, DATE_FORMAT)?.into()
            },
        )
    }

    /// Attempt to coerce this timestamp to a specific [`DfType`].
    pub(crate) fn coerce_to(&self, to_ty: &DfType) -> ReadySetResult<DfValue> {
        match *to_ty {
            DfType::Timestamp { subsecond_digits } => {
                // Conversion into timestamp without tz.
                let mut ts: TimestampTz = self.to_chrono().naive_local().into();
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
            DfType::Date => Ok(DfValue::TimestampTz(self.to_chrono().date().into())),
            // TODO(ENG-1833): Use `subsecond_digits` value.
            DfType::Time { .. } => Ok(self.to_chrono().naive_local().time().into()),

            DfType::BigInt => Ok(DfValue::Int(self.datetime_as_int())),
            DfType::UnsignedBigInt => Ok(DfValue::UnsignedInt(self.datetime_as_int() as _)),

            DfType::Int if self.has_date_only() => Ok(DfValue::Int(self.date_as_int())),
            DfType::UnsignedInt if self.has_date_only() => {
                Ok(DfValue::UnsignedInt(self.date_as_int() as _))
            }

            DfType::Int
            | DfType::UnsignedInt
            | DfType::SmallInt
            | DfType::UnsignedSmallInt
            | DfType::TinyInt
            | DfType::UnsignedTinyInt => Err(ReadySetError::DfValueConversionError {
                src_type: "DfValue::TimestampTz".to_string(),
                target_type: format!("{:?}", to_ty),
                details: "Out of range".to_string(),
            }),

            DfType::Double => Ok(DfValue::Double(self.datetime_as_int() as _)),
            DfType::Float(_) => Ok(DfValue::Float(self.datetime_as_int() as _)),

            DfType::Numeric { .. } => Ok(DfValue::Numeric(std::sync::Arc::new(
                self.datetime_as_int().into(),
            ))),

            DfType::Bool => Ok(DfValue::from(
                self.to_chrono().naive_local() != NaiveDate::from_ymd(0, 0, 0).and_hms(0, 0, 0),
            )),

            DfType::Text { .. } => Ok(self.to_string().into()),
            DfType::Char(l, ..) | DfType::VarChar(l, ..) => {
                let mut string = self.to_string();
                string.truncate(l as usize);
                Ok(string.into())
            }

            DfType::Blob(_) => Ok(DfValue::ByteArray(std::sync::Arc::new(
                self.to_string().as_bytes().into(),
            ))),

            DfType::Binary(l) | DfType::VarBinary(l) => {
                let mut string = self.to_string();
                string.truncate(l as usize);
                Ok(DfValue::ByteArray(std::sync::Arc::new(
                    string.as_bytes().into(),
                )))
            }

            DfType::Json(_) => {
                let mut ts = *self;
                ts.set_subsecond_digits(6); // Set max precision before json conversion
                Ok(DfValue::from(format!("\"{}\"", ts).as_str()))
            }

            DfType::Unknown
            | DfType::Enum(_, _)
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
        self.to_chrono() == other.to_chrono()
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
        self.to_chrono().hash(state)
    }
}

impl Arbitrary for TimestampTz {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<TimestampTz>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use launchpad::arbitrary::{
            arbitrary_date, arbitrary_date_time, arbitrary_naive_date_time,
            arbitrary_timestamp_naive_date_time,
        };
        use proptest::prop_oneof;
        use proptest::strategy::Strategy;

        prop_oneof![
            arbitrary_naive_date_time().prop_map(|n| n.into()),
            arbitrary_date().prop_map(|n| n.into()),
            arbitrary_date_time().prop_map(|n| n.into()),
            arbitrary_timestamp_naive_date_time().prop_map(|n| n.into()),
        ]
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use nom_sql::Dialect;

    use super::*;
    use crate::{Collation, DfType};

    #[test]
    fn timestamp_coercion() {
        let ts =
            DfValue::from(chrono::NaiveDate::from_ymd(2022, 2, 9).and_hms_milli(13, 14, 15, 169));

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
                ts.coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
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
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
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
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
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
                .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
                .unwrap()
            ),
            "2022-02-09 13:14:15.2"
        );

        assert_eq!(
            &format!(
                "{}",
                ts.coerce_to(&DfType::Date, &DfType::Unknown)
                    .unwrap()
                    .coerce_to(&DfType::Text(Collation::default()), &DfType::Unknown)
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
                .coerce_to(&DfType::Json(Dialect::MySQL), &DfType::Unknown)
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
            chrono::NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0)
        );

        assert_eq!(
            TimestampTz::from_str("9999-12-31 23:59:59.999999")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd(9999, 12, 31).and_hms_micro(23, 59, 59, 999999)
        );

        assert_eq!(
            TimestampTz::from_str("9999-12-31 23:59:59.99")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd(9999, 12, 31).and_hms_micro(23, 59, 59, 990000)
        );

        assert_eq!(
            TimestampTz::from_str("2012-02-09 12:12:12")
                .unwrap()
                .to_chrono()
                .naive_local(),
            chrono::NaiveDate::from_ymd(2012, 2, 9).and_hms(12, 12, 12)
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54+02")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east(2 * 60 * 60)
                .ymd(2004, 10, 19)
                .and_hms(10, 23, 54)
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54.1234+02")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east(2 * 60 * 60)
                .ymd(2004, 10, 19)
                .and_hms_micro(10, 23, 54, 123400)
        );

        assert_eq!(
            TimestampTz::from_str("2004-10-19 10:23:54+02 BC")
                .unwrap()
                .to_chrono(),
            chrono::FixedOffset::east(2 * 60 * 60)
                .ymd(-2003, 10, 19)
                .and_hms(10, 23, 54)
        );
    }
}
