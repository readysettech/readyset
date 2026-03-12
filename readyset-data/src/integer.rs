use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use bit_vec::BitVec;
use readyset_decimal::Decimal;
use readyset_errors::{ReadySetError, ReadySetResult};

use crate::{r#enum, DfType, DfValue};

/// A convenience trait that implements casts of i64 and u64 to f32 and f64
pub(crate) trait IntAsFloat {
    fn to_f64(self) -> f64;
    fn to_f32(self) -> f32;
}

impl IntAsFloat for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

impl IntAsFloat for u64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

impl IntAsFloat for u32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn to_f32(self) -> f32 {
        self as f32
    }
}

/// Coerce an integer value according to MySQL rules to the best of our abilities.
///
/// Note that this only handles converting *integer types*, not other types with values represented
/// as integers (for example, enums). The conversion logic for other such types is implemented
/// elsewhere.
pub(crate) fn coerce_integer<I>(val: I, to_ty: &DfType, from_ty: &DfType) -> ReadySetResult<DfValue>
where
    i8: TryFrom<I>,
    i16: TryFrom<I>,
    i32: TryFrom<I>,
    i64: TryFrom<I>,
    u8: TryFrom<I>,
    u16: TryFrom<I>,
    u32: TryFrom<I>,
    u64: TryFrom<I>,
    Decimal: From<I>,
    usize: TryFrom<I>,
    I: std::ops::BitXor<I, Output = I> + IntAsFloat + std::cmp::Eq + Copy + fmt::Display,
{
    let err = || ReadySetError::DfValueConversionError {
        src_type: from_ty.to_string(),
        target_type: to_ty.to_string(),
        details: "out of bounds".to_string(),
    };

    match *to_ty {
        #[allow(clippy::eq_op)]
        DfType::Bool => Ok(DfValue::from(val ^ val != val)),

        DfType::TinyInt => i8::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedTinyInt => u8::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::SmallInt => i16::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedSmallInt => u16::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::MediumInt => i32::try_from(val)
            .ok()
            .filter(|i| ((-1 << 23)..(1 << 23)).contains(i))
            .ok_or_else(err)
            .map(DfValue::from),
        DfType::UnsignedMediumInt => u32::try_from(val)
            .ok()
            .filter(|&i| i < (1 << 24))
            .ok_or_else(err)
            .map(DfValue::from),
        DfType::Int => i32::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedInt => u32::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::BigInt => i64::try_from(val).map_err(|_| err()).map(DfValue::from),
        DfType::UnsignedBigInt => u64::try_from(val).map_err(|_| err()).map(DfValue::from),

        DfType::Float => Ok(DfValue::Float(val.to_f32())),
        DfType::Double => Ok(DfValue::Double(val.to_f64())),

        DfType::Text(collation) => Ok(DfValue::from_str_and_collation(&val.to_string(), collation)),

        DfType::VarChar(l, ..) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            Ok(val.to_string().into())
        }

        DfType::Char(l, ..) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            val.extend(std::iter::repeat_n(
                ' ',
                (l as usize).saturating_sub(val.len()),
            ));
            Ok(val.into())
        }

        DfType::Blob => Ok(val.to_string().into_bytes().into()),

        DfType::VarBinary(l) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            Ok(val.into_bytes().into())
        }

        DfType::Binary(l) => {
            let mut val = val.to_string();
            val.truncate(l as usize);
            val.extend(std::iter::repeat_n(
                ' ',
                (l as usize).saturating_sub(val.len()),
            ));
            Ok(val.into_bytes().into())
        }

        DfType::Json | DfType::Jsonb => Ok(format!("\"{val}\"").into()),

        DfType::Date => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in either YYYYMMDD or YYMMDD format, provided that the number makes sense
            // as a date. For example, 19830905 and 830905 are interpreted as '1983-09-05'.
            let val = u64::try_from(val).map_err(|_| err())?;
            let year = match val / 1_00_00 {
                y @ 00..=69 => y + 2000, // Year values in the range 00-69 become 2000-2069.
                y @ 70..=99 => y + 1900, // Year values in the range 70-99 become 1970-1999.
                year => year,
            };

            let month = val / 1_00 % 1_00;
            let day = val % 1_00;
            Ok(
                chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _)
                    .ok_or_else(err)?
                    .into(),
            )
        }

        DfType::Timestamp { .. } | DfType::TimestampTz { .. } | DfType::DateTime { .. } => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in either YYYYMMDDhhmmss or YYMMDDhhmmss format, provided that the number
            // makes sense as a date. For example, 19830905132800 and 830905132800 are interpreted
            // as '1983-09-05 13:28:00'.
            let val = u64::try_from(val).map_err(|_| err())?;
            let year = match val / 1_00_00_00_00_00 {
                y @ 00..=69 => y + 2000, // Year values in the range 00-69 become 2000-2069.
                y @ 70..=99 => y + 1900, // Year values in the range 70-99 become 1970-1999.
                year => year,
            };
            let month = val / 1_00_00_00_00 % 1_00;
            let day = val / 1_00_00_00 % 1_00;
            let hh = val / 1_00_00 % 1_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(
                chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _)
                    .and_then(|d| d.and_hms_micro_opt(hh as _, mm as _, ss as _, 0))
                    .ok_or_else(err)?
                    .into(),
            )
        }

        DfType::Time { .. } => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in hhmmss format, provided that it makes sense as a time. For example,
            // 101112 is understood as '10:11:12'. The following alternative formats are also
            // understood: ss, mmss, or hhmmss.
            // MySQL supports negative times (e.g. -101112 → -10:11:12).
            let ival = i64::try_from(val).map_err(|_| err())?;
            let positive = ival >= 0;
            let val = ival.unsigned_abs();
            let hh = val / 1_00_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            if mm >= 60 || ss >= 60 {
                return Err(err());
            }
            Ok(mysql_time::MySqlTime::from_hmsus(positive, hh as _, mm as _, ss as _, 0).into())
        }

        DfType::Numeric { .. } => Ok(DfValue::Numeric(Arc::new(val.into()))),

        DfType::Enum { ref variants, .. } => {
            // Values above the number of variants are converted to 0 by MySQL, and anything that
            // can't be held in a usize is certainly too high, hence the .unwrap_or(0)
            let idx = usize::try_from(val).unwrap_or(0);
            Ok(DfValue::from(r#enum::apply_enum_limits(idx, variants)))
        }

        DfType::Bit(/* TODO */ _len) => u64::try_from(val)
            .map_err(|_| err())
            .map(|v| DfValue::BitVector(Arc::new(BitVec::from_bytes(&v.to_be_bytes())))),

        DfType::Unknown
        | DfType::MacAddr
        | DfType::Inet
        | DfType::Uuid
        | DfType::VarBit(_)
        | DfType::Array(_)
        | DfType::Row
        | DfType::Point
        | DfType::PostgisPoint
        | DfType::PostgisPolygon
        | DfType::Tsvector => Err(ReadySetError::DfValueConversionError {
            src_type: from_ty.to_string(),
            target_type: to_ty.to_string(),
            details: "Not allowed".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_to_mediumint_bounds() {
        // MediumInt range: -8388608 (-1 << 23) to 8388607 ((1 << 23) - 1)
        assert_eq!(
            coerce_integer(-8388608i64, &DfType::MediumInt, &DfType::Unknown).unwrap(),
            DfValue::Int(-8388608)
        );
        assert_eq!(
            coerce_integer(8388607i64, &DfType::MediumInt, &DfType::Unknown).unwrap(),
            DfValue::Int(8388607)
        );
        coerce_integer(-8388609i64, &DfType::MediumInt, &DfType::Unknown).unwrap_err();
        coerce_integer(8388608i64, &DfType::MediumInt, &DfType::Unknown).unwrap_err();

        // UnsignedMediumInt range: 0 to 16777215 ((1 << 24) - 1)
        assert_eq!(
            coerce_integer(0u64, &DfType::UnsignedMediumInt, &DfType::Unknown).unwrap(),
            DfValue::UnsignedInt(0)
        );
        assert_eq!(
            coerce_integer(16777215u64, &DfType::UnsignedMediumInt, &DfType::Unknown).unwrap(),
            DfValue::UnsignedInt(16777215)
        );
        coerce_integer(16777216u64, &DfType::UnsignedMediumInt, &DfType::Unknown).unwrap_err();
    }

    #[test]
    fn integer_to_date_two_digit_year() {
        // MySQL: 00-69 → 2000-2069, 70-99 → 1970-1999
        // YYMMDD format
        let val = coerce_integer(250115i64, &DfType::Date, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(chrono::NaiveDate::from_ymd_opt(2025, 1, 15).unwrap())
        );

        let val = coerce_integer(700101i64, &DfType::Date, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        );

        let val = coerce_integer(690101i64, &DfType::Date, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(chrono::NaiveDate::from_ymd_opt(2069, 1, 1).unwrap())
        );

        // Full YYYYMMDD bypasses two-digit year logic
        let val = coerce_integer(19830905i64, &DfType::Date, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(chrono::NaiveDate::from_ymd_opt(1983, 9, 5).unwrap())
        );
    }

    #[test]
    fn integer_to_datetime_two_digit_year() {
        // YYMMDDhhmmss format
        let subsecond_digits = 0;
        let dt_type = DfType::DateTime { subsecond_digits };

        let val = coerce_integer(250115101112i64, &dt_type, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(
                chrono::NaiveDate::from_ymd_opt(2025, 1, 15)
                    .unwrap()
                    .and_hms_opt(10, 11, 12)
                    .unwrap()
            )
        );

        let val = coerce_integer(690101120000i64, &dt_type, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(
                chrono::NaiveDate::from_ymd_opt(2069, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap()
            )
        );

        let val = coerce_integer(700101000000i64, &dt_type, &DfType::Unknown).unwrap();
        assert_eq!(
            val,
            DfValue::from(
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            )
        );
    }

    #[test]
    fn integer_to_time() {
        // hhmmss format
        let time_type = DfType::Time {
            subsecond_digits: 0,
        };

        assert_eq!(
            coerce_integer(101112i64, &time_type, &DfType::Unknown).unwrap(),
            DfValue::from(mysql_time::MySqlTime::from_hmsus(true, 10, 11, 12, 0))
        );

        // Short forms: ss and mmss
        assert_eq!(
            coerce_integer(30i64, &time_type, &DfType::Unknown).unwrap(),
            DfValue::from(mysql_time::MySqlTime::from_hmsus(true, 0, 0, 30, 0))
        );
        assert_eq!(
            coerce_integer(1130i64, &time_type, &DfType::Unknown).unwrap(),
            DfValue::from(mysql_time::MySqlTime::from_hmsus(true, 0, 11, 30, 0))
        );

        // Negative integers produce negative times (MySQL supports these)
        assert_eq!(
            coerce_integer(-1i64, &time_type, &DfType::Unknown).unwrap(),
            DfValue::from(mysql_time::MySqlTime::from_hmsus(false, 0, 0, 1, 0))
        );
    }

    #[test]
    fn integer_to_date_negative_rejected() {
        coerce_integer(-1i64, &DfType::Date, &DfType::Unknown).unwrap_err();
    }
}
