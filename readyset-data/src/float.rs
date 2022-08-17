use std::convert::TryInto;

use nom_sql::SqlType;
use readyset_errors::{ReadySetError, ReadySetResult};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::DfValue;

fn coerce_f64_to_int<I>(val: f64) -> Option<I>
where
    i64: TryInto<I>,
{
    if -0.5 < val && val < 0.5 {
        return 0.try_into().ok();
    }

    if val <= i64::MIN as f64 - 0.5 || val >= i64::MAX as f64 + 0.5 {
        return None;
    }

    (val.round() as i64).try_into().ok()
}

fn coerce_f64_to_uint<I>(val: f64) -> Option<I>
where
    u64: TryInto<I>,
{
    if -0.5 < val && val < 0.5 {
        return 0.try_into().ok();
    }

    if val <= -0.5 || val >= u64::MAX as f64 + 0.5 {
        return None;
    }

    (val.round() as u64).try_into().ok()
}

pub(crate) fn coerce_f64(val: f64, sql_type: &SqlType) -> ReadySetResult<DfValue> {
    let err = |deets: &str| ReadySetError::DfValueConversionError {
        src_type: "Double".to_string(),
        target_type: sql_type.to_string(),
        details: deets.to_string(),
    };

    let bounds_err = || err("out of bounds");

    if val.is_infinite() {
        return Err(err("±inf not allowed"));
    }

    if val.is_nan() {
        return Err(err("Nan not allowed"));
    }

    match sql_type {
        SqlType::Bool => Ok(DfValue::from(val != 0.0)),

        SqlType::Float | SqlType::Real => {
            let val = val as f32;
            if val.is_infinite() {
                return Err(bounds_err());
            }
            Ok(DfValue::Float(val))
        }
        SqlType::Double => Ok(DfValue::Double(val)),

        SqlType::Numeric(_) | SqlType::Decimal(_, _) => Decimal::from_f64_retain(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),

        SqlType::TinyInt(_) => coerce_f64_to_int::<i8>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::SmallInt(_) => coerce_f64_to_int::<i16>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::Int(_) | SqlType::Serial => coerce_f64_to_int::<i32>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::BigInt(_) | SqlType::BigSerial => coerce_f64_to_int::<i64>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),

        SqlType::UnsignedTinyInt(_) => coerce_f64_to_uint::<u8>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::UnsignedSmallInt(_) => coerce_f64_to_uint::<u16>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::UnsignedInt(_) => coerce_f64_to_uint::<u32>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),
        SqlType::UnsignedBigInt(_) => coerce_f64_to_uint::<u64>(val)
            .ok_or_else(bounds_err)
            .map(DfValue::from),

        SqlType::TinyText
        | SqlType::MediumText
        | SqlType::Text
        | SqlType::LongText
        | SqlType::Char(None)
        | SqlType::VarChar(None) => Ok(val.to_string().into()),

        SqlType::VarChar(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            Ok(val.to_string().into())
        }

        SqlType::Char(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            val.extend(std::iter::repeat(' ').take((*l as usize).saturating_sub(val.len())));
            Ok(val.into())
        }

        SqlType::TinyBlob
        | SqlType::MediumBlob
        | SqlType::Blob
        | SqlType::LongBlob
        | SqlType::ByteArray
        | SqlType::Binary(None) => Ok(DfValue::ByteArray(val.to_string().into_bytes().into())),

        SqlType::VarBinary(l) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            Ok(val.to_string().into_bytes().into())
        }

        SqlType::Binary(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            val.extend(std::iter::repeat(' ').take((*l as usize).saturating_sub(val.len())));
            Ok(val.into_bytes().into())
        }

        SqlType::Json | SqlType::Jsonb => Ok(val.to_string().into()),

        SqlType::Time => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in hhmmss format, provided that it makes sense as a time. For
            // example, 101112 is understood as '10:11:12'. The following
            // alternative formats are also understood: ss, mmss, or hhmmss.
            let ms = (val.fract() * 1000000.0).round() as u64;
            let val = val.trunc() as u64;
            let hh = val / 1_00_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(mysql_time::MysqlTime::from_hmsus(true, hh as _, mm as _, ss as _, ms).into())
        }

        SqlType::Date | SqlType::DateTime(_) | SqlType::Timestamp | SqlType::TimestampTz => {
            crate::integer::coerce_integer(
                coerce_f64_to_int::<i64>(val).ok_or_else(bounds_err)?,
                "Double",
                sql_type,
            )
        }

        SqlType::Enum(_)
        | SqlType::MacAddr
        | SqlType::Inet
        | SqlType::Uuid
        | SqlType::Bit(_)
        | SqlType::VarBit(_)
        | SqlType::Array(_) => Err(err("not allowed")),
    }
}

pub(crate) fn coerce_decimal(val: &Decimal, sql_type: &SqlType) -> ReadySetResult<DfValue> {
    let err = || ReadySetError::DfValueConversionError {
        src_type: "Decimal".to_string(),
        target_type: sql_type.to_string(),
        details: "out of bounds".to_string(),
    };

    match sql_type {
        SqlType::Bool => Ok(DfValue::from(!val.is_zero())),

        SqlType::Float | SqlType::Real => val
            .to_f32()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DfValue::Float),
        SqlType::Double => val
            .to_f64()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DfValue::Double),

        SqlType::Decimal(_, _) | SqlType::Numeric(_) => Ok(DfValue::from(*val)),

        SqlType::TinyInt(_) => val.to_i8().ok_or_else(err).map(DfValue::from),
        SqlType::SmallInt(_) => val.to_i16().ok_or_else(err).map(DfValue::from),
        SqlType::Int(_) | SqlType::Serial => val.to_i32().ok_or_else(err).map(DfValue::from),
        SqlType::BigInt(_) | SqlType::BigSerial => val.to_i64().ok_or_else(err).map(DfValue::from),

        SqlType::UnsignedTinyInt(_) => val.to_u8().ok_or_else(err).map(DfValue::from),
        SqlType::UnsignedSmallInt(_) => val.to_u16().ok_or_else(err).map(DfValue::from),
        SqlType::UnsignedInt(_) => val.to_u32().ok_or_else(err).map(DfValue::from),
        SqlType::UnsignedBigInt(_) => val.to_u64().ok_or_else(err).map(DfValue::from),

        SqlType::TinyText
        | SqlType::MediumText
        | SqlType::Text
        | SqlType::LongText
        | SqlType::Char(None)
        | SqlType::VarChar(None) => Ok(val.to_string().into()),

        SqlType::VarChar(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            Ok(val.to_string().into())
        }

        SqlType::Char(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            val.extend(std::iter::repeat(' ').take((*l as usize).saturating_sub(val.len())));
            Ok(val.into())
        }

        SqlType::TinyBlob
        | SqlType::MediumBlob
        | SqlType::Blob
        | SqlType::LongBlob
        | SqlType::ByteArray
        | SqlType::Binary(None) => Ok(DfValue::ByteArray(val.to_string().into_bytes().into())),

        SqlType::VarBinary(l) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            Ok(val.to_string().into_bytes().into())
        }

        SqlType::Binary(Some(l)) => {
            let mut val = val.to_string();
            val.truncate(*l as usize);
            val.extend(std::iter::repeat(' ').take((*l as usize).saturating_sub(val.len())));
            Ok(val.into_bytes().into())
        }

        SqlType::Json | SqlType::Jsonb => Ok(val.to_string().into()),

        SqlType::Date
        | SqlType::DateTime(_)
        | SqlType::Time
        | SqlType::Timestamp
        | SqlType::TimestampTz => {
            crate::integer::coerce_integer(val.to_i64().ok_or_else(err)?, "Decimal", sql_type)
        }

        SqlType::Enum(_)
        | SqlType::MacAddr
        | SqlType::Inet
        | SqlType::Uuid
        | SqlType::Bit(_)
        | SqlType::VarBit(_)
        | SqlType::Array(_) => Err(ReadySetError::DfValueConversionError {
            src_type: "Decimal".to_string(),
            target_type: sql_type.to_string(),
            details: "Not allowed".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;
    use crate::DfValue;

    #[proptest]
    fn float_to_tinyint(val: f32) {
        if val < i8::MIN as f32 - 0.5 || val >= i8::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::TinyInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::TinyInt(None)),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        // Yes, this is indeed the valid range for MySQL float to u8 conversion
        if val < -0.5f32 || val >= u8::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::UnsignedTinyInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::UnsignedTinyInt(None)),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_smallint(val: f32) {
        if val < i16::MIN as f32 - 0.5 || val >= i16::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::SmallInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::SmallInt(None)),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f32 || val >= u16::MAX as f32 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::UnsignedSmallInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::UnsignedSmallInt(None)),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_int(val: f64) {
        if val < i32::MIN as f64 - 0.5 || val >= i32::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::Int(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::Int(None)),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f64 || val >= u32::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::UnsignedInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::UnsignedInt(None)),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[proptest]
    fn float_to_bigint(val: f64) {
        if val < i64::MIN as f64 - 0.5 || val >= i64::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::BigInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::BigInt(None)),
                Ok(DfValue::Int(val.round() as i64))
            );
        }
        if val < -0.5f64 || val >= u64::MAX as f64 + 0.5 {
            DfValue::Double(val as _)
                .coerce_to(&SqlType::UnsignedBigInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DfValue::Double(val as f64).coerce_to(&SqlType::UnsignedBigInt(None)),
                Ok(DfValue::UnsignedInt(val.round() as u64))
            );
        }
    }

    #[test]
    fn float_to_text() {
        assert_eq!(
            DfValue::Double(312.0).coerce_to(&SqlType::Text).unwrap(),
            DfValue::from("312")
        );

        assert_eq!(
            DfValue::Double(312.222222)
                .coerce_to(&SqlType::Text)
                .unwrap(),
            DfValue::from("312.222222")
        );

        assert_eq!(
            DfValue::Double(1e4).coerce_to(&SqlType::Text).unwrap(),
            DfValue::from("10000")
        );

        assert_eq!(
            DfValue::Double(1e4)
                .coerce_to(&SqlType::VarChar(Some(2)))
                .unwrap(),
            DfValue::from("10")
        );

        assert_eq!(
            DfValue::Double(1e4)
                .coerce_to(&SqlType::Char(Some(8)))
                .unwrap(),
            DfValue::from("10000   ")
        );

        assert_eq!(
            DfValue::Double(-1.0 / 3.0)
                .coerce_to(&SqlType::Text)
                .unwrap(),
            DfValue::from("-0.3333333333333333")
        );
    }

    #[test]
    fn float_to_json() {
        assert_eq!(
            DfValue::Double(-5000.0).coerce_to(&SqlType::Json).unwrap(),
            DfValue::from("-5000")
        );
    }

    #[test]
    fn float_to_int_manual() {
        assert_eq!(
            DfValue::Float(-0.6874218).coerce_to(&SqlType::TinyInt(None)),
            Ok(DfValue::Int(-1))
        );

        assert_eq!(
            DfValue::Float(0.6874218).coerce_to(&SqlType::TinyInt(None)),
            Ok(DfValue::Int(1))
        );

        assert!(DfValue::Float(-128.9039)
            .coerce_to(&SqlType::TinyInt(None))
            .is_err());

        assert!(DfValue::Float(127.9039)
            .coerce_to(&SqlType::TinyInt(None))
            .is_err());

        assert_eq!(
            DfValue::Float(-128.4039).coerce_to(&SqlType::TinyInt(None)),
            Ok(DfValue::Int(-128))
        );

        assert_eq!(
            DfValue::Float(127.4039).coerce_to(&SqlType::TinyInt(None)),
            Ok(DfValue::Int(127))
        );

        assert_eq!(
            DfValue::Float(0.5).coerce_to(&SqlType::UnsignedTinyInt(None)),
            Ok(DfValue::UnsignedInt(1))
        );

        assert_eq!(
            DfValue::Float(0.687_421_9).coerce_to(&SqlType::UnsignedTinyInt(None)),
            Ok(DfValue::UnsignedInt(1))
        );

        assert_eq!(
            DfValue::Float(255.49).coerce_to(&SqlType::UnsignedTinyInt(None)),
            Ok(DfValue::UnsignedInt(255))
        );

        assert!(DfValue::Float(255.5)
            .coerce_to(&SqlType::UnsignedTinyInt(None))
            .is_err());

        assert_eq!(
            DfValue::Float(65535.49).coerce_to(&SqlType::UnsignedSmallInt(None)),
            Ok(DfValue::UnsignedInt(65535))
        );

        assert!(DfValue::Float(65535.5)
            .coerce_to(&SqlType::UnsignedSmallInt(None))
            .is_err());

        assert_eq!(
            DfValue::Double(4294967295.49).coerce_to(&SqlType::UnsignedInt(None)),
            Ok(DfValue::UnsignedInt(4294967295))
        );

        assert!(DfValue::Double(4294967295.5)
            .coerce_to(&SqlType::UnsignedInt(None))
            .is_err());

        // Since u64 max is not accurately representable as a doulbe, there are no exact conversions
        assert!(DfValue::Double(18446744073709551613.49)
            .coerce_to(&SqlType::UnsignedBigInt(None))
            .is_err());

        assert_eq!(
            DfValue::Double(17946744073709551610.49).coerce_to(&SqlType::UnsignedBigInt(None)),
            Ok(DfValue::UnsignedInt(17946744073709551616))
        );
    }
}
