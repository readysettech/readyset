use std::convert::TryInto;

use nom_sql::SqlType;
use noria_errors::{ReadySetError, ReadySetResult};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::DataType;

fn coerce_f64_to_int<I>(val: f64) -> Option<I>
where
    i64: TryInto<I>,
{
    // First get the integer part of the float, then downcast to the appropriate type
    let val = val.trunc();
    let val_int = val as i64;
    let val_back = val_int as f64;
    // If the float represent an actual integer value, return it. Around the value of 0.0 there are
    // rounding errors, so for example a value that is negative but close to 0 should get a special
    // treatment.
    if val.to_bits() == val_back.to_bits() || val == -0.0 && val_back == 0.0 {
        val_int.try_into().ok()
    } else {
        None
    }
}

fn coerce_f64_to_uint<I>(val: f64) -> Option<I>
where
    u64: TryInto<I>,
{
    if -0.5 < val && val < 0.5 {
        return 0.try_into().ok();
    }

    let val = val.trunc();
    let val_int = val as u64;
    let val_back = val_int as f64;

    // If the float represent an actual integer value, return it
    if val.to_bits() == val_back.to_bits() {
        val_int.try_into().ok()
    } else {
        None
    }
}

pub(crate) fn coerce_f64(val: f64, sql_type: &SqlType) -> ReadySetResult<DataType> {
    let err = |deets: &str| ReadySetError::DataTypeConversionError {
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
        SqlType::Bool => Ok(DataType::from(val != 0.0)),

        SqlType::Float | SqlType::Real => {
            let val = val as f32;
            if val.is_infinite() {
                return Err(bounds_err());
            }
            Ok(DataType::Float(val))
        }
        SqlType::Double => Ok(DataType::Double(val)),

        SqlType::Numeric(_) | SqlType::Decimal(_, _) => Decimal::from_f64_retain(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),

        SqlType::Tinyint(_) => coerce_f64_to_int::<i8>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::Smallint(_) => coerce_f64_to_int::<i16>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::Int(_) | SqlType::Serial => coerce_f64_to_int::<i32>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::Bigint(_) | SqlType::BigSerial => coerce_f64_to_int::<i64>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),

        SqlType::UnsignedTinyint(_) => coerce_f64_to_uint::<u8>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::UnsignedSmallint(_) => coerce_f64_to_uint::<u16>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::UnsignedInt(_) => coerce_f64_to_uint::<u32>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),
        SqlType::UnsignedBigint(_) => coerce_f64_to_uint::<u64>(val)
            .ok_or_else(bounds_err)
            .map(DataType::from),

        SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Text
        | SqlType::Longtext
        | SqlType::Char(None)
        | SqlType::Varchar(None) => Ok(val.to_string().into()),

        SqlType::Varchar(Some(l)) => {
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

        SqlType::Tinyblob
        | SqlType::Mediumblob
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::ByteArray
        | SqlType::Binary(None) => Ok(DataType::ByteArray(val.to_string().into_bytes().into())),

        SqlType::Varbinary(l) => {
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
        | SqlType::Uuid
        | SqlType::Bit(_)
        | SqlType::Varbit(_) => Err(err("not allowed")),
    }
}

pub(crate) fn coerce_decimal(val: &Decimal, sql_type: &SqlType) -> ReadySetResult<DataType> {
    let err = || ReadySetError::DataTypeConversionError {
        src_type: "Decimal".to_string(),
        target_type: sql_type.to_string(),
        details: "out of bounds".to_string(),
    };

    match sql_type {
        SqlType::Bool => Ok(DataType::from(!val.is_zero())),

        SqlType::Float | SqlType::Real => val
            .to_f32()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DataType::Float),
        SqlType::Double => val
            .to_f64()
            .and_then(|v| if v.is_finite() { Some(v) } else { None })
            .ok_or_else(err)
            .map(DataType::Double),

        SqlType::Decimal(_, _) | SqlType::Numeric(_) => Ok(DataType::from(*val)),

        SqlType::Tinyint(_) => val.to_i8().ok_or_else(err).map(DataType::from),
        SqlType::Smallint(_) => val.to_i16().ok_or_else(err).map(DataType::from),
        SqlType::Int(_) | SqlType::Serial => val.to_i32().ok_or_else(err).map(DataType::from),
        SqlType::Bigint(_) | SqlType::BigSerial => val.to_i64().ok_or_else(err).map(DataType::from),

        SqlType::UnsignedTinyint(_) => val.to_u8().ok_or_else(err).map(DataType::from),
        SqlType::UnsignedSmallint(_) => val.to_u16().ok_or_else(err).map(DataType::from),
        SqlType::UnsignedInt(_) => val.to_u32().ok_or_else(err).map(DataType::from),
        SqlType::UnsignedBigint(_) => val.to_u64().ok_or_else(err).map(DataType::from),

        SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Text
        | SqlType::Longtext
        | SqlType::Char(None)
        | SqlType::Varchar(None) => Ok(val.to_string().into()),

        SqlType::Varchar(Some(l)) => {
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

        SqlType::Tinyblob
        | SqlType::Mediumblob
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::ByteArray
        | SqlType::Binary(None) => Ok(DataType::ByteArray(val.to_string().into_bytes().into())),

        SqlType::Varbinary(l) => {
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
        | SqlType::Uuid
        | SqlType::Bit(_)
        | SqlType::Varbit(_) => Err(ReadySetError::DataTypeConversionError {
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
    use crate::DataType;

    #[proptest]
    fn float_to_tinyint(val: f32) {
        if val < i8::MIN as f32 || val > i8::MAX as f32 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::Tinyint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::Tinyint(None)),
                Ok(DataType::Int(val as i64))
            );
        }
        // Yes, this is indeed the valid range for MySQL float to u8 conversion
        if val < -0.5f32 || val > u8::MAX as f32 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::UnsignedTinyint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::UnsignedTinyint(None)),
                Ok(DataType::UnsignedInt(val as u64))
            );
        }
    }

    #[proptest]
    fn float_to_smallint(val: f32) {
        if val < i16::MIN as f32 || val > i16::MAX as f32 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::Smallint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::Smallint(None)),
                Ok(DataType::Int(val as i64))
            );
        }
        if val < -0.5f32 || val > u16::MAX as f32 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::UnsignedSmallint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::UnsignedSmallint(None)),
                Ok(DataType::UnsignedInt(val as u64))
            );
        }
    }

    #[proptest]
    fn float_to_int(val: f64) {
        if val < i32::MIN as f64 || val > i32::MAX as f64 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::Int(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::Int(None)),
                Ok(DataType::Int(val as i64))
            );
        }
        if val < -0.5f64 || val > u32::MAX as f64 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::UnsignedInt(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::UnsignedInt(None)),
                Ok(DataType::UnsignedInt(val as u64))
            );
        }
    }

    #[proptest]
    fn float_to_bigint(val: f64) {
        if val < i64::MIN as f64 || val > i64::MAX as f64 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::Bigint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::Bigint(None)),
                Ok(DataType::Int(val as i64))
            );
        }
        if val < -0.5f64 || val > u64::MAX as f64 {
            DataType::Double(val as _)
                .coerce_to(&SqlType::UnsignedBigint(None))
                .expect_err("OOB");
        } else {
            assert_eq!(
                DataType::Double(val as f64).coerce_to(&SqlType::UnsignedBigint(None)),
                Ok(DataType::UnsignedInt(val as u64))
            );
        }
    }

    #[test]
    fn float_to_text() {
        assert_eq!(
            DataType::Double(312.0).coerce_to(&SqlType::Text).unwrap(),
            DataType::from("312")
        );

        assert_eq!(
            DataType::Double(312.222222)
                .coerce_to(&SqlType::Text)
                .unwrap(),
            DataType::from("312.222222")
        );

        assert_eq!(
            DataType::Double(1e4).coerce_to(&SqlType::Text).unwrap(),
            DataType::from("10000")
        );

        assert_eq!(
            DataType::Double(1e4)
                .coerce_to(&SqlType::Varchar(Some(2)))
                .unwrap(),
            DataType::from("10")
        );

        assert_eq!(
            DataType::Double(1e4)
                .coerce_to(&SqlType::Char(Some(8)))
                .unwrap(),
            DataType::from("10000   ")
        );

        assert_eq!(
            DataType::Double(-1.0 / 3.0)
                .coerce_to(&SqlType::Text)
                .unwrap(),
            DataType::from("-0.3333333333333333")
        );
    }

    #[test]
    fn float_to_json() {
        assert_eq!(
            DataType::Double(-5000.0).coerce_to(&SqlType::Json).unwrap(),
            DataType::from("-5000")
        );
    }
}
