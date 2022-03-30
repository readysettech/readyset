use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use noria_errors::{ReadySetError, ReadySetResult};
use rust_decimal::Decimal;

use crate::{DataType, SqlType};

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

/// Coerce an integer value according to MySQL rules to the best of our abilities
pub(crate) fn coerce_integer<I, S>(
    val: I,
    src_type_name: S,
    sql_type: &SqlType,
) -> ReadySetResult<DataType>
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
    I: std::ops::BitXor<I, Output = I> + std::cmp::Eq + Copy + fmt::Display + IntAsFloat,
    S: ToString,
{
    let err = || ReadySetError::DataTypeConversionError {
        src_type: src_type_name.to_string(),
        target_type: sql_type.to_string(),
        details: "out of bounds".to_string(),
    };

    match sql_type {
        SqlType::Bool => {
            #[allow(clippy::eq_op)]
            {
                Ok(DataType::from(val ^ val != val))
            }
        }
        SqlType::Tinyint(_) => i8::try_from(val).map_err(|_| err()).map(DataType::from),
        SqlType::Smallint(_) => i16::try_from(val).map_err(|_| err()).map(DataType::from),
        SqlType::Int(_) | SqlType::Serial => {
            i32::try_from(val).map_err(|_| err()).map(DataType::from)
        }
        SqlType::Bigint(_) | SqlType::BigSerial => {
            i64::try_from(val).map_err(|_| err()).map(DataType::from)
        }
        SqlType::UnsignedTinyint(_) => u8::try_from(val).map_err(|_| err()).map(DataType::from),
        SqlType::UnsignedSmallint(_) => u16::try_from(val).map_err(|_| err()).map(DataType::from),
        SqlType::UnsignedInt(_) => u32::try_from(val).map_err(|_| err()).map(DataType::from),
        SqlType::UnsignedBigint(_) => u64::try_from(val).map_err(|_| err()).map(DataType::from),

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

        SqlType::Json | SqlType::Jsonb => Ok(format!("\"{}\"", val).into()),

        SqlType::Date => {
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

        SqlType::Timestamp | SqlType::TimestampTz | SqlType::DateTime(_) => {
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

        SqlType::Time => {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
            // As a number in hhmmss format, provided that it makes sense as a time. For example,
            // 101112 is understood as '10:11:12'. The following alternative formats are also
            // understood: ss, mmss, or hhmmss.
            let val = u64::try_from(val).map_err(|_| err())?;
            let hh = val / 1_00_00;
            let mm = val / 1_00 % 1_00;
            let ss = val % 1_00;
            Ok(mysql_time::MysqlTime::from_hmsus(true, hh as _, mm as _, ss as _, 0).into())
        }

        SqlType::Double => Ok(DataType::Double(val.to_f64())),
        SqlType::Real | SqlType::Float => Ok(DataType::Float(val.to_f32())),
        SqlType::Numeric(_) | SqlType::Decimal(_, _) => Ok(DataType::Numeric(Arc::new(val.into()))),

        SqlType::Enum(_)
        | SqlType::MacAddr
        | SqlType::Inet
        | SqlType::Uuid
        | SqlType::Bit(_)
        | SqlType::Varbit(_) => Err(ReadySetError::DataTypeConversionError {
            src_type: src_type_name.to_string(),
            target_type: sql_type.to_string(),
            details: "Not allowed".to_string(),
        }),
    }
}
