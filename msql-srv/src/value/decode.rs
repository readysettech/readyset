use crate::error::MsqlSrvError;
use crate::myc::constants::ColumnType;
use crate::myc::io::ReadMysqlExt;
use byteorder::{LittleEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::io;

/// MySQL value as provided when executing prepared statements.
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Value<'a>(ValueInner<'a>);

/// A representation of a concrete, typed MySQL value.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ValueInner<'a> {
    /// The MySQL `NULL` value.
    Null,
    /// An untyped sequence of bytes (usually a text type or `MYSQL_TYPE_BLOB`).
    Bytes(&'a [u8]),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A floating point number.
    Double(f64),
    /// A [binary encoding](https://mariadb.com/kb/en/library/resultset-row/#date-binary-encoding)
    /// of a `MYSQL_TYPE_DATE`.
    Date(&'a [u8]),
    /// A [binary encoding](https://mariadb.com/kb/en/library/resultset-row/#time-binary-encoding)
    /// of a `MYSQL_TYPE_TIME`.
    Time(&'a [u8]),
    /// A [binary
    /// encoding](https://mariadb.com/kb/en/library/resultset-row/#timestamp-binary-encoding) of a
    /// `MYSQL_TYPE_TIMESTAMP` or `MYSQL_TYPE_DATETIME`.
    Datetime(&'a [u8]),
}

impl<'a> Value<'a> {
    /// Return the inner stored representation of this value.
    ///
    /// This may be useful for when you do not care about the exact data type used for a column,
    /// but instead wish to introspect a value you are given at runtime. Note that the contained
    /// value may be stored in a type that is more general than what the corresponding parameter
    /// type allows (e.g., a `u8` will be stored as an `u64`).
    pub fn into_inner(self) -> ValueInner<'a> {
        self.0
    }

    pub(crate) fn null() -> Self {
        Value(ValueInner::Null)
    }

    /// Returns true if this is a NULL value
    pub fn is_null(&self) -> bool {
        matches!(self.0, ValueInner::Null)
    }

    pub(crate) fn parse_from(
        input: &mut &'a [u8],
        ct: ColumnType,
        unsigned: bool,
    ) -> io::Result<Self> {
        ValueInner::parse_from(input, ct, unsigned).map(Value)
    }

    pub(crate) fn bytes(input: &'a [u8]) -> Value<'a> {
        Value(ValueInner::Bytes(input))
    }
}

macro_rules! read_bytes {
    ($input:expr, $len:expr) => {
        if $len as usize > $input.len() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while reading length-encoded string",
            ))
        } else {
            let (bits, rest) = $input.split_at($len as usize);
            *$input = rest;
            Ok(bits)
        }
    };
}

impl<'a> ValueInner<'a> {
    fn parse_from(input: &mut &'a [u8], ct: ColumnType, unsigned: bool) -> io::Result<Self> {
        match ct {
            ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_BIT
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_GEOMETRY
            | ColumnType::MYSQL_TYPE_JSON => {
                let len = input.read_lenenc_int()?;
                Ok(ValueInner::Bytes(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_TINY => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(input.read_u8()?)))
                } else {
                    Ok(ValueInner::Int(i64::from(input.read_i8()?)))
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(
                        input.read_u16::<LittleEndian>()?,
                    )))
                } else {
                    Ok(ValueInner::Int(i64::from(
                        input.read_i16::<LittleEndian>()?,
                    )))
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if unsigned {
                    Ok(ValueInner::UInt(u64::from(
                        input.read_u32::<LittleEndian>()?,
                    )))
                } else {
                    Ok(ValueInner::Int(i64::from(
                        input.read_i32::<LittleEndian>()?,
                    )))
                }
            }
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(ValueInner::UInt(input.read_u64::<LittleEndian>()?))
                } else {
                    Ok(ValueInner::Int(input.read_i64::<LittleEndian>()?))
                }
            }
            ColumnType::MYSQL_TYPE_FLOAT => {
                let f = input.read_f32::<LittleEndian>()?;
                println!("read {}", f);
                Ok(ValueInner::Double(f64::from(f)))
            }
            ColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(ValueInner::Double(input.read_f64::<LittleEndian>()?))
            }
            ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
                let len = input.read_u8()?;
                Ok(ValueInner::Datetime(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_DATE => {
                let len = input.read_u8()?;
                Ok(ValueInner::Date(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_TIME => {
                let len = input.read_u8()?;
                Ok(ValueInner::Time(read_bytes!(input, len)?))
            }
            ColumnType::MYSQL_TYPE_NULL => Ok(ValueInner::Null),
            ct => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown column type {:?}", ct),
            )),
        }
    }
}

macro_rules! impl_try_from {
    ($t:ty, $n:expr, $($variant:path),*) => {
        impl<'a> TryFrom<Value<'a>> for $t {
            type Error = MsqlSrvError;
            fn try_from(val: Value<'a>) -> Result<Self, Self::Error> {
                match val.0 {
                    $($variant(v) => Ok(v as $t)),*,
                    _ => Err(MsqlSrvError::InvalidConversion {target_type: $n , src_type: format!("{:?}",val) }),
                }
            }
        }
    }
}

impl_try_from!(u8, "u8".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(u16, "u16".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(u32, "u32".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(u64, "u64".to_string(), ValueInner::UInt);
impl_try_from!(i8, "i8".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(i16, "i16".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(i32, "i32".to_string(), ValueInner::UInt, ValueInner::Int);
impl_try_from!(i64, "i64".to_string(), ValueInner::Int);
impl_try_from!(f32, "f32".to_string(), ValueInner::Double);
impl_try_from!(f64, "f64".to_string(), ValueInner::Double);
impl_try_from!(&'a [u8], "[u8]".to_string(), ValueInner::Bytes);

impl<'a> TryFrom<Value<'a>> for &'a str {
    type Error = MsqlSrvError;
    fn try_from(val: Value<'a>) -> Result<&'a str, Self::Error> {
        if let ValueInner::Bytes(v) = val.0 {
            match ::std::str::from_utf8(v) {
                Ok(val) => Ok(val),
                Err(e) => Err(MsqlSrvError::Utf8Error(e)),
            }
        } else {
            Err(MsqlSrvError::InvalidConversion {
                target_type: "str".to_string(),
                src_type: format!("{:?}", val),
            })
        }
    }
}

use chrono::{NaiveDate, NaiveDateTime};
impl<'a> TryFrom<Value<'a>> for NaiveDate {
    type Error = MsqlSrvError;
    fn try_from(val: Value<'a>) -> Result<NaiveDate, Self::Error> {
        if let ValueInner::Date(mut v) = val.0 {
            if v.len() != 4 {
                return Err(MsqlSrvError::InvalidDate);
            }
            Ok(NaiveDate::from_ymd(
                i32::from(v.read_u16::<LittleEndian>()?),
                u32::from(v.read_u8()?),
                u32::from(v.read_u8()?),
            ))
        } else {
            Err(MsqlSrvError::InvalidConversion {
                target_type: "NaiveDate".to_string(),
                src_type: format!("{:?}", val),
            })
        }
    }
}

impl<'a> TryFrom<Value<'a>> for NaiveDateTime {
    type Error = MsqlSrvError;
    fn try_from(val: Value<'a>) -> Result<NaiveDateTime, Self::Error> {
        if let ValueInner::Datetime(mut v) = val.0 {
            if !(v.len() == 7 || v.len() == 11) {
                return Err(MsqlSrvError::InvalidDatetime);
            }
            let d = NaiveDate::from_ymd(
                i32::from(v.read_u16::<LittleEndian>()?),
                u32::from(v.read_u8()?),
                u32::from(v.read_u8()?),
            );

            let h = u32::from(v.read_u8()?);
            let m = u32::from(v.read_u8()?);
            let s = u32::from(v.read_u8()?);

            if v.len() == 11 {
                let us = v.read_u32::<LittleEndian>()?;
                Ok(d.and_hms_micro(h, m, s, us))
            } else {
                Ok(d.and_hms(h, m, s))
            }
        } else {
            Err(MsqlSrvError::InvalidConversion {
                target_type: "NaiveDateTime".to_string(),
                src_type: format!("{:?}", val),
            })
        }
    }
}

use mysql_time::MysqlTime;

impl<'a> TryFrom<Value<'a>> for MysqlTime {
    type Error = MsqlSrvError;
    fn try_from(val: Value<'a>) -> Result<MysqlTime, Self::Error> {
        if let ValueInner::Time(mut v) = val.0 {
            let is_positive = v.read_u8()? == 0; // sign: 1 negative, 0 positive
            let d = v.read_u32::<LittleEndian>()? as u16;
            let h = v.read_u8()? as u16;
            let m = v.read_u8()?;
            let s = v.read_u8()?;
            let us = v.read_u32::<LittleEndian>().unwrap_or(0) as u64;
            Ok(MysqlTime::from_hmsus(is_positive, d * 24 + h, m, s, us))
        } else {
            Err(MsqlSrvError::InvalidConversion {
                target_type: "MysqlTime".to_string(),
                src_type: format!("{:?}", val),
            })
        }
    }
}

use std::time::Duration;

impl<'a> TryFrom<Value<'a>> for Duration {
    type Error = MsqlSrvError;
    fn try_from(val: Value<'a>) -> Result<Duration, Self::Error> {
        if let ValueInner::Time(mut v) = val.0 {
            if !(v.len() == 8 || v.len() == 12) {
                return Err(MsqlSrvError::InvalidDuration);
            }

            let neg = v.read_u8()?;
            if neg != 0u8 {
                return Err(MsqlSrvError::Unimplemented {
                    operation: "Negative durations".to_string(),
                });
            }

            let days = u64::from(v.read_u32::<LittleEndian>()?);
            let hours = u64::from(v.read_u8()?);
            let minutes = u64::from(v.read_u8()?);
            let seconds = u64::from(v.read_u8()?);
            let micros = if v.len() == 12 {
                v.read_u32::<LittleEndian>()?
            } else {
                0
            };

            Ok(Duration::new(
                days * 86_400 + hours * 3_600 + minutes * 60 + seconds,
                micros * 1_000,
            ))
        } else {
            Err(MsqlSrvError::InvalidConversion {
                target_type: "Duration".to_string(),
                src_type: format!("{:?}", val),
            })
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::Value;
    use crate::myc;
    use crate::myc::io::WriteMysqlExt;
    use crate::{Column, ColumnFlags, ColumnType};
    use chrono::{self, TimeZone};
    use mysql_time::MysqlTime;
    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::time;

    macro_rules! rt {
        ($name:ident, $t:ty, $v:expr, $ct:expr) => {
            rt!($name, $t, $v, $ct, false);
        };
        ($name:ident, $t:ty, $v:expr, $ct:expr, $sig:expr) => {
            #[test]
            fn $name() {
                let mut data = Vec::new();
                let mut col = Column {
                    table: String::new(),
                    column: String::new(),
                    coltype: $ct,
                    colflags: ColumnFlags::empty(),
                };

                if !$sig {
                    col.colflags.insert(ColumnFlags::UNSIGNED_FLAG);
                }

                let v: $t = $v;
                data.write_bin_value(
                    &myc::value::Value::try_from(v).expect("try_from returned an error"),
                )
                .unwrap();
                assert_eq!(
                    TryInto::<$t>::try_into(Value::parse_from(&mut &data[..], $ct, !$sig).unwrap())
                        .expect("try_into returned an error"),
                    v
                );
            }
        };
    }

    rt!(u8_one, u8, 1, ColumnType::MYSQL_TYPE_TINY, false);
    rt!(i8_one, i8, 1, ColumnType::MYSQL_TYPE_TINY, true);
    rt!(u8_one_short, u8, 1, ColumnType::MYSQL_TYPE_SHORT, false);
    rt!(i8_one_short, i8, 1, ColumnType::MYSQL_TYPE_SHORT, true);
    rt!(u8_one_long, u8, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i8_one_long, i8, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u8_one_longlong,
        u8,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i8_one_longlong,
        i8,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u16_one, u16, 1, ColumnType::MYSQL_TYPE_SHORT, false);
    rt!(i16_one, i16, 1, ColumnType::MYSQL_TYPE_SHORT, true);
    rt!(u16_one_long, u16, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i16_one_long, i16, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u16_one_longlong,
        u16,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i16_one_longlong,
        i16,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u32_one_long, u32, 1, ColumnType::MYSQL_TYPE_LONG, false);
    rt!(i32_one_long, i32, 1, ColumnType::MYSQL_TYPE_LONG, true);
    rt!(
        u32_one_longlong,
        u32,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i32_one_longlong,
        i32,
        1,
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );
    rt!(u64_one, u64, 1, ColumnType::MYSQL_TYPE_LONGLONG, false);
    rt!(i64_one, i64, 1, ColumnType::MYSQL_TYPE_LONGLONG, true);

    // NOTE:
    // this test currently doesn't work since `mysql` *always* encodes floats as doubles.
    // it gets away with this in practice by forcing the parameter types on every query:
    // https://github.com/blackbeam/rust-mysql-simple/blob/47b7883a7e0eb47a45b06d726a55e89e78c0477e/src/conn/mod.rs#L994
    // however, since we don't take parameter type forcing into account here, the test won't pass
    //
    // rt!(f32_one_float, f32, 1.0, ColumnType::MYSQL_TYPE_FLOAT, false);

    /*see ENG-385
    rt!(
        f32_one_double,
        f32,
        1.0,
        ColumnType::MYSQL_TYPE_DOUBLE,
        false
    );*/
    rt!(f64_one, f64, 1.0, ColumnType::MYSQL_TYPE_DOUBLE, false);

    rt!(
        u8_max,
        u8,
        u8::max_value(),
        ColumnType::MYSQL_TYPE_TINY,
        false
    );
    rt!(
        i8_max,
        i8,
        i8::max_value(),
        ColumnType::MYSQL_TYPE_TINY,
        true
    );
    rt!(
        u16_max,
        u16,
        u16::max_value(),
        ColumnType::MYSQL_TYPE_SHORT,
        false
    );
    rt!(
        i16_max,
        i16,
        i16::max_value(),
        ColumnType::MYSQL_TYPE_SHORT,
        true
    );
    rt!(
        u32_max,
        u32,
        u32::max_value(),
        ColumnType::MYSQL_TYPE_LONG,
        false
    );
    rt!(
        i32_max,
        i32,
        i32::max_value(),
        ColumnType::MYSQL_TYPE_LONG,
        true
    );
    rt!(
        u64_max,
        u64,
        u64::max_value(),
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i64_max,
        i64,
        i64::max_value(),
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );

    rt!(
        date_only_time,
        chrono::NaiveDate,
        chrono::Local::today().naive_local(),
        ColumnType::MYSQL_TYPE_DATE
    );
    rt!(
        time,
        MysqlTime,
        MysqlTime::from_hmsus(true, 20, 15, 14, 123_456),
        ColumnType::MYSQL_TYPE_TIME
    );
    rt!(
        datetime,
        chrono::NaiveDateTime,
        chrono::Utc.ymd(1989, 12, 7).and_hms(8, 0, 4).naive_utc(),
        ColumnType::MYSQL_TYPE_DATETIME
    );
    rt!(
        dur,
        time::Duration,
        time::Duration::from_secs(1893),
        ColumnType::MYSQL_TYPE_TIME
    );
    rt!(
        bytes,
        &[u8],
        &[0x42, 0x00, 0x1a],
        ColumnType::MYSQL_TYPE_BLOB
    );
    rt!(string, &str, "foobar", ColumnType::MYSQL_TYPE_STRING);
}
