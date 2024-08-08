use std::io::{self, Write};

use byteorder::{LittleEndian, WriteBytesExt};
use myc::proto::MySerialize;
use mysql_time::MySqlTime;
use readyset_data::TimestampTz;

use crate::error::{other_error, OtherErrorKind};
use crate::myc::constants::{ColumnFlags, ColumnType};
use crate::myc::io::WriteMysqlExt;
use crate::{myc, Column};

/// Implementors of this trait can be sent as a single resultset value to a MySQL/MariaDB client.
pub trait ToMySqlValue {
    /// Encode value using the text-based protocol.
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()>;

    /// Encode value using the binary protocol.
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()>;

    /// Is this value NULL?
    fn is_null(&self) -> bool {
        false
    }
}

macro_rules! mysql_text_trivial {
    () => {
        fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_lenenc_str(self.to_string().as_bytes()).map(|_| ())
        }
    };
}

use std::fmt;
fn bad<V: fmt::Debug>(v: V, c: &Column) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("tried to use {:?} as {:?}", v, c.coltype),
    )
}

impl<T> ToMySqlValue for Option<T>
where
    T: ToMySqlValue,
{
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        if let Some(ref v) = *self {
            v.to_mysql_text(w)
        } else {
            w.write_u8(0xFB)
        }
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, ct: &Column) -> io::Result<()> {
        if let Some(ref v) = *self {
            v.to_mysql_bin(w, ct)
        } else {
            // should be handled by NULL map
            Err(other_error(OtherErrorKind::Unexpected {
                error: "Value should be handled by null map".to_string(),
            }))
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }
}

// NOTE: these rules can all go away when TryFrom stabilizes
//       see https://github.com/jonhoo/mysql-srv/commit/13e5e753e5042a42cc45ad57c2b760561da2fb50
// NOTE: yes, I know the = / => distinction is ugly
macro_rules! like_try_into {
    ($self:ident, $source:ty = $target:ty, $w:ident, $m:ident, $c:ident) => {{
        let min = <$target>::MIN as $source;
        let max = <$target>::MAX as $source;
        if *$self <= max && *$self >= min {
            $w.$m(*$self as $target)
        } else {
            Err(bad($self, $c))
        }
    }};
    ($self:ident, $source:ty => $target:ty, $w:ident, $m:ident, $c:ident) => {{
        let min = <$target>::MIN as $source;
        let max = <$target>::MAX as $source;
        if *$self <= max && *$self >= min {
            $w.$m::<LittleEndian>(*$self as $target)
        } else {
            Err(bad($self, $c))
        }
    }};
}

macro_rules! forgiving_numeric {
    ($t:ty) => {
        impl ToMySqlValue for $t {
            mysql_text_trivial!();
            fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
                let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
                match c.coltype {
                    ColumnType::MYSQL_TYPE_LONGLONG => {
                        if signed {
                            like_try_into!(self, $t => i64, w, write_i64, c)
                        } else {
                            like_try_into!(self, $t => u64, w, write_u64, c)
                        }
                    }
                    ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                        if signed {
                            like_try_into!(self, $t => i32, w, write_i32, c)
                        } else {
                            like_try_into!(self, $t => u32, w, write_u32, c)
                        }
                    }
                    ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                        if signed {
                            like_try_into!(self, $t => i16, w, write_i16, c)
                        } else {
                            like_try_into!(self, $t => u16, w, write_u16, c)
                        }
                    }
                    ColumnType::MYSQL_TYPE_TINY => {
                        if signed {
                            like_try_into!(self, $t = i8, w, write_i8, c)
                        } else {
                            like_try_into!(self, $t = u8, w, write_u8, c)
                        }
                    }
                    _ => Err(bad(self, c)),
                }
            }
        }
    };
}

forgiving_numeric!(usize);
forgiving_numeric!(isize);

impl ToMySqlValue for u8 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(u64::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if signed {
                    w.write_i32::<LittleEndian>(i32::from(*self))
                } else {
                    w.write_u32::<LittleEndian>(u32::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                if signed {
                    w.write_i16::<LittleEndian>(i16::from(*self))
                } else {
                    w.write_u16::<LittleEndian>(u16::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_TINY => {
                assert!(!signed);
                w.write_u8(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for i8 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(*self as u64)
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if signed {
                    w.write_i32::<LittleEndian>(i32::from(*self))
                } else {
                    w.write_u32::<LittleEndian>(*self as u32)
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                if signed {
                    w.write_i16::<LittleEndian>(i16::from(*self))
                } else {
                    w.write_u16::<LittleEndian>(*self as u16)
                }
            }
            ColumnType::MYSQL_TYPE_TINY => {
                assert!(signed);
                w.write_i8(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for u16 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(u64::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if signed {
                    w.write_i32::<LittleEndian>(i32::from(*self))
                } else {
                    w.write_u32::<LittleEndian>(u32::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                assert!(!signed);
                w.write_u16::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for i16 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(*self as u64)
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                if signed {
                    w.write_i32::<LittleEndian>(i32::from(*self))
                } else {
                    w.write_u32::<LittleEndian>(*self as u32)
                }
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                assert!(signed);
                w.write_i16::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for u32 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(u64::from(*self))
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                assert!(!signed);
                w.write_u32::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for i32 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if signed {
                    w.write_i64::<LittleEndian>(i64::from(*self))
                } else {
                    w.write_u64::<LittleEndian>(*self as u64)
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                assert!(signed);
                w.write_i32::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for u64 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                assert!(!signed);
                w.write_u64::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for i64 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
        match c.coltype {
            ColumnType::MYSQL_TYPE_LONGLONG => {
                assert!(signed);
                w.write_i64::<LittleEndian>(*self)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for f32 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
            ColumnType::MYSQL_TYPE_DOUBLE => w.write_f64::<LittleEndian>(f64::from(*self)),
            ColumnType::MYSQL_TYPE_FLOAT => w.write_f32::<LittleEndian>(*self),
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for f64 {
    mysql_text_trivial!();
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
            ColumnType::MYSQL_TYPE_DOUBLE => w.write_f64::<LittleEndian>(*self),
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for String {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.as_bytes().to_mysql_text(w)
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        self.as_bytes().to_mysql_bin(w, c)
    }
}

impl ToMySqlValue for str {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.as_bytes().to_mysql_text(w)
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        self.as_bytes().to_mysql_bin(w, c)
    }
}

impl ToMySqlValue for [u8] {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_lenenc_str(self).map(|_| ())
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
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
            | ColumnType::MYSQL_TYPE_JSON => w.write_lenenc_str(self).map(|_| ()),
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for Vec<u8> {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        (self[..]).to_mysql_text(w)
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        (self[..]).to_mysql_bin(w, c)
    }
}

impl<'a, T> ToMySqlValue for &'a T
where
    T: ToMySqlValue + ?Sized,
{
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        (*self).to_mysql_text(w)
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        (*self).to_mysql_bin(w, c)
    }
    fn is_null(&self) -> bool {
        (*self).is_null()
    }
}

use chrono::{self, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
impl ToMySqlValue for NaiveDate {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_lenenc_str(
            format!("{:04}-{:02}-{:02}", self.year(), self.month(), self.day()).as_bytes(),
        )
        .map(|_| ())
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
            ColumnType::MYSQL_TYPE_DATE => {
                w.write_u8(4u8)?;
                w.write_u16::<LittleEndian>(self.year() as u16)?;
                w.write_u8(self.month() as u8)?;
                w.write_u8(self.day() as u8)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for NaiveTime {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let us = self.nanosecond() / 1_000;
        if us == 0 {
            w.write_lenenc_str(
                format!(
                    "{:02}:{:02}:{:02}",
                    self.hour(),
                    self.minute(),
                    self.second(),
                )
                .as_bytes(),
            )
        } else {
            w.write_lenenc_str(
                format!(
                    "{:02}:{:02}:{:02}.{:06}",
                    self.hour(),
                    self.minute(),
                    self.second(),
                    us
                )
                .as_bytes(),
            )
        }
        .map(|_| ())
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
            ColumnType::MYSQL_TYPE_TIME => {
                w.write_u8(0x0cu8)?;
                w.write_u8(1u8)?; // sign
                w.write_u32::<LittleEndian>(0u32)?; // days, unused for NaiveTime
                w.write_u8(self.hour() as u8)?;
                w.write_u8(self.minute() as u8)?;
                w.write_u8(self.second() as u8)?;
                w.write_u32::<LittleEndian>(self.nanosecond())
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for MySqlTime {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_lenenc_str(self.to_string().as_bytes()).map(|_| ())
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let sign = u8::from(!self.is_positive());
        match c.coltype {
            ColumnType::MYSQL_TYPE_TIME => {
                w.write_u8(0x0cu8)?;
                w.write_u8(sign)?; // 0: positive, 1: negative
                w.write_u32::<LittleEndian>((self.hour() / 24) as u32)?;
                w.write_u8((self.hour() % 24) as u8)?;
                w.write_u8(self.minutes())?;
                w.write_u8(self.seconds())?;
                w.write_u32::<LittleEndian>(self.microseconds())
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for TimestampTz {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_lenenc_str(self.to_string().as_bytes()).map(|_| ())
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let ts = self.to_chrono();
        match c.coltype {
            ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
                // To save space the packet can be compressed:
                // if year, month, day, hour, minutes, seconds and microseconds are all 0, length is
                // 0 and no other field is sent. if hour, seconds and microseconds
                // are all 0, length is 4 and no other field is sent.
                // if microseconds is 0, length is 7 and micro_seconds is not sent.
                // otherwise the length is 11
                // TODO(marce): Currently TimestampTZ will produce NULL for zero/invalid dates.
                let us = ts.nanosecond() / 1_000;
                let packet_len = if us != 0 {
                    11
                } else if (ts.hour(), ts.minute(), ts.second()) != (0, 0, 0) {
                    7
                } else if (ts.year(), ts.month(), ts.day()) != (0, 0, 0) {
                    4
                } else {
                    0
                };

                w.write_u8(packet_len)?;

                if packet_len == 0 {
                    return Ok(()); // no need to write anything else
                }

                w.write_u16::<LittleEndian>(ts.year() as u16)?;
                w.write_u8(ts.month() as u8)?;
                w.write_u8(ts.day() as u8)?;

                if packet_len == 4 {
                    return Ok(()); // no need to write time
                }

                w.write_u8(ts.hour() as u8)?;
                w.write_u8(ts.minute() as u8)?;
                w.write_u8(ts.second() as u8)?;

                if packet_len == 7 {
                    return Ok(()); // no need to write microseconds
                }

                w.write_u32::<LittleEndian>(us)?;

                Ok(())
            }
            ColumnType::MYSQL_TYPE_DATE => {
                if ts.time() != NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
                    return Err(bad(self, c));
                }
                ts.date_naive().to_mysql_bin(w, c)
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for NaiveDateTime {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let us = self.nanosecond() / 1_000;

        if us != 0 {
            w.write_lenenc_str(
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    self.year(),
                    self.month(),
                    self.day(),
                    self.hour(),
                    self.minute(),
                    self.second(),
                    us
                )
                .as_bytes(),
            )
            .map(|_| ())
        } else {
            w.write_lenenc_str(
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    self.year(),
                    self.month(),
                    self.day(),
                    self.hour(),
                    self.minute(),
                    self.second()
                )
                .as_bytes(),
            )
            .map(|_| ())
        }
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match c.coltype {
            ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
                let us = self.nanosecond() / 1_000;

                if us != 0 {
                    w.write_u8(11u8)?;
                } else {
                    w.write_u8(7u8)?;
                }
                w.write_u16::<LittleEndian>(self.year() as u16)?;
                w.write_u8(self.month() as u8)?;
                w.write_u8(self.day() as u8)?;
                w.write_u8(self.hour() as u8)?;
                w.write_u8(self.minute() as u8)?;
                w.write_u8(self.second() as u8)?;

                if us != 0 {
                    w.write_u32::<LittleEndian>(us)?;
                }
                Ok(())
            }
            ColumnType::MYSQL_TYPE_DATE => {
                if self.time() != NaiveTime::from_hms_opt(0, 0, 0).unwrap() {
                    return Err(bad(self, c));
                }
                self.date().to_mysql_bin(w, c)
            }
            _ => Err(bad(self, c)),
        }
    }
}

use std::time::Duration;

impl ToMySqlValue for Duration {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let s = self.as_secs();
        //let d = s / (24 * 3600);
        // assert!(d <= 34);
        //let h = (s % (24 * 3600)) / 3600;
        let h = s / 3600;
        let m = (s % 3600) / 60;
        let s = s % 60;
        let us = self.subsec_micros();
        if us != 0 {
            w.write_lenenc_str(format!("{:02}:{:02}:{:02}.{:06}", h, m, s, us).as_bytes())
                .map(|_| ())
        } else {
            w.write_lenenc_str(format!("{:02}:{:02}:{:02}", h, m, s).as_bytes())
                .map(|_| ())
        }
    }

    #[allow(clippy::many_single_char_names)]
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        let s = self.as_secs();
        let d = s / (24 * 3600);
        assert!(d <= 34);
        let h = (s % (24 * 3600)) / 3600;
        let m = (s % 3600) / 60;
        let s = s % 60;
        let us = self.subsec_micros();

        match c.coltype {
            ColumnType::MYSQL_TYPE_TIME => {
                if us != 0 {
                    w.write_u8(12u8)?;
                } else {
                    w.write_u8(9u8)?;
                }

                w.write_u8(0u8)?; // positive only (for now)
                w.write_u32::<LittleEndian>(d as u32)?;
                w.write_u8(h as u8)?;
                w.write_u8(m as u8)?;
                w.write_u8(s as u8)?;

                if us != 0 {
                    w.write_u32::<LittleEndian>(us)?;
                }
                Ok(())
            }
            _ => Err(bad(self, c)),
        }
    }
}

impl ToMySqlValue for myc::value::Value {
    #[allow(clippy::many_single_char_names)]
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match *self {
            myc::value::Value::NULL => None::<u8>.to_mysql_text(w),
            myc::value::Value::Bytes(ref bytes) => bytes.to_mysql_text(w),
            myc::value::Value::Int(n) => n.to_mysql_text(w),
            myc::value::Value::UInt(n) => n.to_mysql_text(w),
            myc::value::Value::Float(f) => f.to_mysql_text(w),
            myc::value::Value::Double(d) => d.to_mysql_text(w),
            myc::value::Value::Date(y, mo, d, h, mi, s, us) => {
                NaiveDate::from_ymd_opt(i32::from(y), u32::from(mo), u32::from(d))
                    .unwrap()
                    .and_hms_micro_opt(u32::from(h), u32::from(mi), u32::from(s), us)
                    .unwrap()
                    .to_mysql_text(w)
            }
            myc::value::Value::Time(neg, d, h, m, s, us) => {
                if neg {
                    return Err(other_error(OtherErrorKind::Unexpected {
                        error: "negative times not yet supported".to_string(),
                    }));
                }
                (chrono::Duration::days(i64::from(d))
                    + chrono::Duration::hours(i64::from(h))
                    + chrono::Duration::minutes(i64::from(m))
                    + chrono::Duration::seconds(i64::from(s))
                    + chrono::Duration::microseconds(i64::from(us)))
                .to_std()
                .map_err(|_| {
                    other_error(OtherErrorKind::Unexpected {
                        error: "negative times not yet supported".to_string(),
                    })
                })?
                .to_mysql_text(w)
            }
        }
    }

    #[allow(clippy::many_single_char_names)]
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match *self {
            myc::value::Value::NULL => Err(other_error(OtherErrorKind::Unexpected {
                error: "NULL value not expected".to_string(),
            })),
            myc::value::Value::Bytes(ref bytes) => bytes.to_mysql_bin(w, c),
            myc::value::Value::Int(n) => {
                // we *could* just delegate to i64 impl here, but then you couldn't use
                // myc::value::Value and return, say, a short. also, myc uses i64
                // for *every* number type, *except* u64, so we even need to coerce
                // across unsigned :( the good news is that our impls for numbers
                // auto-upgrade to wider coltypes, so we can just downcast to the
                // smallest containing type, and then call on that
                let signed = !c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
                if signed {
                    if n >= i64::from(i8::MIN) && n <= i64::from(i8::MAX) {
                        (n as i8).to_mysql_bin(w, c)
                    } else if n >= i64::from(i16::MIN) && n <= i64::from(i16::MAX) {
                        (n as i16).to_mysql_bin(w, c)
                    } else if n >= i64::from(i32::MIN) && n <= i64::from(i32::MAX) {
                        (n as i32).to_mysql_bin(w, c)
                    } else {
                        n.to_mysql_bin(w, c)
                    }
                } else if n < 0 {
                    Err(bad(self, c))
                } else if n <= i64::from(u8::MAX) {
                    (n as u8).to_mysql_bin(w, c)
                } else if n <= i64::from(u16::MAX) {
                    (n as u16).to_mysql_bin(w, c)
                } else if n <= i64::from(u32::MAX) {
                    (n as u32).to_mysql_bin(w, c)
                } else {
                    // must work since u64::MAX > i64::MAX, and n >= 0
                    (n as u64).to_mysql_bin(w, c)
                }
            }
            myc::value::Value::UInt(n) => {
                // we are not as lenient with unsigned ints because the mysql crate isn't either
                n.to_mysql_bin(w, c)
            }
            myc::value::Value::Float(f) => f.to_mysql_bin(w, c),
            myc::value::Value::Double(d) => d.to_mysql_bin(w, c),
            myc::value::Value::Date(..) => {
                let mut buf = Vec::new();
                self.serialize(&mut buf);
                w.write_all(buf.as_slice())
            }
            myc::value::Value::Time(neg, d, h, m, s, us) => {
                if neg {
                    return Err(other_error(OtherErrorKind::Unexpected {
                        error: "negative times not yet supported".to_string(),
                    }));
                }
                (chrono::Duration::days(i64::from(d))
                    + chrono::Duration::hours(i64::from(h))
                    + chrono::Duration::minutes(i64::from(m))
                    + chrono::Duration::seconds(i64::from(s))
                    + chrono::Duration::microseconds(i64::from(us)))
                .to_std()
                .map_err(|_| {
                    other_error(OtherErrorKind::Unexpected {
                        error: "negative times not yet supported".to_string(),
                    })
                })?
                .to_mysql_bin(w, c)
            }
        }
    }

    fn is_null(&self) -> bool {
        matches!(*self, myc::value::Value::NULL)
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use std::convert::TryFrom;
    use std::time;

    use chrono::{self, TimeZone};

    use super::ToMySqlValue;
    use crate::myc::io::ParseBuf;
    use crate::myc::proto::MyDeserialize;
    use crate::myc::value::convert::from_value;
    use crate::myc::value::{BinValue, TextValue, Value, ValueDeserializer};
    use crate::{Column, ColumnFlags, ColumnType};

    mod roundtrip_text {
        use mysql_time::MySqlTime;

        use super::*;

        macro_rules! rt {
            ($name:ident, $t:ty, $v:expr) => {
                #[test]
                fn $name() {
                    let mut data = Vec::new();
                    let v: $t = $v;
                    v.to_mysql_text(&mut data).unwrap();
                    let mut buf = ParseBuf(&data[..]);
                    let value = ValueDeserializer::<TextValue>::deserialize((), &mut buf)
                        .unwrap()
                        .0;
                    assert_eq!(from_value::<$t>(value), v);
                }
            };
        }

        rt!(u8_one, u8, 1);
        rt!(i8_one, i8, 1);
        rt!(u16_one, u16, 1);
        rt!(i16_one, i16, 1);
        rt!(u32_one, u32, 1);
        rt!(i32_one, i32, 1);
        rt!(u64_one, u64, 1);
        rt!(i64_one, i64, 1);
        rt!(f32_one, f32, 1.0);
        rt!(f64_one, f64, 1.0);

        rt!(u8_max, u8, u8::MAX);
        rt!(i8_max, i8, i8::MAX);
        rt!(u16_max, u16, u16::MAX);
        rt!(i16_max, i16, i16::MAX);
        rt!(u32_max, u32, u32::MAX);
        rt!(i32_max, i32, i32::MAX);
        rt!(u64_max, u64, u64::MAX);
        rt!(i64_max, i64, i64::MAX);

        rt!(opt_none, Option<u8>, None);
        rt!(opt_some, Option<u8>, Some(1));

        rt!(time, chrono::NaiveDate, chrono::Local::now().date_naive());
        rt!(
            datetime,
            chrono::NaiveDateTime,
            chrono::Utc
                .with_ymd_and_hms(1989, 12, 7, 8, 0, 4)
                .unwrap()
                .naive_utc()
        );
        rt!(dur, time::Duration, time::Duration::from_secs(1893));
        rt!(bytes, Vec<u8>, vec![0x42, 0x00, 0x1a]);
        rt!(string, String, "foobar".to_owned());
    }

    mod roundtrip_bin {
        use mysql_time::MySqlTime;

        use super::*;

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
                        column_length: None,
                        colflags: ColumnFlags::empty(),
                        character_set: 33,
                    };

                    if !$sig {
                        col.colflags.insert(ColumnFlags::UNSIGNED_FLAG);
                    }

                    let v: $t = $v;
                    v.to_mysql_bin(&mut data, &col).unwrap();
                    let mut buf = ParseBuf(&data[..]);
                    let value = ValueDeserializer::<BinValue>::deserialize(
                        (col.coltype, col.colflags),
                        &mut buf,
                    )
                    .unwrap()
                    .0;
                    assert_eq!(from_value::<$t>(value), v);
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

        rt!(f32_one, f32, 1.0, ColumnType::MYSQL_TYPE_FLOAT, false);
        /* See ENG-385
        rt!(
            f32_one_double,
            f32,
            1.0,
            ColumnType::MYSQL_TYPE_DOUBLE,
            false
        );*/
        rt!(f64_one, f64, 1.0, ColumnType::MYSQL_TYPE_DOUBLE, false);

        rt!(u8_max, u8, u8::MAX, ColumnType::MYSQL_TYPE_TINY, false);
        rt!(i8_max, i8, i8::MAX, ColumnType::MYSQL_TYPE_TINY, true);
        rt!(u16_max, u16, u16::MAX, ColumnType::MYSQL_TYPE_SHORT, false);
        rt!(i16_max, i16, i16::MAX, ColumnType::MYSQL_TYPE_SHORT, true);
        rt!(u32_max, u32, u32::MAX, ColumnType::MYSQL_TYPE_LONG, false);
        rt!(i32_max, i32, i32::MAX, ColumnType::MYSQL_TYPE_LONG, true);
        rt!(
            u64_max,
            u64,
            u64::MAX,
            ColumnType::MYSQL_TYPE_LONGLONG,
            false
        );
        rt!(
            i64_max,
            i64,
            i64::MAX,
            ColumnType::MYSQL_TYPE_LONGLONG,
            true
        );

        rt!(opt_some, Option<u8>, Some(1), ColumnType::MYSQL_TYPE_TINY);

        rt!(
            date,
            chrono::NaiveDate,
            chrono::Local::now().date_naive(),
            ColumnType::MYSQL_TYPE_DATE
        );
        rt!(
            datetime,
            chrono::NaiveDateTime,
            chrono::Utc
                .with_ymd_and_hms(1989, 12, 7, 8, 0, 4)
                .single()
                .unwrap()
                .naive_utc(),
            ColumnType::MYSQL_TYPE_DATETIME
        );
        rt!(
            time,
            MySqlTime,
            MySqlTime::from_hmsus(true, 20, 15, 14, 123_456),
            ColumnType::MYSQL_TYPE_TIME
        );
        rt!(
            bytes,
            Vec<u8>,
            vec![0x42, 0x00, 0x1a],
            ColumnType::MYSQL_TYPE_BLOB
        );
        rt!(
            string,
            String,
            "foobar".to_owned(),
            ColumnType::MYSQL_TYPE_STRING
        );
        rt!(
            mysql_date_zero,
            myc::value::Value,
            myc::value::Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32),
            ColumnType::MYSQL_TYPE_DATE
        );
        rt!(
            mysql_date_only,
            myc::value::Value,
            myc::value::Value::Date(2024u16, 1u8, 1u8, 0u8, 0u8, 0u8, 0u32),
            ColumnType::MYSQL_TYPE_DATE
        );
        rt!(
            mysql_datetime_zero,
            myc::value::Value,
            myc::value::Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32),
            ColumnType::MYSQL_TYPE_DATETIME
        );
        rt!(
            mysql_datetime_date_only,
            myc::value::Value,
            myc::value::Value::Date(2024u16, 1u8, 1u8, 0u8, 0u8, 0u8, 0u32),
            ColumnType::MYSQL_TYPE_DATETIME
        );
        rt!(
            mysql_datetime_date_invalid,
            myc::value::Value,
            myc::value::Value::Date(2024u16, 2u8, 31u8, 0u8, 0u8, 0u8, 0u32),
            ColumnType::MYSQL_TYPE_DATETIME
        );
    }
}
