//! Formatting utilities.

use std::fmt::*;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

const DIGIT_TABLE: [&[u8; 2]; 100] = [
    b"00", b"01", b"02", b"03", b"04", b"05", b"06", b"07", b"08", b"09", b"10", b"11", b"12",
    b"13", b"14", b"15", b"16", b"17", b"18", b"19", b"20", b"21", b"22", b"23", b"24", b"25",
    b"26", b"27", b"28", b"29", b"30", b"31", b"32", b"33", b"34", b"35", b"36", b"37", b"38",
    b"39", b"40", b"41", b"42", b"43", b"44", b"45", b"46", b"47", b"48", b"49", b"50", b"51",
    b"52", b"53", b"54", b"55", b"56", b"57", b"58", b"59", b"60", b"61", b"62", b"63", b"64",
    b"65", b"66", b"67", b"68", b"69", b"70", b"71", b"72", b"73", b"74", b"75", b"76", b"77",
    b"78", b"79", b"80", b"81", b"82", b"83", b"84", b"85", b"86", b"87", b"88", b"89", b"90",
    b"91", b"92", b"93", b"94", b"95", b"96", b"97", b"98", b"99",
];

// const USECS_PER_DAY: i64 = USECS_PER_HOUR * 24;
const SECS_PER_HOUR: i32 = 60 * 60;
const SECS_PER_MINUTE: i32 = 60;

/// Like [`std::format_args!`] but with ownership of arguments.
#[macro_export]
macro_rules! fmt_args {
    ($($tt:tt)+) => {
        $crate::fmt::fmt_with(move |__format_args_formatter__| {
            ::std::write!(__format_args_formatter__, $($tt)+)
        })
    };
}

/// See [`fmt_with()`].
#[derive(Clone, Copy)]
pub struct FmtWith<F = fn(&mut Formatter) -> Result> {
    fmt: F,
}

/// Formats via a closure.
pub fn fmt_with<F: Fn(&mut Formatter) -> Result>(fmt: F) -> FmtWith<F> {
    fmt.into()
}

impl<F: Fn(&mut Formatter) -> Result> From<F> for FmtWith<F> {
    fn from(fmt: F) -> Self {
        Self { fmt }
    }
}

impl<F: Fn(&mut Formatter) -> Result> Debug for FmtWith<F> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        (self.fmt)(f)
    }
}

impl<F: Fn(&mut Formatter) -> Result> Display for FmtWith<F> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        (self.fmt)(f)
    }
}

fn write_u32_inner(mut value: u32, length: u32, dst: &mut BytesMut) {
    if value == 0 {
        dst.put_u8(b'0');
    } else {
        dst.reserve(length as usize);
        let a = dst.len();

        for _ in 0..length {
            dst.put_u8(0);
        }

        let buf = dst.as_mut();
        let mut i = 0;

        while value >= 10000 {
            let c = value - 10000 * (value / 10000);
            let c0 = c % 100;
            let c1 = c / 100;
            let pos = a + length as usize - i;

            value /= 10000;

            buf[(pos - 2)..pos].copy_from_slice(DIGIT_TABLE[c0 as usize]);
            buf[(pos - 4)..(pos - 2)].copy_from_slice(DIGIT_TABLE[c1 as usize]);
            i += 4;
        }

        if value >= 100 {
            let c = value % 100;
            let pos = a + length as usize - i;

            value /= 100;

            buf[(pos - 2)..pos].copy_from_slice(DIGIT_TABLE[c as usize]);
            i += 2;
        }

        if value >= 10 {
            let pos = a + length as usize - i;

            buf[(pos - 2)..pos].copy_from_slice(DIGIT_TABLE[value as usize]);
        } else {
            buf[a] = b'0' + value as u8;
        }
    }
}

/// docs
pub fn write_u32(value: u32, dst: &mut BytesMut) {
    write_u32_inner(value, u32_length(value), dst)
}

/// docs
pub fn u32_length(value: u32) -> u32 {
    const POWERS_OF_TEN: [u32; 10] = [
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
    ];

    if value == 0 {
        1
    } else {
        let t = ((31 - value.leading_zeros()) + 1) * 1233 / 4096;

        if value >= POWERS_OF_TEN[t as usize] {
            t + 1
        } else {
            t
        }
    }
}

/// docs
pub fn write_padded_u32(value: u32, min_width: u32, dst: &mut BytesMut) {
    if value < 100 && min_width == 2 {
        dst.put_slice(DIGIT_TABLE[value as usize]);
    } else {
        let length = u32_length(value);
        let padding = if min_width > length {
            min_width - length
        } else {
            0
        };

        for _ in 0..padding {
            dst.put_u8(b'0');
        }

        write_u32_inner(value, length, dst);
    }
}

/// docs
pub fn write_date(dst: &mut BytesMut, date: NaiveDate) {
    let year = date.year();
    let month = date.month();
    let day = date.day();

    write_date_inner(year, month, day, dst);
}

fn write_date_inner(year: i32, month: u32, day: u32, dst: &mut BytesMut) {
    // There is no year 0
    let year = if year > 0 {
        year as u32
    } else {
        dst.put_u8(b'-');
        -year as u32
    };
    write_padded_u32(year, 4, dst);
    dst.put_u8(b'-');
    write_padded_u32(month, 2, dst);
    dst.put_u8(b'-');
    write_padded_u32(day, 2, dst);
}

/// docs
pub fn write_seconds_fraction(mut fraction: u32, mut precision: u32, dst: &mut BytesMut) {
    let mut gotnonzero = false;

    dst.put_u8(b'.');

    while precision > 0 {
        let oldval = fraction;
        fraction /= 10;
        let remainder = oldval - fraction * 10;

        if remainder > 0 {
            gotnonzero = true;
        }

        if gotnonzero {
            dst.put_u8(b'0' + remainder as u8);
            precision -= 1;
        } else {
            break;
        }
    }
}

/// docs
pub fn write_time(dst: &mut BytesMut, ts: NaiveTime) {
    write_padded_u32(ts.hour(), 2, dst);
    dst.put_u8(b':');
    write_padded_u32(ts.minute(), 2, dst);
    dst.put_u8(b':');
    write_padded_u32(ts.second(), 2, dst);

    let nanos = ts.nanosecond();

    if nanos > 0 {
        write_seconds_fraction(nanos, 6, dst);
    }
}

/// docs
pub fn write_timestamp(dst: &mut BytesMut, ts: NaiveDateTime) {
    let year = ts.year();
    let month = ts.month();
    let day = ts.day();

    write_date_inner(year, month, day, dst);

    dst.put_u8(b' ');

    write_time(dst, ts.time());
}

/// docs
pub fn write_timestamp_tz(dst: &mut BytesMut, ts: DateTime<FixedOffset>) {
    write_timestamp(dst, ts.naive_local());

    let mut tz_offset_secs = ts.timezone().local_minus_utc();

    if tz_offset_secs >= 0 {
        dst.put_slice(b" +");
    } else {
        dst.put_slice(b" -");
        tz_offset_secs = -tz_offset_secs;
    }

    let tz_hour = tz_offset_secs / SECS_PER_HOUR;
    tz_offset_secs -= tz_hour * SECS_PER_HOUR;
    let tz_min = tz_offset_secs / SECS_PER_MINUTE;
    tz_offset_secs -= tz_min * SECS_PER_MINUTE;

    write_padded_u32(tz_hour as u32, 2, dst);
    dst.put_u8(b':');
    write_padded_u32(tz_min as u32, 2, dst);

    if tz_offset_secs > 0 {
        dst.put_u8(b':');
        write_padded_u32(tz_offset_secs as u32, 2, dst);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use bytes::BytesMut;
    use proptest::prelude::*;

    use crate::arbitrary::{arbitrary_date_time, arbitrary_naive_date, arbitrary_naive_time};

    #[test]
    fn u32_length() {
        assert_eq!(super::u32_length(1890481092), 10);
        assert_eq!(super::u32_length(129312), 6);
        assert_eq!(super::u32_length(1), 1);
    }

    #[test]
    fn write_u32() {
        let mut buf = BytesMut::new();
        super::write_u32(0, &mut buf);
        assert_eq!(b"0".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(1, &mut buf);
        assert_eq!(b"1".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(10, &mut buf);
        assert_eq!(b"10".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(100, &mut buf);
        assert_eq!(b"100".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(10000, &mut buf);
        assert_eq!(b"10000".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(10001, &mut buf);
        assert_eq!(b"10001".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(1000011, &mut buf);
        assert_eq!(b"1000011".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(1024, &mut buf);
        assert_eq!(b"1024".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_u32(10248192, &mut buf);
        assert_eq!(b"10248192".as_slice(), buf);
    }

    #[test]
    fn write_padded_u32() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 1, &mut buf);
        assert_eq!(b"1".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 2, &mut buf);
        assert_eq!(b"01".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(10, 3, &mut buf);
        assert_eq!(b"010".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(100, 3, &mut buf);
        assert_eq!(b"100".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(100, 4, &mut buf);
        assert_eq!(b"0100".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(1000, 4, &mut buf);
        assert_eq!(b"1000".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(1000, 5, &mut buf);
        assert_eq!(b"01000".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(10000, 5, &mut buf);
        assert_eq!(b"10000".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(10000, 6, &mut buf);
        assert_eq!(b"010000".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 10, &mut buf);
        assert_eq!(b"0000000001".as_slice(), buf);

        let mut buf = BytesMut::new();
        super::write_padded_u32(1293012, 10, &mut buf);
        assert_eq!(b"0001293012".as_slice(), buf);
    }

    #[test]
    fn write_date() {
        use chrono::NaiveDate;

        let mut buf = BytesMut::new();
        let date = NaiveDate::from_ymd_opt(2024, 2, 27).unwrap();
        super::write_date(&mut buf, date);
        assert_eq!(b"2024-02-27".as_slice(), buf);

        let mut buf = BytesMut::new();
        let date = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        super::write_date(&mut buf, date);
        assert_eq!(b"2024-02-01".as_slice(), buf);
    }

    #[test]
    fn write_timestamptz() {
        const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %:z";
        use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

        let mut actual = BytesMut::new();
        let dt = FixedOffset::east_opt(0)
            .unwrap()
            .from_utc_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1995, 6, 21).unwrap(),
                NaiveTime::from_hms_opt(12, 34, 56).unwrap(),
            ));
        super::write_timestamp_tz(&mut actual, dt);

        let expected = format!("{}", dt.format(TIMESTAMP_TZ_FORMAT));

        assert_eq!(expected, actual);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1000000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn test_write_u32(i in 0..1_000_000_000u32) {
            let mut actual = BytesMut::new();
            super::write_u32(i, &mut actual);

            let mut expected = BytesMut::new();
            write!(expected, "{i}").unwrap();

            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn test_u32_length(i in 0..u32::MAX) {
            let actual = super::u32_length(i);
            let expected = format!("{i}").len() as u32;

            assert_eq!(actual, expected);
        }

        #[test]
        fn test_write_padded_u32(i in 0..100_000u32, min_width in 1..8u32) {
            let mut actual = BytesMut::new();
            super::write_padded_u32(i, min_width, &mut actual);

            let mut expected = BytesMut::new();
            write!(expected, "{:01$}", i, min_width as usize).unwrap();

            assert_eq!(actual, expected);
        }

        #[test]
        fn test_write_date(date in arbitrary_naive_date()) {
            const DATE_FORMAT: &str = "%Y-%m-%d";

            let mut actual = BytesMut::new();
            super::write_date(&mut actual, date);

            let mut expected = BytesMut::new();
            let output = date.format(DATE_FORMAT).to_string();
            let slice = output.trim_start_matches('+');

            write!(expected, "{}", slice).unwrap();

            assert_eq!(actual, expected);
        }

        #[test]
        fn test_write_time(time in arbitrary_naive_time()) {
            const TIME_FORMAT: &str = "%H:%M:%S%.f";

            let mut actual = BytesMut::new();
            super::write_time(&mut actual, time);

            let mut expected = BytesMut::new();
            let output = time.format(TIME_FORMAT).to_string();
            write!(expected, "{}", output).unwrap();

            assert_eq!(actual, expected);
        }

        #[test]
        fn test_write_timestamptz(ts in arbitrary_date_time()) {
            let mut actual = BytesMut::new();
            super::write_timestamp_tz(&mut actual, ts);

            let mut expected = BytesMut::new();

            let output = if ts.timezone().local_minus_utc() % 60 == 0 {
                const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %:z";
               ts.format(TIMESTAMP_TZ_FORMAT).to_string()
            } else {
               const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %::z";
               ts.format(TIMESTAMP_TZ_FORMAT).to_string()
            };

            let slice = output.trim_start_matches('+');
            write!(expected, "{}", slice).unwrap();

            assert_eq!(actual, expected);
        }
    }
}
