//! Utilities for formatting dates and times.

use bytes::{BufMut, BytesMut};
use chrono::round::SubsecRound;
use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::fmt::{num, FastEncode};

impl FastEncode for NaiveDate {
    fn put(self, dst: &mut BytesMut) {
        write_date_inner(&self, dst);

        if self.year() < 0 {
            dst.put_slice(b" BC");
        }
    }
}

fn write_date_inner<T>(t: &T, dst: &mut BytesMut)
where
    T: Datelike,
{
    let mut year = t.year();
    if year < 0 {
        year -= 1;
    }

    num::write_padded_u32(year.unsigned_abs(), 4, dst);
    dst.put_u8(b'-');
    num::write_padded_u32(t.month(), 2, dst);
    dst.put_u8(b'-');
    num::write_padded_u32(t.day(), 2, dst);
}

impl FastEncode for NaiveTime {
    fn put(mut self, dst: &mut BytesMut) {
        let prev_hour = self.hour();
        self = self.round_subsecs(6);
        let new_hour = self.hour();

        // When rounding up from 23:59:59.999999XXX, Postgres rounds to 24:00:00
        if new_hour == 0 && prev_hour == 23 {
            num::write_padded_u32(24, 2, dst);
        } else {
            num::write_padded_u32(new_hour, 2, dst);
        }

        dst.put_u8(b':');
        num::write_padded_u32(self.minute(), 2, dst);
        dst.put_u8(b':');
        num::write_padded_u32(self.second(), 2, dst);

        write_seconds_fraction(&self, dst);
    }
}

fn write_seconds_fraction<T>(t: &T, dst: &mut BytesMut)
where
    T: Timelike,
{
    const NANOS_PER_MICRO: u32 = 1000;
    const SECS_FRACTION_PRECISION: usize = 6;

    let nanos = t.nanosecond();

    if nanos >= NANOS_PER_MICRO {
        let mut micros = nanos / NANOS_PER_MICRO;
        let mut got_non_zero = false;

        let mut end = dst.len();
        dst.put_slice(b".000000");

        let len = dst.len();
        let buf = dst.as_mut();

        for i in 0..SECS_FRACTION_PRECISION {
            let oldval = micros;
            micros /= 10;
            let remainder = oldval - micros * 10;

            if remainder > 0 && !got_non_zero {
                got_non_zero = true;
                end = end + 1 + (SECS_FRACTION_PRECISION - i);
            }

            buf[len - i - 1] = b'0' + remainder as u8;
        }

        dst.truncate(end);
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use chrono::{NaiveDate, NaiveTime};

    use crate::fmt::FastEncode;

    #[test]
    fn test_write_time_fractional_round_up() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 500).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00.000001", actual);
    }

    #[test]
    fn test_write_time_fractional_round_down() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 499).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00", actual);
    }

    #[test]
    fn test_write_time_round_up_to_whole_second() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 999_999_900).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:01", actual);
    }

    #[test]
    fn test_write_time_round_down_to_zero() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 100).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00", actual);
    }

    #[test]
    fn test_write_time_round_up_to_24() {
        let time = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_900).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("24:00:00", actual);
    }

    mod postgres_oracle {
        use std::env;

        use chrono::{Datelike, Timelike};
        use postgres::{Client, Config, NoTls, SimpleQueryMessage};

        use super::*;

        fn config() -> Config {
            let mut config = Config::new();
            config
                .host(env::var("PGHOST").as_deref().unwrap_or("localhost"))
                .port(
                    env::var("PGPORT")
                        .unwrap_or_else(|_| "5432".into())
                        .parse()
                        .unwrap(),
                )
                .user(env::var("PGUSER").as_deref().unwrap_or("postgres"))
                .password(env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()));
            config
        }

        fn postgres_query(query: &str, client: &mut Client) -> String {
            match client
                .simple_query(query)
                .unwrap()
                .first()
                .unwrap()
                .to_owned()
            {
                SimpleQueryMessage::Row(r) => r.get(0).unwrap().to_owned(),
                _ => panic!(),
            }
        }

        fn compare_time_format(time: NaiveTime, client: &mut Client) {
            let mut actual = BytesMut::new();
            time.put(&mut actual);

            let query = format!(
                "SELECT make_time({}, {}, {})",
                time.hour(),
                time.minute(),
                time.format("%S%.f"),
            );
            let expected = postgres_query(&query, client);

            assert_eq!(expected, actual);
        }

        fn compare_date_format(date: NaiveDate, client: &mut Client) {
            let mut actual = BytesMut::new();
            date.put(&mut actual);

            let mut year = date.year();
            if year < 0 {
                year -= 1;
            }

            let query = format!(
                "SELECT make_date({}, {}, {})",
                year,
                date.month(),
                date.day()
            );
            let expected = postgres_query(&query, client);

            assert_eq!(expected, actual);
        }

        #[test]
        fn test_dates_same_as_postgres() {
            let mut client = config().connect(NoTls).unwrap();

            for t in [
                NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
                NaiveDate::from_ymd_opt(-1, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(10000, 10, 10).unwrap(),
            ] {
                compare_date_format(t, &mut client);
            }
        }

        #[test]
        fn test_times_same_as_postgres() {
            let mut client = config().connect(NoTls).unwrap();

            for t in [
                NaiveTime::from_hms_opt(12, 34, 56).unwrap(),
                NaiveTime::from_hms_micro_opt(12, 34, 56, 123456).unwrap(),
            ] {
                compare_time_format(t, &mut client);
            }
        }

        mod proptests {
            use std::cell::RefCell;

            use proptest::prelude::*;

            use super::*;
            use crate::arbitrary::{arbitrary_naive_date, arbitrary_naive_time};

            #[test]
            fn test_write_date() {
                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(1000000), |(date in arbitrary_naive_date())| {
                    compare_date_format(date, &mut client.borrow_mut());
                })
            }

            #[test]
            fn test_write_time() {
                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(1000000), |(time in arbitrary_naive_time())| {
                    compare_time_format(time, &mut client.borrow_mut());
                })
            }
        }
    }
}
