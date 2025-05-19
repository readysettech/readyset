//! Utilities for formatting dates and times.

use bytes::{BufMut, BytesMut};
use chrono::round::SubsecRound;
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

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

impl FastEncode for NaiveDateTime {
    /// Writes the given [`NaiveDateTime`] to the given [`BytesMut`] using the format that Postgres
    /// uses to display timestamps without time zones.
    ///
    /// This function diverges from Postgres's formatting of timestamps when it comes to the
    /// precision of the seconds fraction. When a timestamp includes fractional seconds,
    /// Postgres sometimes formats the timestamp with a fractional part that is inconsistent
    /// with the input. This function will always round to the nearest 6 decimal places
    /// according to the decimal digit in the 7th position. See [this Stackoverflow answer](https://stackoverflow.com/a/42148967) for more
    /// details.
    fn put(mut self, dst: &mut BytesMut) {
        self = self.round_subsecs(6);
        write_timestamp_inner(&self, dst);

        if self.year() < 0 {
            dst.put_slice(b" BC");
        }
    }
}

fn write_timestamp_inner<T>(t: &T, dst: &mut BytesMut)
where
    T: Datelike + Timelike,
{
    write_date_inner(t, dst);

    dst.put_u8(b' ');

    num::write_padded_u32(t.hour(), 2, dst);
    dst.put_u8(b':');
    num::write_padded_u32(t.minute(), 2, dst);
    dst.put_u8(b':');
    num::write_padded_u32(t.second(), 2, dst);

    write_seconds_fraction(t, dst);
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

impl FastEncode for DateTime<FixedOffset> {
    /// Writes the given [`DateTime`] to the given [`BytesMut`] using the format that Postgres uses
    /// to display timestamps with time zones.
    ///
    /// There are a few places where this formatting function diverges from Postgres's formatting:
    /// - When a timestamp includes fractional seconds, Postgres sometimes formats the timestamp with
    ///   a fractional part that is inconsistent with the input. This function will always round to the
    ///   nearest 6 decimal places according to the decimal digit in the 7th position. See
    ///   [this Stackoverflow answer](https://stackoverflow.com/a/42148967) for more details
    /// - Both chrono-tz (the library we use to handle timezones) and Postgres rely on the upstream
    ///   [IANA tz database](https://www.iana.org/time-zones), but the versions of these databases
    ///   being used differs across minor chrono-tz/Postgres versions. This means that, for certain
    ///   ranges of dates for certain timezones, we may display a timezone offset that differs from
    ///   Postgres's. Realistically, the differences here will only affect very specific ranges of
    ///   dates in very specific timezones and will very likely not affect the dates most often used in
    ///   Postgres databases
    /// - Due to a standing bug in chrono-tz, this function will not correctly handle daylight savings
    ///   time for years past 2099. See
    ///   [this Github issue](https://github.com/chronotope/chrono-tz/issues/155) for more information
    fn put(self, dst: &mut BytesMut) {
        const SECS_PER_HOUR: i32 = 60 * 60;
        const SECS_PER_MINUTE: i32 = 60;

        write_timestamp_inner(&self, dst);

        let mut tz_offset_secs = self.timezone().local_minus_utc();

        if tz_offset_secs >= 0 {
            dst.put_slice(b"+");
        } else {
            dst.put_slice(b"-");
            tz_offset_secs = -tz_offset_secs;
        }

        let tz_hour = tz_offset_secs / SECS_PER_HOUR;
        tz_offset_secs -= tz_hour * SECS_PER_HOUR;
        let tz_min = tz_offset_secs / SECS_PER_MINUTE;
        tz_offset_secs -= tz_min * SECS_PER_MINUTE;

        num::write_padded_u32(tz_hour as u32, 2, dst);

        if tz_min > 0 || tz_offset_secs > 0 {
            dst.put_u8(b':');
            num::write_padded_u32(tz_min as u32, 2, dst);
        }

        if tz_offset_secs > 0 {
            dst.put_u8(b':');
            num::write_padded_u32(tz_offset_secs as u32, 2, dst);
        }

        if self.year() < 0 {
            dst.put_slice(b" BC");
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
    use chrono_tz::{OffsetComponents, Tz};

    use test_utils::tags;

    use crate::fmt::FastEncode;

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_time_fractional_round_up() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 500).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00.000001", actual);
    }

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_time_fractional_round_down() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 499).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00", actual);
    }

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_time_round_up_to_whole_second() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 999_999_900).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:01", actual);
    }

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_time_round_down_to_zero() {
        let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 100).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("00:00:00", actual);
    }

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_time_round_up_to_24() {
        let time = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_900).unwrap();
        let mut actual = BytesMut::new();
        time.put(&mut actual);

        assert_eq!("24:00:00", actual);
    }

    #[tags(postgres_upstream)]
    #[test]
    fn test_write_timestamp_round_up_to_next_day() {
        let dt = NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 999999500)
            .unwrap();
        let mut actual = BytesMut::new();
        dt.put(&mut actual);

        assert_eq!("0001-01-02 00:00:00", actual);
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

        fn compare_timestamp_format(ts: NaiveDateTime, client: &mut Client) {
            let mut actual = BytesMut::new();
            ts.put(&mut actual);

            let mut year = ts.year();
            if year < 0 {
                year -= 1;
            }

            let query = format!(
                "SELECT make_timestamp({}, {}, {}, {}, {}, {})",
                year,
                ts.month(),
                ts.day(),
                ts.hour(),
                ts.minute(),
                ts.format("%S%.f"),
            );
            let expected = postgres_query(&query, client);
            assert_eq!(expected, actual);
        }

        fn compare_timestamptz_format(ts: NaiveDateTime, tz: Tz, client: &mut Client) {
            let mut actual = BytesMut::new();

            let datetime = tz.from_local_datetime(&ts).single().unwrap();
            let offset = datetime.offset().base_utc_offset() + datetime.offset().dst_offset();
            let tstz = FixedOffset::east_opt(offset.num_seconds() as i32)
                .unwrap()
                .from_local_datetime(&ts)
                .single()
                .unwrap();
            tstz.put(&mut actual);

            client
                .simple_query(&format!("SET TIME ZONE '{}'", tz.name()))
                .unwrap();

            let mut year = tstz.year();
            if year < 0 {
                year -= 1;
            }

            let query = format!(
                "SELECT make_timestamptz({}, {}, {}, {}, {}, {}, '{}')",
                year,
                tstz.month(),
                tstz.day(),
                tstz.hour(),
                tstz.minute(),
                ts.format("%S%.f"),
                tz.name(),
            );
            let expected = postgres_query(&query, client);
            assert_eq!(expected, actual);
        }

        #[tags(postgres_upstream)]
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

        #[tags(postgres_upstream)]
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

        #[tags(postgres15_upstream)]
        #[test]
        fn test_timestamps_same_as_postgres() {
            let mut client = config().connect(NoTls).unwrap();

            for t in [
                NaiveDate::from_ymd_opt(2024, 4, 1)
                    .unwrap()
                    .and_hms_micro_opt(12, 34, 45, 123456)
                    .unwrap(),
                NaiveDate::from_ymd_opt(-1, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ] {
                compare_timestamp_format(t, &mut client);
            }
        }

        #[tags(postgres15_upstream)]
        #[test]
        fn test_timestamptzs_same_as_postgres() {
            let mut client = config().connect(NoTls).unwrap();

            for (ts, tz) in [
                (
                    NaiveDate::from_ymd_opt(2024, 4, 1)
                        .unwrap()
                        .and_hms_micro_opt(12, 34, 45, 123456)
                        .unwrap(),
                    Tz::America__New_York,
                ),
                (
                    NaiveDate::from_ymd_opt(-1, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                    Tz::America__New_York,
                ),
            ] {
                compare_timestamptz_format(ts, tz, &mut client);
            }
        }

        mod proptests {
            use std::cell::RefCell;

            use chrono::{LocalResult, NaiveDateTime};
            use chrono_tz::Tz;
            use proptest::prelude::*;

            use super::*;
            use crate::arbitrary::{
                arbitrary_naive_date, arbitrary_naive_date_in_range, arbitrary_naive_time,
            };

            fn arbitrary_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
                (
                    // We limit to the year 2099 because of a standing bug in chrono-tz. See this
                    // Github issue for more info: https://github.com/chronotope/chrono-tz/issues/155
                    arbitrary_naive_date_in_range(-4712i32..=2099),
                    arbitrary_naive_time(),
                )
                    .prop_map(|(date, time)| NaiveDateTime::new(date, time))
            }

            #[tags(postgres_upstream)]
            #[test]
            fn test_write_date() {
                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(10_000), |(date in arbitrary_naive_date())| {
                    compare_date_format(date, &mut client.borrow_mut());
                })
            }

            #[tags(postgres_upstream)]
            #[test]
            fn test_write_time() {
                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(10_000), |(time in arbitrary_naive_time())| {
                    compare_time_format(time, &mut client.borrow_mut());
                })
            }

            /// A property test that generates arbitrary timestamps and tests that we format them in
            /// a way that is equivalent to a Postgres oracle.
            ///
            /// Postgres formats timestamps with inconsistent fractional seconds due to floating
            /// point error. For this reason, this property test does not generate test
            /// inputs that include fractional seconds.
            ///
            /// See the documentation for [`write_timestamp`] for more information about these
            /// discrepancies.
            #[tags(postgres15_upstream)]
            #[test]
            fn test_write_timestamp() {
                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(10_000), |(ts in arbitrary_naive_date_time())| {
                    compare_timestamp_format(ts, &mut client.borrow_mut());
                })
            }

            /// A property test that generates arbitrary timestamps and arbitrary time zones (chosen
            /// from a limited list of time zones) and tests that we format them in a way that is
            /// equivalent to a Postgres oracle. The list of time zones from which an arbitrary one
            /// is chosen is limited to account for differences in the versions of the
            /// [IANA tz database](https://www.iana.org/time-zones) that chrono-tz and Postgres use.
            ///
            /// Postgres also formats timestamps with inconsistent fractional seconds due to
            /// floating point error. For this reason, this property test does not
            /// generate test inputs that include fractional seconds.
            ///
            /// See the documentation for [`write_timestamp_tz`] for more information about these
            /// discrepancies.
            #[tags(postgres15_upstream)]
            #[test]
            fn test_write_timestamp_tz() {
                fn arbitrary_stable_timezone() -> impl Strategy<Value = Tz> {
                    prop_oneof![
                        Just(Tz::Africa__Cairo),
                        Just(Tz::Africa__Johannesburg),
                        Just(Tz::Africa__Nairobi),
                        Just(Tz::America__Barbados),
                        Just(Tz::America__Belize),
                        Just(Tz::America__Buenos_Aires),
                        Just(Tz::America__Chicago),
                        Just(Tz::America__Costa_Rica),
                        Just(Tz::America__Detroit),
                        Just(Tz::America__Indianapolis),
                        Just(Tz::America__Los_Angeles),
                        Just(Tz::America__Montevideo),
                        Just(Tz::America__Montreal),
                        Just(Tz::America__New_York),
                        Just(Tz::America__Puerto_Rico),
                        Just(Tz::America__Toronto),
                        Just(Tz::America__Vancouver),
                        Just(Tz::Asia__Bangkok),
                        Just(Tz::Asia__Chongqing),
                        Just(Tz::Asia__Damascus),
                        Just(Tz::Asia__Dubai),
                        Just(Tz::Asia__Hong_Kong),
                        Just(Tz::Asia__Jakarta),
                        Just(Tz::Asia__Kabul),
                        Just(Tz::Asia__Qatar),
                        Just(Tz::Asia__Saigon),
                        Just(Tz::Asia__Seoul),
                        Just(Tz::Asia__Shanghai),
                        Just(Tz::Asia__Singapore),
                        Just(Tz::Asia__Taipei),
                        Just(Tz::Asia__Tehran),
                        Just(Tz::Asia__Tokyo),
                        Just(Tz::Atlantic__Bermuda),
                        Just(Tz::Australia__Brisbane),
                        Just(Tz::Australia__Melbourne),
                        Just(Tz::Australia__Sydney),
                        Just(Tz::Australia__Victoria),
                        Just(Tz::EST),
                        Just(Tz::EST5EDT),
                        Just(Tz::Egypt),
                        Just(Tz::Etc__GMT),
                        Just(Tz::Etc__GMTPlus0),
                        Just(Tz::Etc__GMTPlus1),
                        Just(Tz::Etc__GMTPlus10),
                        Just(Tz::Etc__GMTPlus11),
                        Just(Tz::Etc__GMTPlus12),
                        Just(Tz::Etc__GMTPlus2),
                        Just(Tz::Etc__GMTPlus3),
                        Just(Tz::Etc__GMTPlus4),
                        Just(Tz::Etc__GMTPlus5),
                        Just(Tz::Etc__GMTPlus6),
                        Just(Tz::Etc__GMTPlus7),
                        Just(Tz::Etc__GMTPlus8),
                        Just(Tz::Etc__GMTPlus9),
                        Just(Tz::Etc__GMTMinus0),
                        Just(Tz::Etc__GMTMinus1),
                        Just(Tz::Etc__GMTMinus10),
                        Just(Tz::Etc__GMTMinus11),
                        Just(Tz::Etc__GMTMinus12),
                        Just(Tz::Etc__GMTMinus13),
                        Just(Tz::Etc__GMTMinus14),
                        Just(Tz::Etc__GMTMinus2),
                        Just(Tz::Etc__GMTMinus3),
                        Just(Tz::Etc__GMTMinus4),
                        Just(Tz::Etc__GMTMinus5),
                        Just(Tz::Etc__GMTMinus6),
                        Just(Tz::Etc__GMTMinus7),
                        Just(Tz::Etc__GMTMinus8),
                        Just(Tz::Etc__GMTMinus9),
                        Just(Tz::Etc__GMT0),
                        Just(Tz::Etc__Greenwich),
                        Just(Tz::Etc__UCT),
                        Just(Tz::Etc__UTC),
                        Just(Tz::Etc__Universal),
                        Just(Tz::Etc__Zulu),
                        Just(Tz::Europe__Athens),
                        Just(Tz::Europe__Berlin),
                        Just(Tz::Europe__Brussels),
                        Just(Tz::Europe__Budapest),
                        Just(Tz::Europe__Dublin),
                        Just(Tz::Europe__Helsinki),
                        Just(Tz::Europe__Kyiv),
                        Just(Tz::Europe__Lisbon),
                        Just(Tz::Europe__London),
                        Just(Tz::Europe__Madrid),
                        Just(Tz::Europe__Moscow),
                        Just(Tz::Europe__Paris),
                        Just(Tz::Europe__Prague),
                        Just(Tz::Europe__Rome),
                        Just(Tz::Europe__Vatican),
                        Just(Tz::Europe__Vienna),
                        Just(Tz::Europe__Warsaw),
                        Just(Tz::Europe__Zurich),
                        Just(Tz::GB),
                        Just(Tz::GMT),
                        Just(Tz::GMTPlus0),
                        Just(Tz::GMTMinus0),
                        Just(Tz::GMT0),
                        Just(Tz::Greenwich),
                        Just(Tz::HST),
                        Just(Tz::Hongkong),
                        Just(Tz::Iceland),
                        Just(Tz::Iran),
                        Just(Tz::Israel),
                        Just(Tz::Jamaica),
                        Just(Tz::Japan),
                        Just(Tz::MST),
                        Just(Tz::MST7MDT),
                        Just(Tz::NZ),
                        Just(Tz::NZCHAT),
                        Just(Tz::PRC),
                        Just(Tz::PST8PDT),
                        Just(Tz::Pacific__Auckland),
                        Just(Tz::Pacific__Fiji),
                        Just(Tz::Pacific__Guam),
                        Just(Tz::Pacific__Honolulu),
                        Just(Tz::Poland),
                        Just(Tz::Portugal),
                        Just(Tz::Singapore),
                        Just(Tz::Turkey),
                        Just(Tz::UCT),
                        Just(Tz::US__Arizona),
                        Just(Tz::US__Central),
                        Just(Tz::US__EastIndiana),
                        Just(Tz::US__Eastern),
                        Just(Tz::US__Hawaii),
                        Just(Tz::US__Mountain),
                        Just(Tz::US__Pacific),
                        Just(Tz::UTC),
                        Just(Tz::Universal),
                        Just(Tz::WSU),
                        Just(Tz::Zulu),
                    ]
                }

                fn arbitrary_naive_date_time_and_timezone(
                ) -> impl Strategy<Value = (NaiveDateTime, Tz)> {
                    (arbitrary_naive_date_time(), arbitrary_stable_timezone()).prop_filter(
                        "the time must be representable in the generated timezone",
                        |(ts, tz)| matches!(ts.and_local_timezone(*tz), LocalResult::Single(_)),
                    )
                }

                let client = RefCell::new(config().connect(NoTls).unwrap());

                proptest!(ProptestConfig::with_cases(10_000), |((ts, tz) in arbitrary_naive_date_time_and_timezone())| {
                    compare_timestamptz_format(ts, tz, &mut client.borrow_mut());
                })
            }
        }
    }
}
