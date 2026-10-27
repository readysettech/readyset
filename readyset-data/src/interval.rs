//! The Postgres `interval` type, which a [`DfValue`](crate::DfValue) holds as text in the
//! `postgres` IntervalStyle.

use pg_interval::Interval;
use readyset_errors::{ReadySetError, ReadySetResult};

const USECS_PER_SEC: u64 = 1_000_000;
const USECS_PER_MINUTE: u64 = 60 * USECS_PER_SEC;
const USECS_PER_HOUR: u64 = 60 * USECS_PER_MINUTE;

const INFINITY: Interval = Interval {
    months: i32::MAX,
    days: i32::MAX,
    microseconds: i64::MAX,
};
const NEG_INFINITY: Interval = Interval {
    months: i32::MIN,
    days: i32::MIN,
    microseconds: i64::MIN,
};

/// The parts of a `postgres` style interval, in the order they appear.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Part {
    Years,
    Months,
    Days,
    Time,
}

/// Parses an interval in the text form Postgres writes in the `postgres` IntervalStyle.
pub fn parse_interval(s: &str) -> ReadySetResult<Interval> {
    let invalid = || ReadySetError::DfValueConversionError {
        src_type: "text".into(),
        target_type: "interval".into(),
        details: format!("invalid interval value: {s:?}"),
    };
    match s.trim() {
        "infinity" => return Ok(INFINITY),
        "-infinity" => return Ok(NEG_INFINITY),
        _ => {}
    }

    let mut months = 0i32;
    let mut days = 0i32;
    let mut microseconds = 0i64;
    let mut last_part = None;
    let mut tokens = s.split_ascii_whitespace();
    while let Some(token) = tokens.next() {
        let part = if token.contains(':') {
            microseconds = parse_time(token).ok_or_else(invalid)?;
            Part::Time
        } else {
            let value: i32 = token.parse().map_err(|_| invalid())?;
            match tokens.next() {
                Some("year" | "years") => {
                    months = value
                        .checked_mul(12)
                        .and_then(|years| months.checked_add(years))
                        .ok_or_else(invalid)?;
                    Part::Years
                }
                Some("mon" | "mons") => {
                    months = months.checked_add(value).ok_or_else(invalid)?;
                    Part::Months
                }
                Some("day" | "days") => {
                    days = value;
                    Part::Days
                }
                _ => return Err(invalid()),
            }
        };
        // Each part appears at most once, in order.
        if last_part.is_some_and(|last| part <= last) {
            return Err(invalid());
        }
        last_part = Some(part);
    }
    if last_part.is_none() {
        return Err(invalid());
    }
    Ok(Interval::new(months, days, microseconds))
}

/// Parses `[+|-]HH:MM:SS[.ffffff]` into microseconds. Hours may run past 24 and past two digits.
fn parse_time(s: &str) -> Option<i64> {
    let (negative, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let mut fields = rest.splitn(3, ':');
    let hours = parse_digits(fields.next()?)?;
    let minutes = parse_digits(fields.next()?)?;
    let seconds = fields.next()?;
    let (seconds, fraction) = match seconds.split_once('.') {
        Some((seconds, fraction)) => (seconds, Some(fraction)),
        None => (seconds, None),
    };
    let seconds = parse_digits(seconds)?;
    if minutes > 59 || seconds > 59 {
        return None;
    }
    let fraction = match fraction {
        None => 0,
        Some(f) if (1..=6).contains(&f.len()) => parse_digits(f)? * 10i128.pow(6 - f.len() as u32),
        Some(_) => return None,
    };
    let total = ((hours * 60 + minutes) * 60 + seconds) * i128::from(USECS_PER_SEC) + fraction;
    i64::try_from(if negative { -total } else { total }).ok()
}

/// Parses a non-empty run of ASCII digits.
fn parse_digits(s: &str) -> Option<i128> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Formats an interval as Postgres writes it in the `postgres` IntervalStyle, matching
/// `interval_out`: `1 year 2 mons -3 days +04:05:06.5`, `00:00:00` when zero, and `infinity` or
/// `-infinity` for the infinite values.
pub fn format_interval(interval: &Interval) -> String {
    if *interval == INFINITY {
        return "infinity".to_owned();
    }
    if *interval == NEG_INFINITY {
        return "-infinity".to_owned();
    }

    let mut out = String::new();
    // A positive part after a negative one carries an explicit `+`.
    let mut after_negative = false;
    for (value, unit) in [
        (interval.months / 12, "year"),
        (interval.months % 12, "mon"),
        (interval.days, "day"),
    ] {
        if value == 0 {
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        if after_negative && value > 0 {
            out.push('+');
        }
        out.push_str(&value.to_string());
        out.push(' ');
        out.push_str(unit);
        if value != 1 {
            out.push('s');
        }
        after_negative = value < 0;
    }

    let time = interval.microseconds;
    if out.is_empty() || time != 0 {
        if !out.is_empty() {
            out.push(' ');
        }
        if time < 0 {
            out.push('-');
        } else if after_negative {
            out.push('+');
        }
        let time = time.unsigned_abs();
        let hours = time / USECS_PER_HOUR;
        let minutes = time % USECS_PER_HOUR / USECS_PER_MINUTE;
        let seconds = time % USECS_PER_MINUTE / USECS_PER_SEC;
        out.push_str(&format!("{hours:02}:{minutes:02}:{seconds:02}"));
        let fraction = time % USECS_PER_SEC;
        if fraction != 0 {
            out.push('.');
            out.push_str(format!("{fraction:06}").trim_end_matches('0'));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_postgres::types::{FromSql, ToSql, Type};

    use super::*;
    use crate::DfValue;

    const SEC: i64 = 1_000_000;
    const MIN: i64 = SEC * 60;
    const HOUR: i64 = MIN * 60;

    /// Each interval with the text Postgres writes for it.
    const CASES: &[((i32, i32, i64), &str)] = &[
        ((0, 0, 0), "00:00:00"),
        (
            (14, 3, 4 * HOUR + 5 * MIN + 6 * SEC + 500_000),
            "1 year 2 mons 3 days 04:05:06.5",
        ),
        (
            (10, 3, 4 * HOUR + 5 * MIN + 6 * SEC + 500_000),
            "10 mons 3 days 04:05:06.5",
        ),
        (
            (-14, 3, 4 * HOUR + 5 * MIN + 6 * SEC + 500_000),
            "-1 years -2 mons +3 days 04:05:06.5",
        ),
        ((12, 0, 0), "1 year"),
        ((-12, 0, 0), "-1 years"),
        ((0, -1, 2 * HOUR), "-1 days +02:00:00"),
        ((-14, 0, 0), "-1 years -2 mons"),
        ((-10, 0, 0), "-10 mons"),
        ((-1, 2, 0), "-1 mons +2 days"),
        ((12, -3, 0), "1 year -3 days"),
        ((1, 1, 0), "1 mon 1 day"),
        ((0, 2, -HOUR), "2 days -01:00:00"),
        ((0, -1, -2 * HOUR), "-1 days -02:00:00"),
        ((0, 1, -500_000), "1 day -00:00:00.5"),
        ((0, 0, SEC), "00:00:01"),
        ((0, 0, SEC + 500_000), "00:00:01.5"),
        ((0, 0, 120_000), "00:00:00.12"),
        ((0, 0, 1), "00:00:00.000001"),
        ((0, 0, -1), "-00:00:00.000001"),
        ((0, 0, 100 * HOUR), "100:00:00"),
        ((0, 0, i64::MAX), "2562047788:00:54.775807"),
        ((0, 0, -i64::MAX), "-2562047788:00:54.775807"),
        ((0, 0, i64::MIN), "-2562047788:00:54.775808"),
        ((i32::MAX, 0, 0), "178956970 years 7 mons"),
        ((i32::MIN, 0, 0), "-178956970 years -8 mons"),
        ((0, i32::MAX, 0), "2147483647 days"),
        ((0, i32::MIN, 0), "-2147483648 days"),
        ((i32::MAX, i32::MAX, i64::MAX), "infinity"),
        ((i32::MIN, i32::MIN, i64::MIN), "-infinity"),
        (
            (-14, 3, 40 * HOUR + 10 * MIN + 6 * SEC + 500_000),
            "-1 years -2 mons +3 days 40:10:06.5",
        ),
    ];

    #[test]
    fn format_matches_postgres() {
        for &((months, days, microseconds), text) in CASES {
            assert_eq!(
                format_interval(&Interval::new(months, days, microseconds)),
                text
            );
        }
    }

    #[test]
    fn parse_round_trips_postgres_text() {
        for &((months, days, microseconds), text) in CASES {
            assert_eq!(
                parse_interval(text).unwrap(),
                Interval::new(months, days, microseconds),
                "{text}"
            );
        }
    }

    #[test]
    fn parse_accepts_forms() {
        let text = "1:0:3";
        assert_eq!(
            parse_interval(text).unwrap(),
            Interval::new(0, 0, HOUR + 3 * SEC)
        );
    }

    #[test]
    fn parse_rejects_other_forms() {
        for text in [
            "",
            "1",
            "1 fortnight",
            "1 day 1 year",
            "1 day 2 days",
            "00:00:00 1 day",
            "1:00",
            "1:60:00",
            "1:00:60",
            "1:00:00.1234567",
            "1:00:00.",
            "--1:00:00",
            "P1D",
            "@ 1 day",
            "178956971 years",
            "9999999999:00:00",
        ] {
            assert!(parse_interval(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn df_value_round_trips_through_binary() {
        let text = "-1 mons +2 days -01:00:00";
        let mut buf = BytesMut::new();
        DfValue::from(text)
            .to_sql(&Type::INTERVAL, &mut buf)
            .unwrap();
        assert_eq!(
            DfValue::from_sql(&Type::INTERVAL, &buf).unwrap(),
            DfValue::from(text)
        );
    }
}
