//! Utilities for generating arbitrary values with [`proptest`]

use std::iter::FromIterator;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::RangeInclusive;
use std::str::FromStr as _;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use bit_vec::BitVec;
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, NaiveTime,
    Offset, TimeZone,
};
use chrono_tz::Tz;
use cidr::IpInet;
use eui48::MacAddress;
use prop::string::bytes_regex;
use proptest::prelude::*;
use proptest::sample::SizeRange;
use readyset_decimal::Decimal;
use uuid::Uuid;

const NANOS_IN_SEC: u32 = 1_000_000_000;

/// Strategy to generate an arbitrary [`NaiveDate`] that falls within a variable range
pub fn arbitrary_naive_date_in_range(y: RangeInclusive<i32>) -> impl Strategy<Value = NaiveDate> {
    (y, 1u32..=366)
        .prop_filter("there is no year 0", |(y, _)| *y != 0)
        .prop_map(|(y, mut doy)| {
            // If this is a leap day and *not* a leap year, drop the day back to 365
            if doy == 366 && NaiveDate::from_ymd_opt(y, 2, 29).is_none() {
                doy = 365
            }
            NaiveDate::from_yo_opt(y, doy).unwrap()
        })
}

/// Strategy to generate an arbitrary [`NaiveDate`]
pub fn arbitrary_naive_date() -> impl Strategy<Value = NaiveDate> {
    arbitrary_naive_date_in_range(-4712i32..=262142)
}

/// Strategy to generate an arbitrary [`NaiveDate`] with a positive year value
pub fn arbitrary_positive_naive_date() -> impl Strategy<Value = NaiveDate> {
    (0i32..3000, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo_opt(y, doy).unwrap())
}

/// Generate an arbitrary [`NaiveDate`] with a valid MySQL DATE value
pub fn arbitrary_mysql_date() -> impl Strategy<Value = NaiveDate> {
    (1000i32..9999, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo_opt(y, doy).unwrap())
}

/// Generate an arbitrary [`NaiveTime`]
pub fn arbitrary_naive_time() -> impl Strategy<Value = NaiveTime> {
    (0u32..23, 0u32..59, 0u32..59)
        .prop_map(|(hour, min, sec)| NaiveTime::from_hms_opt(hour, min, sec).unwrap())
}

/// Generate an arbitrary [`NaiveTime`] with arbitrary fractional seconds of microsecond precision
pub fn arbitrary_naive_time_with_seconds_fraction() -> impl Strategy<Value = NaiveTime> {
    (0u32..23, 0u32..59, 0u32..59, 0u32..NANOS_IN_SEC).prop_map(|(hour, min, sec, nano)| {
        NaiveTime::from_hms_nano_opt(hour, min, sec, nano).unwrap()
    })
}

/// Generate an arbitrary [`Duration`] within a MySQL TIME valid range.
pub fn arbitrary_duration() -> impl Strategy<Value = Duration> {
    (-3020399i64..3020399i64).prop_map(Duration::microseconds)
}

///Generate an arbitrary [`Duration`] within a MySQL TIME provided hour range. without microseconds.
pub fn arbitrary_duration_without_microseconds_in_range(
    hours: RangeInclusive<i32>,
) -> impl Strategy<Value = Duration> {
    (hours, 0u32..=59, 0u32..=59).prop_map(|(hours, minutes, seconds)| {
        Duration::hours(hours as i64)
            + Duration::minutes(minutes as i64)
            + Duration::seconds(seconds as i64)
    })
}

///Generate an arbitrary [`Duration`] within a MySQL TIME valid range. without microseconds.
pub fn arbitrary_duration_without_microseconds() -> impl Strategy<Value = Duration> {
    // range from '-838:59:59.000000' to '838:59:59.000000'.
    arbitrary_duration_without_microseconds_in_range(-838i32..=838)
}

///Generate an arbitrary [`Decimal`] within a given valid range.
pub fn arbitrary_decimal(precision: u16, scale: u8) -> impl Strategy<Value = Decimal> {
    arbitrary_decimal_string_with_digits(precision, scale)
        .prop_map(|s| Decimal::from_str(&s).unwrap())
}

/// Generate an arbitrary `Vec<u8>` which is the string representation of a [`Decimal`] with up to
/// the given number of digits.
pub fn arbitrary_decimal_bytes_with_digits(m: u16, d: u8) -> impl Strategy<Value = Vec<u8>> {
    let left = m - d as u16;
    let right = d;
    bytes_regex(&format!("-?[0-9]{{0,{left}}}\\.[0-9]{{0,{right}}}"))
        .expect("Should not generate an invalid regex")
        .prop_filter("Should specify at least one digit", |s| {
            s[..] != b"."[..] && s[..] != b"-."[..]
        })
}

/// Generate an arbitrary `String` which is the string representation of a [`Decimal`] with up to
/// the given number of digits.
pub fn arbitrary_decimal_string_with_digits(m: u16, d: u8) -> impl Strategy<Value = String> {
    arbitrary_decimal_bytes_with_digits(m, d).prop_map(|bytes| String::from_utf8(bytes).unwrap())
}

/// Strategy to generate an arbitrary [`NaiveDateTime`]
pub fn arbitrary_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    (arbitrary_naive_date(), arbitrary_naive_time())
        .prop_map(|(date, time)| NaiveDateTime::new(date, time))
}

/// Strategy to generate an arbitrary [`DateTime<FixedOffset>`]
pub fn arbitrary_date() -> impl Strategy<Value = DateTime<FixedOffset>> {
    // The numbers correspond to the restrictions of `Date` and `FixedOffset`.
    // Looks like, PG accepts timezone offset in the range: -15:59:59 .. +15:59:59
    (-2000i32..3000, 1u32..365, (-57_599..57_599))
        .prop_map(|(y, doy, offset)| {
            FixedOffset::west_opt(offset)
                .unwrap_or_else(|| {
                    panic!(
                    "FixedOffset::west(secs) requires that -86_400 < secs < 86_400. Secs used: {offset}"
                )
                })
                .from_utc_datetime(&NaiveDateTime::new(
                    NaiveDate::from_yo_opt(y, doy).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ))
        })
        .prop_filter("there is no year 0", |dt| dt.year() != 0)
}

/// Strategy to generate an arbitrary [`DateTime<FixedOffset>`]
pub fn arbitrary_date_time() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (arbitrary_date(), arbitrary_naive_time()).prop_map(|(date, time)| {
        date.timezone()
            .from_utc_datetime(&NaiveDateTime::new(date.date_naive(), time))
    })
}

/// Strategy to generate an arbitrary [`DateTime<FixedOffset>`] with nanoseconds
pub fn arbitrary_date_time_with_nanos() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (
        arbitrary_date(),
        arbitrary_naive_time_with_seconds_fraction(),
    )
        .prop_map(|(date, time)| {
            date.timezone()
                .from_utc_datetime(&NaiveDateTime::new(date.date_naive(), time))
        })
}

/// Strategy to generate an arbitrary [`NaiveDateTime`] with nanoseconds
pub fn arbitrary_naive_date_time_with_nanos() -> impl Strategy<Value = NaiveDateTime> {
    (
        arbitrary_date(),
        arbitrary_naive_time_with_seconds_fraction(),
    )
        .prop_map(|(date, time)| NaiveDateTime::new(date.date_naive(), time))
}

/// Strategy to generate an arbitrary [`NaiveDateTime`] within Timestamp range.
pub fn arbitrary_timestamp_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    let to_date = |(y, doy)| NaiveDate::from_yo_opt(y, doy).unwrap();
    let to_time = |(hour, min, sec)| NaiveTime::from_hms_opt(hour, min, sec).unwrap();
    let dates = (1970i32..2037, 1u32..365).prop_map(to_date);
    let times = (0u32..23, 0u32..59, 0u32..59).prop_map(to_time);
    let last_dates = (2038i32..2039, 1u32..19).prop_map(to_date);
    let last_times = (0u32..4, 0u32..15, 0u32..8).prop_map(to_time);
    (dates, times)
        .prop_union((last_dates, last_times))
        .prop_map(|(date, time)| NaiveDateTime::new(date, time))
}

/// Strategy to generate an arbitrary [`SystemTime`] with a microsecond resolution
pub fn arbitrary_systemtime() -> impl Strategy<Value = SystemTime> {
    (proptest::num::i32::ANY, 0..1_000_000u32).prop_map(|(s, us)| {
        if s >= 0 {
            UNIX_EPOCH + StdDuration::new(s as u64, us * 1000)
        } else {
            UNIX_EPOCH - StdDuration::new((-(s as i64)) as u64, us * 1000)
        }
    })
}

/// Strategy to generate an arbitrary [`DateTime<FixedOffset>`], with an arbitrary timezone.
pub fn arbitrary_date_time_timezone() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (arbitrary_timestamp_naive_date_time(), arbitrary_timezone()).prop_map(|(date, zone)| {
        let offset = zone.from_utc_datetime(&date).offset().fix();
        offset.from_local_datetime(&date).single().unwrap()
    })
}

/// Strategy to generate an arbitrary [`NaiveDateTime`] that represents a valid time in the given
/// timezone.
pub fn arbitrary_timestamp_naive_date_time_for_timezone(
    tz: Tz,
) -> impl Strategy<Value = NaiveDateTime> {
    arbitrary_timestamp_naive_date_time().prop_filter(
        "timestamp must represent valid times in both the given timezones",
        move |timestamp: &NaiveDateTime| {
            matches!(timestamp.and_local_timezone(tz), LocalResult::Single(_))
        },
    )
}

/// Strategy to generate an arbitrary timezone from the full range of timezones
/// as supported by `chrono-tz`.
pub fn arbitrary_timezone() -> impl Strategy<Value = Tz> {
    (0..chrono_tz::TZ_VARIANTS.len()).prop_map(|idx| chrono_tz::TZ_VARIANTS[idx])
}

/// Strategy to generate an arbitrary [`MacAddress`].
pub fn arbitrary_mac_address() -> impl Strategy<Value = MacAddress> {
    any::<[u8; 6]>().prop_map(|bytes| {
        // We know the length and format of the bytes, so this should always be parsable as a
        // `MacAddress`.
        #[allow(clippy::unwrap_used)]
        MacAddress::from_bytes(&bytes[..]).unwrap()
    })
}

/// Strategy to generate an arbitrary [`Uuid`].
pub fn arbitrary_uuid() -> impl Strategy<Value = Uuid> {
    any::<u128>().prop_map(Uuid::from_u128)
}

/// Strategy to generate an arbitrary [`Json`] without any `f64` numbers in it.
// TODO(fran): We are configuring if we want to generate floats, because this is causing trouble
//  in the way we serialize/deserialize `JSONB` types (with `FromSql`/`ToSql`).
//  The serdes is using the `arbitrary_precision` feature (which treats numbers as strings), so we
//  are compliant with PostgreSQL way of storing JSON numbers (which are `NUMERIC`). The problem
//  is that comparing the json values yields "differences" which are not real differences, which
//  make our tests fail with cases like `0.0 != -0.0` or `0.0000054 != 5.4e-6`, when semantically
//  they are indeed the same.
pub fn arbitrary_json_without_f64() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::from),
        any::<i64>().prop_map(serde_json::Value::from),
        "[^\u{0}]{0,100}".prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(
        3,   // 8 levels deep
        256, // Shoot for maximum size of 256 nodes
        10,  // We put up to 10 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..10).prop_map(serde_json::Value::from),
                prop::collection::hash_map("[^\u{0}]{0,100}", inner, 0..10).prop_map(|h| {
                    serde_json::Value::from(serde_json::Map::from_iter(
                        h.iter().map(|(s, v)| (s.clone(), v.clone())),
                    ))
                }),
            ]
        },
    )
}

/// Strategy to generate an arbitrary [`Json`].
pub fn arbitrary_json() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::from),
        any::<i64>().prop_map(serde_json::Value::from),
        any::<f64>().prop_map(serde_json::Value::from),
        "[^\u{0}]{0,100}".prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(
        3,   // 8 levels deep
        256, // Shoot for maximum size of 256 nodes
        10,  // We put up to 10 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..10).prop_map(serde_json::Value::from),
                prop::collection::hash_map("[^\u{0}]{0,100}", inner, 0..10).prop_map(|h| {
                    serde_json::Value::from(serde_json::Map::from_iter(
                        h.iter().map(|(s, v)| (s.clone(), v.clone())),
                    ))
                }),
            ]
        },
    )
}

/// Strategy to generate an arbitrary [`BitVec`].
pub fn arbitrary_bitvec<T>(size_range: T) -> impl Strategy<Value = BitVec>
where
    T: Into<SizeRange>,
{
    prop::collection::vec(any::<bool>(), size_range.into()).prop_map(BitVec::from_iter)
}

/// Strategy to generate an arbitrary [`IpInet`].
pub fn arbitrary_ipinet() -> impl Strategy<Value = IpInet> {
    let ipv4 = (any::<Ipv4Addr>(), 0u8..=32)
        .prop_map(|(ip_addr, netmask)| IpInet::new(ip_addr.into(), netmask).unwrap());
    let ipv6 = (any::<Ipv6Addr>(), 0u8..=128)
        .prop_map(|(ip_addr, netmask)| IpInet::new(ip_addr.into(), netmask).unwrap());

    prop_oneof![ipv4, ipv6]
}

/// Strategy to generate an arbitrary string from a reasonable subset of UTF-8.
///
/// A string of arbitrary codepoints will generate a bunch of stuff we don't necessarily want,
/// to wit:  control codes, unassigned codepoints, combining accents with no preceding base
/// character.  Also, ICU4X collation doesn't exactly match that of MySQL.  We restrict the
/// codepoint space to a subset of what is likely to be used soon.
pub fn arbitrary_collatable_string() -> impl Strategy<Value = String> {
    String::arbitrary_with(
        concat!(
            "[\u{0020}-\u{007e}",  // basic latin, excluding control codes
            "\u{00a0}-\u{02ff}",   // extended latin
            "\u{4e00}-\u{9fff}",   // cjk unified ideographs
            "\u{ac00}-\u{d7af}]*"  // hangul syllables
        )
        .into(),
    )
}
