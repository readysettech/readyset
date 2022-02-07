//! Utilities for generating arbitrary values with [`proptest`]

use std::iter::FromIterator;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use bit_vec::BitVec;
use chrono::{Date, DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use eui48::MacAddress;
use proptest::prelude::*;
use proptest::sample::SizeRange;
use rust_decimal::Decimal;
use uuid::Uuid;

/// Strategy to generate an arbitrary [`NaiveDate`]
pub fn arbitrary_naive_date() -> impl Strategy<Value = NaiveDate> {
    (-2000i32..3000, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo(y, doy))
}

/// Strategy to generate an arbitrary [`NaiveDate`] with a positive year value
pub fn arbitrary_positive_naive_date() -> impl Strategy<Value = NaiveDate> {
    (0i32..3000, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo(y, doy))
}

/// Generate an arbitrary [`NaiveTime`]
pub fn arbitrary_naive_time() -> impl Strategy<Value = NaiveTime> {
    (0u32..23, 0u32..59, 0u32..59).prop_map(|(hour, min, sec)| NaiveTime::from_hms(hour, min, sec))
}

/// Generate an arbitrary [`Duration`] within a MySQL TIME valid range.
pub fn arbitrary_duration() -> impl Strategy<Value = Duration> {
    (-3020399i64..3020399i64).prop_map(Duration::microseconds)
}

/// Generate an arbitrary [`Decimal`]
pub fn arbitrary_decimal() -> impl Strategy<Value = Decimal> {
    // Numeric range compatible with `rust_decimal::Decimal`
    (
        -0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_i128
            ..0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_i128,
        0..28_u32,
    )
        .prop_map(|(i, s)| Decimal::from_i128_with_scale(i, s))
}

/// Strategy to generate an arbitrary [`NaiveDateTime`]
pub fn arbitrary_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    (arbitrary_naive_date(), arbitrary_naive_time())
        .prop_map(|(date, time)| NaiveDateTime::new(date, time))
}

/// Strategy to generate an arbitrary [`Date<FixedOffset>`]
pub fn arbitrary_date() -> impl Strategy<Value = Date<FixedOffset>> {
    // The numbers correspond to the restrictions of `Date` and `FixedOffset`.
    (-2000i32..3000, 1u32..365, (-86_399..86_399)).prop_map(|(y, doy, offset)| {
        Date::<FixedOffset>::from_utc(
            NaiveDate::from_yo(y, doy),
            FixedOffset::west_opt(offset).unwrap_or_else(|| {
                panic!(
                    "FixedOffset::west(secs) requires that -86_400 < secs < 86_400. Secs used: {}",
                    offset
                )
            }),
        )
    })
}

/// Strategy to generate an arbitrary [`DateTime<FixedOffset>`]
pub fn arbitrary_date_time() -> impl Strategy<Value = DateTime<FixedOffset>> {
    (arbitrary_date(), arbitrary_naive_time()).prop_map(|(date, time)| date.and_time(time).unwrap())
}

/// Strategy to generate an arbitrary [`NaiveDateTime`] within Timestamp range.
pub fn arbitrary_timestamp_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    let to_date = |(y, doy)| NaiveDate::from_yo(y, doy);
    let to_time = |(hour, min, sec)| NaiveTime::from_hms(hour, min, sec);
    let dates = (1970i32..2037, 1u32..365).prop_map(to_date);
    let times = (0u32..23, 0u32..59, 0u32..59).prop_map(to_time);
    let last_dates = (2038i32..2039, 1u32..20).prop_map(to_date);
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
        "[^\u{0}]*".prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(
        3,   // 8 levels deep
        256, // Shoot for maximum size of 256 nodes
        10,  // We put up to 10 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..10).prop_map(serde_json::Value::from),
                prop::collection::hash_map("[^\u{0}]*", inner, 0..10).prop_map(|h| {
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
        "[^\u{0}]*".prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(
        3,   // 8 levels deep
        256, // Shoot for maximum size of 256 nodes
        10,  // We put up to 10 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..10).prop_map(serde_json::Value::from),
                prop::collection::hash_map("[^\u{0}]*", inner, 0..10).prop_map(|h| {
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
