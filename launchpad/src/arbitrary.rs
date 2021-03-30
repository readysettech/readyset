//! Utilities for generating arbitrary values with [`proptest`]

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use proptest::prelude::*;

/// Strategy to generate an arbitrary [`NaiveDate`]
pub fn arbitrary_naive_date() -> impl Strategy<Value = NaiveDate> {
    (-2000i32..3000, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo(y, doy))
}

/// Generate an arbitrary [`NaiveTime`]
pub fn arbitrary_naive_time() -> impl Strategy<Value = NaiveTime> {
    (0u32..23, 0u32..59, 0u32..59).prop_map(|(hour, min, sec)| NaiveTime::from_hms(hour, min, sec))
}

/// Generate an arbitrary [`Duration`] within a MySQL TIME valid range.
pub fn arbitrary_duration() -> impl Strategy<Value = Duration> {
    (-3020399i64..3020399i64).prop_map(Duration::microseconds)
}

/// Strategy to generate an arbitrary [`NaiveDateTime`]
pub fn arbitrary_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    (arbitrary_naive_date(), arbitrary_naive_time())
        .prop_map(|(date, time)| NaiveDateTime::new(date, time))
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
