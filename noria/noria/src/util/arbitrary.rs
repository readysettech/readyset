//! Utilities for generating arbitrary values with [`proptest`]

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use proptest::prelude::*;

/// Strategy to generate an arbitrary [`NaiveDate`]
pub fn arbitrary_naive_date() -> impl Strategy<Value = NaiveDate> {
    (-2000i32..3000, 1u32..365).prop_map(|(y, doy)| NaiveDate::from_yo(y, doy))
}

/// Generate an arbitrary [`NaiveTime`]
pub fn arbitrary_naive_time() -> impl Strategy<Value = NaiveTime> {
    (0u32..23, 0u32..59, 0u32..59).prop_map(|(hour, min, sec)| NaiveTime::from_hms(hour, min, sec))
}

/// Strategy to generate an arbitrary [`NaiveDateTime`]
pub fn arbitrary_naive_date_time() -> impl Strategy<Value = NaiveDateTime> {
    (arbitrary_naive_date(), arbitrary_naive_time())
        .prop_map(|(date, time)| NaiveDateTime::new(date, time))
}
