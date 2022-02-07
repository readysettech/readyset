use std::fmt;

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use serde::{Deserialize, Serialize};

/// An optimized storage for a timestamp with timezone. Sadly the way chrono implements
/// DateTime<Tz> occupies at least 16 bytes, and therefore overflows DataType. However
/// it really only requires the offset to be within a 24 hours range with a second
/// precision which we can store in a 24 bit array.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[repr(C, packed)] // So we can actually fit into the 15 bytes
pub struct TimestampTz {
    // offset is 3 bytes
    offset: [u8; 3],
    // datetime is 12 bytes for a total of 15 bytes
    pub(super) datetime: NaiveDateTime,
}

impl From<&TimestampTz> for DateTime<FixedOffset> {
    fn from(ts: &TimestampTz) -> Self {
        let o = &ts.offset;
        // Convert from the 3 byte representation. Top byte is sign extension
        let offset = i32::from_le_bytes([o[0], o[1], o[2], 0u8.wrapping_sub(o[2] >> 7)]);
        DateTime::from_utc(ts.datetime, FixedOffset::east(offset))
    }
}

impl fmt::Debug for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_chrono().fmt(f)
    }
}

impl From<DateTime<FixedOffset>> for TimestampTz {
    fn from(dt: DateTime<FixedOffset>) -> Self {
        let offset = dt.offset().local_minus_utc().to_le_bytes();

        TimestampTz {
            datetime: dt.naive_utc(),
            offset: [offset[0], offset[1], offset[2]],
        }
    }
}

impl TimestampTz {
    pub fn to_chrono(&self) -> DateTime<FixedOffset> {
        self.into()
    }
}
