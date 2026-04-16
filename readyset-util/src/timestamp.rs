//! Wall-clock timestamp utilities.

#[cfg(not(target_os = "linux"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as milliseconds since the Unix epoch.
#[cfg(target_os = "linux")]
pub fn current_timestamp_ms() -> u64 {
    use rustix::time::{clock_gettime, ClockId};
    let ts = clock_gettime(ClockId::RealtimeCoarse);
    (ts.tv_sec as u64) * 1000 + (ts.tv_nsec as u64) / 1_000_000
}

/// Current wall-clock time as milliseconds since the Unix epoch.
#[cfg(not(target_os = "linux"))]
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}
