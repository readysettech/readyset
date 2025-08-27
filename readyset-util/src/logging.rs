//! Utility functions for logging

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Key for parsing mismatch logging.
pub const PARSING_LOG_PARSING_MISMATCH_SQLPARSER_FAILED: &str = "PARSING";

/// Key for TCP connection logging.
pub const TCP_CONNECTION_LOG_RECEIVED_FROM_UNKNOWN_SOURCE: &str = "TCP";

/// Key for sampler logging.
pub const SAMPLER_LOG_SAMPLER: &str = "SAMPLER";

fn interval_for_key(key: &str) -> Duration {
    let var_name = format!("{}_LOG_RATE_LIMIT_SECS", key.to_ascii_uppercase());
    std::env::var(var_name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(60))
}

fn check_rate_limit(key: &str) -> bool {
    static NEXT_LOG_TIME: LazyLock<Mutex<HashMap<String, AtomicU64>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Couldn't get system time");

    let mut map = NEXT_LOG_TIME.lock().expect("lock poisoned");
    let entry = map.entry(key.to_string()).or_insert(AtomicU64::new(0));
    let next_log = entry.load(Ordering::Relaxed);
    let should_log = now.as_secs() >= next_log;

    if should_log {
        let interval = interval_for_key(key);
        entry.store((now + interval).as_secs(), Ordering::Relaxed);
    }

    should_log
}

/// Rate limit a function call.
///
/// If `should_limit` is true, the function will be called only if the rate limit has not been
/// exceeded.
///
/// `key` is a string that will be used to identify the rate limit.
///
/// The rate limit is controlled by the `{KEY}_LOG_RATE_LIMIT_SECS` environment variable.
/// Note that this is evaluated at the time of the function call, so it's not suitable for
/// hot paths.
pub fn rate_limit<F>(should_limit: bool, key: &str, f: F)
where
    F: FnOnce(),
{
    if !should_limit || check_rate_limit(key) {
        f();
    }
}
