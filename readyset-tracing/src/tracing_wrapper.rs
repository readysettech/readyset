//! Tracing Wrapper
//!
//! Wraps the tracing logs to inject configurable fields to each log line.

use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result};

use clap::lazy_static::lazy_static;
use parking_lot::RwLock;

#[derive(Debug, Default, Clone)]
pub struct LogContext(HashMap<String, String>);

impl LogContext {
    pub fn insert(&mut self, k: String, v: String) {
        self.0.insert(k, v);
    }

    pub fn get(&self, k: &String) -> Option<&String> {
        self.0.get(k)
    }
}

impl Display for LogContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        // TODO (luke): It may be better to display this differently--maybe with comma-separated
        // field_name=field_value
        write!(f, "{:?}", self)
    }
}

/// Adds a field to the [`LOG_CONTEXT`] with the provided name and value.
/// Must be called before any logging happens in order to be included in the context.
pub fn set_log_field(field_name: String, field_value: String) {
    let mut init_log_fields = INIT_LOG_CONTEXT.write();
    init_log_fields.insert(field_name, field_value);
}

// Transfer the context to an unlocked static var so we don't have to pay for locking it each
// time it's used.
lazy_static! {
    // TODO(luke): We could make this an Option and return an error if we try to set log fields
    // after it is taken by the LOG_CONTEXT
    static ref INIT_LOG_CONTEXT: RwLock<LogContext> = RwLock::new(LogContext::default());
    // For LOG_CONTEXT to be populated, INIT_LOG_CONTEXT must be populated before the first log
    // event, which will lazily clone it into LOG_CONTEXT, after which it will be immutable.
    pub static ref LOG_CONTEXT: LogContext = INIT_LOG_CONTEXT.read().clone();
}

#[cfg(test)]
mod test {
    use super::*;
    // The macro uses `readyset_tracing` instead of crate so it works extrenally
    use crate as readyset_tracing;
    use crate::info;

    #[test]
    fn test_log() {
        crate::init_test_logging();
        let key = "deployment".to_string();
        let value = "Test".to_string();
        set_log_field(key.clone(), value.clone());
        info!(target: "test", field = "ok", num = 1, "{} test", "formatting things");
        assert_eq!(*LOG_CONTEXT.get(&key).unwrap(), value);
    }
}
