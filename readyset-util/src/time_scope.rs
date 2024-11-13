//! Functionality to time how long a scope takes to execute.

use std::time::{Duration, Instant};

use tracing::{info, Span};

/// A scope currently being timed.  Drop the object to stop timing and maybe log.
pub struct TimedScope {
    span: Span,
    limit: Duration,
    started: Instant,
}

impl TimedScope {
    fn new(span: Span, limit: Duration) -> Self {
        Self {
            span,
            limit,
            started: Instant::now(),
        }
    }
}

impl Drop for TimedScope {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if elapsed > self.limit {
            let _entered = self.span.enter();
            info!("operation took {:.3} seconds", elapsed.as_secs_f32());
        }
    }
}

/// Start timing the current scope, which is executing the provided operation.
pub fn time_scope(span: Span, limit: Duration) -> TimedScope {
    TimedScope::new(span, limit)
}
