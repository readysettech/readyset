//! Functionality to time how long a scope takes to execute.

use std::fmt::Debug;
use std::time::{Duration, Instant};

use tracing::{info, Span};

/// A scope currently being timed.  Drop the object to stop timing and maybe log.
pub struct TimedScope<T>
where
    T: Debug,
{
    span: Span,
    op: T,
    limit: Duration,
    started: Instant,
}

impl<T> TimedScope<T>
where
    T: Debug,
{
    fn new(span: Span, op: T, limit: Duration) -> Self {
        Self {
            span,
            op,
            limit,
            started: Instant::now(),
        }
    }
}

impl<T> Drop for TimedScope<T>
where
    T: Debug,
{
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if elapsed > self.limit {
            let _entered = self.span.enter();
            info!(
                "operation took {:.3} seconds: {:?}",
                elapsed.as_secs_f32(),
                self.op
            );
        }
    }
}

/// Start timing the current scope, which is executing the provided operation.
pub fn time_scope<T>(span: Span, op: T, limit: Duration) -> TimedScope<T>
where
    T: Debug,
{
    TimedScope::new(span, op, limit)
}
