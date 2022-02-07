//! Utilities for working with futures, async/await, and tokio

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::process;

use futures::{FutureExt, TryFutureExt};
use tracing::error;

/// A version of the [`tokio::select`] macro that also emits an `allow` annotation for
/// `clippy::unreachable` and `clippy::panic`, since both are internal to the expansion of the macro
/// and things we don't have control over.
#[macro_export]
macro_rules! select {
    ($($args:tt)*) => {
        #[allow(clippy::unreachable, clippy::panic)]
        {
            tokio::select!($($args)*)
        }
    };
}

/// Wrap the given future in a handler that will cause the entire process to exit if the future
/// panics during its execution
pub fn abort_on_panic<F, A>(f: F) -> impl Future<Output = A> + Send + 'static
where
    F: Future<Output = A> + Send + 'static,
{
    AssertUnwindSafe(f) // safe because we don't actually use the future when handling errors
        .catch_unwind()
        .unwrap_or_else(|e| {
            if let Some(panic_message) = e.downcast_ref::<String>() {
                error!(%panic_message, "Task panicked; aborting");
            } else {
                error!("Task panicked with non-string message; aborting");
            }
            process::abort()
        })
}
