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

/// Assert that the given async expression eventually yields `true`, after a configurable number of
/// tries and sleeping a configurable amount between tries.
///
/// Defaults to 40 attempts, sleeping 500 milliseconds between attempts
///
/// # Examples
///
/// Using the default configuration:
/// ```
/// # use launchpad::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually! {
///   let fut = futures::future::ready(x);
///   fut.await == 1
/// }
/// # })
/// ```
///
/// Configuring the number of attempts:
/// ```
/// # use launchpad::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually!(attempts: 5, {
///   let fut = futures::future::ready(x);
///   fut.await == 1
/// })
/// # })
/// ```
///
/// Configuring the number of attempts and the sleep duration:
/// ```
/// use std::time::Duration;
///
/// # use launchpad::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually!(attempts: 5, sleep: Duration::from_millis(100), {
///   let fut = futures::future::ready(x);
///   fut.await == 1
/// })
/// # })
/// ```
#[macro_export]
macro_rules! eventually {
    (attempts: $attempts: expr, { $($body: tt)* }) => {
        eventually!(
            attempts: $attempts,
            sleep: std::time::Duration::from_millis(500),
            { $($body)* }
        )
    };
    (sleep: $sleep: expr, { $($body: tt)* }) => {
        eventually!(
            attempts: 40,
            sleep: $sleep,
            { $($body)* }
        )
    };
    (sleep: $sleep: expr, attempts: $attempts: expr, { $($body: tt)* }) => {
        eventually!(
            attempts: $attempts,
            sleep: $sleep,
            { $($body)* }
        )
    };
    (attempts: $attempts: expr, sleep: $sleep: expr, { $($body: tt)* }) => {{
        let attempts = $attempts;
        let sleep = $sleep;
        let mut attempt = 0;
        while !async { $($body)* }.await {
            if attempt > attempts {
                panic!(
                    "{} did not become true after {} attempts",
                    stringify!({ $($body)* }),
                    attempts
                );
            } else {
                attempt += 1;
                tokio::time::sleep(sleep).await;
            }
        }
    }};
	($($body: tt)*) => {
		eventually!(
            attempts: 40,
            sleep: std::time::Duration::from_millis(500),
            { $($body)* }
        )
	};
}
