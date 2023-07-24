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

/// Assert that the given async expression eventually succeeds after a configurable number of
/// tries and sleeping a configurable amount between tries. Useful for testing eventually
/// consistent parts of the system.
///
/// In the basic form, we passing a single block of code and repeat it until it evaluates to
/// `true`. However, this can be limiting for some tests since it doesn't catch panics, so test
/// code that relies on unwraps and asserts will fail. To accommodate this, [eventually] can also be
/// called in in `run_test`/`then_assert` form, which catches panics in the `then_assert` block and
/// retries the entire test.
///
/// There are two reasons for specifying `run_test` and `then_assert` as separate blocks:
///
/// * Oftentimes we don't expect panics from the test code itself, and only want to retry if we get
///   an incorrect result (i.e. if an unwrap or an assertion on the result of a test fails).
///   Structuring this macro to take two separate blocks allows the macro to do just that.
/// * Frequently our tests make use of values that are not unwind-safe, so the compiler will
///   complain if we try to catch panics with `[futures::FutureExt::catch_unwind]` (which this macro
///   does for the assertion code). Assertions, by contrast, usually just involve unwrapping or
///   comparing values in ways that are easily written in an unwind-safe way.
///
/// NOTE: although this macro is written to make `then_assert` look like a closure, this is
/// actually syntax sugar. Due to some quirks of async Rust, you cannot define a closure outside
/// the macro invocation and pass it in; you must write the `then_assert` body inline as part of
/// the macro invocation.
///
/// Defaults to 40 attempts, sleeping 500 milliseconds between attempts
///
/// # Examples
///
/// Using the default configuration:
/// ```
/// # use readyset_util::eventually;
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
/// Using the `run_test`/`then_assert` form:
/// ```
/// # use readyset_util::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually!(
///     run_test: { futures::future::ready(x).await },
///     then_assert: |result| assert_eq!(result, 1)
/// );
/// # })
/// ```
///
/// Incorporating a block containing multiple assertion steps:
/// ```
/// # use readyset_util::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually!(
///     run_test: { Some(futures::future::ready(x).await) },
///     then_assert: |result| {
///         let result = result.unwrap();
///         assert!(result > 0);
///         assert_eq!(result, 1);
///     }
/// );
/// # })
/// ```
///
/// Configuring the number of attempts (these next two examples also work the same way for the
/// `run_test`/`then_assert` form):
/// ```
/// # use readyset_util::eventually;
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
/// # use readyset_util::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// eventually!(attempts: 5, sleep: Duration::from_millis(100), {
///   let fut = futures::future::ready(x);
///   fut.await == 1
/// })
/// # })
/// ```
///
/// As noted above, `then_assert` must be defined inline. The following will fail to compile:
/// ```no_compile
/// # use readyset_util::eventually_assert;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// let assert_closure = |result| assert_eq!(result, 1);
/// eventually_assert!(
///     run_test: { futures::future::ready(x).await },
///     then_assert: assert_closure
/// );
/// # })
/// ```
#[macro_export]
macro_rules! eventually {
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? run_test: { $($test_body: tt)* },
            then_assert: |$test_res: pat_param| $($assert_body: tt)+) => {
        let attempts = 40;
        let sleep = std::time::Duration::from_millis(500);
        // Shadow the above defaults if custom values were provided:
        $(let attempts = $attempts;)?
        $(let sleep = $sleep;)?

        for attempt in 1..=attempts {
            let $test_res = async { $($test_body)* }.await;
            if attempt == attempts {
                // Run the last attempt without the catch_unwind wrapper so that panics are visible
                // in the test failure results if they occur:
                async { $($assert_body)* }.await;
            } else {
                if ::futures::FutureExt::catch_unwind(async { $($assert_body)* }).await.is_err() {
                    println!("Assertion failed on attempt {attempt}, retrying after delay...");
                    tokio::time::sleep(sleep).await;
                } else {
                    break;
                }
            }
        }
    };
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? run_test: { $($test_body: tt)* },
            then_assert: $($assert_body: tt)+) => {
        compile_error!(concat!("`then_assert` must be specified using closure syntax",
                               "(see `eventually` docs for detail)"))
    };
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? { $($body: tt)* }) => {{
        #[allow(unused_variables)]
        let attempts = 40;
        #[allow(unused_variables)]
        let sleep = std::time::Duration::from_millis(500);
        $(let attempts = $attempts;)?
        // Shadow the above defaults if custom values were provided:
        $(let sleep = $sleep;)?

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
