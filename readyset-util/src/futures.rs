//! Utilities for working with futures, async/await, and tokio

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::process;

use chrono::{SecondsFormat, Utc};
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

/// Retries an asynchronous operation with exponential backoff.
///
/// This macro supports both code blocks and async closures as the retryable operation.
/// Delays between retries follow the exponential function:
///
/// ```text
/// delay = base_delay * (backoff ^ attempts)
/// ```
///
/// # Parameters
///
/// This macro accepts either:
/// - A `{ block }`, or
/// - A closure expression `|| async { ... }`
///
/// ## Named Parameters
/// - `retries:` – Maximum number of attempts before giving up.
/// - `delay:` – The initial base delay between retries, in milliseconds.
/// - `backoff:` – A multiplier applied exponentially on each retry
///
/// # Example: Using a code block
/// ```ignore
/// use readyset_util::retry_with_exponential_backoff;
/// use tokio::time::{sleep, Duration};
/// let result = retry_with_exponential_backoff!({
///     return Result::Ok(());
/// }, retries: 3, delay: 100, backoff: 2);
/// assert!(result.is_ok());
/// ```
///
/// # Example: Using a closure
/// ```ignore
/// use readyset_util::retry_with_exponential_backoff;
/// use tokio::time::{sleep, Duration};
/// let closure = async || { Result::Ok(()) };
/// let result = retry_with_exponential_backoff!(
///     closure,
///     retries: 3,
///     delay: 100,
///     backoff: 2
/// );
/// assert!(result.is_ok());
/// ```
///
/// # Returns
///
/// A `Result<T, E>`:
/// - `Ok(T)` if the operation succeeds within the retry limit
/// - `Err(E)` if all attempts fail
#[macro_export]
macro_rules! retry_with_exponential_backoff {
    (
        $body:block,
        retries: $max_retries:expr,
        delay: $base_delay:expr,
        backoff: $backoff:expr $(,)?
    ) => {{
        let mut attempts_completed: u32 = 0;

        let base_delay = $base_delay as f64;
        let backoff = $backoff as f64;

        loop {
            let result = async $body.await;

            let delay = base_delay * backoff.powi(attempts_completed as i32);

            match result {
                Ok(val) => break Ok(val),
                Err(e) => {
                    if attempts_completed >= $max_retries {
                        break Err(e);
                    }

                    sleep(Duration::from_millis(delay as u64)).await;
                    attempts_completed += 1;
                }
            }
        }
    }};

    (
        $closure:expr,
        retries: $max_retries:expr,
        delay: $base_delay:expr,
        backoff: $backoff:expr $(,)?
    ) => {{
        use tokio::time::{sleep, Duration};

        let mut attempts_completed = 0;
        let op = $closure;

        let base_delay = $base_delay as f64;
        let backoff = $backoff as f64;

        loop {
            let result = op().await;

            let delay = base_delay * backoff.powi(attempts_completed);

            match result {
                Ok(val) => break Ok(val),
                Err(e) => {
                    if attempts_completed >= $max_retries {
                        break Err(e);
                    }

                    sleep(Duration::from_millis(delay as u64)).await;
                    attempts_completed += 1;
                }
            }
        }
    }};
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

#[doc(hidden)]
pub fn __eventually_warn(message: &str) {
    let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    println!("{} {}", ts, message);
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
/// the `run_test`/`then_assert` form can yield a result, which will be returned by the overall
/// `eventually!` invocation:
/// ```
/// # use readyset_util::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// let res = eventually!(
///     run_test: { Some(futures::future::ready(x).await) },
///     then_assert: |result| {
///         let result = result.unwrap();
///         assert!(result > 0);
///         assert_eq!(result, 1);
///         result
///     }
/// );
/// assert_eq!(res, x);
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
/// # use readyset_util::eventually;
/// # let mut rt = tokio::runtime::Runtime::new().unwrap();
/// # rt.block_on(async move {
/// let x = 1;
/// let assert_closure = |result| assert_eq!(result, 1);
/// eventually!(
///     run_test: { futures::future::ready(x).await },
///     then_assert: assert_closure
/// );
/// # })
/// ```
#[macro_export]
macro_rules! eventually {
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? $(token: $token: expr,)? run_test: { $($test_body: tt)* },
            then_assert: |$test_res: pat_param| $($assert_body: tt)+) => {{
        let attempts = 40;
        let sleep = std::time::Duration::from_millis(500);
        // Shadow the above defaults if custom values were provided:
        $(let attempts = $attempts;)?
        $(let sleep = $sleep;)?


        let mut res = None;
        let token_opt: Option<&str> = None; $(let token_opt = Some($token);)?
        for attempt in 1..=(attempts - 1) {
            let $test_res = async { $($test_body)* }.await;
            match ::futures::FutureExt::catch_unwind(async { $($assert_body)* }).await {
                Err(_) => {
                    if let Some(token) = token_opt {
                        $crate::futures::__eventually_warn(&format!(
                            "Assertion failed on attempt {attempt} (token: {}), retrying after delay...",
                            token
                        ));
                    } else {
                        $crate::futures::__eventually_warn(&format!(
                            "Assertion failed on attempt {attempt}, retrying after delay..."
                        ));
                    }
                    tokio::time::sleep(sleep).await;
                }
                Ok(r) => {
                    res = Some(r);
                    break;
                },
            }
        }

        match res {
            Some(r) => r,
            None => {
                // Run the last attempt without the catch_unwind wrapper so that panics are visible
                // in the test failure results if they occur:
                let $test_res = async { $($test_body)* }.await;
                async { $($assert_body)* }.await
            }
        }
    }};
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? $(token: $token: expr,)? run_test: { $($test_body: tt)* },
            then_assert: $($assert_body: tt)+) => {
        compile_error!(concat!("`then_assert` must be specified using closure syntax",
                               "(see `eventually` docs for detail)"))
    };
    ($(attempts: $attempts: expr,)? $(sleep: $sleep: expr,)? $(message: $message: expr,)? { $($body: tt)* }) => {{
        #[allow(unused_variables)]
        let attempts = 40;
        #[allow(unused_variables)]
        let sleep = std::time::Duration::from_millis(500);
        // Shadow the above defaults if custom values were provided:
        $(let attempts = $attempts;)?
        $(let sleep = $sleep;)?

        let mut attempt = 0;
        while !async { $($body)* }.await {
            #[allow(unreachable_code)]
            if attempt > attempts {
                // Will panic with custom message if provided, otherwise default message
                $(
                    panic!("{}", (|| $message )());
                )?
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::time::{sleep, Duration};

    use crate::retry_with_exponential_backoff;

    #[tokio::test]
    async fn test_retry_success_first_try() {
        let result = retry_with_exponential_backoff!(
            { Result::<String, String>::Ok("success".to_string()) },
            retries: 3,
            delay: 100,
            backoff: 1
        );
        assert_eq!(result, Ok("success".to_string()));
    }

    #[tokio::test]
    async fn test_retry_eventual_success() {
        let attempts = Arc::new(AtomicI32::new(0));
        let operation = {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    if attempts.load(Ordering::SeqCst) < 2 {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err("failure".to_string())
                    } else {
                        Ok(())
                    }
                }
            }
        };
        let result = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 1);
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn test_retry_failure() {
        let operation = || async { Result::<(), String>::Err("test failure".to_string()) };
        let result = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 1);
        assert_eq!(result, Err("test failure".to_string()));
    }

    #[tokio::test]
    async fn test_backoff_delay_increases() {
        let start = Instant::now();
        let attempts = Arc::new(AtomicI32::new(0));
        let operation = {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Result::<(), String>::Err("failure".to_string())
                }
            }
        };
        let _ = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 2);
        let duration = start.elapsed();
        assert!(duration >= Duration::from_millis(700)); // 100 + 200 + 400 = 700ms
    }

    #[tokio::test]
    async fn test_respects_max_retries() {
        let attempts = Arc::new(AtomicI32::new(0));
        let operation = {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Result::<(), String>::Err("failure".to_string())
                }
            }
        };
        let _ = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 4); // 1 initial try + 3 retries
    }
}
