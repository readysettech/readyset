//! readyset-client utilities

use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;

/// Attempts to execute an asynchronous operation with exponential backoff and returns a `Result`.
/// The provided `base_delay` will be doubled up to `max_retries` times.
///
/// # Arguments
///
/// * `operation` - A closure that returns a future representing the operation to be retried.
/// * `max_retries` - The maximum number of times to retry the operation before giving up.
/// * `base_delay` - The initial delay before the first retry, which doubles on each subsequent
///   retry.
///
/// # Examples
///
/// ```
/// // Example usage:
/// // let result = retry_with_exponential_backoff(my_async_operation, 5, Duration::from_secs(1)).await;
/// ```
///
/// This function returns a `Result` indicating whether the operation was successful (`Ok(T)`)
/// or not (`Err(E)`) after the maximum number of retries has been reached.
pub async fn retry_with_exponential_backoff<F, Fut, T, E>(
    mut operation: F,
    max_retries: usize,
    base_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempts_completed = 0;
    let mut delay = base_delay;

    while attempts_completed <= max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempts_completed == max_retries {
                    return Err(e);
                } else {
                    sleep(delay).await;
                    attempts_completed += 1;
                    // Exponential backoff
                    delay = delay.checked_mul(2).unwrap_or(delay);
                }
            }
        }
    }

    unreachable!();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    use readyset_errors::{internal_err, ReadySetError, ReadySetResult};

    use super::*;

    #[tokio::test]
    async fn test_retry_success_first_try() {
        let operation = || async { ReadySetResult::Ok("success") };
        let result = retry_with_exponential_backoff(operation, 3, Duration::from_millis(100)).await;
        assert_eq!(result, Ok("success"));
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
                        Err(internal_err!("failure"))
                    } else {
                        Ok(())
                    }
                }
            }
        };
        let result = retry_with_exponential_backoff(operation, 3, Duration::from_millis(100)).await;
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn test_retry_failure() {
        let operation = || async {
            ReadySetResult::<()>::Err(ReadySetError::Internal("test failure".to_string()))
        };
        let result = retry_with_exponential_backoff(operation, 3, Duration::from_millis(100)).await;
        assert_eq!(
            result,
            Err(ReadySetError::Internal("test failure".to_string()))
        );
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
                    ReadySetResult::<()>::Err(internal_err!("failure"))
                }
            }
        };
        let _ = retry_with_exponential_backoff(operation, 3, Duration::from_millis(100)).await;
        let duration = start.elapsed();
        // The total wait time should be at least 100ms + 200ms + 400ms = 700ms
        assert!(duration >= Duration::from_millis(700));
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
                    ReadySetResult::<()>::Err(internal_err!("failure"))
                }
            }
        };
        let _ = retry_with_exponential_backoff(operation, 3, Duration::from_millis(100)).await;
        // 3 retries means there are 4 total attempts.
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}
