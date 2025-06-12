//! readyset-client utilities

// TODO: move these out of this module
// should be in readyset-util, however readyset_errors causes
// a circular dependency. Need to replace readyset_errors with a
// std Result if possible
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::time::{sleep, Duration};

    use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
    use readyset_util::retry_with_exponential_backoff;

    #[tokio::test]
    async fn test_retry_success_first_try() {
        let result = retry_with_exponential_backoff!({ ReadySetResult::Ok("success") }, retries: 3, delay:100, backoff:1,);
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
        let result = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 1);
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn test_retry_failure() {
        let operation = || async {
            ReadySetResult::<()>::Err(ReadySetError::Internal("test failure".to_string()))
        };
        let result = retry_with_exponential_backoff!(operation, retries: 3, delay:100, backoff:1);
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
        let _ = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 2);
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
        let _ = retry_with_exponential_backoff!(operation, retries: 3, delay: 100, backoff: 1);
        // 3 retries means there are 4 total attempts.
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}
