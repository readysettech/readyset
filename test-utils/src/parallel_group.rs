use std::hint;
use std::sync::atomic::{AtomicU32, Ordering};

/// Handle for limiting the number of (async) tests that can run simultaneously within a group.
/// Intended for use with the [`parallel_group`] macro.
///
/// [`parallel_group`]: crate::parallel_group
///
/// # Examples
///
/// ```
/// #[cfg(test)]
/// mod tests {
///     use test_utils::{parallel_group, AsyncParallelGroup};
///
///     // Initialize a new `AsyncParallelGroup` with a concurrency limit of 3
///     static GROUP: AsyncParallelGroup = AsyncParallelGroup::new(3);
///
///     // Mark your tests as part of the group using the `parallel_group` macro.
///     // Note that the `parallel_group` macro must go *above* `#[tokio::test]`
///
///     #[parallel_group(GROUP)]
///     #[tokio::test]
///     async fn first_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     #[tokio::test]
///     async fn second_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     #[tokio::test]
///     async fn third_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     #[tokio::test]
///     async fn fourth_test() {
///         // ...
///     }
///
///     // Only 3 of those tests will be permitted to run in parallel
/// }
/// ```
#[derive(Debug)]
pub struct AsyncParallelGroup {
    semaphore: tokio::sync::Semaphore,
}

impl AsyncParallelGroup {
    /// Initialize a new `AsyncParallelGroup` with the given concurrency limit
    pub const fn new(parallelism: usize) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::const_new(parallelism),
        }
    }

    /// Acquire a handle to run a test within this parallel test group.
    ///
    /// Avoid using this function in user code - instead, prefer the [`parallel_group`] macro
    ///
    /// [`parallel_group`]: crate::parallel_group
    pub async fn acquire_async(&self) -> tokio::sync::SemaphorePermit {
        self.semaphore.acquire().await.unwrap()
    }
}

/// Handle for limiting the number of tests that can run simultaneously within a group. Intended for
/// use with the [`parallel_group`] macro.
///
/// [`parallel_group`]: crate::parallel_group
///
/// # Examples
///
/// ```
/// #[cfg(test)]
/// mod tests {
///     use test_utils::{parallel_group, ParallelGroup};
///
///     // Initialize a new `ParallelGroup` with a concurrency limit of 3
///     static GROUP: ParallelGroup = ParallelGroup::new(3);
///
///     // Mark your tests as part of the group using the `parallel_group` macro.
///
///     #[parallel_group(GROUP)]
///     fn first_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     fn second_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     fn third_test() {
///         // ...
///     }
///
///     #[parallel_group(GROUP)]
///     fn fourth_test() {
///         // ...
///     }
///
///     // Only 3 of those tests will be permitted to run in parallel
/// }
/// ```
#[derive(Debug)]
pub struct ParallelGroup {
    permits: AtomicU32,
}

#[must_use]
#[derive(Debug)]
pub struct ParallelGroupPermit<'a> {
    permits: &'a AtomicU32,
}

impl<'a> Drop for ParallelGroupPermit<'a> {
    fn drop(&mut self) {
        self.permits.fetch_add(1, Ordering::Release);
    }
}

impl ParallelGroup {
    /// Initialize a new `ParallelGroup` with the given concurrency limit
    pub const fn new(parallelism: u32) -> Self {
        Self {
            permits: AtomicU32::new(parallelism),
        }
    }

    /// Acquire a handle to run a test within this parallel test group.
    ///
    /// Avoid using this function in user code - instead, prefer the [`parallel_group`] macro
    ///
    /// [`parallel_group`]: crate::parallel_group
    pub fn acquire(&self) -> ParallelGroupPermit {
        while !self.try_acquire() {
            // TODO: If this gets expensive, add a waiter queue with condvars
            hint::spin_loop()
        }

        self.new_permit()
    }

    fn try_acquire(&self) -> bool {
        let mut curr = self.permits.load(Ordering::Acquire);
        loop {
            if curr == 0 {
                return false;
            }

            let next = curr - 1;
            match self
                .permits
                .compare_exchange(curr, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => curr = actual,
            }
        }
    }

    fn new_permit(&self) -> ParallelGroupPermit {
        ParallelGroupPermit {
            permits: &self.permits,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread;

    use super::*;

    #[test]
    fn sync_parallel_group() {
        const MAX_CONCURRENCY: u32 = 5;
        const NUM_THREADS: u32 = MAX_CONCURRENCY * 5;

        let val = Arc::new(Mutex::new(0));
        static GROUP: ParallelGroup = ParallelGroup::new(MAX_CONCURRENCY);
        let (tx, rx) = mpsc::channel();

        thread::scope(|s| {
            let threads = (0..NUM_THREADS)
                .map(|_| {
                    let tx = tx.clone();
                    let val = val.clone();
                    s.spawn(move || {
                        let _permit = GROUP.acquire();
                        {
                            let Ok(mut val) = val.lock() else {
                            tx.send(Err("lock failed".into())).unwrap();
                            return;
                        };
                            *val += 1;
                            if *val > MAX_CONCURRENCY {
                                tx.send(Err(format!("Went over max concurrency (val: {val})!")))
                                    .unwrap();
                                return;
                            }
                        }

                        tx.send(Ok(())).unwrap();

                        let Ok(mut val) = val.lock() else {
                            tx.send(Err("lock failed".into())).unwrap();
                            return;
                        };
                        *val -= 1;
                    })
                })
                .collect::<Vec<_>>();

            for _ in 0..NUM_THREADS {
                rx.recv().unwrap().unwrap();
            }

            for thr in threads {
                thr.join().unwrap();
            }
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_parallel_group() {
        const MAX_CONCURRENCY: u32 = 5;
        const NUM_THREADS: u32 = MAX_CONCURRENCY * 5;

        let val = Arc::new(tokio::sync::Mutex::new(0));
        static GROUP: AsyncParallelGroup = AsyncParallelGroup::new(MAX_CONCURRENCY as _);
        let (tx, mut rx) = tokio::sync::mpsc::channel(NUM_THREADS as _);

        tokio_scoped::scope(|s| {
            for _ in 0..NUM_THREADS {
                let tx = tx.clone();
                let val = val.clone();
                s.spawn(async move {
                    let _permit = GROUP.acquire_async().await;
                    {
                        let mut val = val.lock().await;
                        *val += 1;
                        if *val > MAX_CONCURRENCY {
                            tx.send(Err(format!("Went over max concurrency (val: {val})!")))
                                .await
                                .unwrap();
                            return;
                        }
                    }

                    tx.send(Ok(())).await.unwrap();

                    let mut val = val.lock().await;
                    *val -= 1;
                });
            }
        });

        for _ in 0..NUM_THREADS {
            rx.recv().await.unwrap().unwrap();
        }
    }
}
