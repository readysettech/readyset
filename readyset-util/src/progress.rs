//! Utilities for reporting progress of asynchronous tasks

use exponential_backoff::Backoff;
use futures::{Future, FutureExt};
use tokio::time::sleep;

/// While `task` is running, periodically calls `report_progress` with delays according to
/// `backoff`, then returns the result of `task`.
///
/// This can be used to periodically report continued progress on a long-running task in an
/// unobtrusive way, for example:
///
/// ```
/// # use readyset_util::progress::report_progress_with;
/// # use std::time::Duration;
/// # use tokio::time::sleep;
/// # use tracing::info;
/// # use exponential_backoff::Backoff;
/// async fn do_some_expensive_thing() {
///     let expensive_task = sleep(Duration::from_secs(5 * 60));
///     tokio::pin!(expensive_task);
///
///     report_progress_with(
///         Backoff::new(
///             u32::MAX,
///             Duration::from_secs(10),
///             Some(Duration::from_secs(300)),
///         ),
///         || async { info!("still working...") },
///         expensive_task,
///     )
///     .await;
/// }
/// ```
pub async fn report_progress_with<F, Progress, T, R>(backoff: Backoff, report: F, task: T) -> R
where
    F: Fn() -> Progress + Send + Sync + 'static,
    Progress: Future + Send + 'static,
    T: Future<Output = R> + Unpin,
{
    let mut backoff = backoff.into_iter();
    let task = task.fuse();

    let progress = tokio::spawn(async move {
        loop {
            let Some(Some(wait)) = backoff.next() else {
                break;
            };
            sleep(wait).await;
            report().await;
        }
    });

    let aborter = progress.abort_handle();
    let res = task.await;
    aborter.abort();
    res
}
