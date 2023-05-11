//! Utilities for reporting progress of asynchronous tasks

use backoff::backoff::Backoff;
use futures::future::pending;
use futures::{select_biased, Future, FutureExt};
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
/// # use backoff::ExponentialBackoff;
/// async fn do_some_expensive_thing() {
///     let expensive_task = sleep(Duration::from_secs(5 * 60));
///     tokio::pin!(expensive_task);
///
///     report_progress_with(
///         ExponentialBackoff::default(),
///         || async { info!("still working...") },
///         expensive_task,
///     )
///     .await;
/// }
/// ```
pub async fn report_progress_with<B, F, Progress, T, R>(
    mut backoff: B,
    report_progress: F,
    task: T,
) -> R
where
    B: Backoff + Send + 'static,
    F: Fn() -> Progress + Send + Sync + 'static,
    Progress: Future + Send + 'static,
    T: Future<Output = R> + Unpin,
{
    let mut task = task.fuse();

    let progress_reporter = tokio::spawn(async move {
        if let Some(dur) = backoff.next_backoff() {
            sleep(dur).await;
            let _ = backoff::future::retry::<(), _, _, _, _>(backoff, || async {
                report_progress().await;
                Err(backoff::Error::Transient {
                    err: (),
                    retry_after: None,
                })
            })
            .await;
        }
        pending::<()>().await; // If we get here we never want to exit, since we still want `task`
                               // to finish
    });
    let abort_progress_reporter = progress_reporter.abort_handle();

    select_biased!(
        res = task => {
            abort_progress_reporter.abort();
            res
        },
        progress_res = progress_reporter.fuse() => {
            progress_res.unwrap();
            panic!("Progress reporter task should never exit")
        }
    )
}
