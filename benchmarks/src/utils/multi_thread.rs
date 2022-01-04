use crate::benchmark::BenchmarkResults;
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::Interval;
use tracing::error;

/// A group of methods that facilitate executing a single benchmark from multiple
/// threads. This should be used in conjunction with `run_multithread_benchmark`
/// to spawn the threads to run the benchmark.
#[async_trait]
pub(crate) trait MultithreadBenchmark {
    /// The result messages passed to the result's thread via an UnboundedSender.
    type BenchmarkResult: Send;
    /// Thset of parameters used to initialize the benchmark threads.
    type Parameters: Clone;
    /// Process a batch of benchmark results collected over `interval`. This aggregates
    /// all updates send on the `sender` parameter fo `benchmark_thread`.
    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
        results: &mut BenchmarkResults,
    ) -> Result<()>;

    /// Benchmarking code that is initialized using `params` that sends `BenchmarkResult`
    /// to be batched along `sender`.
    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()>;
}

/// Returns after `duration` if it is Some, otherwise, never returns. Useful
/// within select! loops to optional break after a duration.
async fn return_after_duration(duration: Option<Duration>) {
    if let Some(d) = duration {
        tokio::time::sleep(d).await;
    } else {
        let () = futures::future::pending().await;
    }
}

/// Aggregates benchmark results received over `THREAD_UPDATE_INTERVAL`.
/// Every `THREAD_UPDATE_INTERVAL`, the batch of benchmark results is
/// passed to the MultithreadBenchmark's function: handle_benchmark_results().
async fn benchmark_results_thread<B>(
    mut reciever: UnboundedReceiver<B::BenchmarkResult>,
    run_for: Option<Duration>,
) -> Result<BenchmarkResults>
where
    B: MultithreadBenchmark,
{
    const THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
    let mut interval = tokio::time::interval(THREAD_UPDATE_INTERVAL);
    interval.tick().await; // First tick is immediate
    let mut updates = Vec::new();

    // Pin the future so we can poll on it repeatedly in the select loop.
    let return_after = return_after_duration(run_for);
    tokio::pin!(return_after);

    let mut results = BenchmarkResults::new();

    let mut last_update = std::time::Instant::now();
    loop {
        select! {
            // If we reach our thread update interval, run the provided function
            // to handle a batch of updates.
            _ = interval.tick() => {
                let mut new_updates = Vec::new();
                std::mem::swap(&mut new_updates, &mut updates);
                let elapsed = last_update.elapsed();
                last_update = std::time::Instant::now();
                B::handle_benchmark_results(new_updates, elapsed, &mut results).await?;
            }
            // If we receive an update push it to the next batch of updates.
            r = reciever.recv() => {
                if let Some(r) = r {
                    updates.push(r);
                } else {
                    break; // All threads have dropped their sender.
                }
            }
            _ = &mut return_after => {
                break;
            }
        }
    }

    Ok(results)
}

/// Spawns a multi-threaded benchmark across `num_threads` threads running
/// MultithreadBenchmark's `benchmark_results_thread`. An additional thread
/// used to aggregates results over an interval is created from
/// `benchmark_results_thread`.
pub(crate) async fn run_multithread_benchmark<B>(
    num_threads: u64,
    params: B::Parameters,
    run_for: Option<Duration>,
) -> Result<BenchmarkResults>
where
    B: MultithreadBenchmark + 'static,
{
    let (sender, receiver) = unbounded_channel::<B::BenchmarkResult>();

    let mut workers: FuturesUnordered<_> = (0..num_threads)
        .map(|_| tokio::spawn(B::benchmark_thread(params.clone(), sender.clone())))
        .collect();
    let mut results = tokio::spawn(benchmark_results_thread::<B>(receiver, run_for));

    loop {
        select! {
            w = workers.next() => {
                match w {
                    // Error returned from future.
                    Some(Err(e)) => {
                        error!("Error executing future in multi-threaded benchmark");
                        return Err(e.into());
                    }
                    // Error returned from benchmark thread.
                    Some(Ok(Err(e))) => {
                        error!("Error executing benchmark thread: {}", e);
                        return Err(e);
                    }
                    // Success returned from benchmark thread.
                    Some(_) => {}
                    // No more threads to run.
                    None => break,
                }
            }
            res = &mut results => {
                return res?;
            }
        }
    }
    Ok(BenchmarkResults::new())
}

pub(crate) fn throttle_interval(target_qps: Option<u64>, num_threads: u64) -> Option<Interval> {
    target_qps
        .as_ref()
        .map(|qps| tokio::time::interval(Duration::from_nanos(1000000000 * num_threads / qps)))
}
