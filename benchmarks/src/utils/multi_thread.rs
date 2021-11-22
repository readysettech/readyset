use anyhow::Result;
use async_trait::async_trait;
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
    ) -> Result<()>;

    /// Benchmarking code that is initialized using `params` that sends `BenchmarkResult`
    /// to be batched along `sender`.
    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()>;
}

/// Aggregates benchmark results received over `THREAD_UPDATE_INTERVAL`.
/// Every `THREAD_UPDATE_INTERVAL`, the batch of benchmark results is
/// passed to the MultithreadBenchmark's function: handle_benchmark_results().
async fn benchmark_results_thread<B>(
    mut reciever: UnboundedReceiver<B::BenchmarkResult>,
) -> Result<()>
where
    B: MultithreadBenchmark,
{
    const THREAD_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
    let mut interval = tokio::time::interval(THREAD_UPDATE_INTERVAL);
    let mut updates = Vec::new();
    loop {
        select! {
            // If we reach our thread update interval, run the provided function
            // to handle a batch of updates.
            _ = interval.tick() => {
                let mut new_updates = Vec::new();
                std::mem::swap(&mut new_updates, &mut updates);
                B::handle_benchmark_results(new_updates, THREAD_UPDATE_INTERVAL).await?
            }
            // If we receive an update push it to the next batch of updates.
            r = reciever.recv() => {
                if let Some(r) = r {
                    updates.push(r);
                } else {
                    break;// All threads have dropped their sender.
                }
            }
        }
        // Read
    }

    Ok(())
}

/// Spawns a multi-threaded benchmark across `num_threads` threads running
/// MultithreadBenchmark's `benchmark_results_thread`. An additional thread
/// used to aggregates results over an interval is created from
/// `benchmark_results_thread`.
pub(crate) async fn run_multithread_benchmark<B>(
    num_threads: u64,
    params: B::Parameters,
) -> Result<()>
where
    B: MultithreadBenchmark + 'static,
{
    let (sender, receiver) = unbounded_channel::<B::BenchmarkResult>();

    let mut threads: Vec<_> = (0..num_threads)
        .map(|_| tokio::spawn(B::benchmark_thread(params.clone(), sender.clone())))
        .collect();
    threads.push(tokio::spawn(benchmark_results_thread::<B>(receiver)));

    let res = futures::future::join_all(threads).await;
    for err_res in res.iter().filter(|e| e.is_err()) {
        if let Err(e) = err_res {
            error!("Error executing benchmark {}", e);
        }
    }
    Ok(())
}

pub(crate) fn throttle_interval(target_qps: Option<u64>, num_threads: u64) -> Option<Interval> {
    target_qps
        .as_ref()
        .map(|qps| tokio::time::interval(Duration::from_nanos(1000000000 * num_threads / qps)))
}
