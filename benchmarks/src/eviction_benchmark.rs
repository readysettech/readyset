//! A ReadySet query benchmark that supports arbitrary parameterized queries.
//! This is a multi-threaded benchmark that runs a single query with
//! randomly generated parameters across several threads. It can be used to
//! evaluate a ReadySet deployment at various loads and request patterns.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use futures::stream::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;
use tracing::{debug, error};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::{forward, ForwardPrometheusMetrics};
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram, benchmark_increment_counter};
use noria_logictest::upstream::DatabaseURL;

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct EvictionBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    query: ArbitraryQueryParameters,

    /// The number of threads to execute the "normal" portion of the benchmark.
    #[clap(long, default_value = "1")]
    threads: usize,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,

    /// The duration, specified as the number of seconds that the benchmark
    /// should be running. If `None` is provided, the benchmark will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = crate::utils::seconds_as_str_to_duration))]
    run_for: Option<Duration>,
}

#[derive(Clone)]
pub struct EvictionBenchmarkReadThreadParams {
    query: ArbitraryQueryParameters,
    mysql_conn_str: String,
}

#[async_trait]
impl BenchmarkControl for EvictionBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;

        let mut conn = DatabaseURL::from_str(&deployment.setup_conn_str)?
            .connect()
            .await?;
        conn.query_drop(
            "CREATE TABLE eviction_benchmark_empty (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)",
        )
        .await?;
        debug!("created table");

        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // For now drop the result of migrate as CREATE CACHED QUERY does not support
        // non-select queries.
        let _ = self.query.migrate(&mut conn).await;
        // Migrate the eviction query too
        let _ = conn
            .query_drop(
                "CREATE CACHED QUERY e AS SELECT * FROM eviction_benchmark_empty WHERE id = ?;",
            )
            .await;

        let thread_data = EvictionBenchmarkReadThreadParams {
            query: self.query.clone(),
            mysql_conn_str: deployment.target_conn_str.clone(),
        };
        benchmark_counter!(
            "query_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run"
        );

        let terminate_eviction = Arc::new(AtomicBool::from(false));
        let eviction_tasks = self.run_eviction_tasks(deployment, terminate_eviction.clone());

        let result = multi_thread::run_multithread_benchmark::<Self>(
            self.threads as u64,
            thread_data.clone(),
            self.run_for,
        )
        .await?;

        terminate_eviction.store(true, Ordering::Relaxed);
        eviction_tasks.await??;

        Ok(result)
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self, deployment: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![forward(
            deployment.prometheus_endpoint.clone().unwrap(),
            |metric| metric.name.starts_with("domain_eviction_"),
        )]
    }
}

impl EvictionBenchmark {
    // Starting with one task querying for nonexistent data on unique u64 IDs,
    // every 20 seconds, see if Noria is evicting.  If not, double the number
    // of tasks dedicated to this purpose.
    fn run_eviction_tasks(
        &self,
        deployment: &DeploymentParameters,
        terminate: Arc<AtomicBool>,
    ) -> task::JoinHandle<Result<()>> {
        let forward = self.forward_metrics(deployment)[0].clone();
        let target_conn_str = deployment.target_conn_str.clone();
        task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(20));

            let mut eviction_tasks = vec![tokio::spawn(run_eviction_queries(
                0,
                u64::MAX,
                terminate.clone(),
                target_conn_str.clone(),
            ))];
            interval.tick().await;

            loop {
                interval.tick().await;
                if terminate.load(Ordering::Relaxed) {
                    debug!("--- Terminating");
                    terminate_eviction_tasks(&mut eviction_tasks).await;
                    break;
                }
                if !is_evicting(&forward).await? {
                    let cap = eviction_tasks.capacity();
                    debug!(
                        "--- Not evicting; increasing eviction threads to {}",
                        cap * 2
                    );
                    terminate_eviction_tasks(&mut eviction_tasks).await;
                    eviction_tasks = Vec::with_capacity(cap * 2);
                    let chunk_size = u64::MAX / eviction_tasks.capacity() as u64;
                    for i in 0..eviction_tasks.capacity() {
                        eviction_tasks.push(tokio::spawn(run_eviction_queries(
                            chunk_size * i as u64,
                            chunk_size * (i as u64 + 1),
                            terminate.clone(),
                            target_conn_str.clone(),
                        )));
                    }
                } else {
                    debug!("--- Evicting");
                }
            }
            Ok(())
        })
    }
}

async fn terminate_eviction_tasks(tasks: &mut Vec<task::JoinHandle<Result<()>>>) {
    for task in tasks.iter() {
        task.abort();
    }
    for task in tasks {
        // These will all fail with JoinError because they were aborted
        let _ = task.await;
    }
}

async fn is_evicting(forward: &ForwardPrometheusMetrics) -> Result<bool> {
    let mut metrics = Vec::new();
    let mut stream = forward.metrics().await?;
    while let Some(metric) = stream.next().await {
        metrics.push(metric?);
    }
    Ok(!metrics.is_empty())
}

async fn run_eviction_queries(
    id_start: u64,
    id_end: u64,
    terminate: Arc<AtomicBool>,
    target_conn_str: String,
) -> Result<()> {
    let opts = mysql_async::Opts::from_url(&target_conn_str)?;
    let mut conn = mysql_async::Conn::new(opts).await?;
    let stmt = conn
        .prep("SELECT * FROM eviction_benchmark_empty WHERE id = ?")
        .await?;
    'outer: loop {
        for i in id_start..id_end {
            if terminate.load(Ordering::Relaxed) {
                break 'outer;
            }
            conn.exec_drop(&stmt, (i,)).await?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct EvictionBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
}

impl EvictionBenchmarkResultBatch {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }
}

#[async_trait]
impl MultithreadBenchmark for EvictionBenchmark {
    type BenchmarkResult = EvictionBenchmarkResultBatch;
    type Parameters = EvictionBenchmarkReadThreadParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut queries_this_interval = 0;
        for u in results {
            queries_this_interval += u.queries.len() as u64;
            for l in u.queries {
                hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "query_benchmark.query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }
        }
        benchmark_increment_counter!(
            "query_benchmark.queries_executed",
            Count,
            queries_this_interval
        );
        let qps = hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!("query_benchmark.qps", Count, "Queries per second", qps);
        debug!(
            "qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999))
        );

        // This benchmark returns the last seen benchmark results.
        *benchmark_results = BenchmarkResults::from(&[
            ("qps", qps),
            ("latency p50", us_to_ms(hist.value_at_quantile(0.5))),
            ("latency p90", us_to_ms(hist.value_at_quantile(0.9))),
            ("latency p99", us_to_ms(hist.value_at_quantile(0.99))),
            ("latency p99.99", us_to_ms(hist.value_at_quantile(0.9999))),
        ]);

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()> {
        // Prepare the query to retrieve the query schema.
        let opts = mysql_async::Opts::from_url(&params.mysql_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let prepared_statement = params.query.prepared_statement(&mut conn).await?;

        let mut last_report = Instant::now();
        let mut result_batch = EvictionBenchmarkResultBatch::new();
        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = EvictionBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            let (query, params) = prepared_statement.generate_query();
            let start = Instant::now();
            let res: mysql_async::Result<Vec<Row>> = conn.exec(query, params).await;
            if let Err(e) = res {
                error!(err = %e, "Error on exec");
                return Err(e.into());
            }
            result_batch.queries.push(start.elapsed().as_micros());
        }
    }
}
