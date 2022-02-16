//! A ReadySet query benchmark that supports arbitrary parameterized queries.
//! This is a multi-threaded benchmark that runs a single query with
//! randomly generated parameters across several threads. It can be used to
//! evaluate a ReadySet deployment at various loads and request patterns.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::{ArbitraryQueryParameters, PreparedStatement};
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram, benchmark_increment_counter};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct QueryBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    query: ArbitraryQueryParameters,

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[clap(long)]
    target_qps: Option<u64>,

    /// The number of threads to execute the read benchmark across.
    #[clap(long, default_value = "1")]
    threads: u64,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,

    /// The duration, specified as the number of seconds that the benchmark
    /// should be running. If `None` is provided, the benchmark will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = crate::utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,
}

#[derive(Clone)]
pub struct QueryBenchmarkThreadParams {
    target_qps: Option<u64>,
    threads: u64,
    mysql_conn_str: String,
    prepared_statement: Arc<Mutex<PreparedStatement>>,
}

#[async_trait]
impl BenchmarkControl for QueryBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;

        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // For now drop the result of migrate as CREATE CACHED QUERY does not support
        // non-select queries.
        let _ = self.query.migrate(&mut conn).await;

        Ok(())
    }

    async fn reset(&self, deployment: &DeploymentParameters) -> Result<()> {
        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // For now drop the result of migrate as CREATE CACHED QUERY does not support
        // non-select queries.
        let _ = self.query.unmigrate(&mut conn).await;
        Ok(())
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // For now drop the result of migrate as CREATE CACHED QUERY does not support
        // non-select queries.
        let _ = self.query.migrate(&mut conn).await;
        let prepared_statement =
            Arc::new(Mutex::new(self.query.prepared_statement(&mut conn).await?));

        let thread_data = QueryBenchmarkThreadParams {
            target_qps: self.target_qps,
            threads: self.threads,
            mysql_conn_str: deployment.target_conn_str.clone(),
            prepared_statement: prepared_statement.clone(),
        };
        benchmark_counter!(
            "query_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run"
        );
        multi_thread::run_multithread_benchmark::<Self>(
            self.threads,
            thread_data.clone(),
            self.run_for,
        )
        .await
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct QueryBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
}

impl QueryBenchmarkResultBatch {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }
}

#[async_trait]
impl MultithreadBenchmark for QueryBenchmark {
    type BenchmarkResult = QueryBenchmarkResultBatch;
    type Parameters = QueryBenchmarkThreadParams;

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
            ("qps", (qps, Unit::Count)),
            (
                "latency p50",
                (us_to_ms(hist.value_at_quantile(0.5)), Unit::Milliseconds),
            ),
            (
                "latency p90",
                (us_to_ms(hist.value_at_quantile(0.9)), Unit::Milliseconds),
            ),
            (
                "latency p99",
                (us_to_ms(hist.value_at_quantile(0.99)), Unit::Milliseconds),
            ),
            (
                "latency p99.99",
                (us_to_ms(hist.value_at_quantile(0.9999)), Unit::Milliseconds),
            ),
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

        let mut throttle_interval =
            multi_thread::throttle_interval(params.target_qps, params.threads);
        let mut last_report = Instant::now();
        let mut result_batch = QueryBenchmarkResultBatch::new();
        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = QueryBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }

            let (query, params) = params.prepared_statement.lock().generate_query();
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
