use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct ReadWriteBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    read_query: ArbitraryQueryParameters,

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[clap(long)]
    target_read_qps: u64,

    /// The rate at which to issue update queries if attainable.
    #[clap(long)]
    target_update_qps: u64,

    /// The number of threads to execute the read benchmark across.
    #[clap(long, default_value = "1")]
    threads: u64,

    /// A path to the query that we are benchmarking.
    #[clap(long)]
    update_query: PathBuf,

    /// A annotation spec for each of the parameters in query. See
    /// `DistributionAnnotations` for the format of the file.
    #[clap(long)]
    update_query_spec_file: Option<PathBuf>,

    /// An query spec passed in as a comma separated list. See
    /// `DistributionAnnotation` for the format for each parameters annotation.
    #[clap(long, conflicts_with = "query-spec-file")]
    update_query_spec: Option<String>,

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
pub struct ReadWriteBenchmarkThreadParams {
    read_query: ArbitraryQueryParameters,
    update_query: ArbitraryQueryParameters,
    target_read_qps: u64,
    target_update_qps: u64,
    threads: u64,
    mysql_conn_str: String,
}

#[async_trait]
impl BenchmarkControl for ReadWriteBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;
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
        let _ = self.read_query.migrate(&mut conn).await;

        let thread_data = ReadWriteBenchmarkThreadParams {
            read_query: self.read_query.clone(),
            // clap does not support flattening with prefix/suffix so we need to
            // do this manually so it does not clash.
            update_query: ArbitraryQueryParameters::new(
                self.update_query.clone(),
                self.update_query_spec_file.clone(),
                self.update_query_spec.clone(),
            ),
            target_read_qps: self.target_read_qps,
            target_update_qps: self.target_update_qps,
            threads: self.threads,
            mysql_conn_str: deployment.target_conn_str.clone(),
        };
        benchmark_counter!(
            "read_write_benchmark.queries_executed",
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
        labels.extend(self.read_query.labels());
        labels.insert(
            "update_query_file".to_string(),
            self.update_query.to_string_lossy().to_string(),
        );
        if let Some(update_query_spec_file) = self.update_query_spec_file.as_ref() {
            labels.insert(
                "update_query_spec_file".to_string(),
                update_query_spec_file.to_string_lossy().to_string(),
            );
        }
        if let Some(update_query_spec) = self.update_query_spec.clone() {
            labels.insert("update_query_spec".to_string(), update_query_spec);
        }

        //labels.extend(self.update_query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct ReadWriteBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    read_queries: Vec<u128>,
    update_queries: Vec<u128>,
}

impl ReadWriteBenchmarkResultBatch {
    fn new() -> Self {
        Self {
            read_queries: Vec::new(),
            update_queries: Vec::new(),
        }
    }
}

#[async_trait]
impl MultithreadBenchmark for ReadWriteBenchmark {
    type BenchmarkResult = ReadWriteBenchmarkResultBatch;
    type Parameters = ReadWriteBenchmarkThreadParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> Result<()> {
        let mut read_hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut update_hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for u in results {
            for l in u.read_queries {
                read_hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "read_write_benchmark.read_query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }

            for l in u.update_queries {
                update_hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "read_write_benchmark.update_query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }
        }
        let read_qps = read_hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!(
            "read_write_benchmark.read_qps",
            Count,
            "Queries per second",
            read_qps
        );
        let update_qps = update_hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!(
            "read_write_benchmark.update_qps",
            Count,
            "Queries per second",
            update_qps
        );
        debug!(
            "read qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            read_qps,
            us_to_ms(read_hist.value_at_quantile(0.5)),
            us_to_ms(read_hist.value_at_quantile(0.9)),
            us_to_ms(read_hist.value_at_quantile(0.99)),
            us_to_ms(read_hist.value_at_quantile(0.9999))
        );

        debug!(
            "update qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            update_qps,
            us_to_ms(update_hist.value_at_quantile(0.5)),
            us_to_ms(update_hist.value_at_quantile(0.9)),
            us_to_ms(update_hist.value_at_quantile(0.99)),
            us_to_ms(update_hist.value_at_quantile(0.9999))
        );

        // This benchmark returns the last seen benchmark results.
        *benchmark_results = BenchmarkResults::from(&[
            ("read qps", (read_qps, Unit::Count)),
            (
                "read latency p50",
                (
                    us_to_ms(read_hist.value_at_quantile(0.5)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "read latency p90",
                (
                    us_to_ms(read_hist.value_at_quantile(0.9)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "read latency p99",
                (
                    us_to_ms(read_hist.value_at_quantile(0.99)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "read latency p99.99",
                (
                    us_to_ms(read_hist.value_at_quantile(0.9999)),
                    Unit::Milliseconds,
                ),
            ),
            ("update qps", (update_qps, Unit::Count)),
            (
                "update latency p50",
                (
                    us_to_ms(update_hist.value_at_quantile(0.5)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "update latency p90",
                (
                    us_to_ms(update_hist.value_at_quantile(0.9)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "update latency p99",
                (
                    us_to_ms(update_hist.value_at_quantile(0.99)),
                    Unit::Milliseconds,
                ),
            ),
            (
                "update latency p99.99",
                (
                    us_to_ms(update_hist.value_at_quantile(0.9999)),
                    Unit::Milliseconds,
                ),
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
        let read_prepared_statement = params.read_query.prepared_statement(&mut conn).await?;
        let update_prepared_statement = params.update_query.prepared_statement(&mut conn).await?;

        let mut throttle_interval = multi_thread::throttle_interval(
            Some(params.target_read_qps + params.target_update_qps),
            params.threads,
        );
        let mut last_report = Instant::now();
        let mut result_batch = ReadWriteBenchmarkResultBatch::new();

        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = ReadWriteBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }

            // Pull a random number out of the qps bucket, if it is in the read range, we
            // issue a read query.
            let is_read = {
                let mut rng = thread_rng();
                rng.gen_range(0..(params.target_read_qps + params.target_update_qps))
                    <= params.target_read_qps
            };

            let (query, params) = if is_read {
                read_prepared_statement.generate_query()
            } else {
                update_prepared_statement.generate_query()
            };

            let start = Instant::now();
            let res: mysql_async::Result<Vec<Row>> = conn.exec(query, params).await;
            if let Err(e) = res {
                error!(err = %e, "Error on exec");
                return Err(e.into());
            }
            if is_read {
                result_batch.read_queries.push(start.elapsed().as_micros());
            } else {
                result_batch
                    .update_queries
                    .push(start.elapsed().as_micros());
            }
        }
    }
}
