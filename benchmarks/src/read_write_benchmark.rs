use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use database_utils::{DatabaseURL, QueryableConnection};
use metrics::Unit;
use rand::{thread_rng, Rng};
use readyset_data::DfValue;
use readyset_sql::Dialect;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::{ArbitraryQueryParameters, QuerySpec};
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct ReadWriteBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[command(flatten)]
    read_query: ArbitraryQueryParameters,

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[arg(long)]
    target_read_qps: u64,

    /// The rate at which to issue update queries if attainable.
    #[arg(long)]
    target_update_qps: u64,

    /// The number of threads to execute the read benchmark across.
    #[arg(long, default_value = "1")]
    threads: u64,

    /// A path to the query that we are benchmarking.
    #[arg(long)]
    update_query: PathBuf,

    /// A annotation spec for each of the parameters in query. See
    /// `DistributionAnnotations` for the format of the file.
    #[arg(long)]
    update_query_spec_file: Option<PathBuf>,

    /// An query spec passed in as a comma separated list. See
    /// `DistributionAnnotation` for the format for each parameters annotation.
    #[arg(long, conflicts_with = "query_spec_file")]
    update_query_spec: Option<String>,

    // The dialect that should be used to parse the query string.
    #[arg(long, default_value = "mysql")]
    dialect: Dialect,

    /// Install and generate from an arbitrary schema.
    #[command(flatten)]
    data_generator: DataGenerator,

    /// The duration, specified as the number of seconds that the benchmark
    /// should be running. If `None` is provided, the benchmark will run
    /// until it is interrupted.
    #[arg(long, value_parser = crate::utils::seconds_as_str_to_duration)]
    pub run_for: Option<Duration>,
}

#[derive(Clone)]
pub struct ReadWriteBenchmarkThreadParams {
    read_query: ArbitraryQueryParameters,
    update_query: ArbitraryQueryParameters,
    target_read_qps: u64,
    target_update_qps: u64,
    threads: u64,
    upstream_conn_str: String,
}

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
        // Explicitly migrate the query before benchmarking.
        let mut conn = DatabaseURL::from_str(&deployment.target_conn_str)?
            .connect(None)
            .await?;
        // For now drop the result of migrate as CREATE CACHE does not support
        // non-select queries.
        let _ = self.read_query.migrate(&mut conn).await;

        let thread_data = ReadWriteBenchmarkThreadParams {
            read_query: self.read_query.clone(),
            // clap does not support flattening with prefix/suffix so we need to
            // do this manually so it does not clash.
            update_query: ArbitraryQueryParameters::new(
                QuerySpec::File(self.update_query.clone().try_into().unwrap()),
                self.update_query_spec_file.clone(),
                self.update_query_spec.clone(),
                self.dialect,
            ),
            target_read_qps: self.target_read_qps,
            target_update_qps: self.target_update_qps,
            threads: self.threads,
            upstream_conn_str: deployment.target_conn_str.clone(),
        };
        benchmark_counter!(
            "read_write_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run".into()
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

    fn name(&self) -> &'static str {
        "read_write_benchmark"
    }

    fn data_generator(&mut self) -> Option<&mut DataGenerator> {
        Some(&mut self.data_generator)
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
        let mut read_data = vec![];
        let mut update_data = vec![];
        for u in results {
            for l in u.read_queries {
                read_data.push(l as f64);
                read_hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "read_write_benchmark.read_query_duration",
                    Microseconds,
                    "Duration of queries executed".into(),
                    l as f64
                );
            }

            for l in u.update_queries {
                update_data.push(l as f64);
                update_hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "read_write_benchmark.update_query_duration",
                    Microseconds,
                    "Duration of queries executed".into(),
                    l as f64
                );
            }
        }
        benchmark_results
            .entry("read_duration", Unit::Microseconds, MetricGoal::Decreasing)
            .extend(read_data);
        benchmark_results
            .entry(
                "update_duration",
                Unit::Microseconds,
                MetricGoal::Decreasing,
            )
            .extend(update_data);
        let read_qps = read_hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!(
            "read_write_benchmark.read_qps",
            Count,
            "Queries per second".into(),
            read_qps
        );
        let update_qps = update_hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!(
            "read_write_benchmark.update_qps",
            Count,
            "Queries per second".into(),
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

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()> {
        // Prepare the query to retrieve the query schema.
        let mut conn = DatabaseURL::from_str(&params.upstream_conn_str)?
            .connect(None)
            .await?;

        let mut read_prepared_statement = params.read_query.prepared_statement(&mut conn).await?;
        let mut update_prepared_statement =
            params.update_query.prepared_statement(&mut conn).await?;

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
            let res: Result<Vec<Vec<DfValue>>, _> = conn.execute(query, params).await?.try_into();
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
