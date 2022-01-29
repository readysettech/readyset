use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, ValueHint};
use metrics::Unit;
use mysql_async::prelude::Queryable;
use parking_lot::Mutex;
use query_generator::TableSpec;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::spec::{DatabaseGenerationSpec, DatabaseSchema};
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct WriteBenchmark {
    /// Path to the desired database SQL schema. Each table must have
    /// a primary key to generate data.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[clap(long)]
    target_qps: Option<u64>,

    /// The number of threads to execute the read benchmark across.
    #[clap(long, default_value = "1")]
    threads: u64,

    /// The duration, specified as the number of seconds that the benchmark
    /// should be running. If `None` is provided, the benchmark will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = crate::utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,
}

#[derive(Clone)]
pub struct WriteBenchmarkThreadData {
    mysql_conn_str: String,
    target_qps: Option<u64>,
    threads: u64,

    /// Tables we will generate data for from the schema.
    tables: Vec<Arc<Mutex<TableSpec>>>,
}

impl WriteBenchmarkThreadData {
    fn new(w: &WriteBenchmark, deployment: &DeploymentParameters) -> Result<Self> {
        let schema = DatabaseSchema::try_from((w.schema.clone(), HashMap::new()))?;
        let database_spec = DatabaseGenerationSpec::new(schema);
        let tables = database_spec
            .tables
            .into_iter()
            .map(|(_, v)| Arc::new(Mutex::new(v.table)))
            .collect();

        Ok(Self {
            mysql_conn_str: deployment.target_conn_str.clone(),
            target_qps: w.target_qps,
            threads: w.threads,
            tables,
        })
    }
}

#[async_trait]
impl BenchmarkControl for WriteBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        let opts = mysql_async::Opts::from_url(&deployment.setup_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let ddl = std::fs::read_to_string(self.schema.as_path())?;
        conn.query_drop(ddl).await?;
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        multi_thread::run_multithread_benchmark::<Self>(
            self.threads,
            WriteBenchmarkThreadData::new(self, deployment)?,
            self.run_for,
        )
        .await
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(
            "schema".to_string(),
            self.schema.to_string_lossy().to_string(),
        );
        labels.insert(
            "target_qps".to_string(),
            self.target_qps.unwrap_or(0).to_string(),
        );
        labels.insert("threads".to_string(), self.threads.to_string());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        Vec::new()
    }
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct WriteBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
}

impl WriteBenchmarkResultBatch {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }
}

#[async_trait]
impl MultithreadBenchmark for WriteBenchmark {
    type BenchmarkResult = WriteBenchmarkResultBatch;
    type Parameters = WriteBenchmarkThreadData;

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
                    "write_benchmark.query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }
        }
        benchmark_counter!(
            "write_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run",
            queries_this_interval
        );
        let qps = hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!("write_benchmark.qps", Count, "Queries per second", qps);
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
        let opts = mysql_async::Opts::from_url(&params.mysql_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

        let mut last_report = Instant::now();
        let mut result_batch = WriteBenchmarkResultBatch::new();
        let mut throttle_interval =
            multi_thread::throttle_interval(params.target_qps, params.threads);
        loop {
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = WriteBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }

            let insert = {
                // TODO(justin): Evaluate if we can improve performance by precomputing these
                // variables outside of the loop.
                let mut rng = rand::thread_rng();
                let index: usize = rng.gen_range(0..(params.tables.len()));
                let mut spec = params.tables.get(index).unwrap().lock();
                let table_name = spec.name.clone();
                let data = spec.generate_data_from_index(1, 0, false);
                let columns = spec.columns.keys().collect::<Vec<_>>();
                nom_sql::InsertStatement {
                    table: table_name.into(),
                    fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
                    data: data
                        .into_iter()
                        .map(|mut row| {
                            columns
                                .iter()
                                .map(|col| row.remove(col).unwrap().try_into().unwrap())
                                .collect()
                        })
                        .collect(),
                    ignore: false,
                    on_duplicate: None,
                }
            };

            let start = Instant::now();
            let _ = conn.query_drop(insert.to_string()).await;
            result_batch.queries.push(start.elapsed().as_micros());
        }
    }
}
