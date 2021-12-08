//! A ReadySet query benchmark that supports arbitrary parameterized queries.
//! This is a multi-threaded benchmark that runs a single query with
//! randomly generated parameters across several threads. It can be used to
//! evaluate a ReadySet deployment at various loads and request patterns.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

use crate::benchmark::{BenchmarkControl, BenchmarkParameters};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram, benchmark_increment_counter};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone)]
pub struct QueryBenchmarkParams {
    /// Common shared benchmark parameters.
    #[clap(flatten)]
    common: BenchmarkParameters,

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
}

#[derive(Parser, Clone)]
pub struct QueryBenchmark {
    #[clap(flatten)]
    params: QueryBenchmarkParams,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,
}

#[async_trait]
impl BenchmarkControl for QueryBenchmark {
    async fn setup(&self) -> Result<()> {
        self.data_generator.install().await?;
        self.data_generator.generate().await?;
        Ok(())
    }

    async fn is_already_setup(&self) -> Result<bool> {
        // TODO(mc):  If this uses a constant schema, implement a check here.  If not, keep
        // returning false.
        Ok(false)
    }

    async fn benchmark(&self) -> Result<()> {
        benchmark_counter!(
            "read_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run"
        );
        multi_thread::run_multithread_benchmark::<Self>(self.params.threads, self.params.clone())
            .await
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.params.query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self) -> Vec<ForwardPrometheusMetrics> {
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
    type Parameters = QueryBenchmarkParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
    ) -> Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut queries_this_interval = 0;
        for u in results {
            queries_this_interval += u.queries.len() as u64;
            for l in u.queries {
                hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "read_benchmark.query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }
        }
        benchmark_increment_counter!(
            "read_benchmark.queries_executed",
            Count,
            queries_this_interval
        );
        let qps = hist.len() as f64 / interval.as_secs() as f64;
        benchmark_histogram!("read_benchmark.qps", Count, "Queries per second", qps);
        println!(
            "qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999))
        );
        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()> {
        // Prepare the query to retrieve the query schema.
        let opts = mysql_async::Opts::from_url(&params.common.mysql_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let prepared_statement = params.query.prepared_statement(&mut conn).await?;

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
