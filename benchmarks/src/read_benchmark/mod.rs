//! A ReadySet read benchmark that supports arbitrary parameterized queries.
//! This is a multi-threaded benchmark that runs a single query with
//! randomly generated parameters across several threads. It can be used to
//! evaluate a ReadySet deployment at various loads and request patterns.
use std::convert::TryFrom;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use tokio::sync::mpsc::UnboundedSender;

use crate::benchmark::{BenchmarkControl, BenchmarkParameters};
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::query::ArbitraryQueryParameters;

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Clone)]
pub struct ReadBenchmarkParams {
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

    /// The connection string of a database that accepts MySQL queries.
    #[clap(long)]
    mysql_conn_str: String,
}

#[derive(Parser, Clone)]
pub struct ReadBenchmark {
    #[clap(flatten)]
    params: ReadBenchmarkParams,
}

#[async_trait]
impl BenchmarkControl for ReadBenchmark {
    async fn setup(&self) -> Result<()> {
        // TODO(justin): Support data generation before benchmarking.
        Ok(())
    }

    async fn benchmark(&self) -> Result<()> {
        multi_thread::run_multithread_benchmark::<Self>(self.params.threads, self.params.clone())
            .await
    }
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct ReadBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
}

impl ReadBenchmarkResultBatch {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.
}

#[async_trait]
impl MultithreadBenchmark for ReadBenchmark {
    type BenchmarkResult = ReadBenchmarkResultBatch;
    type Parameters = ReadBenchmarkParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
    ) -> Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        for u in results {
            for l in u.queries {
                hist.record(u64::try_from(l).unwrap()).unwrap();
            }
        }
        let qps = hist.len() as f64 / interval.as_secs() as f64;
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
        let opts = mysql_async::Opts::from_url(&params.mysql_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let prepared_statement = params.query.prepared_statement(&mut conn).await?;

        let mut throttle_interval =
            multi_thread::throttle_interval(params.target_qps, params.threads);
        let mut last_report = Instant::now();
        let mut result_batch = ReadBenchmarkResultBatch::new();
        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = ReadBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }

            let (query, params) = prepared_statement.generate_query();
            let start = Instant::now();
            let _: Vec<Row> = conn.exec(query, params).await?;
            result_batch.queries.push(start.elapsed().as_micros());
        }
    }
}
