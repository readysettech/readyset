//! A ReadySet query benchmark that supports arbitrary parameterized queries.
//! This is a multi-threaded benchmark that runs a single query with
//! randomly generated parameters across several threads. It can be used to
//! evaluate a ReadySet deployment at various loads and request patterns.
//! Moreover it attempts to get to a state where noria server is evicting
//! state at a rate sufficient to achieve a given cache hit rate.
//! In order to get the desired hit rate, the benchmark will automatically
//! scale down the query key space to narrow it down and improve cache hit.
//! It can not scale up, therefore the provided query spec must be able to
//! achieve a cache hit rate lower than the desired one (i.e have a wider
//! gamut than needed for the desired hit rate).
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use noria::metrics::recorded;
use prometheus_parse::Scrape;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::{forward, ForwardPrometheusMetrics};
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram, benchmark_increment_counter};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

static SCALE: AtomicU64 = AtomicU64::new(unsafe { std::mem::transmute(1.0f64) });

fn get_scale() -> f64 {
    f64::from_bits(SCALE.load(Relaxed))
}

fn set_scale(scale: f64) {
    SCALE.store(scale.to_bits(), Relaxed);
}

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

    /// Attempt to scale down query range from query_spec in order to hit the desired
    /// hit rate. This will only work if the range would naturally achieve a lower hit
    /// rate for a given eviction policy, since it will scale the range down to get
    /// a higher hit rate. Range 1 - 100 percent.
    #[clap(long, default_value = "100")]
    target_hit_rate: u8,
}

#[derive(Clone)]
pub struct EvictionBenchmarkParams {
    query: ArbitraryQueryParameters,
    deployment_params: DeploymentParameters,
    mysql_conn_str: String,
    target_hit_rate: f64,
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
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        // For now drop the result of migrate as CREATE CACHE does not support
        // non-select queries.
        let _ = self.query.migrate(&mut conn).await;

        assert!(self.target_hit_rate > 0 && self.target_hit_rate <= 100);

        let thread_data = EvictionBenchmarkParams {
            query: self.query.clone(),
            deployment_params: deployment.clone(),
            mysql_conn_str: deployment.target_conn_str.clone(),
            target_hit_rate: self.target_hit_rate as f64 / 100.0,
        };

        benchmark_counter!(
            "query_benchmark.queries_executed",
            Count,
            "Number of queries executed in this benchmark run"
        );

        let result = multi_thread::run_multithread_benchmark::<Self>(
            self.threads as u64,
            thread_data.clone(),
            self.run_for,
        )
        .await?;

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

    fn name(&self) -> &'static str {
        "eviction_benchmark"
    }
}

#[derive(Debug, Clone, Default)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct EvictionBenchmarkResultBatch {
    /// Query end-to-end latency in ms.
    queries: Vec<u128>,
    /// Amount of memory evicted since last batch, in bytes
    evicted_bytes: Option<f64>,
    /// Number of cache hits since last batch
    cache_hit: Option<f64>,
    /// Number of cache misses since last batch
    cache_miss: Option<f64>,
}

impl EvictionBenchmarkResultBatch {
    fn new() -> Self {
        Default::default()
    }
}

fn get_total_for_metric(scrape: &prometheus_parse::Scrape, metric: &'static str) -> f64 {
    let metric_name = metric.replace('.', "_") + "_sum";
    scrape
        .samples
        .iter()
        .filter(|m| m.metric == metric_name)
        .map(|m| match m.value {
            prometheus_parse::Value::Counter(f) => f,
            prometheus_parse::Value::Gauge(f) => f,
            prometheus_parse::Value::Histogram(_) => todo!(),
            prometheus_parse::Value::Summary(_) => todo!(),
            prometheus_parse::Value::Untyped(f) => f,
        })
        .sum()
}

fn get_metric(scrape: &prometheus_parse::Scrape, metric: &'static str) -> f64 {
    let metric_name = metric.replace('.', "_");
    scrape
        .samples
        .iter()
        .find(|m| m.metric == metric_name)
        .map(|m| match m.value {
            prometheus_parse::Value::Counter(f) => f,
            prometheus_parse::Value::Gauge(f) => f,
            prometheus_parse::Value::Histogram(_) => todo!(),
            prometheus_parse::Value::Summary(_) => todo!(),
            prometheus_parse::Value::Untyped(f) => f,
        })
        .unwrap_or(0.)
}

async fn scrape_metrics(url: &str, client: &reqwest::Client) -> Result<Scrape> {
    let metrics = client.get(url).send().await?.text().await?;
    let lines = metrics.lines().map(|s| Ok(s.to_owned()));
    Ok(prometheus_parse::Scrape::parse(lines)?)
}

fn hit_rate(hits: f64, misses: f64) -> f64 {
    if hits > 0. {
        hits / (hits + misses)
    } else {
        0.
    }
}

fn within_tolerance(desired: f64, actual: f64) -> bool {
    (desired - actual).abs() < 0.003
}

fn adjust_scale(
    target_hit_rate: f64,
    cur_hit_rate: f64,
    prev_hit_rate: f64,
    cur_scale: f64,
) -> Option<f64> {
    if within_tolerance(target_hit_rate, cur_hit_rate) {
        // We are at desired rate
        return None;
    }

    debug!(%cur_hit_rate, %prev_hit_rate, %cur_scale);

    if cur_hit_rate < target_hit_rate {
        // We need to improve hit rate, for that we need to scale down so more
        // keys remain in cache
        if cur_hit_rate > prev_hit_rate * 1.2 {
            // We are trending up, so do nothing
            return None;
        }

        if cur_scale > 0.005 {
            // We can reduce our scale
            return Some(f64::max(
                0.005,
                cur_scale
                    - f64::min(
                        cur_scale * 0.1, // Don't adjust scale by more than 10% at a time
                        (target_hit_rate - prev_hit_rate).abs() * 1.5,
                    ),
            ));
        }
    }

    if cur_hit_rate > target_hit_rate {
        // We need to make hit rate worse, for that we need to scale up so
        // more keys are evicted
        if prev_hit_rate > cur_hit_rate * 1.2 {
            // We are trending down, so do nothing
            return None;
        }

        if cur_scale < 1.0 {
            // We can increase our scale
            return Some(f64::min(
                1.0,
                cur_scale
                    + f64::min(
                        cur_scale * 0.1, // Don't adjust scale by more than 10% at a time
                        (target_hit_rate - prev_hit_rate).abs() * 1.5,
                    ),
            ));
        }
    }

    None
}

async fn metrics_task(
    params: DeploymentParameters,
    sender: UnboundedSender<EvictionBenchmarkResultBatch>,
    target_hit_rate: f64,
) -> Result<()> {
    info!("Target cache hit rate {:.2}", target_hit_rate);
    let metrics_url = params.prometheus_endpoint.unwrap().metrics_url;
    let metrics_client = reqwest::Client::new();

    let mut interval = tokio::time::interval(REPORT_RESULTS_INTERVAL);
    interval.tick().await; // First tick is immediate

    let initial = scrape_metrics(&metrics_url, &metrics_client).await?;
    let mut last_evicted = get_total_for_metric(&initial, recorded::DOMAIN_EVICTION_FREED_MEMORY);
    let mut last_hits = get_metric(&initial, recorded::SERVER_VIEW_QUERY_HIT);
    let mut last_misses = get_metric(&initial, recorded::SERVER_VIEW_QUERY_MISS);
    let mut scale = 1.0;

    let mut last_hit_rate = hit_rate(last_hits, last_misses);

    // Report results every REPORT_RESULTS_INTERVAL.
    loop {
        interval.tick().await;

        let metrics = scrape_metrics(&metrics_url, &metrics_client).await?;

        let evicted = get_total_for_metric(&metrics, recorded::DOMAIN_EVICTION_FREED_MEMORY);
        let hit = get_metric(&metrics, recorded::SERVER_VIEW_QUERY_HIT);
        let miss = get_metric(&metrics, recorded::SERVER_VIEW_QUERY_MISS);

        sender.send(EvictionBenchmarkResultBatch {
            queries: Vec::new(),
            evicted_bytes: Some(evicted - last_evicted),
            cache_hit: Some(hit - last_hits),
            cache_miss: Some(miss - last_misses),
        })?;

        let cur_hit_rate = hit_rate(hit - last_hits, miss - last_misses);

        last_evicted = evicted;
        last_hits = hit;
        last_misses = miss;

        if let Some(new_scale) = adjust_scale(target_hit_rate, cur_hit_rate, last_hit_rate, scale) {
            scale = new_scale;
            set_scale(scale);
        }

        last_hit_rate = cur_hit_rate;
    }
}

#[async_trait]
impl MultithreadBenchmark for EvictionBenchmark {
    type BenchmarkResult = EvictionBenchmarkResultBatch;
    type Parameters = EvictionBenchmarkParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> Result<()> {
        let mut evicted = 0.;
        let mut hit = 0.;
        let mut miss = 0.;
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut queries_this_interval = 0;
        let results_data =
            benchmark_results.entry("query_duration", Unit::Microseconds, MetricGoal::Decreasing);
        for u in results {
            queries_this_interval += u.queries.len() as u64;
            for l in u.queries {
                results_data.push(l as f64);
                hist.record(u64::try_from(l).unwrap()).unwrap();
                benchmark_histogram!(
                    "query_benchmark.query_duration",
                    Microseconds,
                    "Duration of queries executed",
                    l as f64
                );
            }
            evicted += u.evicted_bytes.unwrap_or(0.);
            hit += u.cache_hit.unwrap_or(0.);
            miss += u.cache_miss.unwrap_or(0.);
        }
        benchmark_increment_counter!(
            "query_benchmark.queries_executed",
            Count,
            queries_this_interval
        );
        let qps = hist.len() as f64 / interval.as_secs() as f64;
        let cache_hit_rate = if hit > 0. { hit / (hit + miss) } else { 0. };
        benchmark_histogram!("query_benchmark.qps", Count, "Queries per second", qps);
        info!(
            "qps: {:.0}\tevicted: {:.2} MiB\thit rate: {:.1}%\tscale: {:.3}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            evicted/1024./1024.,
            cache_hit_rate * 100.,
            get_scale(),
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999)),
        );

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> Result<()> {
        static RUNNING_METRICS: AtomicBool = AtomicBool::new(false);

        // Prepare the query to retrieve the query schema.
        let opts = mysql_async::Opts::from_url(&params.mysql_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let prepared_statement = params.query.prepared_statement(&mut conn).await?;
        if !RUNNING_METRICS.swap(true, Relaxed) {
            // Spawn (only once) the task that scrapes metrics and adjust the scale
            tokio::spawn(metrics_task(
                params.deployment_params,
                sender.clone(),
                params.target_hit_rate,
            ));
        }

        let mut last_report = Instant::now();
        let mut result_batch = EvictionBenchmarkResultBatch::new();

        let (query, mut genset) = prepared_statement.query_generators();

        let mut scale = get_scale();

        // We want to be able to scale down the key range in order to achieve
        // the desired hit rate. In order to to that we find the first Uniform
        // or Zipf generator, and adjust it with a scaled

        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = EvictionBenchmarkResultBatch::new();
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
                scale = get_scale();
                debug!(%scale);
            }

            let start = Instant::now();
            let res: mysql_async::Result<Vec<Row>> =
                conn.exec(&query, genset.generate_scaled(scale)).await;
            if let Err(e) = res {
                error!(err = %e, "Error on exec");
                return Err(e.into());
            }
            result_batch.queries.push(start.elapsed().as_micros());
        }
    }
}
