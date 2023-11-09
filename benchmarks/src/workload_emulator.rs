//! This benchmark generates a mixed load of queries and sends them to upstream/upstream adapter.
//! Further, it allows three different testing modes:
//! - direct to the upstream database (bypassing readyset completely)
//! - use a look-aside cache (like memcached or redis) and an upstream database
//! (no readyset use)
//! - readyset backed by an upstream database (the standard model)
//!
//! The benchmark accepts a yaml file describing the workload, with the schema described in
//! [`crate::spec`].
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use database_utils::{DatabaseConnection, DatabaseStatement, QueryableConnection};
use metrics::Unit;
use nom_sql::SqlQuery;
use rand::distributions::Uniform;
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;
use readyset_data::{DfType, DfValue, Dialect};
use redis::{AsyncCommands, SetExpiry, SetOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;
use vmemcached::{Client, ConnectionManager, Pool, Settings};
use zipf::ZipfDistribution;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::spec::WorkloadSpec;
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::us_to_ms;
use crate::{benchmark_counter, benchmark_histogram, benchmark_increment_counter};

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

pub type Distributions = HashMap<String, Arc<(Vec<Vec<DfValue>>, Sampler)>>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum BenchmarkType {
    /// Use a look-aside cache, like memcached, and query the upstream database
    /// on cache misses (no readyset).
    #[value(name = "cache")]
    Cache,

    /// Send all queries to the ReadySet instance, but do not migrate them.
    /// This will force ReadySet to proxy queries to the upstream.
    #[value(name = "proxy")]
    Proxy,

    /// Send all statements to a ReadySet instance, which is backed by an upstream database.
    #[value(name = "readyset")]
    #[default]
    ReadySet,

    /// Only execute statements against the upstream database (no cache nor readyset).
    #[value(name = "upstream")]
    Upstream,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum LookAsideCacheType {
    #[value(name = "memcached")]
    Memcached,

    #[value(name = "redis")]
    #[default]
    Redis,
}

#[derive(Debug, Parser, Clone, Default, Serialize, Deserialize)]
pub struct LookAsideCache {
    #[arg(long, requires = "cache_url")]
    cache_type: Option<LookAsideCacheType>,

    #[arg(long)]
    cache_url: Option<String>,

    /// Time-To-Live, in seconds, for items in the cache.
    #[arg(long, default_value = "10")]
    ttl_secs: u32,
}

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadEmulator {
    /// Path to the workload yaml schema
    #[arg(long, short)]
    spec: PathBuf,

    /// Install and generate from an arbitrary schema.
    #[command(flatten)]
    data_generator: Option<DataGenerator>,

    /// The type of benchmark to run.
    #[arg(long, value_enum)]
    benchmark_type: BenchmarkType,

    /// Parameters to any look-aside cache, if used.
    #[command(flatten)]
    look_aside_cache: LookAsideCache,

    /// Number of worker connections
    #[arg(long, short)]
    workers: u64,

    /// Duration of the benchmark in seconds
    #[arg(long, short, value_parser = crate::utils::seconds_as_str_to_duration)]
    run_for: Option<Duration>,

    #[arg(skip)]
    #[serde(skip)]
    query_set: Arc<Mutex<Option<Arc<QuerySet>>>>,
}

/// A query with its index and generator
pub struct Query {
    pub(crate) spec: String,
    pub idx: usize,
    pub(crate) cols: Vec<ColGenerator>,
    pub migrate: bool,
}

/// A vector of queries and weights
pub struct QuerySet {
    pub(crate) queries: Vec<Query>,
    pub(crate) weights: WeightedAliasIndex<usize>,
}

#[derive(Clone)]
pub(crate) struct WorkloadThreadParams {
    deployment: DeploymentParameters,
    query_set: Arc<QuerySet>,
    benchmark_type: BenchmarkType,
    look_aside_cache: LookAsideCache,
}

pub enum Sampler {
    Zipf(ZipfDistribution),
    Uniform(Uniform<usize>),
}

/// Generates parameter data for a single placeholder in the query
pub(crate) struct ColGenerator {
    pub(crate) dist: Arc<(Vec<Vec<DfValue>>, Sampler)>,
    pub(crate) sql_type: nom_sql::SqlType,
    pub(crate) col: usize,
}

#[derive(Debug, Clone, Copy)]
struct WorkloadResult {
    /// u32 should suffice for any practical benchmark we run
    latency_us: u32,
    /// Probably not gonna run benchmarks with billions of queries
    query_id: u32,
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct WorkloadResultBatch {
    /// Number of queries tested
    n: usize,
    queries: Vec<WorkloadResult>,
}

impl WorkloadResultBatch {
    fn new(n: usize) -> Self {
        Self {
            n,
            queries: Vec::new(),
        }
    }
}

#[async_trait]
impl BenchmarkControl for WorkloadEmulator {
    async fn setup(&self, deployment: &DeploymentParameters) -> anyhow::Result<()> {
        if let Some(ref data_generator) = self.data_generator {
            // assume the target database exists, so create schema and insert data
            data_generator.install(&deployment.setup_conn_str).await?;
            data_generator.generate(&deployment.setup_conn_str).await?;
        }

        Ok(())
    }

    async fn benchmark(
        &self,
        deployment: &DeploymentParameters,
    ) -> anyhow::Result<BenchmarkResults> {
        let yaml = std::fs::read_to_string(&self.spec).unwrap();
        let mut spec = WorkloadSpec::from_yaml(&yaml).unwrap();

        // only migrate when running readyset benches. we still need the
        // QuerySet we can get from the workload parsing, though.
        if self.benchmark_type != BenchmarkType::ReadySet {
            for query in &mut spec.queries {
                query.migrate = false;
            }
        }

        let distributions = spec
            .load_distributions(&mut deployment.connect_to_setup().await?)
            .await?;

        let mut conn = match self.benchmark_type {
            BenchmarkType::ReadySet | BenchmarkType::Proxy => {
                deployment.connect_to_target().await?
            }
            _ => deployment.connect_to_setup().await?,
        };

        let queries = spec.load_queries(&distributions, &mut conn).await?;
        *self.query_set.lock().unwrap() = Some(Arc::new(queries));

        let thread_data = WorkloadThreadParams {
            deployment: deployment.clone(),
            query_set: Arc::clone(self.query_set.lock().unwrap().deref().as_ref().unwrap()),
            benchmark_type: self.benchmark_type,
            look_aside_cache: self.look_aside_cache.clone(),
        };

        benchmark_counter!(
            "workload_emulator.total_query_count",
            Count,
            "Total number of queries executed in this benchmark run".into()
        );

        multi_thread::run_multithread_benchmark::<Self>(
            self.workers,
            thread_data.clone(),
            self.run_for,
        )
        .await
    }

    async fn reset(&self, _deployment: &DeploymentParameters) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels: HashMap<String, String> = [
            ("spec".to_string(), self.spec.display().to_string()),
            ("workers".to_string(), self.workers.to_string()),
            (
                "bench_type".to_string(),
                format!("{:?}", self.benchmark_type),
            ),
        ]
        .into();

        if let Some(queryset) = self.query_set.lock().unwrap().as_ref() {
            for Query { spec, idx, .. } in &queryset.queries {
                labels.insert(format!("query_{idx}"), spec.to_string());
            }
        }

        labels
    }

    fn forward_metrics(&self, _deployment: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }

    fn name(&self) -> &'static str {
        "workload_emulator"
    }

    fn data_generator(&mut self) -> Option<&mut DataGenerator> {
        self.data_generator.as_mut()
    }
}

impl Sampler {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Sampler::Zipf(z) => z.sample(rng),
            Sampler::Uniform(u) => u.sample(rng),
        }
    }
}

impl QuerySet {
    pub async fn prepare_all(
        &self,
        conn: &mut DatabaseConnection,
    ) -> anyhow::Result<Vec<DatabaseStatement>> {
        let mut prepared = Vec::with_capacity(self.queries.len());
        for query in self.queries.iter() {
            prepared.push(conn.prepare(query.spec.to_string()).await?);
        }
        Ok(prepared)
    }

    pub fn get_query(&self) -> &Query {
        let mut rng = rand::thread_rng();
        &self.queries[self.weights.sample(&mut rng)]
    }

    pub fn queries(&self) -> &[Query] {
        &self.queries
    }
}

impl Query {
    /// Get params for this query in a specific index
    pub fn get_params_index(&self, index: usize) -> Option<Vec<DfValue>> {
        if self.cols.is_empty() {
            return None;
        }

        let mut ret = Vec::with_capacity(self.cols.len());
        let mut last_row: &Vec<DfValue> = &vec![];
        let mut last_set: Option<Arc<_>> = None;

        for ColGenerator {
            dist,
            sql_type,
            col,
        } in &self.cols
        {
            if *col == 0
                || last_set
                    .as_ref()
                    .map(|s| !Arc::ptr_eq(dist, s))
                    .unwrap_or(false)
            {
                last_set = Some(dist.clone());
                last_row = dist.0.get(index)?;
            }

            let target_type =
                DfType::from_sql_type(sql_type, Dialect::DEFAULT_MYSQL, |_| None).unwrap();

            ret.push(
                last_row[*col]
                    .coerce_to(&target_type, &DfType::Unknown) // No from_ty, we're dealing with literal params
                    .unwrap(),
            )
        }

        Some(ret)
    }

    pub fn get_params(&self) -> Vec<DfValue> {
        let mut ret = Vec::with_capacity(self.cols.len());
        let mut rng = rand::thread_rng();

        let mut last_row: &Vec<DfValue> = &vec![];
        let mut last_set: Option<Arc<_>> = None;

        for ColGenerator {
            dist,
            sql_type,
            col,
        } in &self.cols
        {
            if *col == 0
                || last_set
                    .as_ref()
                    .map(|s| !Arc::ptr_eq(dist, s))
                    .unwrap_or(false)
            {
                last_set = Some(dist.clone());
                last_row = &dist.0[dist.1.sample(&mut rng)];
            }

            let target_type =
                DfType::from_sql_type(sql_type, Dialect::DEFAULT_MYSQL, |_| None).unwrap();

            ret.push(
                last_row[*col]
                    .coerce_to(&target_type, &DfType::Unknown) // No from_ty, we're dealing with literal params
                    .unwrap(),
            )
        }

        ret
    }
}

pub enum LookAsideCacheConnection {
    Memcached(vmemcached::Client, u32),
    Redis(redis::aio::Connection, u32),
}

impl LookAsideCacheConnection {
    async fn new(cache_params: LookAsideCache) -> Result<Self> {
        match cache_params.cache_type.expect("must provide cache type") {
            LookAsideCacheType::Memcached => {
                let pool = Pool::builder()
                    .max_size(1)
                    .build(ConnectionManager::try_from(
                        cache_params
                            .cache_url
                            .expect("must provide cache url")
                            .as_str(),
                    )?)
                    .await?;
                let options = Settings::new();
                let client = Client::with_pool(pool, options);
                Ok(Self::Memcached(client, cache_params.ttl_secs))
            }
            LookAsideCacheType::Redis => {
                let client =
                    redis::Client::open(cache_params.cache_url.expect("must provide cache url"))?;
                let conn = client.get_async_connection().await?;
                Ok(Self::Redis(conn, cache_params.ttl_secs))
            }
        }
    }

    async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Memcached(client, _) => match client.get(key).await? {
                Some(v) => Ok(Some(v)),
                None => Ok(None),
            },
            Self::Redis(conn, _) => {
                let data = conn.get(key).await?;
                Ok(data)
            }
        }
    }

    async fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match self {
            Self::Memcached(client, ttl) => {
                client
                    .set(
                        key,
                        value.as_bytes(),
                        Duration::from_secs(ttl_jitter(*ttl) as u64),
                    )
                    .await?;
                Ok(())
            }
            Self::Redis(conn, ttl) => {
                let opts =
                    SetOptions::default().with_expiration(SetExpiry::EX(ttl_jitter(*ttl) as usize));
                conn.set_options(key, value, opts).await?;
                Ok(())
            }
        }
    }

    /// Generate a unique key for this entry by concatenating the parameters,
    /// which are in the `[DfValue]`.
    fn munge_key(&self, parts: &[DfValue]) -> String {
        if parts.is_empty() {
            String::from("<empty_key>")
        } else {
            parts
                .iter()
                .map(|d| format!("{d}"))
                .collect::<Vec<String>>()
                .join("-")
        }
    }
}

/// Add some slight jitter to the ttl value else everything
/// will expire around the same time.
fn ttl_jitter(ttl_secs: u32) -> u32 {
    let variance: f32 = rand::random();
    ttl_secs + (ttl_secs as f32 * variance) as u32
}

#[async_trait]
impl MultithreadBenchmark for WorkloadEmulator {
    type BenchmarkResult = WorkloadResultBatch;
    type Parameters = WorkloadThreadParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: std::time::Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> anyhow::Result<()> {
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();

        let mut per_query_hist = match results.get(0) {
            Some(r) => std::iter::repeat(hdrhistogram::Histogram::<u64>::new(3).unwrap())
                .take(r.n)
                .collect::<Vec<_>>(),
            None => return Ok(()),
        };

        let mut overall = vec![];
        let mut per_query: HashMap<u32, Vec<f64>> = HashMap::new();
        for u in results {
            for WorkloadResult {
                query_id,
                latency_us,
            } in u.queries
            {
                overall.push(latency_us as f64);
                per_query
                    .entry(query_id)
                    .or_default()
                    .push(latency_us as f64);
                hist.record(latency_us as _).unwrap();
                per_query_hist[query_id as usize]
                    .record(latency_us as _)
                    .unwrap();
                benchmark_histogram!(
                    format!("workload_emulator.query_{}_duration", query_id),
                    Microseconds,
                    format!("Duration of query {}", query_id).into(),
                    latency_us as f64
                );
            }
        }
        benchmark_results
            .entry(
                "duration_overall",
                Unit::Microseconds,
                MetricGoal::Decreasing,
            )
            .extend(overall);
        for (query_id, data) in per_query {
            benchmark_results
                .entry(
                    &format!("duration_{}", query_id),
                    Unit::Microseconds,
                    MetricGoal::Decreasing,
                )
                .extend(data);
        }

        let qps = hist.len() as f64 / interval.as_secs() as f64;
        benchmark_increment_counter!("workload_emulator.total_query_count", Count, qps as u64);
        info!(
            "overall -\tqps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.9: {:.1} ms",
            qps,
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.999))
        );

        for (i, hist) in per_query_hist.into_iter().enumerate() {
            let qps = hist.len() as f64 / interval.as_secs() as f64;
            info!(
                "  query {i} -\tqps: {qps:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.9: {:.1} ms",
                us_to_ms(hist.value_at_quantile(0.5)),
                us_to_ms(hist.value_at_quantile(0.9)),
                us_to_ms(hist.value_at_quantile(0.99)),
                us_to_ms(hist.value_at_quantile(0.999))
            );
        }

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> anyhow::Result<()> {
        let mut conn = match params.benchmark_type {
            BenchmarkType::ReadySet | BenchmarkType::Proxy => {
                params.deployment.connect_to_target().await?
            }
            _ => params.deployment.connect_to_setup().await?,
        };
        let query_set = &params.query_set;

        // Only some upstream databases support interrogating the number of cached statements
        if let Some(stmt_cache_size) = conn.cached_statements() {
            assert!(stmt_cache_size >= params.query_set.queries.len());
        }

        // Generate a prepared version for all of the statements
        let prepared = query_set.prepare_all(&mut conn).await?;

        let mut cache_conn = match params.benchmark_type {
            BenchmarkType::Cache => {
                Some(LookAsideCacheConnection::new(params.look_aside_cache).await?)
            }
            _ => None,
        };

        let mut last_report = Instant::now();
        let mut result_batch = WorkloadResultBatch::new(query_set.queries.len());

        // a simple cache to know if a given query is a SELECT so we don't need to call parse()
        // on each loop iteration. this implementation is barely sufficient, but serviceable
        // nonetheless. WorkloadSpec.load_queries() does give us the queries in a
        // consistent order, and the index is the index of an enumeration, and thus monotonic.
        let is_select: Vec<bool> = params
            .query_set
            .queries
            .iter()
            .map(|query| matches!(query.spec.parse::<SqlQuery>(), Ok(SqlQuery::Select(_))))
            .collect();

        loop {
            // Report results every REPORT_RESULTS_INTERVAL.
            if last_report.elapsed() > REPORT_RESULTS_INTERVAL {
                let mut new_results = WorkloadResultBatch::new(query_set.queries.len());
                std::mem::swap(&mut new_results, &mut result_batch);
                sender.send(new_results)?;
                last_report = Instant::now();
            }

            let query = params.query_set.get_query();
            let params = query.get_params();
            let use_cache = cache_conn.is_some() && is_select[query.idx];
            let start = Instant::now();

            // if using a cache, see if it's already got the data and bail early
            let cache_key = if use_cache {
                let conn = cache_conn.as_mut().expect("must be a cache connection");
                let key = conn.munge_key(&params);
                let cache_data = conn.get(&key).await?;
                if cache_data.is_some() {
                    result_batch.queries.push(WorkloadResult {
                        query_id: query.idx as _,
                        latency_us: start.elapsed().as_micros() as _,
                    });
                    continue;
                }
                Some(key)
            } else {
                None
            };

            // Execute the prepared statement, with the generated params
            let results = conn.execute(&prepared[query.idx], params).await?;

            // if using a cache, put the select'ed data into it
            if use_cache {
                let data = if !results.is_empty() {
                    // naively concat all the resultant column values
                    let rows = Vec::<Vec<DfValue>>::try_from(results).unwrap();
                    // guesstimate some number of bytes per row
                    let mut s = String::with_capacity(rows.len() * 64);

                    for row in rows {
                        s += &row
                            .into_iter()
                            .map(|d| format!("{d}"))
                            .collect::<Vec<String>>()
                            .join("-");
                    }
                    s
                } else {
                    String::from("placeholder data")
                };

                let conn = cache_conn.as_mut().expect("must be a cache connection");
                conn.set(&cache_key.expect("must have"), &data).await?;
            };

            result_batch.queries.push(WorkloadResult {
                query_id: query.idx as _,
                latency_us: start.elapsed().as_micros() as _,
            });
        }
    }
}
