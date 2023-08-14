//! This benchmark generates a mixed load of queries and sends them to upstream/upstream adapter.
//! The benchmark accepts a yaml file describing the workload, with the schema described in
//! [`crate::spec`].
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser;
use database_utils::{DatabaseConnection, DatabaseStatement, QueryableConnection};
use metrics::Unit;
use rand::distributions::Uniform;
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;
use readyset_data::{DfType, DfValue, Dialect};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;
use zipf::ZipfDistribution;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::spec::WorkloadSpec;
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::us_to_ms;

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

pub type Distributions = HashMap<String, Arc<(Vec<Vec<DfValue>>, Sampler)>>;

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadEmulator {
    /// Path to the workload yaml schema
    #[clap(long, short)]
    spec: PathBuf,
    /// Number of worker connections
    #[clap(long, short)]
    workers: u64,
    /// Duration of the benchmark in seconds
    #[clap(long, short, value_parser = crate::utils::seconds_as_str_to_duration)]
    run_for: Option<Duration>,
    #[clap(skip)]
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
        let yaml = std::fs::read_to_string(&self.spec).unwrap();
        let spec = WorkloadSpec::from_yaml(&yaml).unwrap();

        let distributions = spec
            .load_distributions(&mut deployment.connect_to_setup().await?)
            .await?;

        let queries = spec
            .load_queries(&distributions, &mut deployment.connect_to_target().await?)
            .await?;

        *self.query_set.lock().unwrap() = Some(Arc::new(queries));

        Ok(())
    }

    async fn benchmark(
        &self,
        deployment: &DeploymentParameters,
    ) -> anyhow::Result<BenchmarkResults> {
        let thread_data = WorkloadThreadParams {
            deployment: deployment.clone(),
            query_set: Arc::clone(self.query_set.lock().unwrap().deref().as_ref().unwrap()),
        };

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
        None
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
        info!(
            "qps: {:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
            qps,
            us_to_ms(hist.value_at_quantile(0.5)),
            us_to_ms(hist.value_at_quantile(0.9)),
            us_to_ms(hist.value_at_quantile(0.99)),
            us_to_ms(hist.value_at_quantile(0.9999))
        );

        for (i, hist) in per_query_hist.into_iter().enumerate() {
            let qps = hist.len() as f64 / interval.as_secs() as f64;
            info!(
                "query {i} qps: {qps:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
                us_to_ms(hist.value_at_quantile(0.5)),
                us_to_ms(hist.value_at_quantile(0.9)),
                us_to_ms(hist.value_at_quantile(0.99)),
                us_to_ms(hist.value_at_quantile(0.9999))
            );
        }

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> anyhow::Result<()> {
        let mut conn = params.deployment.connect_to_target().await?;
        let query_set = &params.query_set;

        // Only some upstream databases support interrogating the number of cached statements
        if let Some(stmt_cache_size) = conn.cached_statements() {
            assert!(stmt_cache_size >= params.query_set.queries.len());
        }

        // Generate a prepared version for all of the statements
        let prepared = query_set.prepare_all(&mut conn).await?;

        let mut last_report = Instant::now();
        let mut result_batch = WorkloadResultBatch::new(query_set.queries.len());

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

            let start = Instant::now();
            // Execute the prepared statement, with the generated params
            conn.execute(&prepared[query.idx], params).await?;
            result_batch.queries.push(WorkloadResult {
                query_id: query.idx as _,
                latency_us: start.elapsed().as_micros() as _,
            });
        }
    }
}
