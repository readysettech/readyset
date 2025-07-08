//! This benchmark generates a mixed load of queries and sends them to upstream/upstream adapter.
//! Further, it allows three different testing modes:
//! - direct to the upstream database (bypassing readyset completely)
//! - readyset backed by an upstream database (the standard model)
//!
//! The benchmark accepts a yaml file describing the workload, with the schema described in
//! [`crate::spec`].
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use database_utils::{DatabaseConnection, DatabaseStatement, DatabaseType, QueryableConnection};
use hdrhistogram::Histogram;
use metrics::Unit;
use rand::distr::Uniform;
use rand_distr::weighted::WeightedAliasIndex;
use rand_distr::{Distribution, Zipf};
use readyset_data::{DfType, DfValue, Dialect};
use readyset_sql::ast::SqlType;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::spec::WorkloadSpec;
use crate::utils::generate::DataGenerator;
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::query::interpolate_params;
use crate::utils::us_to_ms;

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_millis(500);

pub type Distributions = HashMap<String, Arc<(Vec<Vec<DfValue>>, Sampler)>>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum BenchmarkType {
    /// Send all queries to the ReadySet instance, but do not migrate them.
    /// This will force ReadySet to proxy queries to the upstream.
    #[value(name = "proxy")]
    Proxy,

    /// Send all statements to a ReadySet instance, which is backed by an upstream database.
    #[value(name = "readyset")]
    #[default]
    ReadySet,

    /// Only execute statements against the upstream database.
    #[value(name = "upstream")]
    Upstream,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum QueryExecutionMode {
    /// Use prepared statements and reuse them (current behavior)
    #[default]
    #[value(name = "prepared")]
    PreparedReuse,
    /// Use extended query protocol with unnamed prepared statements (like many ORMs)
    #[value(name = "unnamed")]
    ExtendedUnnamed,
    /// Use simple text protocol (no preparation)
    #[value(name = "text")]
    SimpleText,
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

    /// The target rate to issue queries at if attainable on this
    /// machine with up to `threads`.
    #[arg(long)]
    target_qps: Option<u64>,

    /// Number of worker connections
    #[arg(long, short)]
    workers: u64,

    /// Duration of the benchmark in seconds
    #[arg(long, short, value_parser = crate::utils::seconds_as_str_to_duration)]
    run_for: Option<Duration>,

    /// How to execute queries against the database
    #[arg(long, value_enum, default_value_t = QueryExecutionMode::PreparedReuse)]
    query_execution_mode: QueryExecutionMode,

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
    query_execution_mode: QueryExecutionMode,
    target_qps: Option<u64>,
    workers: u64,
}

pub enum Sampler {
    Zipf(Zipf<f64>),
    Uniform(Uniform<usize>),
}

/// Generates parameter data for a single placeholder in the query
pub(crate) struct ColGenerator {
    pub(crate) dist: Arc<(Vec<Vec<DfValue>>, Sampler)>,
    pub(crate) sql_type: SqlType,
    pub(crate) col: usize,
}

#[derive(Debug, Clone)]
/// A batched set of results sent on an interval by the read benchmark thread.
pub(crate) struct WorkloadResultBatch {
    /// Number of queries tested
    queries: Vec<Histogram<u64>>,
}

impl WorkloadResultBatch {
    fn new(n: usize) -> Self {
        Self {
            queries: vec![Histogram::<u64>::new(3).unwrap(); n],
        }
    }
}

impl BenchmarkControl for WorkloadEmulator {
    async fn setup(&self, deployment: &DeploymentParameters) -> anyhow::Result<()> {
        if self.query_execution_mode == QueryExecutionMode::ExtendedUnnamed
            && deployment.database_type != DatabaseType::PostgreSQL
        {
            return Err(anyhow::anyhow!(
                "Extended unnamed queries are only supported for PostgreSQL"
            ));
        }

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
            BenchmarkType::Upstream => deployment.connect_to_setup().await?,
        };

        for query in &spec.setup {
            conn.query_drop(query).await?;
        }

        if self.benchmark_type == BenchmarkType::Proxy {
            conn.query_drop("DROP ALL CACHES").await?;
        }

        let queries = spec.load_queries(&distributions, &mut conn).await?;
        *self.query_set.lock().unwrap() = Some(Arc::new(queries));

        let thread_data = WorkloadThreadParams {
            deployment: deployment.clone(),
            query_set: Arc::clone(self.query_set.lock().unwrap().deref().as_ref().unwrap()),
            benchmark_type: self.benchmark_type,
            query_execution_mode: self.query_execution_mode,
            target_qps: self.target_qps,
            workers: self.workers,
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
            Sampler::Zipf(z) => z.sample(rng).round() as _,
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
        if self.queries.len() == 1 {
            return &self.queries[0];
        }

        let mut rng = rand::rng();
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
                DfType::from_sql_type(sql_type, Dialect::DEFAULT_MYSQL, |_| None, None).unwrap();

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
        let mut rng = rand::rng();

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
                DfType::from_sql_type(sql_type, Dialect::DEFAULT_MYSQL, |_| None, None).unwrap();

            ret.push(
                last_row[*col]
                    .coerce_to(&target_type, &DfType::Unknown) // No from_ty, we're dealing with literal params
                    .unwrap(),
            )
        }

        ret
    }
}

impl MultithreadBenchmark for WorkloadEmulator {
    type BenchmarkResult = WorkloadResultBatch;
    type Parameters = WorkloadThreadParams;

    async fn handle_benchmark_results(
        results: Vec<Self::BenchmarkResult>,
        interval: std::time::Duration,
        benchmark_results: &mut BenchmarkResults,
    ) -> anyhow::Result<()> {
        let mut overall = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut per_query =
            vec![hdrhistogram::Histogram::<u64>::new(3).unwrap(); results[0].queries.len()];
        for result in &results {
            for (i, query) in result.queries.iter().enumerate() {
                overall.add(query).unwrap();
                per_query[i].add(query).unwrap();
            }
        }

        benchmark_results.push(
            "duration_overall",
            Unit::Microseconds,
            MetricGoal::Decreasing,
            overall.clone(),
        );
        for (i, query) in per_query.iter().enumerate() {
            benchmark_results.push(
                &format!("duration_{i}"),
                Unit::Microseconds,
                MetricGoal::Decreasing,
                query.clone(),
            );
        }

        let qps = overall.len() as f64 / interval.as_secs() as f64;
        info!(
            "overall -\tqps: {qps:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.9: {:.1} ms",
            us_to_ms(overall.value_at_quantile(0.5)),
            us_to_ms(overall.value_at_quantile(0.9)),
            us_to_ms(overall.value_at_quantile(0.99)),
            us_to_ms(overall.value_at_quantile(0.999))
        );

        // only print out per-query stats if there are multiple queries.
        if per_query.len() > 1 {
            for (i, query) in per_query.iter().enumerate() {
                let qps = query.len() as f64 / interval.as_secs() as f64;
                info!(
                    "query {i} -\tqps: {qps:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.9: {:.1} ms",
                    us_to_ms(query.value_at_quantile(0.5)),
                    us_to_ms(query.value_at_quantile(0.9)),
                    us_to_ms(query.value_at_quantile(0.99)),
                    us_to_ms(query.value_at_quantile(0.999))
                );
            }
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

        // Only prepare statements if we're in prepared reuse mode
        let prepared = if params.query_execution_mode == QueryExecutionMode::PreparedReuse {
            if let Some(stmt_cache_size) = conn.cached_statements() {
                assert!(stmt_cache_size >= params.query_set.queries.len());
            }
            Some(query_set.prepare_all(&mut conn).await?)
        } else {
            None
        };

        let mut throttle_interval =
            multi_thread::throttle_interval(params.target_qps, params.workers);
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

            if let Some(interval) = &mut throttle_interval {
                interval.tick().await;
            }

            let query = params.query_set.get_query();
            let query_params = query.get_params();

            // allow the specific execution mode to set up the query/params before starting the timer.
            let duration = match params.query_execution_mode {
                QueryExecutionMode::SimpleText => {
                    let interpolated_query = interpolate_params(
                        &query.spec,
                        query_params,
                        params.deployment.database_type,
                    )?;
                    let start = Instant::now();
                    // use `conn.simple_query()` to avoid the overhead of creating prepared statements
                    // (which happens in `conn.query()`, under the covers)
                    conn.simple_query(&interpolated_query).await?;
                    start.elapsed()
                }
                QueryExecutionMode::ExtendedUnnamed => {
                    let typed_params = derive_typed_params(query_params);
                    let start = Instant::now();
                    conn.query_typed(&query.spec, typed_params).await?;
                    start.elapsed()
                }
                QueryExecutionMode::PreparedReuse => {
                    let start = Instant::now();
                    conn.execute(&prepared.as_ref().unwrap()[query.idx], query_params)
                        .await?;
                    start.elapsed()
                }
            };

            result_batch.queries[query.idx]
                .record(duration.as_micros() as u64)
                .unwrap();
        }
    }
}

/// Derive typed params from a list of DfValues.
///
/// This is a "good enough" implementation that will work for the workload emulator,
/// for testing out postgres unnamed prepared statements.
fn derive_typed_params(
    query_params: Vec<DfValue>,
) -> Vec<(
    Box<dyn tokio_postgres::types::ToSql + Send + Sync>,
    tokio_postgres::types::Type,
)> {
    let mut typed_params = Vec::with_capacity(query_params.len());
    for p in query_params {
        let pg_type = match p {
            DfValue::Int(_) => tokio_postgres::types::Type::INT4,
            DfValue::Float(_) => tokio_postgres::types::Type::FLOAT4,
            DfValue::Double(_) => tokio_postgres::types::Type::FLOAT8,
            DfValue::Text(_) => tokio_postgres::types::Type::TEXT,
            DfValue::TinyText(_) => tokio_postgres::types::Type::TEXT,
            DfValue::TimestampTz(_) => tokio_postgres::types::Type::TIMESTAMP,
            DfValue::Time(_) => tokio_postgres::types::Type::TIME,
            DfValue::Numeric(_) => tokio_postgres::types::Type::NUMERIC,
            DfValue::None => tokio_postgres::types::Type::TEXT,
            _ => tokio_postgres::types::Type::TEXT, // fallback
        };
        typed_params.push((
            Box::new(p.clone()) as Box<dyn tokio_postgres::types::ToSql + Send + Sync>,
            pg_type,
        ));
    }
    typed_params
}
