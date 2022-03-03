//! This benchmark generates a mixed load of queries and sends them to MySQL/MySQL adapter.
//! The benchmark accepts a yaml file describing the workload, with the schema described in
//! [`crate::spec`].
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use nom_sql::SqlQuery;
use noria_data::DataType;
use rand::distributions::Uniform;
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;
use zipf::ZipfDistribution;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::spec::{
    WorkloadDistribution, WorkloadDistributionKind, WorkloadDistributionSource, WorkloadQuery,
    WorkloadSpec,
};
use crate::utils::multi_thread::{self, MultithreadBenchmark};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::us_to_ms;

const REPORT_RESULTS_INTERVAL: Duration = Duration::from_secs(2);

type Distributions = HashMap<String, Arc<(Vec<Vec<DataType>>, Sampler)>>;

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadEmulator {
    /// Path to the workload yaml schema
    #[clap(long, short)]
    spec: PathBuf,
    /// Number of worker connections
    #[clap(long, short)]
    workers: u64,
    /// Duration of the benchmark in seconds
    #[clap(long, short, parse(try_from_str = crate::utils::seconds_as_str_to_duration))]
    run_for: Option<Duration>,
    #[clap(skip)]
    #[serde(skip)]
    query_set: Arc<Mutex<Option<Arc<QuerySet>>>>,
}

/// A query with its index and generator
struct Query {
    spec: SqlQuery,
    idx: usize,
    cols: Vec<ColGenerator>,
}

/// A vector of queries and weights
struct QuerySet {
    queries: Vec<Query>,
    weights: WeightedAliasIndex<usize>,
}

#[derive(Clone)]
pub(crate) struct WorkloadThreadParams {
    deployment: DeploymentParameters,
    query_set: Arc<QuerySet>,
}

enum Sampler {
    Zipf(ZipfDistribution),
    Uniform(Uniform<usize>),
}

/// Generates parameter data for a single placeholder in the query
struct ColGenerator {
    dist: Arc<(Vec<Vec<DataType>>, Sampler)>,
    sql_type: nom_sql::SqlType,
    col: usize,
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
        let WorkloadSpec {
            distributions,
            queries,
        } = serde_yaml::from_str(&yaml).unwrap();

        let distributions = self.load_distributions(distributions, deployment).await?;
        let queries = self
            .load_queries(queries, &distributions, deployment)
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
}

impl Sampler {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Sampler::Zipf(z) => z.sample(rng),
            Sampler::Uniform(u) => u.sample(rng),
        }
    }
}

impl WorkloadEmulator {
    async fn load_distributions(
        &self,
        from: Vec<WorkloadDistribution>,
        deployment: &DeploymentParameters,
    ) -> anyhow::Result<Distributions> {
        let mut distributions = HashMap::new();

        for WorkloadDistribution {
            name,
            from,
            distribution,
        } in from
        {
            info!("Generating distribution {name}");

            let data: Vec<Vec<DataType>> = match from {
                WorkloadDistributionSource::Range { range } => {
                    range.into_iter().map(|n| vec![DataType::from(n)]).collect()
                }
                WorkloadDistributionSource::Query { query } => {
                    let mut conn = deployment.connect_to_setup().await?;
                    let data: Vec<Row> = conn.query(query).await?;

                    data.into_iter()
                        .map(|row| {
                            row.unwrap()
                                .into_iter()
                                .map(DataType::try_from)
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                }
            };

            info!("Generated distribution {name}, rows {}", data.len());

            let generator = match distribution {
                WorkloadDistributionKind::Uniform { .. } => {
                    Sampler::Uniform(Uniform::new(0, data.len()))
                }
                WorkloadDistributionKind::Zipf { zipf } => {
                    Sampler::Zipf(ZipfDistribution::new(data.len(), zipf).unwrap())
                }
            };

            distributions.insert(name, Arc::new((data, generator)));
        }

        Ok(distributions)
    }

    async fn load_queries(
        &self,
        from: Vec<WorkloadQuery>,
        distributions: &Distributions,
        deployment: &DeploymentParameters,
    ) -> anyhow::Result<QuerySet> {
        let mut conn = deployment.connect_to_target().await?;

        let weights = WeightedAliasIndex::new(from.iter().map(|q| q.weight).collect()).unwrap();
        let mut queries = Vec::with_capacity(from.len());

        for (
            i,
            WorkloadQuery {
                spec,
                params,
                migrate,
                ..
            },
        ) in from.into_iter().enumerate()
        {
            if migrate {
                let _ = conn
                    .query_drop(format!("CREATE CACHED QUERY {i} AS {spec}"))
                    .await;
            }

            let cols = params
                .into_iter()
                .map(|p| {
                    let dist = distributions[p.distribution.as_str()].clone();
                    ColGenerator {
                        dist,
                        sql_type: p.sql_type,
                        col: p.col,
                    }
                })
                .collect();

            queries.push(Query { idx: i, spec, cols })
        }

        Ok(QuerySet { weights, queries })
    }
}

impl QuerySet {
    fn get_query(&self) -> &Query {
        let mut rng = rand::thread_rng();
        &self.queries[self.weights.sample(&mut rng)]
    }
}

impl Query {
    fn get_params(&self) -> Vec<Value> {
        let mut ret = Vec::with_capacity(self.cols.len());
        let mut rng = rand::thread_rng();

        let mut last_row: &Vec<DataType> = &vec![];
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

            ret.push(
                last_row[*col]
                    .coerce_to(sql_type)
                    .unwrap()
                    .try_into()
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

        for u in results {
            for WorkloadResult {
                query_id,
                latency_us,
            } in u.queries
            {
                hist.record(latency_us as _).unwrap();
                per_query_hist[query_id as usize]
                    .record(latency_us as _)
                    .unwrap();
            }
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

        let mut results = vec![
            ("qps".to_string(), (qps, Unit::Count)),
            (
                "latency p50".to_string(),
                (us_to_ms(hist.value_at_quantile(0.5)), Unit::Milliseconds),
            ),
            (
                "latency p90".to_string(),
                (us_to_ms(hist.value_at_quantile(0.9)), Unit::Milliseconds),
            ),
            (
                "latency p99".to_string(),
                (us_to_ms(hist.value_at_quantile(0.99)), Unit::Milliseconds),
            ),
            (
                "latency p99.99".to_string(),
                (us_to_ms(hist.value_at_quantile(0.9999)), Unit::Milliseconds),
            ),
        ];

        for (i, hist) in per_query_hist.into_iter().enumerate() {
            let qps = hist.len() as f64 / interval.as_secs() as f64;
            info!(
                "query {i} qps: {qps:.0}\tp50: {:.1} ms\tp90: {:.1} ms\tp99: {:.1} ms\tp99.99: {:.1} ms",
                us_to_ms(hist.value_at_quantile(0.5)),
                us_to_ms(hist.value_at_quantile(0.9)),
                us_to_ms(hist.value_at_quantile(0.99)),
                us_to_ms(hist.value_at_quantile(0.9999))
            );

            results.push((format!("query {i} qps"), (qps, Unit::Count)));
            results.push((
                format!("query {i} latency p50"),
                (us_to_ms(hist.value_at_quantile(0.5)), Unit::Milliseconds),
            ));
            results.push((
                format!("query {i} latency p90"),
                (us_to_ms(hist.value_at_quantile(0.9)), Unit::Milliseconds),
            ));
            results.push((
                format!("query {i} latency p99"),
                (us_to_ms(hist.value_at_quantile(0.99)), Unit::Milliseconds),
            ));
            results.push((
                format!("query {i} latency p99.99"),
                (us_to_ms(hist.value_at_quantile(0.9999)), Unit::Milliseconds),
            ));
        }

        // This benchmark returns the last seen benchmark results.
        *benchmark_results = BenchmarkResults::from(&results);

        Ok(())
    }

    async fn benchmark_thread(
        params: Self::Parameters,
        sender: UnboundedSender<Self::BenchmarkResult>,
    ) -> anyhow::Result<()> {
        let mut conn = params.deployment.connect_to_target().await?;
        let query_set = &params.query_set;

        let mut prepared = Vec::new();

        assert!(conn.opts().stmt_cache_size() >= params.query_set.queries.len());

        // Generate a prepared version for all of the statements
        for query in &query_set.queries {
            prepared.push(conn.prep(query.spec.to_string()).await?);
        }

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
            // Execute the prepared satement, with the generated params
            conn.exec_drop(&prepared[query.idx], params).await?;
            result_batch.queries.push(WorkloadResult {
                query_id: query.idx as _,
                latency_us: start.elapsed().as_micros() as _,
            });
        }
    }
}
