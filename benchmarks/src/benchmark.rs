//! Abstractions and data types required for the definition and execution of
//! an abstract benchmark.
//!
//! Each benchmark implements `BenchmarkControl`, an async trait that includes
//! the set of functions required to execute the benchmark in `BenchmarkRunner`.
//! Every benchmark should be a variant of the `Benchmark` enum, which handles
//! dynamically dispatching `BenchmarkControl`'s functions to variants.
//!
//! Each new benchmark implemented should:
//!     - Create a type that implements `BenchmarkControl`,
//!     - Add the type's name as a variant `Benchmark`.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use enum_dispatch::enum_dispatch;
use hdrhistogram::Histogram;
use mysql_async::{Conn, Opts};
use serde::{Deserialize, Serialize};

use crate::cache_hit_benchmark::CacheHitBenchmark;
use crate::eviction_benchmark::EvictionBenchmark;
use crate::fallback_benchmark::FallbackBenchmark;
use crate::migration_benchmark::MigrationBenchmark;
use crate::query_benchmark::QueryBenchmark;
use crate::read_write_benchmark::ReadWriteBenchmark;
use crate::scale_connections::ScaleConnections;
use crate::scale_views::ScaleViews;
use crate::single_query_benchmark::SingleQueryBenchmark;
use crate::template::Template;
use crate::utils::prometheus::{ForwardPrometheusMetrics, PrometheusEndpoint};
use crate::workload_emulator::WorkloadEmulator;
use crate::write_benchmark::WriteBenchmark;
use crate::write_latency_benchmark::WriteLatencyBenchmark;

#[allow(clippy::large_enum_variant)]
#[enum_dispatch(BenchmarkControl)]
#[derive(clap::Subcommand, Serialize, Deserialize)]
pub enum Benchmark {
    Template, // Example benchmark that does not execute any commands.
    /// Basic read benchmark
    QueryBenchmark,
    WriteBenchmark,
    CacheHitBenchmark,
    ScaleViews,
    ScaleConnections,
    /// Measures time required to propagate table writes into Noria views
    WriteLatencyBenchmark,
    MigrationBenchmark,
    EvictionBenchmark,
    ReadWriteBenchmark,
    FallbackBenchmark,
    SingleQueryBenchmark,
    WorkloadEmulator,
}

impl Benchmark {
    pub fn name_label(&self) -> &'static str {
        match self {
            Self::Template(_) => "template",
            Self::QueryBenchmark(_) => "query_benchmark",
            Self::WriteBenchmark(_) => "write_benchmark",
            Self::CacheHitBenchmark(_) => "cache_hit_benchmark",
            Self::ScaleViews(_) => "scale_views",
            Self::ScaleConnections(_) => "scale_connections",
            Self::WriteLatencyBenchmark(_) => "write_latency",
            Self::MigrationBenchmark(_) => "migration_benchmark",
            Self::EvictionBenchmark(_) => "eviction",
            Self::ReadWriteBenchmark(_) => "read_write_benchmark",
            Self::FallbackBenchmark(_) => "fallback_benchmark",
            Self::SingleQueryBenchmark(_) => "single_query_benchmark",
            Self::WorkloadEmulator(_) => "workload_emulator",
        }
    }
}

#[derive(Parser, Default, Clone, Deserialize, Serialize)]
pub struct DeploymentParameters {
    /// Instance label, for metrics.  In CI, it makes sense to set this to the
    /// CL# or commit hash.
    #[clap(long, env = "INSTANCE_LABEL", default_value("local"))]
    pub instance_label: String,

    /// Address of a push gateway for a benchmark's prometheus metrics.
    #[clap(long, env = "PROMETHEUS_PUSH_GATEWAY")]
    pub prometheus_push_gateway: Option<String>,

    /// Noria metrics endpoint; Endpoint that can be used to forward metrics from
    /// the server. If not specified, no metrics will be forwarded.
    #[clap(long, env = "PROMETHEUS_SERVER")]
    pub prometheus_endpoint: Option<PrometheusEndpoint>,

    /// Target database connection string. This is the database in the deployment
    /// we are benchmarking operations against.
    #[clap(
        long,
        env = "TARGET_CONN_STR",
        default_value = "",
        required_unless_present("deployment")
    )]
    pub target_conn_str: String,

    /// Setup database connection string.
    #[clap(
        long,
        env = "SETUP_CONN_STR",
        default_value = "",
        required_unless_present("deployment")
    )]
    pub setup_conn_str: String,
}

impl DeploymentParameters {
    pub async fn connect_to_target(&self) -> Result<Conn> {
        let opts = Opts::from_url(&self.target_conn_str)?;
        Ok(Conn::new(opts).await?)
    }

    pub async fn connect_to_setup(&self) -> Result<Conn> {
        let opts = Opts::from_url(&self.setup_conn_str)?;
        Ok(Conn::new(opts).await?)
    }
}

/// Indicates whether increasing or decreasing is the more desirable property for a metric
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MetricGoal {
    Increasing,
    Decreasing,
}

#[derive(Debug)]
pub struct BenchmarkData {
    pub unit: String,
    pub desired_action: MetricGoal,
    pub values: Vec<f64>,
}

impl BenchmarkData {
    pub fn new(unit: metrics::Unit, desired_action: MetricGoal) -> Self {
        Self {
            unit: format!("{:?}", unit),
            desired_action,
            values: vec![],
        }
    }

    pub fn push(&mut self, value: f64) {
        self.values.push(value);
    }

    pub fn to_histogram(&self, lower: f64, upper: f64) -> Histogram<u64> {
        let mut data = self.values.clone();
        data.sort_unstable_by(|a, b| {
            // We shouldn't have any NaNs/infs
            a.partial_cmp(b).unwrap()
        });
        let data = &data
            [(((data.len() as f64) * lower) as usize)..(((data.len() as f64) * upper) as usize)];

        let mut hist = Histogram::<u64>::new(3).unwrap();
        for value in data.iter().copied() {
            hist.record(value as u64).unwrap();
        }
        hist
    }
}

#[derive(Default, Debug)]
pub struct BenchmarkResults {
    pub results: HashMap<String, BenchmarkData>,
}

impl BenchmarkResults {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }

    pub fn entry(
        &mut self,
        key: &str,
        unit: metrics::Unit,
        desired_action: MetricGoal,
    ) -> &mut Vec<f64> {
        &mut self
            .results
            .entry(key.to_string())
            .or_insert_with(|| BenchmarkData::new(unit, desired_action))
            .values
    }

    pub fn push(&mut self, key: &str, unit: metrics::Unit, desired_action: MetricGoal, value: f64) {
        self.results
            .entry(key.to_string())
            .or_insert_with(|| BenchmarkData::new(unit, desired_action))
            .push(value);
    }

    #[must_use]
    pub fn prefix(self, p: &str) -> Self {
        Self {
            results: self
                .results
                .into_iter()
                .map(|(k, v)| (format!("{}_{}", p, k), v))
                .collect(),
        }
    }

    pub fn merge(input: Vec<BenchmarkResults>) -> Self {
        let mut results = HashMap::new();
        for r in input {
            results.extend(r.results);
        }
        BenchmarkResults { results }
    }
}

/// The formatted benchmark parameters and results for serialization
/// to a file.
// TODO(justin): use this struct for serializing and deserializing baselines.
#[allow(dead_code)]
pub struct BenchmarkOutput {
    benchmark: Benchmark,
    deployment: DeploymentParameters,
    results: BenchmarkResults,
}

impl BenchmarkOutput {
    pub fn new(
        benchmark: Benchmark,
        deployment: DeploymentParameters,
        results: BenchmarkResults,
    ) -> Self {
        Self {
            benchmark,
            deployment,
            results,
        }
    }
}

/// The set of control functions needed to execute the benchmark in
/// the `BenchmarkRunner`.
#[async_trait]
#[enum_dispatch]
pub trait BenchmarkControl {
    /// Any code required to perform setup of the benchmark goes here. This
    /// step may optionally be skipped if setup can be shared with other
    /// benchmarks.
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()>;

    /// Code required to reset the benchmark for running in a second iteration.
    /// If this returns an error, a benchmark cannot be run for more than
    /// one iteration.
    async fn reset(&self, deployment: &DeploymentParameters) -> Result<()>;

    /// Perform actual benchmarking, writing results to prometheus.
    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults>;

    /// Get Prometheus labels for this benchmark run.
    fn labels(&self) -> HashMap<String, String>;

    // Has there been a regression in the benchmarks performance?
    // async fn regression_check(&self) -> Result<bool>;

    /// Set of (endpoint, filter predicate) pairs for metrics to pull in from Prometheus URLs and
    /// re-export as part of the benchmark's metrics. Only called if `deployment` has a
    /// PrometheusEndpoint.
    fn forward_metrics(&self, deployment: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics>;

    /// The benchmark template's name
    fn name(&self) -> &'static str;
}
