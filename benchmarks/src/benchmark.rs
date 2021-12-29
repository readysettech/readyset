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

use crate::cache_hit_benchmark::CacheHitBenchmark;
use crate::eviction_benchmark::EvictionBenchmark;
use crate::fallback_benchmark::FallbackBenchmark;
use crate::migration_benchmark::MigrationBenchmark;
use crate::query_benchmark::QueryBenchmark;
use crate::read_write_benchmark::ReadWriteBenchmark;
use crate::scale_connections::ScaleConnections;
use crate::scale_views::ScaleViews;
use crate::template::Template;
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::prometheus::PrometheusEndpoint;
use crate::write_benchmark::WriteBenchmark;
use crate::write_latency_benchmark::WriteLatencyBenchmark;
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use mysql_async::{Conn, Opts};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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

/// Key-value pair of benchmark results that can be used to calculate
/// distributions across multiple benchmark iterations.
#[derive(Debug)]
pub struct BenchmarkResults {
    results: HashMap<String, f64>,
}

impl BenchmarkResults {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }

    pub fn from<T>(results: &[(&str, T)]) -> Self
    where
        T: fmt::Display + Clone,
        f64: From<T>,
    {
        Self {
            results: results
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone().into()))
                .collect(),
        }
    }

    pub fn append<T>(&mut self, results: &[(&str, T)])
    where
        T: fmt::Display + Clone,
        f64: From<T>,
    {
        for (k, v) in results {
            self.results.insert(k.to_string(), v.clone().into());
        }
    }

    /// Prefixes the set of keys in this set of results with `p`.
    /// If a key with it's prefix already exists, the key may be
    /// value may be overwritten.
    pub fn prefix(mut self, p: &str) -> Self {
        // Make a copy of all the keys so we can mutably borrow the map as we
        // iterate.
        let keys: Vec<_> = self.results.keys().cloned().collect();
        for k in keys.iter() {
            // Iterating over the set of keys that are in the map.
            #[allow(clippy::unwrap_used)]
            self.results
                .insert(format!("{} {}", p, k), *self.results.get(k).unwrap());
            self.results.remove(k);
        }

        self
    }

    /// Merges a set of benchmark results into a single set of results.
    /// The set of benchmark results should have *different* keys otherwise
    /// keys will be overwritten by other benchmark results.
    ///
    /// [`BenchmarkResults::prefix`] can be used to give different sets of
    /// results unique keys, via unique prefixes.
    pub fn merge(results: Vec<BenchmarkResults>) -> Self {
        let mut merged = BenchmarkResults {
            results: HashMap::new(),
        };

        for r in results {
            merged.results.extend(r.results);
        }

        merged
    }

    /// Aggregates a set of benchmark results into a single set of
    /// results. For each metric in the benchmark results, we calculate
    /// the min, max, median and mean for each metric. This assumes that
    /// each result in `results` has the same set of metrics.
    pub fn aggregate(results: &[BenchmarkResults]) -> Self {
        let mut agg = BenchmarkResults {
            results: HashMap::new(),
        };

        // For each metric get all values from the set of benchmarks. Calculate the
        // min, max, median, mean over the values for the result.

        let keys = results[0].results.keys().clone();
        for k in keys {
            let mut values: Vec<_> = results.iter().map(|r| r.results.get(k).unwrap()).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let len = values.len();

            agg.results.insert(k.clone() + " min", *values[0]);
            agg.results
                .insert(k.clone() + " max", *values[values.len() - 1]);
            agg.results
                .insert(k.clone() + " median", *values[values.len() / 2]);
            agg.results.insert(
                k.clone() + " mean",
                values.into_iter().sum::<f64>() / len as f64,
            );
        }

        agg
    }
}

impl Default for BenchmarkResults {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BenchmarkResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for key in self.results.keys().sorted() {
            writeln!(f, "{}: {}", key, self.results[key])?;
        }
        Ok(())
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
}
