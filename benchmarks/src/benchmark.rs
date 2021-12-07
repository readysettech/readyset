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

use crate::cache_hit_benchmark::CacheHitBenchmark;
use crate::query_benchmark::QueryBenchmark;
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
use mysql_async::{Conn, Opts};
use serde::{Deserialize, Serialize};

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
        }
    }
}

#[derive(Parser, Clone, Deserialize, Serialize)]
pub struct DeploymentParameters {
    /// Instance label, for metrics.  In CI, it makes sense to set this to the
    /// CL# or commit hash.
    #[clap(long, default_value("local"))]
    pub instance_label: String,

    /// Address of a push gateway for a benchmark's prometheus metrics.
    #[clap(long)]
    pub prometheus_push_gateway: Option<String>,

    /// Noria metrics endpoint; Endpoint that can be used to forward metrics from
    /// the server. If not specified, no metrics will be forwarded.
    #[clap(long)]
    pub prometheus_endpoint: Option<PrometheusEndpoint>,

    /// Target database connection string. This is the database in the deployment
    /// we are benchmarking operations against.
    #[clap(long, default_value = "")]
    pub target_conn_str: String,

    /// Setup database connection string.    
    #[clap(long, default_value = "")]
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

/// The set of control functions needed to execute the benchmark in
/// the `BenchmarkRunner`.
#[async_trait]
#[enum_dispatch]
pub trait BenchmarkControl {
    /// Any code required to perform setup of the benchmark goes here. This
    /// step may optionally be skipped if setup can be shared with other
    /// benchmarks.
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()>;

    /// In order to support the runner short-circuiting setup, this function is
    /// run to check whether or not setup is already complete.
    async fn is_already_setup(&self, deployment: &DeploymentParameters) -> Result<bool>;

    /// Perform actual benchmarking, writing results to prometheus.
    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<()>;

    /// Get Prometheus labels for this benchmark run.
    fn labels(&self) -> HashMap<String, String>;

    // Has there been a regression in the benchmarks performance?
    // async fn regression_check(&self) -> Result<bool>;

    /// Set of (endpoint, filter predicate) pairs for metrics to pull in from Prometheus URLs and
    /// re-export as part of the benchmark's metrics. Only called if `deployment` has a
    /// PrometheusEndpoint.
    fn forward_metrics(&self, deployment: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics>;
}
