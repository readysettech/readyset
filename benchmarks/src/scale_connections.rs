use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::{benchmark_counter, benchmark_histogram};
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct ScaleConnections {
    /// The number of views to create in the experiment.
    #[clap(long, default_value = "1")]
    num_connections: usize,

    /// Whether to open all the connections in parallel or serially, closing
    /// each connection before opening the next.
    #[clap(long)]
    parallel: bool,
}

#[async_trait]
impl BenchmarkControl for ScaleConnections {
    async fn setup(&self, _: &DeploymentParameters) -> Result<()> {
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        info!(
            "Running benchmark connecting to {} connections.",
            self.num_connections
        );

        let mut connections = Vec::new();
        let mut results = BenchmarkResults::new();
        for i in 0..self.num_connections {
            let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();

            let start = Instant::now();
            let conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
            let connection_time = start.elapsed();

            // Keep the state alive by storing it in the struct.
            if self.parallel {
                connections.push(conn);
            }

            info!(
                "connection:\t{:.1}ms",
                connection_time.as_secs_f64() * 1000.0,
            );

            if i == 0 || i == self.num_connections || i == self.num_connections / 2 {
                results.append(&[(
                    &format!("connect @ {}", i),
                    (connection_time.as_secs_f64() * 1000.0, Unit::Seconds),
                )]);
            }

            benchmark_histogram!(
                "scale_connections.connection_duration",
                Seconds,
                "The number of seconds spent creating a new connection",
                connection_time.as_secs_f64()
            );

            benchmark_counter!(
                "scale_connections.connections",
                Count,
                "The number of connections the benchmark has executed",
                1
            )
        }

        Ok(results)
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(
            "num_connections".to_string(),
            self.num_connections.to_string(),
        );
        labels.insert("parallel".to_string(), self.parallel.to_string());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}
