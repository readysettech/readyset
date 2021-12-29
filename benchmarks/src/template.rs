use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::benchmark_gauge;
use crate::utils::prometheus::ForwardPrometheusMetrics;

#[derive(Parser, Serialize, Deserialize)]
pub struct Template {
    // Benchmark specific parameters go here.
}

#[async_trait]
impl BenchmarkControl for Template {
    async fn setup(&self, _: &DeploymentParameters) -> Result<()> {
        // Any code required to setup the benchmark goes. This may include
        // database schema installation, or generating data, etc. This step
        // supports being skipped if sharing setup with other benchmarks.
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, _: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Performing of the actual thing we want to benchmark, along with recording metrics
        benchmark_gauge!(
            "template.fake_number_of_queries",
            Count,
            "number of queries executed",
            133333337.0,
            "label_key_1" => "label_value_1",
            "label_key_2" => "label_value_2"
        );
        Ok(BenchmarkResults::new())
    }

    fn labels(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}
