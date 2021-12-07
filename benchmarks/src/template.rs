use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::benchmark::{BenchmarkControl, BenchmarkParameters};
use crate::benchmark_gauge;
use crate::utils::prometheus::ForwardPrometheusMetrics;

#[derive(Parser, Serialize, Deserialize)]
pub struct Template {
    /// Common shared benchmark parameters.
    #[clap(flatten)]
    common: BenchmarkParameters,
    // Other benchmark specific parameters go here.
}

#[async_trait]
impl BenchmarkControl for Template {
    async fn setup(&self) -> Result<()> {
        // Any code required to setup the benchmark goes. This may include
        // database schema installation, or generating data, etc. This step
        // supports being skipped if sharing setup with other benchmarks.
        Ok(())
    }
    async fn is_already_setup(&self) -> Result<bool> {
        // You can write code to determine whether or not the benchmark is
        // already setup (e.g. schema already exists, migrations already run,
        // etc) here.
        Ok(false)
    }
    async fn benchmark(&self) -> Result<()> {
        // Performing of the actual thing we want to benchmark, along with recording metrics
        benchmark_gauge!(
            "template.fake_number_of_queries",
            Count,
            "number of queries executed",
            133333337.0,
            "label_key_1" => "label_value_1",
            "label_key_2" => "label_value_2"
        );
        Ok(())
    }
    fn labels(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    fn forward_metrics(&self) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}
