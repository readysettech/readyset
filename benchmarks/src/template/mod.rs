use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;

use crate::benchmark::{BenchmarkControl, BenchmarkParameters};

#[derive(Parser)]
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
    async fn benchmark(&self) -> Result<()> {
        // Performing of the actual thing we want to benchmark.
        Ok(())
    }
}
