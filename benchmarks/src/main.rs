use benchmarks::benchmark::{Benchmark, BenchmarkControl};
use benchmarks::utils;
use clap::Parser;
use std::time::Duration;

#[derive(Parser)]
#[clap(name = "benchmark_runner")]
struct BenchmarkRunner {
    /// Skips the setup setup when executing the `benchmark`.
    #[clap(long)]
    skip_setup: bool,

    /// The duartion, specified as the number of seconds that the experiment
    /// should be running. If `None` is provided, the experiment will run
    /// until it is interrupted.
    #[clap(long, parse(try_from_str = utils::seconds_as_str_to_duration))]
    pub run_for: Option<Duration>,

    #[clap(subcommand)]
    benchmark: Benchmark,
}

impl BenchmarkRunner {
    pub async fn run(self) -> anyhow::Result<()> {
        if !self.skip_setup {
            self.benchmark.setup().await?;
        }
        utils::run_for(self.benchmark.benchmark(), self.run_for).await
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let benchmark_runner = BenchmarkRunner::parse();
    benchmark_runner.run().await
}
