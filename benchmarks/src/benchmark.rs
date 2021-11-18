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

use crate::read_benchmark::ReadBenchmark;
use crate::template::Template;
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use enum_dispatch::enum_dispatch;

#[allow(clippy::large_enum_variant)]
#[enum_dispatch(BenchmarkControl)]
#[derive(clap::Subcommand)]
pub enum Benchmark {
    Template, // Example benchmark that does not execute any commands.
    ReadBenchmark,
}

/// The set of parameters common to all benchmarks.
#[derive(Parser, Clone)]
pub struct BenchmarkParameters {}

/// The set of control functions needed to execute the benchmark in
/// the `BenchmarkRunner`.
#[async_trait]
#[enum_dispatch]
pub trait BenchmarkControl {
    /// Any code required to perform setup of the benchmark goes here. This
    /// step may optionally be skipped if setup can be shared with other
    /// benchmarks.
    async fn setup(&self) -> Result<()>;

    /// Perform actual benchmarking, writing results to prometheus.
    async fn benchmark(&self) -> Result<()>;

    // Has there been a regression in the benchmarks performance?
    // async fn regression_check(&self) -> Result<bool>;
}
