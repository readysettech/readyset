//! Top-level runner for ReadySet product benchmarks.
//!
//! For a concrete example, see `read_benchmark/mod.rs`.  A more minimal example is provided here.
//!
//! ```
//! use core::ops::Range;
//! use std::collections::HashMap;
//!
//! use anyhow::Result;
//! use benchmarks::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
//! use benchmarks::benchmark_gauge;
//! use benchmarks::utils::prometheus::ForwardPrometheusMetrics;
//! use database_utils::QueryableConnection;
//! use itertools::{Itertools, Tuples};
//!
//! #[derive(clap::Parser, Clone)]
//! pub struct MyBenchmark {
//!     #[clap(long, default_value = "10")]
//!     where_value: u32,
//!
//!     #[clap(long, default_value = "1_000_000")]
//!     row_count: u32,
//! }
//!
//! #[async_trait::async_trait]
//! impl BenchmarkControl for MyBenchmark {
//!     async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
//!         let mut conn = deployment.connect_to_setup().await?;
//!         conn.query_drop("CREATE TABLE integers (id INT UNSIGNED NOT NULL)")
//!             .await?;
//!         let stmt = conn
//!             .prepare(r"INSERT INTO integers VALUES (?), (?), (?), (?)")
//!             .await?;
//!         let chunks: Tuples<Range<u32>, (_, _, _, _)> = (0..self.row_count).tuples();
//!         for (v1, v2, v3, v4) in chunks {
//!             conn.execute(&stmt, [v1, v2, v3, v4]).await?;
//!         }
//!         Ok(())
//!     }
//!
//!     async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
//!         Err(anyhow::anyhow!("reset unsupported"))
//!     }
//!
//!     async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
//!         let mut conn = deployment.connect_to_target().await?;
//!         let stmt = conn.prepare(r"SELECT * FROM integers WHERE id = ?").await?;
//!         for _ in 0..10_000_000 {
//!             conn.execute(&stmt, [self.where_value]).await?;
//!         }
//!         benchmark_gauge!(
//!             "my_benchmark.number_of_queries",
//!             Count,
//!             "Number of queries executed in this benchmark run".into(),
//!             self.row_count as f64
//!         );
//!         Ok(BenchmarkResults::new())
//!     }
//!
//!     fn labels(&self) -> HashMap<String, String> {
//!         let mut labels = HashMap::new();
//!         // It can be a good practice to add any of this run's parameters as labels
//!         labels.insert(
//!             "benchmark.my_benchmark.row_count".into(),
//!             self.row_count.to_string(),
//!         );
//!         labels.insert(
//!             "benchmark.my_benchmark.where_value".into(),
//!             self.where_value.to_string(),
//!         );
//!         labels
//!     }
//!
//!     fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
//!         vec![]
//!     }
//!
//!     fn name(&self) -> &'static str {
//!         "my_benchmark"
//!     }
//! }
//! ```
#![feature(never_type)]
#![feature(const_float_bits_conv, type_alias_impl_trait)]

pub mod benchmark;
pub mod reporting;
pub mod spec;
pub mod template;
pub mod utils;

// Benchmarks
mod cache_hit_benchmark;
mod eviction_benchmark;
mod fallback_benchmark;
mod migration_benchmark;
mod query_benchmark;
mod read_write_benchmark;
mod scale_connections;
mod scale_views;
mod single_query_benchmark;
mod workload_emulator;
mod write_benchmark;
mod write_latency_benchmark;

pub use workload_emulator::{QuerySet, WorkloadEmulator};
