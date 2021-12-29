//! Top-level runner for ReadySet product benchmarks.
//!
//! For a concrete example, see `read_benchmark/mod.rs`.  A more minimal example is provided here.
//!
//! ```
//! use core::ops::Range;
//! use std::collections::HashMap;
//!
//! use anyhow::Result;
//! use itertools::{Itertools, Tuples};
//! use metrics::Unit;
//! use mysql_async::prelude::*;
//!
//! use benchmarks::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
//! use benchmarks::benchmark_gauge;
//! use benchmarks::utils::prometheus::ForwardPrometheusMetrics;
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
//!         r"CREATE TABLE integers (id INT UNSIGNED NOT NULL)".ignore(&mut conn).await?;
//!         let stmt = conn.prep(r"INSERT INTO integers VALUES (?), (?), (?), (?)").await?;
//!         let chunks: Tuples<Range<u32>, (_, _, _, _)> = (0..self.row_count).tuples();
//!         conn.exec_batch(stmt, chunks).await?;
//!         Ok(())
//!     }
//!
//!     async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
//!         Err(anyhow::anyhow!("reset unsupported"))
//!     }
//!
//!     async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
//!         let mut conn = deployment.connect_to_target().await?;
//!         let stmt = conn.prep(r"SELECT * FROM integers WHERE id = ?").await?;
//!         for _ in 0..10_000_000 {
//!             let _: Vec<u32> = conn.exec(&stmt, (self.where_value,)).await?;
//!         }
//!         benchmark_gauge!(
//!             "my_benchmark.number_of_queries",
//!             Count,
//!             "Number of queries executed in this benchmark run",
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
//!             self.row_count.to_string()
//!         );
//!         labels.insert(
//!             "benchmark.my_benchmark.where_value".into(),
//!             self.where_value.to_string()
//!         );
//!         labels
//!     }
//!
//!     fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
//!         vec![]
//!     }
//! }
//! ```

#![feature(never_type)]
#![feature(type_alias_impl_trait)]

pub mod benchmark;
pub mod template;
pub mod utils;

// Benchmarks
mod cache_hit_benchmark;
mod eviction_benchmark;
mod migration_benchmark;
mod query_benchmark;
mod read_write_benchmark;
mod scale_connections;
mod scale_views;
mod write_benchmark;
mod write_latency_benchmark;
