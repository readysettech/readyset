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
//! use benchmarks::benchmark::{BenchmarkControl, BenchmarkParameters};
//! use benchmarks::benchmark_gauge;
//!
//! #[derive(clap::Parser, Clone)]
//! pub struct MyBenchmarkParams {
//!     #[clap(flatten)]
//!     common: BenchmarkParameters,
//!
//!     #[clap(long, default_value = "10")]
//!     where_value: u32,
//!
//!     #[clap(long, default_value = "1_000_000")]
//!     row_count: u32,
//! }
//!
//! #[derive(clap::Parser, Clone)]
//! pub struct MyBenchmark {
//!     #[clap(flatten)]
//!     params: MyBenchmarkParams
//! }
//!
//! #[async_trait::async_trait]
//! impl BenchmarkControl for MyBenchmark {
//!     async fn setup(&self) -> Result<()> {
//!         let mut conn = self.params.common.connect().await?;
//!         r"CREATE TABLE integers (id INT UNSIGNED NOT NULL)".ignore(&mut conn).await?;
//!         let stmt = conn.prep(r"INSERT INTO integers VALUES (?), (?), (?), (?)").await?;
//!         let chunks: Tuples<Range<u32>, (_, _, _, _)> = (0..self.params.row_count).tuples();
//!         conn.exec_batch(stmt, chunks).await?;
//!         Ok(())
//!     }
//!
//!     async fn is_already_setup(&self) -> Result<bool> {
//!         let mut conn = self.params.common.connect().await?;
//!         match r"DESCRIBE TABLE integers".ignore(&mut conn).await {
//!             Ok(_) => (),
//!             // 1146 is a "table doesn't exist" error
//!             Err(mysql_async::Error::Server(e)) if e.code == 1146 => return Ok(false),
//!             Err(e) => return Err(e.into())
//!         };
//!         let count: u32 = conn.query_first("SELECT COUNT(*) FROM integers").await?.unwrap();
//!         Ok(count == self.params.row_count)
//!     }
//!
//!     async fn benchmark(&self) -> Result<()> {
//!         let mut conn = self.params.common.connect().await?;
//!         let stmt = conn.prep(r"SELECT * FROM integers WHERE id = ?").await?;
//!         for _ in 0..10_000_000 {
//!             let _: Vec<u32> = conn.exec(&stmt, (self.params.where_value,)).await?;
//!         }
//!         benchmark_gauge!(
//!             "my_benchmark.number_of_queries",
//!             Count,
//!             "Number of queries executed in this benchmark run",
//!             self.params.row_count as f64
//!         );
//!         Ok(())
//!     }
//!
//!     fn labels(&self) -> HashMap<String, String> {
//!         let mut labels = HashMap::new();
//!         // It can be a good practice to add any of this run's parameters as labels
//!         labels.insert(
//!             "benchmark.my_benchmark.row_count".into(),
//!             self.params.row_count.to_string()
//!         );
//!         labels.insert(
//!             "benchmark.my_benchmark.where_value".into(),
//!             self.params.where_value.to_string()
//!         );
//!         labels
//!     }
//! }
//! ```

#![feature(type_alias_impl_trait)]

pub mod benchmark;
pub mod template;
pub mod utils;

// Benchmarks
mod read_benchmark;
mod scale_connections;
mod scale_views;
