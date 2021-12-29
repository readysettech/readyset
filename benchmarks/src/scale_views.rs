//! Evaluates the impact of creating a view in ReadySet on the time it
//! takes to create a connection and prepare a new view.
//!
//! This benchmark serially creates connections and views until
//! `--num-views` is reached. At each point we measure the time
//! it takes to create the connection and the number of views.
//! `--param_count` can be specified to modify the number of
//! parameters in the view.
use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::{benchmark_counter, benchmark_histogram};
use anyhow::{bail, Result};
use async_trait::async_trait;
use clap::Parser;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info};

const MAX_MYSQL_COLUMN_COUNT: usize = 4096;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct ScaleViews {
    /// The number of views to create in the experiment.
    #[clap(long, default_value = "1")]
    num_views: usize,

    /// The number of parameters in each view.
    #[clap(long, default_value = "1")]
    param_count: usize,
}

// This kind of state would be useful to pass from setup() to benchmark().
// TODO: Is there a way we can add this into the API?.
fn get_columns(num_views: usize, param_count: usize) -> Vec<String> {
    // Need enough columns s.t. there are enough permutations over the columns
    // to match num_views.

    let mut num_columns = param_count;
    let mut num_choices = 0;
    while num_choices < num_views {
        num_choices = num_integer::binomial(num_columns, param_count);
        num_columns += 1;
    }

    (0..num_columns).map(|i| format!("c{}", i)).collect()
}

#[async_trait]
impl BenchmarkControl for ScaleViews {
    /// Creates a table with enough columns that we can create `num_views` off a
    /// combination of the columns.
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        info!("Beginning setup");
        let opts = mysql_async::Opts::from_url(&deployment.setup_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

        let columns = get_columns(self.num_views, self.param_count);
        if columns.len() > MAX_MYSQL_COLUMN_COUNT {
            bail!(
                "Too many columns required: {}, the max is: {}",
                columns.len(),
                MAX_MYSQL_COLUMN_COUNT
            );
        }

        let columns_with_type = columns
            .iter()
            .map(|c| format!("{} int", c))
            .collect::<Vec<_>>();
        let create_table_stmt = format!("CREATE TABLE bigtable ({})", columns_with_type.join(","));

        info!("Creating a table with {} columns", columns.len());
        let _ = conn.query_drop(create_table_stmt).await;

        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        info!(
            "Running benchmark with {} views, {} params per view",
            self.num_views, self.param_count
        );
        let columns = get_columns(self.num_views, self.param_count);
        let permutations: Vec<Vec<&String>> =
            columns.iter().combinations(self.param_count).collect();

        let mut results = BenchmarkResults::new();
        assert!(permutations.len() >= self.num_views);
        for (i, c) in permutations.iter().enumerate().take(self.num_views) {
            let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();

            let start = Instant::now();
            let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
            let connection_time = start.elapsed();

            let condition = c
                .iter()
                .map(|p| format!("{} = ?", p))
                .collect::<Vec<String>>()
                .join(" AND ");

            let start = Instant::now();
            let _ = conn
                .prep(format!("SELECT * FROM bigtable WHERE {}", condition))
                .await
                .unwrap();
            let prepare_time = start.elapsed();

            debug!(
                "connection:\t{:.1}ms\tprepare:\t{:.1}ms",
                connection_time.as_secs_f64() * 1000.0,
                prepare_time.as_secs_f64() * 1000.0
            );

            if i == 0 || i == self.num_views || i == self.num_views / 2 {
                results.append(&[
                    (
                        &format!("connect @ view {}", i),
                        connection_time.as_secs_f64() * 1000.0,
                    ),
                    (
                        &format!("prepare @ view {}", i),
                        connection_time.as_secs_f64() * 1000.0,
                    ),
                ]);
            }

            benchmark_histogram!(
                "scale_views.connection_duration",
                Seconds,
                "The number of seconds spent creating a new connection",
                connection_time.as_secs_f64()
            );
            benchmark_histogram!(
                "scale_views.prepare_duration",
                Seconds,
                "The number of seconds spent executing a prepare for a view",
                prepare_time.as_secs_f64()
            );

            benchmark_counter!(
                "scale_views.num_views",
                Count,
                "The number of views prepared against the backend.",
                1
            )
        }

        Ok(results)
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("num_views".to_string(), self.num_views.to_string());
        labels.insert("param_count".to_string(), self.param_count.to_string());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }
}
