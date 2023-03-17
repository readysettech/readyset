use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::benchmark_histogram;
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct MigrationBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    query: ArbitraryQueryParameters,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,

    /// The number of times to create and drop the query in Noria.
    #[clap(long, default_value = "10")]
    num_migrations: u32,
}

#[async_trait]
impl BenchmarkControl for MigrationBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        // Benchmark cleans up its own Noria state.
        Ok(())
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Prepare the query to retrieve the query schema.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

        let mut hist_create = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut hist_drop = hdrhistogram::Histogram::<u64>::new(3).unwrap();

        let mut migrations = vec![];
        let mut unmigrations = vec![];
        for _ in 0..(self.num_migrations) {
            let start = Instant::now();
            self.query.migrate(&mut conn).await?;
            let create_elapsed = start.elapsed();
            migrations.push(create_elapsed.as_micros() as f64);
            hist_create
                .record(u64::try_from(create_elapsed.as_micros()).unwrap())
                .unwrap();
            benchmark_histogram!(
                "migration_benchmark.migrate_duration",
                Microseconds,
                "Duration of a migration against Noria",
                create_elapsed.as_micros() as f64
            );

            let start = Instant::now();
            self.query.unmigrate(&mut conn).await?;
            let drop_elapsed = start.elapsed();
            unmigrations.push(drop_elapsed.as_micros() as f64);
            hist_drop
                .record(u64::try_from(drop_elapsed.as_micros()).unwrap())
                .unwrap();

            benchmark_histogram!(
                "migration_benchmark.unmigrate_duration",
                Microseconds,
                "Duration of a migration to remove a query against Noria",
                drop_elapsed.as_micros() as f64
            );

            debug!(
                "Added: {:.1} ms, Removed: {:.1} ms",
                us_to_ms(create_elapsed.as_micros() as u64),
                us_to_ms(drop_elapsed.as_micros() as u64),
            );
        }
        let mut benchmark_results = BenchmarkResults::new();
        benchmark_results
            .entry("migrate", Unit::Microseconds, MetricGoal::Decreasing)
            .extend(migrations);
        benchmark_results
            .entry("unmigrate", Unit::Microseconds, MetricGoal::Decreasing)
            .extend(unmigrations);

        Ok(benchmark_results)
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.query.labels());
        labels.extend(self.data_generator.labels());
        labels.insert(
            "num_migrations".to_string(),
            self.num_migrations.to_string(),
        );
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }

    fn name(&self) -> &'static str {
        "migration_benchmark"
    }
}
