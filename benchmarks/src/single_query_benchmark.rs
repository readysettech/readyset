//! A benchmark that runs *the same* prepared statement over and over again. As opposed to the
//! QueryBenchmark, which selects different parameters.
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use serde::{Deserialize, Serialize};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::benchmark_histogram;
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::ForwardPrometheusMetrics;
use crate::utils::query::{ArbitraryQueryParameters, PreparedStatement};

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct SingleQueryBenchmark {
    /// Parameters to handle generating parameters for arbitrary queries.
    #[clap(flatten)]
    query: ArbitraryQueryParameters,

    /// Install and generate from an arbitrary schema.
    #[clap(flatten)]
    data_generator: DataGenerator,

    /// The number of times to execute the query.
    #[clap(long, default_value = "10000")]
    num_executions: u32,

    /// Execute this query via `query` instead of `exec`.
    #[clap(long)]
    ad_hoc: bool,
}

#[async_trait]
impl BenchmarkControl for SingleQueryBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        self.data_generator
            .generate(&deployment.setup_conn_str)
            .await?;
        Ok(())
    }

    async fn reset(&self, deployment: &DeploymentParameters) -> Result<()> {
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let _ = self.query.unmigrate(&mut conn).await;
        Ok(())
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        // Explicitely migrate the query before benchmarking.
        let opts = mysql_async::Opts::from_url(&deployment.target_conn_str).unwrap();
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let _ = self.query.migrate(&mut conn).await;
        let mut prepared_statement = self.query.prepared_statement(&mut conn).await?;

        self.run_queries(&mut conn, &mut prepared_statement).await
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.extend(self.query.labels());
        labels.extend(self.data_generator.labels());
        labels
    }

    fn forward_metrics(&self, _: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![]
    }

    fn name(&self) -> &'static str {
        "single_query_benchmark"
    }
}

impl SingleQueryBenchmark {
    async fn run_queries(
        &self,
        conn: &mut mysql_async::Conn,
        statement: &mut PreparedStatement,
    ) -> Result<BenchmarkResults> {
        let mut results = BenchmarkResults::new();
        // Generates 1000 cache misses.
        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let duration = results.entry("duration", Unit::Microseconds, MetricGoal::Decreasing);
        for _ in 0..self.num_executions {
            let elapsed = if self.ad_hoc {
                let query = statement.generate_ad_hoc_query();
                let start = Instant::now();
                let _: Vec<Row> = conn.query(query).await?;
                start.elapsed()
            } else {
                let (query, params) = statement.generate_query();
                let start = Instant::now();
                let _: Vec<Row> = conn.exec(query, params).await?;
                start.elapsed()
            };
            duration.push(elapsed.as_micros() as f64);
            hist.record(u64::try_from(elapsed.as_micros()).unwrap())
                .unwrap();

            benchmark_histogram!(
                "single_query_benchmark.duration",
                Microseconds,
                "Duration of queries executed",
                elapsed.as_micros() as f64
            );
        }

        Ok(results)
    }
}
