use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use database_utils::QueryableConnection;
use metrics::Unit;
use query_generator::{ColumnName, TableName};
use readyset_sql::ast::SqlQuery;
use readyset_sql_parsing::parse_query;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters, MetricGoal};
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::{forward, ForwardPrometheusMetrics};
use crate::utils::query::ArbitraryQueryParameters;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct WriteLatencyBenchmark {
    #[command(flatten)]
    data_generator: DataGenerator,

    #[command(flatten)]
    update_query: ArbitraryQueryParameters,

    /// Field to key on for our SELECT and UPDATE queries
    #[arg(long)]
    key_field: ColumnName,

    /// Number of updates to issue
    #[arg(long, default_value = "1000")]
    updates: u32,
}

impl BenchmarkControl for WriteLatencyBenchmark {
    async fn setup(&self, deployment: &DeploymentParameters) -> Result<()> {
        self.data_generator
            .install(&deployment.setup_conn_str)
            .await?;
        Ok(())
    }

    async fn reset(&self, _: &DeploymentParameters) -> Result<()> {
        Err(anyhow::anyhow!("reset unsupported"))
    }

    async fn benchmark(&self, deployment: &DeploymentParameters) -> Result<BenchmarkResults> {
        let mut db = deployment.connect_to_target().await?;

        let mut data_spec = self
            .data_generator
            .generate(&deployment.target_conn_str)
            .await?;
        info!("Rows inserted");

        let mut prepared_statement = self.update_query.prepared_statement(&mut db).await?;
        let parsed_query =
            parse_query(db.dialect(), &prepared_statement.query).map_err(|e| anyhow!("{}", e))?;
        let table: TableName = match parsed_query {
            SqlQuery::Update(q) => q.table.display_unquoted().to_string().into(),
            _ => bail!("The provided query must be an UPDATE query"),
        };

        let key_value = data_spec
            .tables
            .get_mut(&table)
            .expect("Table from --update-query not found in --schema")
            .table
            .columns
            .get_mut(&self.key_field)
            .expect("--key-field not found in --schema")
            .gen_spec
            .lock()
            .generator
            .gen();
        debug!("Keying on {} <= {}", self.key_field, key_value);

        let select = db
            .prepare(format!(
                "SELECT * FROM {} WHERE {} <= ?",
                table, self.key_field
            ))
            .await?;
        let rows = db.execute(&select, &[key_value]).await?.len();
        debug!("{} rows match", rows);
        debug!("View created");

        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut results = BenchmarkResults::new();
        let duration = results.entry("duration", Unit::Microseconds, MetricGoal::Decreasing);
        for _i in 0..self.updates {
            let start = Instant::now();
            let (query, params) = prepared_statement.generate_query();
            db.execute(query, params).await?;
            let elapsed = start.elapsed();
            duration.push(elapsed.as_micros() as f64);
            hist.record(u64::try_from(elapsed.as_micros()).unwrap())
                .unwrap();
        }

        Ok(results)
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = self.data_generator.labels();
        labels.insert("key_field".to_string(), self.key_field.to_string());
        labels.insert("updates".to_string(), self.updates.to_string());
        labels
    }

    fn forward_metrics(&self, deployment: &DeploymentParameters) -> Vec<ForwardPrometheusMetrics> {
        vec![forward(
            deployment.prometheus_endpoint.clone().unwrap(),
            |metric| metric.name.starts_with("packet_write_propagation_time_us"),
        )]
    }

    fn name(&self) -> &'static str {
        "write_latency_benchmark"
    }

    fn data_generator(&mut self) -> Option<&mut DataGenerator> {
        Some(&mut self.data_generator)
    }
}
