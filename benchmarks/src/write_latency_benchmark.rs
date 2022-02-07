use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use clap::Parser;
use metrics::Unit;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use nom_sql::{parse_query, Dialect, SqlQuery};
use query_generator::{ColumnName, TableName};

use crate::benchmark::{BenchmarkControl, BenchmarkResults, DeploymentParameters};
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::{forward, ForwardPrometheusMetrics};
use crate::utils::query::ArbitraryQueryParameters;
use crate::utils::us_to_ms;

#[derive(Parser, Clone, Serialize, Deserialize)]
pub struct WriteLatencyBenchmark {
    #[clap(flatten)]
    data_generator: DataGenerator,

    #[clap(flatten)]
    update_query: ArbitraryQueryParameters,

    /// Field to key on for our SELECT and UPDATE queries
    #[clap(long)]
    key_field: ColumnName,

    /// Number of updates to issue
    #[clap(long, default_value = "1000")]
    updates: u32,
}

#[async_trait]
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

        let prepared_statement = self.update_query.prepared_statement(&mut db).await?;
        let parsed_query =
            parse_query(Dialect::MySQL, &prepared_statement.query).map_err(|e| anyhow!("{}", e))?;
        let table: TableName = match parsed_query {
            SqlQuery::Update(q) => q.table.to_string().replace('`', "").into(),
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
        let key_value: Value = key_value.try_into()?;

        let select = db
            .prep(format!(
                "SELECT * FROM {} WHERE {} <= ?",
                table, self.key_field
            ))
            .await?;
        let rows = db.exec::<Row, _, _>(&select, (&key_value,)).await?.len();
        debug!("{} rows match", rows);
        debug!("View created");

        let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let (update, _) = prepared_statement.generate_query();
        for _i in 0..self.updates {
            let start = Instant::now();
            db.exec_drop(&update, prepared_statement.generate_parameters())
                .await?;
            let elapsed = start.elapsed();
            hist.record(u64::try_from(elapsed.as_micros()).unwrap())
                .unwrap();
        }

        Ok(BenchmarkResults::from(&[
            (
                "latency p50",
                (us_to_ms(hist.value_at_quantile(0.5)), Unit::Milliseconds),
            ),
            (
                "latency p90",
                (us_to_ms(hist.value_at_quantile(0.9)), Unit::Milliseconds),
            ),
            (
                "latency p99",
                (us_to_ms(hist.value_at_quantile(0.99)), Unit::Milliseconds),
            ),
            (
                "latency p99.99",
                (us_to_ms(hist.value_at_quantile(0.9999)), Unit::Milliseconds),
            ),
        ]))
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
}
