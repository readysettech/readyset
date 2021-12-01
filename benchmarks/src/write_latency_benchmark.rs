use std::collections::HashMap;
use std::convert::TryInto;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use clap::Parser;
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use regex::Regex;
use tracing::info;

use nom_sql::{parse_query, Dialect, SqlQuery};
use query_generator::{ColumnName, TableName};

use crate::benchmark::{BenchmarkControl, BenchmarkParameters};
use crate::benchmark_gauge;
use crate::utils::generate::DataGenerator;
use crate::utils::prometheus::stream_prometheus_lines_with_filter;
use crate::utils::query::ArbitraryQueryParameters;

#[derive(Parser, Clone)]
pub struct WriteLatencyBenchmark {
    #[clap(flatten)]
    common: BenchmarkParameters,

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

    /// Noria metrics endpoint; write latency histogram will be parsed from this
    #[clap(long)]
    prometheus_endpoint: String,
}

#[async_trait]
impl BenchmarkControl for WriteLatencyBenchmark {
    async fn setup(&self) -> Result<()> {
        self.data_generator.install().await?;
        Ok(())
    }

    async fn is_already_setup(&self) -> Result<bool> {
        // TODO(mc):  Implement
        Ok(false)
    }

    async fn benchmark(&self) -> Result<()> {
        let mut db = self.common.connect().await?;

        let mut data_spec = self.data_generator.generate().await?;
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
        info!("Keying on {} <= {}", self.key_field, key_value);
        let key_value: Value = key_value.try_into()?;

        let select = db
            .prep(format!(
                "SELECT * FROM {} WHERE {} <= ?",
                table, self.key_field
            ))
            .await?;
        let rows = db.exec::<Row, _, _>(&select, (&key_value,)).await?.len();
        info!("{} rows match", rows);
        info!("View created");

        let (update, _) = prepared_statement.generate_query();
        for _i in 0..self.updates {
            db.exec_drop(&update, prepared_statement.generate_parameters())
                .await?;
        }
        info!("Rows updated");

        self.forward_metrics().await?;

        Ok(())
    }

    fn labels(&self) -> HashMap<String, String> {
        let mut labels = self.data_generator.labels();
        labels.insert("key_field".to_string(), self.key_field.to_string());
        labels.insert("updates".to_string(), self.updates.to_string());
        labels
    }
}

impl WriteLatencyBenchmark {
    async fn forward_metrics(&self) -> Result<()> {
        let re_quantile = Regex::new(r#"\bquantile="([0-9.]+)""#)?;
        let re_sample = Regex::new(r#"^([a-z_]+)\b.* ([0-9.]+)$"#)?;

        let mut summary_lines =
            stream_prometheus_lines_with_filter(&self.prometheus_endpoint, |s| {
                s.starts_with("packet_write_propagation_time_us")
            })
            .await?;
        while let Some(line) = summary_lines.next().await {
            let line = line?;
            if let Some(sample_matches) = re_sample.captures(&line) {
                let metric_name = sample_matches.get(1).unwrap().as_str().replace("_us", "");
                let value = sample_matches.get(2).unwrap().as_str().parse().unwrap();
                if let Some(quantile_matches) = re_quantile.captures(&line) {
                    let quantile = quantile_matches.get(1).unwrap().as_str().to_string();
                    benchmark_gauge!(
                        metric_name,
                        Microseconds,
                        "Write propagation time",
                        value,
                        "quantile" => quantile
                    );
                } else if metric_name.ends_with("_count") {
                    benchmark_gauge!(
                        metric_name.replace("_count", ""),
                        Count,
                        "Write propagation time",
                        value
                    );
                } else {
                    benchmark_gauge!(metric_name, Microseconds, "Write propagation time", value);
                }
            }
        }

        Ok(())
    }
}
