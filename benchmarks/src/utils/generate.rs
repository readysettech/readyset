//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`,
//! and generate batches of data to write to the connection.

use super::spec::{DatabaseGenerationSpec, DatabaseSchema};
use anyhow::{Context, Result};
use clap::{Parser, ValueHint};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use noria_client::backend::Backend;
use noria_logictest::upstream::DatabaseConnection;
use noria_logictest::upstream::DatabaseURL;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};
use query_generator::{TableName, TableSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::str::FromStr;

const MAX_BATCH_ROWS: usize = 1000;
const MAX_PARTITION_ROWS: usize = 10000;

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct DataGenerator {
    /// Path to the desired database SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// Change or assign values to user variables in the provided schema.
    /// The format is a json map, for example "{ 'user_rows': '10000', 'article_rows': '100' }"
    #[clap(long, default_value = "{}")]
    var_overrides: serde_json::Value,
}

impl DataGenerator {
    pub async fn install(&self, conn_str: &str) -> anyhow::Result<()> {
        let mut conn = DatabaseURL::from_str(conn_str)?.connect().await?;
        let ddl = std::fs::read_to_string(self.schema.as_path())?;
        conn.query_drop(ddl).await
    }

    pub async fn generate(&self, conn_str: &str) -> anyhow::Result<DatabaseGenerationSpec> {
        let user_vars: HashMap<String, String> = self
            .var_overrides
            .as_object()
            .expect("var-overrides should be formatted as a json map")
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.as_str().unwrap().to_owned()))
            .collect();

        let schema = DatabaseSchema::try_from((self.schema.clone(), user_vars))?;
        let mut database_spec = DatabaseGenerationSpec::new(schema);
        let mut conn = DatabaseURL::from_str(conn_str)?.connect().await?;
        load(&mut conn, &mut database_spec).await?;

        Ok(database_spec)
    }

    pub fn labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(
            "data_generator_schema".to_string(),
            self.schema.to_string_lossy().to_string(),
        );
        labels
    }
}

#[derive(Debug)]
pub struct TablePartition {
    rows: usize,
    index: usize,
}

pub async fn load_table_part(
    db_url: DatabaseURL,
    table_name: TableName,
    mut spec: TableSpec,
    partition: TablePartition,
) -> Result<()> {
    let mut db = db_url.connect().await?;

    let mut rows_remaining = partition.rows;
    while rows_remaining > 0 {
        let rows_to_generate = std::cmp::min(MAX_BATCH_ROWS, rows_remaining);
        let index = partition.index + partition.rows - rows_remaining;
        let data = spec.generate_data_from_index(rows_to_generate, index, false);
        let columns = spec.columns.keys().collect::<Vec<_>>();
        let insert = nom_sql::InsertStatement {
            table: table_name.clone().into(),
            fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
            data: data
                .into_iter()
                .map(|mut row| {
                    columns
                        .iter()
                        .map(|col| row.remove(col).unwrap().try_into().unwrap())
                        .collect()
                })
                .collect(),
            ignore: false,
            on_duplicate: None,
        };

        db.query_drop(insert.to_string())
            .await
            .with_context(|| format!("Inserting row into database for {}", table_name))?;

        rows_remaining -= rows_to_generate;
        let _ = stdout().flush();
    }

    Ok(())
}

pub async fn parallel_load(
    db: DatabaseURL,
    spec: &mut DatabaseGenerationSpec,
    threads: usize,
) -> Result<()> {
    // Iterate over the set of tables in the database for each, generate random
    // data.
    for (table_name, table_spec) in spec.tables.iter_mut() {
        if table_spec.num_rows == 0 {
            continue;
        }

        println!(
            "Generating table '{}', {} rows",
            table_name, table_spec.num_rows
        );

        print!("Progress 0.00%");
        let _ = stdout().flush();

        // Create a queue of table parts that still need to be generated.
        let num_partitions = table_spec.num_rows / MAX_PARTITION_ROWS
            + (table_spec.num_rows % MAX_PARTITION_ROWS != 0) as usize;
        let mut to_load: VecDeque<TablePartition> = (0..num_partitions)
            .map(|p| {
                let index = p * MAX_PARTITION_ROWS;
                let rows = if p == (num_partitions - 1) {
                    MAX_PARTITION_ROWS
                        - ((num_partitions * MAX_PARTITION_ROWS) - table_spec.num_rows)
                } else {
                    MAX_PARTITION_ROWS
                };

                TablePartition { rows, index }
            })
            .collect();

        // If we have enough partitions to run the maximum, start of with the
        // maximum number of futures.
        let mut futures = FuturesUnordered::new();
        while futures.len() < threads && !to_load.is_empty() {
            let partition = to_load.pop_back().unwrap();
            futures.push(tokio::spawn(load_table_part(
                db.clone(),
                table_name.clone(),
                table_spec.table.clone(),
                partition,
            )));
        }

        // Until we get through the entire queue of partitions, poll on a
        // future completing, when it does, start the next partition.
        while !to_load.is_empty() || !futures.is_empty() {
            match futures.next().await {
                Some(r) => {
                    r??;

                    print!(
                        "\rProgress {:.2}%",
                        (1.0 - (to_load.len() as f64 + futures.len() as f64)
                            / num_partitions as f64)
                            * 100.0
                    );
                    let _ = stdout().flush();

                    if !to_load.is_empty() {
                        let partition = to_load.pop_back().unwrap();
                        futures.push(tokio::spawn(load_table_part(
                            db.clone(),
                            table_name.clone(),
                            table_spec.table.clone(),
                            partition,
                        )));
                    }
                }
                None => {
                    break;
                }
            }
        }

        println!();
    }

    Ok(())
}

pub async fn load(db: &mut DatabaseConnection, spec: &mut DatabaseGenerationSpec) -> Result<()> {
    // Iterate over the set of tables in the database for each, generate random
    // data.
    for (table_name, table_spec) in spec.tables.iter_mut() {
        if table_spec.num_rows == 0 {
            continue;
        }

        println!(
            "Generating table '{}', {} rows",
            table_name, table_spec.num_rows
        );

        let mut rows_remaining = table_spec.num_rows;
        let row_at_start = table_spec.num_rows as f64;

        print!("Progress 0.00%");
        let _ = stdout().flush();

        while rows_remaining > 0 {
            let rows_to_generate = std::cmp::min(MAX_BATCH_ROWS, rows_remaining);
            let data = table_spec.table.generate_data_from_index(
                rows_to_generate,
                table_spec.num_rows - rows_remaining,
                false,
            );
            let columns = table_spec.table.columns.keys().collect::<Vec<_>>();
            let insert = nom_sql::InsertStatement {
                table: table_name.clone().into(),
                fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
                data: data
                    .into_iter()
                    .map(|mut row| {
                        columns
                            .iter()
                            .map(|col| row.remove(col).unwrap().try_into().unwrap())
                            .collect()
                    })
                    .collect(),
                ignore: false,
                on_duplicate: None,
            };

            db.query_drop(insert.to_string())
                .await
                .with_context(|| format!("Inserting row into database for {}", table_name))?;

            rows_remaining -= rows_to_generate;

            print!(
                "\rProgress {:.2}%",
                (1.0 - rows_remaining as f64 / row_at_start) * 100.0
            );
            let _ = stdout().flush();
        }
        println!();
    }

    Ok(())
}

pub async fn load_to_backend(
    db: &mut Backend<MySqlUpstream, MySqlQueryHandler>,
    mut spec: DatabaseGenerationSpec,
) -> Result<()> {
    // Iterate over the set of tables in the database for each, generate random
    // data.
    for (table_name, table_spec) in spec.tables.iter_mut() {
        if table_spec.num_rows == 0 {
            continue;
        }

        let data = table_spec.table.generate_data(table_spec.num_rows, false);
        let columns = table_spec.table.columns.keys().collect::<Vec<_>>();
        let insert = nom_sql::InsertStatement {
            table: table_name.clone().into(),
            fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
            data: data
                .into_iter()
                .map(|mut row| {
                    columns
                        .iter()
                        .map(|col| row.remove(col).unwrap().try_into().unwrap())
                        .collect()
                })
                .collect(),
            ignore: false,
            on_duplicate: None,
        };

        db.query(&insert.to_string())
            .await
            .with_context(|| format!("Inserting row into database for {}", table_name))?;
    }

    Ok(())
}
