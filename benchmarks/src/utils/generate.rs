//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`,
//! and generate batches of data to write to the connection.

use super::spec::{DatabaseGenerationSpec, DatabaseSchema, TableGenerationSpec};

use anyhow::{Context, Result};
use clap::{Parser, ValueHint};
use futures::StreamExt;
use noria_client::backend::Backend;
use noria_logictest::upstream::DatabaseURL;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};
use query_generator::{TableName, TableSpec};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::str::FromStr;

const MAX_BATCH_ROWS: usize = 500;
const MAX_PARTITION_ROWS: usize = 20000;

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
        let database_spec = DatabaseGenerationSpec::new(schema);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let status = runtime
            .spawn(parallel_load(
                DatabaseURL::from_str(conn_str)?,
                database_spec.clone(),
            ))
            .await
            .unwrap();

        runtime.shutdown_background();

        status?;

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
    progress_bar: indicatif::ProgressBar,
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

        progress_bar.inc(rows_to_generate as _);

        rows_remaining -= rows_to_generate;
    }

    Ok(())
}

async fn load_table(
    db_url: DatabaseURL,
    table_name: TableName,
    spec: TableGenerationSpec,
    progress_bar: indicatif::ProgressBar,
) -> Result<()> {
    let mut sub_tasks =
        futures::stream::iter((0..spec.num_rows).step_by(MAX_PARTITION_ROWS).map(|index| {
            load_table_part(
                db_url.clone(),
                table_name.clone(),
                spec.table.clone(),
                TablePartition {
                    rows: usize::min(MAX_PARTITION_ROWS, spec.num_rows - index),
                    index,
                },
                progress_bar.clone(),
            )
        }))
        .buffer_unordered(32);

    while let Some(task) = sub_tasks.next().await {
        task?;
    }

    progress_bar.finish();

    Ok(())
}

pub async fn parallel_load(db: DatabaseURL, spec: DatabaseGenerationSpec) -> Result<()> {
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

    let multi_progress = MultiProgress::new();

    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    let mut progress_bars: HashMap<String, ProgressBar> = Default::default();

    spec.tables.iter().for_each(|(table_name, table_spec)| {
        let new_bar = multi_progress.add(ProgressBar::new(table_spec.num_rows as _));
        new_bar.set_style(sty.clone());
        new_bar.set_message(table_name.to_string());
        progress_bars.insert(table_name.to_string(), new_bar);
    });

    std::thread::spawn(move || {
        multi_progress.join().unwrap();
    });

    let mut table_tasks =
        futures::stream::iter(spec.tables.into_iter().map(|(table_name, table_spec)| {
            load_table(
                db.clone(),
                table_name.clone(),
                table_spec,
                progress_bars
                    .get(<TableName as Borrow<str>>::borrow(&table_name))
                    .unwrap()
                    .clone(),
            )
        }))
        .buffer_unordered(1); // Sadly MySQL is slower when writing to two tables, if someone can figure it out change this to a higher value

    while let Some(task) = table_tasks.next().await {
        task?;
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
