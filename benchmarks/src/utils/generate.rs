//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`, and generate batches of
//! data to write to the connection.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::iter::repeat;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueHint};
use database_utils::DatabaseURL;
use futures::StreamExt;
use itertools::Itertools;
use nom::multi::many1;
use nom::sequence::delimited;
use nom::IResult;
use nom_sql::parser::{sql_query, SqlQuery};
use nom_sql::whitespace::whitespace0;
use nom_sql::Dialect;
use noria_client::backend::Backend;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};
use query_generator::{ColumnName, TableName, TableSpec};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::spec::{DatabaseGenerationSpec, DatabaseSchema, TableGenerationSpec};
use crate::utils::path::benchmark_path;

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

fn multi_ddl(input: &[u8]) -> IResult<&[u8], Vec<SqlQuery>> {
    many1(delimited(
        whitespace0,
        sql_query(Dialect::MySQL),
        whitespace0,
    ))(input)
}

impl DataGenerator {
    pub async fn install(&self, conn_str: &str) -> anyhow::Result<()> {
        let mut conn = DatabaseURL::from_str(conn_str)?.connect().await?;
        let ddl = std::fs::read_to_string(benchmark_path(self.schema.clone())?.as_path())?;

        let parsed = multi_ddl(ddl.as_bytes())
            .map_err(|e| anyhow!("Error parsing DDL {}", e.to_string()))?;
        // This may be a multi-line DDL, if it is semi-colons terminate the statements.
        for statement in parsed.1 {
            conn.query_drop(statement.to_string()).await?;
        }
        Ok(())
    }

    async fn adjust_mysql_vars(db_url: &DatabaseURL) -> Option<usize> {
        use mysql_async::prelude::Queryable;

        let mysql_opt = match db_url {
            DatabaseURL::MySQL(mysql_opts) => mysql_opts.clone(),
            _ => return None,
        };

        let mut conn = mysql_async::Conn::new(mysql_opt).await.ok()?;
        let size = conn.query_first("SELECT @@innodb_buffer_pool_size").await;
        let old_size: usize = size.ok()??;
        let new_size = std::cmp::min(old_size * 8, 1024 * 1024 * 1024); // We want a buffer of at least 1GiB

        let set_pool_size_q = format!("SET GLOBAL innodb_buffer_pool_size={}", new_size);
        let disable_redo_log_q = "ALTER INSTANCE DISABLE INNODB REDO_LOG";
        let _ = conn.query_drop(set_pool_size_q).await;
        let _ = conn.query_drop(disable_redo_log_q).await;
        Some(old_size)
    }

    async fn revert_mysql_vars(db_url: &DatabaseURL, old_size: Option<usize>) {
        use mysql_async::prelude::Queryable;

        let (mysql_opt, old_size) = match (db_url, old_size) {
            (DatabaseURL::MySQL(opts), Some(sz)) => (opts.clone(), sz),
            _ => return,
        };

        if let Ok(mut conn) = mysql_async::Conn::new(mysql_opt).await {
            let set_pool_size_q = format!("SET GLOBAL innodb_buffer_pool_size={}", old_size);
            let enable_redo_log_q = "ALTER INSTANCE ENABLE INNODB REDO_LOG";
            let _ = conn.query_drop(set_pool_size_q).await;
            let _ = conn.query_drop(enable_redo_log_q).await;
        }
    }

    pub async fn generate(&self, conn_str: &str) -> anyhow::Result<DatabaseGenerationSpec> {
        let user_vars: HashMap<String, String> = self
            .var_overrides
            .as_object()
            .expect("var-overrides should be formatted as a json map")
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.as_str().unwrap().to_owned()))
            .collect();

        let db_url = DatabaseURL::from_str(conn_str)?;

        let old_size = Self::adjust_mysql_vars(&db_url).await;

        let schema = DatabaseSchema::try_from((benchmark_path(self.schema.clone())?, user_vars))?;
        let database_spec = DatabaseGenerationSpec::new(schema);
        let status = parallel_load(db_url.clone(), database_spec.clone()).await;

        Self::revert_mysql_vars(&db_url, old_size).await;

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

fn query_for_prepared_insert(table_name: &TableName, cols: &[ColumnName], rows: usize) -> String {
    let placeholders = format!("({})", repeat("?").take(cols.len()).join(","));
    let placeholders = repeat(placeholders).take(rows).join(",");

    format!(
        "INSERT INTO `{}` (`{}`) VALUES {}",
        table_name,
        cols.iter().join("`,`"),
        placeholders
    )
}

pub async fn load_table_part(
    db_url: DatabaseURL,
    table_name: TableName,
    mut spec: TableSpec,
    partition: TablePartition,
    progress_bar: indicatif::ProgressBar,
) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let mut conn = match db_url {
        DatabaseURL::MySQL(opts) => mysql_async::Conn::new(opts).await?,
        _ => unimplemented!(),
    };

    let columns = spec.columns.keys().cloned().collect::<Vec<_>>();
    let insert_stmt = query_for_prepared_insert(&table_name, &columns, MAX_BATCH_ROWS);
    let prepared_stmt = conn.prep(insert_stmt).await?;

    let mut rows_remaining = partition.rows;

    conn.query_drop("SET autocommit = 0").await?;

    while rows_remaining > 0 {
        let rows_to_generate = std::cmp::min(MAX_BATCH_ROWS, rows_remaining);
        let index = partition.index + partition.rows - rows_remaining;

        let data_as_params = tokio::task::block_in_place(|| {
            let data = spec.generate_data_from_index(rows_to_generate, index, false);

            data.into_iter()
                .flat_map(|mut row| {
                    columns
                        .iter()
                        .map(move |col| row.remove(col).unwrap().try_into().unwrap())
                })
                .collect::<Vec<mysql_async::Value>>()
        });

        let res = if rows_to_generate == MAX_BATCH_ROWS {
            conn.exec_drop(
                &prepared_stmt,
                mysql_async::Params::Positional(data_as_params),
            )
            .await
        } else {
            let tail_insert = query_for_prepared_insert(&table_name, &columns, rows_to_generate);
            let stmt = conn.prep(tail_insert).await?;
            conn.exec_drop(&stmt, mysql_async::Params::Positional(data_as_params))
                .await
        };

        match res {
            Ok(_) => {}
            // If it is a key already exists error try to continue.
            Err(mysql_async::Error::Server(e)) if e.code == 1062 => {
                warn!("Key already exists, skipping");
            }
            Err(e) => return Err(e.into()),
        }

        progress_bar.inc(rows_to_generate as _);
        rows_remaining -= rows_to_generate;
    }

    conn.query_drop("COMMIT").await?;

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
        .buffer_unordered(8);

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
        .buffer_unordered(4);

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
