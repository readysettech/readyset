//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`, and generate batches of
//! data to write to the connection.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueHint};
use database_utils::{DatabaseConnection, DatabaseURL, QueryableConnection};
use futures::StreamExt;
use itertools::Itertools;
use nom::multi::many1;
use nom::sequence::delimited;
use nom_locate::LocatedSpan;
use nom_sql::parser::{sql_query, SqlQuery};
use nom_sql::whitespace::whitespace0;
use nom_sql::{Column, Dialect, Expr, NomSqlResult, Relation};
use query_generator::{ColumnName, TableName, TableSpec};
use readyset_data::DfValue;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::warn;

use super::spec::{DatabaseGenerationSpec, DatabaseSchema, SchemaKind, TableGenerationSpec};
use crate::utils::backend::Backend;
use crate::utils::path::benchmark_path;

const MAX_BATCH_ROWS: usize = 500;
const MAX_PARTITION_ROWS: usize = 20000;

#[derive(Parser, Clone, Default, Serialize, Deserialize)]
pub struct DataGenerator {
    /// Path to the desired database SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// Change or assign values to user variables in the provided schema. Note that user variables
    /// are not supported for Postgres.
    /// The format is a json map, for example "{ 'user_rows': '10000', 'article_rows': '100' }"
    #[clap(long)]
    var_overrides: Option<serde_json::Value>,
}

fn multi_ddl(input: LocatedSpan<&[u8]>, dialect: Dialect) -> NomSqlResult<&[u8], Vec<SqlQuery>> {
    many1(delimited(whitespace0, sql_query(dialect), whitespace0))(input)
}

impl DataGenerator {
    pub fn new<P: Into<PathBuf>>(schema: P) -> Self {
        DataGenerator {
            schema: schema.into(),
            var_overrides: None,
        }
    }

    pub fn update_from(&mut self, json: serde_json::Value) -> anyhow::Result<()> {
        match self.var_overrides.as_mut().and_then(|x| x.as_object_mut()) {
            Some(x) => {
                let mut override_map = json;
                let m = override_map
                    .as_object_mut()
                    .ok_or_else(|| anyhow!("json was not a map"))?;
                x.append(m);
            }
            None => self.var_overrides = Some(json),
        }
        Ok(())
    }

    pub async fn install(&self, conn_str: &str) -> anyhow::Result<()> {
        let mut conn = DatabaseURL::from_str(conn_str)?.connect(None).await?;
        let ddl = std::fs::read_to_string(&benchmark_path(&self.schema)?)?;

        let parsed = multi_ddl(LocatedSpan::new(ddl.as_bytes()), conn.dialect())
            .map_err(|e| anyhow!("Error parsing DDL {}", e.to_string()))?;
        // This may be a multi-line DDL, if it is semi-colons terminate the statements.
        for statement in parsed.1 {
            conn.query_drop(statement.display(conn.dialect()).to_string())
                .await?;
        }
        Ok(())
    }

    async fn adjust_upstream_vars(db_url: &DatabaseURL) -> Option<usize> {
        let mut conn = db_url.connect(None).await.ok()?;

        match conn {
            DatabaseConnection::MySQL(_) => {
                let results: Vec<Vec<DfValue>> = conn
                    .query("SELECT @@innodb_buffer_pool_size")
                    .await
                    .ok()?
                    .try_into()
                    .ok()?;
                let old_size: usize = results.get(0)?.get(0)?.try_into().ok()?;
                let new_size = std::cmp::min(old_size * 8, 1024 * 1024 * 1024); // We want a buffer of at least 1GiB

                let set_pool_size_q = format!("SET GLOBAL innodb_buffer_pool_size={}", new_size);
                let disable_redo_log_q = "ALTER INSTANCE DISABLE INNODB REDO_LOG";
                let _ = conn.query_drop(set_pool_size_q).await;
                let _ = conn.query_drop(disable_redo_log_q).await;
                Some(old_size)
            }
            DatabaseConnection::PostgreSQL(_, _) | DatabaseConnection::PostgreSQLPool(_) => {
                let mut results: Vec<Vec<DfValue>> = conn
                    .query("SHOW shared_buffers")
                    .await
                    .ok()?
                    .try_into()
                    .ok()?;
                let old_size: usize = results.remove(0).remove(0).try_into().ok()?;
                let new_size = std::cmp::min(old_size * 8, 1024 * 1024 * 1024); // We want a buffer of at least 1GiB

                // Changing the shared buffer size via `ALTER SYSTEM` is only supported by Postgres
                // >= 14
                if get_postgres_version(&mut conn).await? >= 14000 {
                    let set_pool_size_q =
                        format!("ALTER SYSTEM SET shared_buffers TO {}", new_size);
                    let _ = conn.query_drop(set_pool_size_q).await;
                }
                Some(old_size)
            }
            DatabaseConnection::Vitess(_) => None,
        }
    }

    async fn revert_upstream_vars(db_url: &DatabaseURL, old_size: Option<usize>) {
        let conn = db_url.connect(None).await;

        match (conn, old_size) {
            (Ok(mut conn @ DatabaseConnection::MySQL(_)), Some(old_size)) => {
                let set_pool_size_q = format!("SET GLOBAL innodb_buffer_pool_size={}", old_size);
                let disable_redo_log_q = "ALTER INSTANCE ENABLE INNODB REDO_LOG";
                let _ = conn.query_drop(set_pool_size_q).await;
                let _ = conn.query_drop(disable_redo_log_q).await;
            }
            (Ok(mut conn @ DatabaseConnection::PostgreSQL(_, _)), Some(old_size)) => {
                if let Some(pg_version) = get_postgres_version(&mut conn).await {
                    // Changing the shared buffer size via `ALTER SYSTEM` is only supported by
                    // Postgres >= 14
                    if pg_version >= 14000 {
                        let set_pool_size_q =
                            format!("ALTER SYSTEM SET shared_buffers TO {}", old_size);
                        let _ = conn.query_drop(set_pool_size_q).await;
                    }
                }
            }
            _ => (),
        }
    }

    pub async fn generate(&self, conn_str: &str) -> anyhow::Result<DatabaseGenerationSpec> {
        let db_url = DatabaseURL::from_str(conn_str)?;

        let schema = match db_url.dialect() {
            Dialect::PostgreSQL => {
                if self.var_overrides.is_some() {
                    warn!("var overrides are set, but var overrides are not supported for PostgreSQL!");
                }

                let ddl = std::fs::read_to_string(benchmark_path(&self.schema)?)?;
                DatabaseSchema::new(&ddl, SchemaKind::PostgreSQL)?
            }
            Dialect::MySQL => {
                let user_vars: HashMap<String, String> = self
                    .var_overrides
                    .clone()
                    .unwrap_or_else(|| json!({}))
                    .as_object()
                    .expect("var-overrides should be formatted as a json map")
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.as_str().unwrap().to_owned()))
                    .collect();

                let ddl = std::fs::read_to_string(benchmark_path(&self.schema)?)?;
                DatabaseSchema::new(&ddl, SchemaKind::MySQL { user_vars })?
            }
        };

        let old_size = Self::adjust_upstream_vars(&db_url).await;

        let database_spec = DatabaseGenerationSpec::new(schema);
        let status = parallel_load(db_url.clone(), database_spec.clone()).await;

        Self::revert_upstream_vars(&db_url, old_size).await;

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

async fn get_postgres_version(conn: &mut DatabaseConnection) -> Option<usize> {
    match conn {
        DatabaseConnection::PostgreSQL(_, _) => {
            let mut results: Vec<Vec<DfValue>> = conn
                .query("SHOW server_version_num")
                .await
                .ok()?
                .try_into()
                .ok()?;
            results.pop()?.pop()?.try_into().ok()
        }
        _ => None,
    }
}

#[derive(Debug)]
pub struct TablePartition {
    rows: usize,
    index: usize,
}

fn query_for_prepared_insert(
    table_name: &TableName,
    cols: &[ColumnName],
    rows: usize,
    dialect: Dialect,
) -> String {
    nom_sql::InsertStatement {
        table: Relation::from(table_name.clone()),
        fields: Some(cols.iter().map(|col| Column::from(col.clone())).collect()),
        data: match dialect {
            Dialect::MySQL => {
                vec![
                    vec![
                        nom_sql::Expr::Literal(nom_sql::ItemPlaceholder::QuestionMark.into());
                        cols.len()
                    ];
                    rows
                ]
            }
            Dialect::PostgreSQL => (1..=(rows * cols.len()))
                .map(|n| {
                    nom_sql::Expr::Literal(nom_sql::ItemPlaceholder::DollarNumber(n as u32).into())
                })
                .chunks(cols.len())
                .into_iter()
                .map(|chunk| chunk.collect())
                .collect(),
        },
        ignore: false,
        on_duplicate: None,
    }
    .display(dialect)
    .to_string()
}

pub async fn load_table_part(
    db_url: DatabaseURL,
    table_name: TableName,
    mut spec: TableSpec,
    partition: TablePartition,
    progress_bar: indicatif::ProgressBar,
) -> Result<()> {
    let mut conn = db_url.connect(None).await?;

    let columns = spec.columns.keys().cloned().collect::<Vec<_>>();
    let insert_stmt =
        query_for_prepared_insert(&table_name, &columns, MAX_BATCH_ROWS, conn.dialect());
    let prepared_stmt = conn.prepare(insert_stmt).await?;

    let mut rows_remaining = partition.rows;

    if let DatabaseConnection::MySQL(_) = conn {
        conn.query_drop("SET autocommit = 0").await?;
    }

    while rows_remaining > 0 {
        let rows_to_generate = std::cmp::min(MAX_BATCH_ROWS, rows_remaining);
        let index = partition.index + partition.rows - rows_remaining;

        let data_as_params = tokio::task::block_in_place(|| {
            let data = spec.generate_data_from_index(rows_to_generate, index, false);

            data.into_iter()
                .flat_map(|mut row| columns.iter().map(move |col| row.remove(col).unwrap()))
                .collect::<Vec<DfValue>>()
        });

        let res = if rows_to_generate == MAX_BATCH_ROWS {
            conn.execute(&prepared_stmt, data_as_params).await
        } else {
            let tail_insert =
                query_for_prepared_insert(&table_name, &columns, rows_to_generate, conn.dialect());
            let stmt = conn.prepare(tail_insert).await?;
            conn.execute(&stmt, data_as_params).await
        };

        match res {
            Ok(_) => {}
            // If the error indicates that a row has already been inserted, skip it
            Err(e) if e.is_mysql_key_already_exists() || e.is_postgres_unique_violation() => {
                warn!("Row already exists, skipping");
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
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")?
        .progress_chars("##-");

    let mut progress_bars: HashMap<String, ProgressBar> = Default::default();

    spec.tables.iter().for_each(|(table_name, table_spec)| {
        let new_bar = multi_progress.add(ProgressBar::new(table_spec.num_rows as _));
        new_bar.set_style(sty.clone());
        new_bar.set_message(table_name.to_string());
        progress_bars.insert(table_name.to_string(), new_bar);
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

pub async fn load_to_backend(db: &mut Backend, mut spec: DatabaseGenerationSpec) -> Result<()> {
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
                        .map(|col| Expr::Literal(row.remove(col).unwrap().try_into().unwrap()))
                        .collect()
                })
                .collect(),
            ignore: false,
            on_duplicate: None,
        };

        db.query(&insert.display(db.dialect()).to_string())
            .await
            .with_context(|| format!("Inserting row into database for {}", table_name))?;
    }

    Ok(())
}
