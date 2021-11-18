//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`,
//! and generate batches of data to write to the connection.

use super::spec::{DatabaseGenerationSpec, DatabaseSchema};
use anyhow::{Context, Result};
use clap::{Parser, ValueHint};
use noria_client::backend::Backend;
use noria_logictest::upstream::DatabaseConnection;
use noria_logictest::upstream::DatabaseURL;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::{stdout, Write};
use std::path::PathBuf;

const MAX_BATCH_ROWS: usize = 10000;

#[derive(Parser, Clone)]
pub struct DataGenerator {
    /// Path to the desired database SQL schema.
    #[clap(long, value_hint = ValueHint::AnyPath)]
    schema: PathBuf,

    /// MySQL database connection string. This parameter is kept separate
    /// from other benchmarks MySQL parameter as data generation typically
    /// will go directly to the upstream MySQL database.
    #[clap(long)]
    database_url: DatabaseURL,

    /// Change or assign values to user variables in the provided schema.
    /// The format is a json map, for example "{ 'user_rows': '10000', 'article_rows': '100' }"
    #[clap(long, default_value = "{}")]
    var_overrides: serde_json::Value,
}

impl DataGenerator {
    pub async fn install(&self) -> anyhow::Result<()> {
        let mut conn = self.database_url.connect().await?;
        let ddl = std::fs::read_to_string(self.schema.as_path())?;
        conn.query_drop(ddl).await
    }

    pub async fn generate(&self) -> anyhow::Result<()> {
        let user_vars: HashMap<String, String> = self
            .var_overrides
            .as_object()
            .expect("var-overrides should be formatted as a json map")
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.as_str().unwrap().to_owned()))
            .collect();

        let schema = DatabaseSchema::try_from((self.schema.clone(), user_vars))?;
        let database_spec = DatabaseGenerationSpec::new(schema);
        let mut conn = self.database_url.connect().await?;
        load(&mut conn, database_spec).await?;

        Ok(())
    }
}

pub async fn load(db: &mut DatabaseConnection, mut spec: DatabaseGenerationSpec) -> Result<()> {
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
