//! Utilities to take a `DatabaseGenerationSpec` and a `DatabaseConnection`,
//! and generate batches of data to write to the connection.

use crate::spec::DatabaseGenerationSpec;
use anyhow::{Context, Result};
use noria::ZookeeperAuthority;
use noria_client::backend::Backend;
use noria_logictest::upstream::DatabaseConnection;
use noria_mysql::MySqlUpstream;
use std::convert::TryInto;

const MAX_BATCH_ROWS: usize = 10000;

pub async fn load(db: &mut DatabaseConnection, mut spec: DatabaseGenerationSpec) -> Result<()> {
    // Iterate over the set of tables in the database for each, generate random
    // data.
    for (table_name, table_spec) in spec.tables.iter_mut() {
        if table_spec.num_rows == 0 {
            continue;
        }

        let mut rows_remaining = table_spec.num_rows;

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
        }
    }

    Ok(())
}

pub async fn load_to_backend(
    db: &mut Backend<ZookeeperAuthority, MySqlUpstream>,
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
