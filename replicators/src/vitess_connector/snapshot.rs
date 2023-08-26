use std::error::Error;
use std::future;

use futures::TryFutureExt;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::TxOpts;
use nom_sql::Relation;
use readyset_client::recipe::changelist::Change;
use readyset_client::recipe::ChangeList;
use readyset_data::Dialect;
use readyset_errors::{internal_err, ReadySetResult};
use tracing::{debug, error, info, warn};

use crate::db_util::DatabaseSchemas;
use crate::mysql_connector::TableKind;
use crate::table_filter::TableFilter;

/// A list of databases MySQL uses internally, they should not be replicated
pub const MYSQL_INTERNAL_DBS: &[&str] =
    &["mysql", "information_schema", "performance_schema", "sys"];

pub(crate) struct VitessReplicator {
    /// This is the underlying (regular) MySQL connection
    pub(crate) pool: mysql_async::Pool,

    /// Filters out the desired tables to snapshot and replicate
    pub(crate) table_filter: TableFilter,
}

impl VitessReplicator {
    pub async fn snapshot_schema_to_noria(
        mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
    ) -> ReadySetResult<()> {
        info!("Starting schema snapshot...");
        let mut tx = self.pool.start_transaction(tx_opts()).await?;

        let _ = tx
            .query_drop("SET SESSION MAX_EXECUTION_TIME=0")
            .await
            .map_err(log_err);

        debug!("Getting list of tables...");
        let all_tables = get_table_list(&mut tx, TableKind::BaseTable).await?;
        let (replicated_tables, non_replicated_tables) = all_tables
            .into_iter()
            .partition::<Vec<_>, _>(|(schema, table)| {
                self.table_filter
                    .should_be_processed(schema.as_str(), table.as_str())
            });

        debug!("Extending recipe with non-replicated tables");
        noria
            .extend_recipe_no_leader_ready(ChangeList::from_changes(
                non_replicated_tables
                    .into_iter()
                    .map(|(schema, name)| {
                        Change::AddNonReplicatedRelation(Relation {
                            schema: Some(schema.into()),
                            name: name.into(),
                        })
                    })
                    .collect::<Vec<_>>(),
                Dialect::DEFAULT_MYSQL,
            ))
            .await?;

        debug!("Getting list of all tables to lock...");
        let all_tables_formatted = replicated_tables
            .iter()
            .map(|(db, tbl)| format!("`{db}`.`{tbl}`"))
            .collect::<Vec<_>>();

        debug!("Locking tables...");
        for tables in all_tables_formatted.chunks(20) {
            // There is a default limit of 61 tables per join, so we chunk into smaller joins just
            // in case
            let metalock = format!("SELECT 1 FROM {} LIMIT 0", tables.iter().join(","));
            tx.query_drop(metalock).await?;
        }

        debug!("Getting schema from of replicated tables...");
        let mut bad_tables = Vec::new();
        // Process `CREATE TABLE` statements
        for (db, table) in replicated_tables.iter() {
            match create_for_table(&mut tx, db, table, TableKind::BaseTable)
                .map_err(|e| e.into())
                .and_then(|create_table| {
                    debug!(%create_table, "Extending recipe");
                    db_schemas.extend_create_schema_for_table(
                        db.to_string(),
                        table.to_string(),
                        create_table.clone(),
                        nom_sql::Dialect::MySQL,
                    );

                    future::ready(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL))
                })
                .and_then(|changelist| {
                    noria.extend_recipe_no_leader_ready(
                        changelist.with_schema_search_path(vec![db.clone().into()]),
                    )
                })
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    warn!(%error, "Error extending CREATE TABLE, table will not be used");
                    // Prevent the table from being snapshotted as well
                    bad_tables.push((db.clone(), table.clone()));

                    noria
                        .extend_recipe_no_leader_ready(ChangeList::from_change(
                            Change::AddNonReplicatedRelation(Relation {
                                schema: Some(db.into()),
                                name: table.into(),
                            }),
                            Dialect::DEFAULT_MYSQL,
                        ))
                        .await?;
                }
            }
        }

        debug!("Denying replication for bad tables");
        bad_tables
            .into_iter()
            .for_each(|(db, table)| self.table_filter.deny_replication(&db, &table));

        // We process all views, regardless of their schemas and the table filter, since a view can
        // exist that only selects from tables in other schemas.
        let all_views = get_table_list(&mut tx, TableKind::View).await?;

        debug!("Getting schema for views...");
        // Process `CREATE VIEW` statements
        for (db, view) in all_views.iter() {
            match create_for_table(&mut tx, db, view, TableKind::View)
                .map_err(|e| e.into())
                .and_then(|create_view| {
                    db_schemas.extend_create_schema_for_view(
                        db.to_string(),
                        view.to_string(),
                        create_view.clone(),
                        nom_sql::Dialect::MySQL,
                    );
                    future::ready(ChangeList::from_str(create_view, Dialect::DEFAULT_MYSQL))
                })
                .and_then(|changelist| {
                    noria.extend_recipe_no_leader_ready(
                        changelist.with_schema_search_path(vec![db.clone().into()]),
                    )
                })
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    warn!(%view, %error, "Error extending CREATE VIEW, view will not be used");
                    noria
                        .extend_recipe_no_leader_ready(ChangeList::from_change(
                            Change::AddNonReplicatedRelation(Relation {
                                schema: Some(db.into()),
                                name: view.into(),
                            }),
                            Dialect::DEFAULT_MYSQL,
                        ))
                        .await?;
                }
            }
        }

        let table_list = replicated_tables
            .into_iter()
            // refilter to remove any bad tables that failed to extend recipe
            .filter(|(schema, table)| {
                self.table_filter
                    .should_be_processed(schema.as_str(), table.as_str())
            })
            .map(|(schema, name)| Relation {
                schema: Some(schema.into()),
                name: name.into(),
            })
            .collect::<Vec<_>>();
        info!("Schema snapshot complete for {} tables", table_list.len());

        // Wait for all connections to finish, not strictly necessary
        self.pool.disconnect().await?;

        Ok(())
    }
}

/// Pass the error forward while logging it
fn log_err<E: Error>(err: E) -> E {
    error!(error = %err);
    err
}

/// Get the list of tables defined in the database
pub async fn load_table_list<Q: Queryable>(
    q: &mut Q,
    kind: TableKind,
    db: &str,
) -> mysql_async::Result<Vec<String>> {
    let query = format!("show full tables in `{db}` where Table_Type={kind}");
    let tables_entries: Vec<(String, String)> = q.query_map(query, std::convert::identity).await?;
    Ok(tables_entries.into_iter().map(|(name, _)| name).collect())
}

/// Get the list of tables defined in the database for all (non-internal) schemas
async fn get_table_list<Q: Queryable>(
    q: &mut Q,
    kind: TableKind,
) -> mysql_async::Result<Vec<(String, String)>> {
    let mut all_tables = Vec::new();
    let schemas = q
        .query_iter("SHOW SCHEMAS")
        .await?
        .collect::<String>()
        .await?;
    for schema in schemas
        .into_iter()
        .filter(|s| !MYSQL_INTERNAL_DBS.contains(&s.as_str()))
    {
        let tables = load_table_list(q, kind, &schema).await?;
        for table in tables {
            all_tables.push((schema.clone(), table));
        }
    }
    Ok(all_tables)
}

/// Get the `CREATE TABLE` or `CREATE VIEW` statement for the named table
pub async fn create_for_table<Q: Queryable>(
    q: &mut Q,
    db: &str,
    table_name: &str,
    kind: TableKind,
) -> mysql_async::Result<String> {
    let query = format!("show create {} `{db}`.`{table_name}`", kind.kind());
    match kind {
        TableKind::View => {
            // For SHOW CREATE VIEW the format is the name of the view, the create DDL,
            // character_set_client and collation_connection
            let r: Option<(String, String, String, String)> = q.query_first(query).await?;
            Ok(r.ok_or_else(|| {
                mysql_async::Error::Other(Box::new(internal_err!(
                    "Empty response for SHOW CREATE VIEW"
                )))
            })?
            .1)
        }
        TableKind::BaseTable => {
            // For SHOW CREATE TABLE format is the name of the table and the create DDL
            let r: Option<(String, String)> = q.query_first(query).await?;
            Ok(r.ok_or_else(|| {
                mysql_async::Error::Other(Box::new(internal_err!(
                    "Empty response for SHOW CREATE TABLE"
                )))
            })?
            .1)
        }
    }
}

fn tx_opts() -> TxOpts {
    let mut tx_opts = mysql_async::TxOpts::default();
    tx_opts
        .with_isolation_level(mysql_async::IsolationLevel::RepeatableRead)
        .with_consistent_snapshot(true)
        .with_readonly(true);
    tx_opts
}
