use std::error::Error;
use std::future;

use database_utils::VitessConfig;
use futures::TryFutureExt;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::TxOpts;
use nom_sql::{NonReplicatedRelation, NotReplicatedReason, Relation};
use readyset_client::recipe::changelist::Change;
use readyset_client::recipe::ChangeList;
use readyset_data::{DfValue, Dialect};
use readyset_errors::{internal_err, invariant, ReadySetError, ReadySetResult};
use replication_offset::vitess::VStreamPosition;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};

use super::VitessConnector;
use crate::db_util::DatabaseSchemas;
use crate::mysql_connector::TableKind;
use crate::table_filter::TableFilter;

/// A list of databases MySQL uses internally, they should not be replicated
pub const MYSQL_INTERNAL_DBS: &[&str] =
    &["mysql", "information_schema", "performance_schema", "sys"];

const BATCH_SIZE: usize = 1000; // How many queries to buffer before pushing to ReadySet

pub(crate) struct VitessReplicator {
    /// The Vitess connection config to use for table replication
    pub(crate) vitess_config: VitessConfig,

    /// This is the underlying (regular) MySQL connection
    pub(crate) pool: mysql_async::Pool,

    /// Filters out the desired tables to snapshot and replicate
    pub(crate) table_filter: TableFilter,

    /// Whether to enable statement logging
    pub(crate) enable_statement_logging: bool,
}

struct TableSnapshotBuffer {
    snapshot_started: bool,

    // The Noria table mutator API
    noria_table: readyset_client::Table,

    // Buffer of rows to be flushed
    row_buffer: Vec<Vec<DfValue>>,

    // The current position in the VStream
    current_position: Option<VStreamPosition>,
}

impl TableSnapshotBuffer {
    fn new(noria_table: readyset_client::Table) -> TableSnapshotBuffer {
        TableSnapshotBuffer {
            snapshot_started: false,
            noria_table,
            row_buffer: Vec::with_capacity(BATCH_SIZE),
            current_position: None,
        }
    }

    async fn push_row(&mut self, event: Vec<DfValue>) -> ReadySetResult<()> {
        trace!(?event, "Pushing row to buffer");
        self.row_buffer.push(event);
        if self.row_buffer.len() >= BATCH_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> ReadySetResult<()> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        if !self.snapshot_started {
            debug!("Setting snapshot mode to true");
            self.noria_table.set_snapshot_mode(true).await?;
            self.snapshot_started = true;
        }

        debug!("Sending {} rows to Noria", self.row_buffer.len());
        let send_rows = std::mem::replace(&mut self.row_buffer, Vec::with_capacity(BATCH_SIZE));
        self.noria_table.insert_many(send_rows).await?;
        Ok(())
    }

    async fn finish(&mut self) -> ReadySetResult<()> {
        debug!("Finishing snapshot to Noria...");
        self.flush().await?;

        invariant!(self.current_position.is_some(), "No VStream position set");

        info!("Setting replication offset");
        self.noria_table
            .set_replication_offset(replication_offset::ReplicationOffset::Vitess(
                self.current_position.clone().unwrap(),
            ))
            .map_err(log_err)
            .await?;

        info!("Set replication offset, compacting table");
        self.noria_table
            .set_snapshot_mode(false)
            .map_err(log_err)
            .await?;

        info!("Compacting finished");
        Ok(())
    }
}

impl VitessReplicator {
    pub async fn snapshot_to_noria(
        mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
    ) -> ReadySetResult<()> {
        let span = info_span!("Taking a Vitess snapshot");

        // Get the list of tables to replicate and copy their schema to Noria
        span.in_scope(|| info!("Starting schema snapshot..."));
        let table_list = self
            .snapshot_schema_to_noria(noria, db_schemas)
            .instrument(span.clone())
            .await?;

        // Now that we have the list of tables, we can start the data snapshot
        span.in_scope(|| info!("Starting data snapshot..."));
        for table in table_list {
            let noria_table = noria.table(table.clone()).instrument(span.clone()).await?;

            self.snapshot_table_data_to_noria(&table, noria_table)
                .instrument(span.clone())
                .await?;
        }
        span.in_scope(|| info!("Data snapshot complete"));

        // Wait for all connections to finish, not strictly necessary
        span.in_scope(|| debug!("Closing snapshot connection..."));
        self.pool.disconnect().await?;
        span.in_scope(|| debug!("Vitess MySQL snapshot connection closed"));

        Ok(())
    }

    async fn snapshot_table_data_to_noria(
        &self,
        table: &Relation,
        noria_table: readyset_client::Table,
    ) -> ReadySetResult<()> {
        let span = info_span!("Copying table to Noria", %table.name);

        let mut vitess = VitessConnector::connect(
            self.vitess_config.clone(),
            Some(table),
            None,
            self.enable_statement_logging,
        )
        .instrument(span.clone())
        .await?;

        let mut snapshot = TableSnapshotBuffer::new(noria_table);

        while let Some(event) = vitess.next_event().instrument(span.clone()).await {
            match event.r#type() {
                vitess_grpc::binlogdata::VEventType::Vgtid => {
                    span.in_scope(|| debug!("Received Snapshot VStream VGTID"));
                    let vgtid = event.vgtid.as_ref().unwrap();
                    snapshot.current_position = Some(vgtid.try_into().map_err(|e| {
                        ReadySetError::ReplicationFailed(format!(
                            "Could not convert VGTID to VStream position: {}",
                            e
                        ))
                    })?);
                }

                vitess_grpc::binlogdata::VEventType::Begin => {
                    span.in_scope(|| debug!("Snapshot stream transaction begin"));
                }

                vitess_grpc::binlogdata::VEventType::Row => {
                    span.in_scope(|| debug!("Received a snapshot stream row event"));
                    let row_event = event.row_event.unwrap();

                    let keyspace = &row_event.keyspace;
                    let qualified_table_name = row_event.table_name.as_str();
                    let table_name = qualified_table_name.split('.').last().ok_or_else(|| {
                        ReadySetError::ReplicationFailed(format!(
                            "Could not extract table name from fully qualified name: {}",
                            qualified_table_name
                        ))
                    })?;

                    let table = vitess.lookup_table(table_name, keyspace)?;

                    for row_change in row_event.row_changes.iter() {
                        let inserted_data = row_change.after.as_ref().unwrap();
                        let row = vitess.row_change_to_noria_row(&table, &inserted_data)?;
                        snapshot.push_row(row).instrument(span.clone()).await?;
                    }
                }

                vitess_grpc::binlogdata::VEventType::Commit => {
                    span.in_scope(|| debug!("Received a snapshot stream commit"));
                    snapshot.flush().instrument(span.clone()).await?;
                }

                vitess_grpc::binlogdata::VEventType::CopyCompleted => {
                    span.in_scope(|| debug!("Received a snapshot stream copy completed"));
                    snapshot.finish().instrument(span.clone()).await?;
                    vitess.disconnect();
                    return Ok(());
                }

                _ => {
                    span.in_scope(|| {
                        warn!(
                            "Received unexpected event type while snapshotting {:?}",
                            event.r#type()
                        )
                    });
                }
            }
        }

        Err(ReadySetError::ReplicationFailed(format!(
            "Snapshot stream ended before copy completed"
        )))
    }

    async fn snapshot_schema_to_noria(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
    ) -> ReadySetResult<Vec<Relation>> {
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
                        Change::AddNonReplicatedRelation(NonReplicatedRelation {
                            name: Relation {
                                schema: Some(schema.into()),
                                name: name.into(),
                            },
                            reason: NotReplicatedReason::Configuration,
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
                            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                name: Relation {
                                    schema: Some(db.into()),
                                    name: table.into(),
                                },
                                reason: NotReplicatedReason::OtherError(format!(
                                    "Error extending CREATE TABLE: {}",
                                    error
                                )),
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
                            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                name: Relation {
                                    schema: Some(db.into()),
                                    name: view.into(),
                                },
                                reason: NotReplicatedReason::OtherError(format!(
                                    "Error extending CREATE VIEW: {}",
                                    error
                                )),
                            }),
                            Dialect::DEFAULT_MYSQL,
                        ))
                        .await?;
                }
            }
        }
        drop(tx);

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
        Ok(table_list)
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
