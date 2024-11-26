use core::str;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};
use std::future;
use std::sync::Arc;
use std::time::Instant;

use futures::future::TryFutureExt;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::{Transaction, TxOpts};
use mysql_async as mysql;
use mysql_common::constants::ColumnType;
use mysql_srv::ColumnFlags;
use nom_sql::{DialectDisplay, NonReplicatedRelation, NotReplicatedReason, Relation};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_data::{DfValue, Dialect};
use readyset_errors::{internal_err, ReadySetResult};
use replication_offset::mysql::MySqlPosition;
use replication_offset::{ReplicationOffset, ReplicationOffsets};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;

use super::utils::{get_mysql_version, mysql_pad_collation_column};
use crate::db_util::DatabaseSchemas;
use crate::mysql_connector::snapshot_type::SnapshotType;
use crate::mysql_connector::utils::MYSQL_BATCH_SIZE;
use crate::table_filter::TableFilter;
use crate::TablesSnapshottingGaugeGuard;
use std::collections::HashSet;

const RS_BATCH_SIZE: usize = 1000; // How many queries to buffer before pushing to ReadySet

const MAX_SNAPSHOT_BATCH: usize = 8; // How many tables to snapshot at the same time

/// A list of databases MySQL uses internally, they should not be replicated
pub const MYSQL_INTERNAL_DBS: &[&str] =
    &["mysql", "information_schema", "performance_schema", "sys"];

/// Pass the error forward while logging it
fn log_err<E: Error>(err: E) -> E {
    error!(error = %err);
    err
}

#[derive(Copy, Clone, Debug)]
pub enum TableKind {
    BaseTable,
    View,
}

pub(crate) struct MySqlReplicator<'a> {
    /// This is the underlying (regular) MySQL connection
    pub(crate) pool: mysql::Pool,
    /// Filters out the desired tables to snapshot and replicate
    pub(crate) table_filter: &'a mut TableFilter,
}

/// Get the list of tables defined in the database
pub async fn load_table_list<Q: Queryable>(
    q: &mut Q,
    kind: TableKind,
    db: &str,
) -> mysql::Result<Vec<String>> {
    let query = format!("show full tables in `{db}` where Table_Type={kind}");
    let tables_entries: Vec<(String, String)> = q.query_map(query, std::convert::identity).await?;
    Ok(tables_entries.into_iter().map(|(name, _)| name).collect())
}

/// Get the list of tables defined in the database for all (non-internal) schemas
async fn get_table_list<Q: Queryable>(
    q: &mut Q,
    kind: TableKind,
) -> mysql::Result<Vec<(String, String)>> {
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
) -> mysql::Result<String> {
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
    let mut tx_opts = mysql::TxOpts::default();
    tx_opts
        .with_isolation_level(mysql::IsolationLevel::RepeatableRead)
        .with_consistent_snapshot(true)
        .with_readonly(true);
    tx_opts
}

impl<'a> MySqlReplicator<'a> {
    /// Load all the `CREATE TABLE` statements for the tables in the MySQL database. Returns the the
    /// transaction that holds the DDL locks for the tables and the Vector of tables that requires
    /// snapshot.
    ///
    /// # Arguments
    ///
    /// * `noria` - The ReadySet handle
    /// * `db_schemas` - The database schemas
    async fn load_recipe_with_meta_lock(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
    ) -> ReadySetResult<(Transaction<'static>, Vec<Relation>)> {
        let mut tx = self.pool.start_transaction(tx_opts()).await?;

        let _ = tx
            .query_drop("SET SESSION MAX_EXECUTION_TIME=0")
            .await
            .map_err(log_err);

        // After we loaded the list of currently existing tables, we will acquire a metadata
        // lock on all of them to prevent DDL changes.
        // This is easy to do: simply use the tables from within a transaction:
        // https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
        // >> To ensure transaction serializability, the server must not permit one session to
        // >> perform a data definition language (DDL) statement on a table that is used in
        // >> an uncompleted explicitly or implicitly started transaction in another session.
        // >> The server achieves this by acquiring metadata locks on tables used within a
        // >> transaction and deferring release of those locks until the transaction ends. A
        // >> metadata lock on a table prevents changes to the table's structure.
        // >> This locking approach has the implication that a table that is being used by a
        // >> transaction within one session cannot be used in DDL statements by other sessions
        // >> until the transaction ends. This principle applies not only to transactional tables,
        // >> but also to nontransactional tables.
        let all_tables = get_table_list(&mut tx, TableKind::BaseTable).await?;
        let (replicated_tables, non_replicated_tables) = all_tables
            .into_iter()
            .partition::<Vec<_>, _>(|(schema, table)| {
                self.table_filter
                    .should_be_processed(schema.as_str(), table.as_str())
            });
        self.drop_leftover_tables(noria, &replicated_tables).await?;
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

        let all_tables_formatted = replicated_tables
            .iter()
            .map(|(db, tbl)| format!("`{db}`.`{tbl}`"))
            .collect::<Vec<_>>();

        for tables in all_tables_formatted.chunks(20) {
            // There is a default limit of 61 tables per join, so we chunk into smaller joins just
            // in case
            let metalock = format!("SELECT 1 FROM {} LIMIT 0", tables.iter().join(","));
            tx.query_drop(metalock).await?;
        }

        let mut bad_tables = Vec::new();
        // Process `CREATE TABLE` statements
        for (db, table) in replicated_tables.iter() {
            let res = create_for_table(&mut tx, db, table, TableKind::BaseTable)
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
                .await;

            match res {
                Ok(_) => {}
                Err(error) => {
                    warn!(%error, "Error extending CREATE TABLE {:?}.{:?}, table will not be used", db, table);
                    // Prevent the table from being snapshotted as well
                    bad_tables.push((db.clone(), table.clone()));

                    noria
                        .extend_recipe_no_leader_ready(ChangeList::from_change(
                            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                                name: Relation {
                                    schema: Some(db.into()),
                                    name: table.into(),
                                },
                                reason: NotReplicatedReason::Configuration,
                            }),
                            Dialect::DEFAULT_MYSQL,
                        ))
                        .await?;
                }
            }
        }

        bad_tables
            .into_iter()
            .for_each(|(db, table)| self.table_filter.deny_replication(&db, &table));

        // We process all views, regardless of their schemas and the table filter, since a view can
        // exist that only selects from tables in other schemas.
        let all_views = get_table_list(&mut tx, TableKind::View).await?;

        // Process `CREATE VIEW` statements
        for (db, view) in all_views.iter() {
            let res = create_for_table(&mut tx, db, view, TableKind::View)
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
                .await;

            match res {
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
                                reason: NotReplicatedReason::Configuration,
                            }),
                            Dialect::DEFAULT_MYSQL,
                        ))
                        .await?;
                }
            }
        }

        // Get the current binlog position, since at this point the tables are not locked, binlog
        // will advance while we are taking the snapshot. This is fine, we will catch up later.
        // We prefer to take the binlog position *after* the recipe is loaded in order to make sure
        // no ddl changes took place between the binlog position and the schema that we loaded
        let binlog_position = self.get_binlog_position().await?;

        noria
            .set_schema_replication_offset(Some(&binlog_position.into()))
            .await?;

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
        Ok((tx, table_list))
    }

    /// Get one transaction that will be used to snapshot table data
    pub(crate) async fn get_one_transaction(&self) -> mysql::Result<Transaction<'static>> {
        let mut tx = self
            .pool
            .start_transaction(tx_opts())
            .await
            .map_err(log_err)?;

        let _ = tx
            .query_drop("SET SESSION MAX_EXECUTION_TIME=0")
            .await
            .map_err(log_err);

        // Set the timezone to UTC
        let _ = tx
            .query_drop("SET SESSION time_zone = '+00:00';")
            .await
            .map_err(log_err);
        Ok(tx)
    }

    /// Use the SHOW MASTER STATUS or SHOW BINARY LOG STATUS statement to determine
    /// the current binary log file name and position.
    async fn get_binlog_position(&self) -> mysql::Result<MySqlPosition> {
        let mut conn = self.pool.get_conn().await?;
        let query = match get_mysql_version(&mut conn).await {
            Ok(version) => {
                if version >= 80400 {
                    // MySQL 8.4.0 and above
                    "SHOW BINARY LOG STATUS"
                } else {
                    // MySQL 8.3.0 and below
                    "SHOW MASTER STATUS"
                }
            }
            Err(err) => {
                return Err(err);
            }
        };

        let pos: mysql::Row = conn.query_first(query).await?.ok_or_else(|| {
            mysql_async::Error::Other(Box::new(internal_err!(
                "Empty response for SHOW MASTER STATUS. \
                 Ensure the binlog_format parameter is set to ROW and, if using RDS, backup \
                 retention is greater than 0"
            )))
        })?;

        let file: String = pos.get(0).expect("Binlog file name");
        let offset: u64 = pos.get(1).expect("Binlog offset");

        MySqlPosition::from_file_name_and_position(file, offset)
            .map_err(|err| mysql_async::Error::Other(Box::new(err)))
    }

    /// Issue a `LOCK TABLES tbl_name READ` for the table name provided
    async fn lock_table(&self, table: &Relation) -> mysql::Result<mysql::Conn> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!(
            "LOCK TABLES {} READ",
            table.display(nom_sql::Dialect::MySQL)
        );
        conn.query_drop(query).await?;
        Ok(conn)
    }

    /// Copy all the rows from the provided table into ReadySet by
    /// converting every MySQL row into ReadySet row and calling `insert_many` in batches.
    /// Depending on the table schema, we may use a key-based snapshotting strategy.
    /// Which consists of batching rows based on the primary key or unique key of the table.
    /// If the table does not have a primary key or unique key, we will use a full table scan.
    ///
    /// # Arguments
    /// * `trx` - The transaction to use for snapshotting. This transaction was opened while the
    ///   table were locked, meaning it will see the same data as the binlog position recorded for
    ///   this table.
    /// * `table_mutator` - The table mutator to insert the rows into
    async fn snapshot_table(
        mut trx: Transaction<'static>,
        mut table_mutator: readyset_client::Table,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;

        let mut snapshot_type = SnapshotType::new(&table_mutator)?;
        let (count_query, initial_query, bound_base_query) =
            snapshot_type.get_queries(&table_mutator);
        // Query for number of rows first
        let nrows: usize = trx
            .query_first(count_query)
            .await
            .map_err(log_err)?
            .unwrap_or(0);

        let mut row_stream = trx
            .exec_iter(&initial_query, mysql::Params::Empty)
            .await
            .map_err(log_err)?;
        let mut rows = Vec::with_capacity(RS_BATCH_SIZE);

        info!(rows = %nrows, "Snapshotting started");

        table_mutator.set_snapshot_mode(true).await?;
        let _tables_snapshotting_metric_handle = TablesSnapshottingGaugeGuard::new();

        let start_time = Instant::now();
        let mut last_report_time = start_time;
        let snapshot_report_interval_secs = snapshot_report_interval_secs as u64;

        // Loop until we have no more batches to process
        while cnt != nrows {
            // Still have rows in this batch
            loop {
                let row = row_stream.next().await.map_err(log_err)?;
                let df_row = match row.as_ref().map(mysql_row_to_noria_row).transpose() {
                    Ok(Some(df_row)) => df_row,
                    Ok(None) => break,
                    Err(err) if cnt == nrows => {
                        info!(error = %err, "Error encountered during snapshot, but all rows replicated successfully");
                        break;
                    }
                    Err(err) => {
                        return Err(log_err(err));
                    }
                };
                rows.push(df_row);
                cnt += 1;

                if rows.len() == RS_BATCH_SIZE {
                    // We aggregate rows into batches and then send them all to noria
                    let send_rows = std::mem::replace(&mut rows, Vec::with_capacity(RS_BATCH_SIZE));
                    table_mutator
                        .insert_many(send_rows)
                        .await
                        .map_err(log_err)?;
                }

                if cnt % MYSQL_BATCH_SIZE == 0 && cnt != nrows && snapshot_type.is_key_based() {
                    // Last row from batch. Update lower bound with last row.
                    // It's safe to unwrap here because we will break out of the loop at
                    // mysql_row_to_noria_row if row was None.
                    snapshot_type.set_lower_bound(row.as_ref().unwrap());
                }

                if snapshot_report_interval_secs != 0
                    && last_report_time.elapsed().as_secs() > snapshot_report_interval_secs
                {
                    last_report_time = Instant::now();
                    crate::log_snapshot_progress(start_time.elapsed(), cnt as i64, nrows as i64);
                }
            }
            if cnt != nrows {
                // Next batch
                row_stream = trx
                    .exec_iter(
                        &bound_base_query,
                        mysql::Params::Positional(snapshot_type.get_lower_bound()?),
                    )
                    .await
                    .map_err(log_err)?;
                if row_stream.is_empty() {
                    return Err(internal_err!(
                        "Snapshotting for table {:?} stopped before all rows were replicated. Next batch query returned no rows.",
                        table_mutator.table_name()
                    ));
                }
            }
        }

        if !rows.is_empty() {
            table_mutator.insert_many(rows).await.map_err(log_err)?;
        }

        info!(rows_replicated = %cnt, "Snapshotting finished");

        Ok(())
    }

    /// This function replicates an entire MySQL database into a clean
    /// ReadySet deployment.
    ///
    /// # Arguments
    ///
    /// * `noria`: The target ReadySet deployment
    /// * `replication_offsets`: The set of replication offsets for already-snapshotted tables and
    ///   the schema
    /// * `extend_recipe`: Replicate and install the recipe (`CREATE TABLE` ...; `CREATE VIEW` ...;)
    ///   in addition to the rows
    ///
    /// If `full_snapshot` is set to `true`, *all* tables will be snapshotted, even those that
    /// already have replication offsets in ReadySet.
    pub(crate) async fn snapshot_to_noria(
        mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
        snapshot_report_interval_secs: u16,
        full_snapshot: bool,
        _max_parallel_snapshot_tables: usize, // TODO: limit parallelism for MySQL
    ) -> ReadySetResult<()> {
        let result = self
            .replicate_to_noria_with_table_locks(
                noria,
                db_schemas,
                snapshot_report_interval_secs,
                full_snapshot,
            )
            .await;

        // Wait for all connections to finish, not strictly necessary
        self.pool.disconnect().await?;
        result
    }

    /// This is a fallback method to obtaining a database lock, that obtains table level locks
    /// instead of a global lock. The only difference between that and obtaining a global lock
    /// is that some `CREATE TABLE` or `CREATE VIEW` statements may be missed if they happen to
    /// execute at the narrow timeframe of us reading the tables list, and us reading the binlog
    /// offset.
    async fn replicate_to_noria_with_table_locks(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
        snapshot_report_interval_secs: u16,
        full_snapshot: bool,
    ) -> ReadySetResult<()> {
        // NOTE: There are two ways to prevent DDL changes in MySQL:
        // `FLUSH TABLES WITH READ LOCK` or `LOCK INSTANCE FOR BACKUP`. Both are not
        // possible in RDS however.

        // It would be really good if we could prevent DDL changes during snapshotting,
        // but in the common case we are running on AWS RDS, and it is simply not allowed
        // to the RDS superuser. We will however lock DDL for the existing tables, so they
        // can't change during replication. If new tables are created during replication
        // we will miss their creation.

        // Attempt to lock the instance for DDL changes anyway, if it fails we will still
        // lock the metadata for the replicated tables, however if new `CREATE TABLE`
        // statements are issued between the time when we collect the existing table list
        // and get the binlog position, we will not be able to detect them.
        let _instance_lock = {
            let mut conn = self.pool.get_conn().await?;
            match conn.query_drop("LOCK INSTANCE FOR BACKUP").await {
                Ok(_) => Some(conn),
                Err(err) => {
                    warn!(%err, "Failed to acquire instance lock, if new tables are created, we might not detect them.");
                    None
                }
            }
        };

        if full_snapshot {
            self.drop_all_tables(noria).await?;
        }

        let (_meta_lock, table_list) = self
            .load_recipe_with_meta_lock(noria, db_schemas)
            .await
            .map_err(log_err)?;

        // Replication offsets could change following a schema update, so get a new list
        let replication_offsets = noria.replication_offsets().await?;

        self.dump_tables(
            noria,
            table_list,
            &replication_offsets,
            snapshot_report_interval_secs,
        )
        .await
    }

    /// Spawns a new tokio task that replicates a given table to noria, returning
    /// the join handle
    async fn dumper_task_for_table(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        table: Relation,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<JoinHandle<(Relation, ReplicationOffset, ReadySetResult<()>)>> {
        let span = info_span!(
            "Snapshotting table",
            table = %table.display(nom_sql::Dialect::MySQL)
        );
        span.in_scope(|| info!("Acquiring read lock"));
        let mut read_lock = self.lock_table(&table).await?;
        // We acquire the position for each table individually, since it changes from
        // one lock to the other
        let repl_offset = ReplicationOffset::from(self.get_binlog_position().await?);
        span.in_scope(|| info!("Snapshotting table"));

        let trx = self.get_one_transaction().instrument(span.clone()).await?;

        // At this point we have a transaction that will see *that* table at *this* binlog
        // position, so we can drop the read lock
        read_lock.query_drop("UNLOCK TABLES").await?;
        span.in_scope(|| info!("Read lock released"));

        let table_mutator = noria.table(table.clone()).instrument(span.clone()).await?;

        Ok(tokio::spawn(async move {
            (
                table,
                repl_offset,
                Self::snapshot_table(trx, table_mutator, snapshot_report_interval_secs)
                    .instrument(span)
                    .await,
            )
        }))
    }

    /// Copy all base tables into noria
    async fn dump_tables(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        mut table_list: Vec<Relation>,
        replication_offsets: &ReplicationOffsets,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let mut replication_tasks = FuturesUnordered::new();
        let mut compacting_tasks = FuturesUnordered::new();

        // For each table we spawn a new task to parallelize the replication process, with a limit
        // We pop front because we add the tables before the views, and the views depend on the
        // tables. TODO: do we need to fully finish tables before views?
        while let Some(table) = table_list.pop() {
            if replication_offsets.has_table(&table) {
                info!(
                    table = %table.display(nom_sql::Dialect::MySQL),
                    "Replication offset already exists for table, skipping snapshot"
                );
            } else {
                replication_tasks.push(
                    self.dumper_task_for_table(noria, table, snapshot_report_interval_secs)
                        .await?,
                );
            }

            if replication_tasks.len() == MAX_SNAPSHOT_BATCH {
                break;
            }
        }

        while let Some(task_result) = replication_tasks.next().await {
            // The unwrap is for the join handle in that case
            match task_result.unwrap() {
                (table, repl_offset, Ok(())) => {
                    let mut noria_table = noria.table(table.clone()).await?;
                    compacting_tasks.push(tokio::spawn(async move {
                        let span = info_span!(
                            "Compacting table",
                            table = %table.display(nom_sql::Dialect::MySQL)
                        );
                        span.in_scope(|| info!("Setting replication offset"));
                        noria_table
                            .set_replication_offset(repl_offset)
                            .map_err(log_err)
                            .instrument(span.clone())
                            .await?;

                        span.in_scope(|| info!("Set replication offset, compacting table"));
                        noria_table
                            .set_snapshot_mode(false)
                            .map_err(log_err)
                            .instrument(span.clone())
                            .await?;

                        span.in_scope(|| info!("Compacting finished"));
                        ReadySetResult::Ok(())
                    }));
                }
                (table, _, Err(err)) => {
                    error!(
                        table = %table.display(nom_sql::Dialect::MySQL),
                        error = %err,
                        "Replication failed, retrying"
                    );
                    replication_tasks.push(
                        self.dumper_task_for_table(noria, table, snapshot_report_interval_secs)
                            .await?,
                    );
                }
            }

            // If still have tables to snapshot add them to the task list
            while replication_tasks.len() < MAX_SNAPSHOT_BATCH && !table_list.is_empty() {
                let table = table_list.pop().expect("Not empty");
                if replication_offsets.has_table(&table) {
                    info!(
                        table = %table.display(nom_sql::Dialect::MySQL),
                        "Replication offset already exists for table, skipping snapshot"
                    );
                } else {
                    replication_tasks.push(
                        self.dumper_task_for_table(noria, table, snapshot_report_interval_secs)
                            .await?,
                    );
                }
            }
        }

        while let Some(compaction_result) = compacting_tasks.next().await {
            // The unwrap is for the join handle
            compaction_result.unwrap()?;
        }

        Ok(())
    }

    /// This function drops all known tables from Noria. This is used when we require a full
    /// re-snapshot of the database.
    ///
    /// # Arguments
    /// * `noria`: The target ReadySet deployment
    async fn drop_all_tables(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
    ) -> ReadySetResult<()> {
        let mut changes = Vec::new();
        for (table, _) in noria.tables().await? {
            changes.push(Change::Drop {
                name: table.clone(),
                if_exists: true,
            });
        }
        noria
            .extend_recipe_no_leader_ready(ChangeList::from_changes(
                changes,
                Dialect::DEFAULT_MYSQL,
            ))
            .await?;
        Ok(())
    }

    /// This functions drops all tables that we have internally and have not been selected as
    /// part of snapshot, normally because table filters have been changed.
    ///
    /// # Arguments
    ///
    /// * `noria`: The target ReadySet deployment
    /// * `snapshot_tables`: The list of tables that are part of the snapshot
    async fn drop_leftover_tables(
        &mut self,
        noria: &mut readyset_client::ReadySetHandle,
        snapshot_tables: &[(String, String)],
    ) -> ReadySetResult<()> {
        let mut changes = Vec::new();
        let current_tables = noria.tables().await?;
        let snapshot_tables_set: HashSet<Relation> = snapshot_tables
            .iter()
            .map(|(schema, name)| Relation {
                schema: Some(schema.clone().into()),
                name: name.clone().into(),
            })
            .collect();

        for table in current_tables.keys() {
            if !snapshot_tables_set.contains(table) {
                warn!(table = %table.display(nom_sql::Dialect::MySQL), "Dropping table that is not part of snapshot");
                changes.push(Change::Drop {
                    name: table.clone(),
                    if_exists: false,
                });
            }
        }
        if !changes.is_empty() {
            noria
                .extend_recipe_no_leader_ready(ChangeList::from_changes(
                    changes,
                    Dialect::DEFAULT_MYSQL,
                ))
                .await?;
        }
        Ok(())
    }
}

/// Convert each entry in a row to a ReadySet type that can be inserted into the base tables
fn mysql_row_to_noria_row(row: &mysql::Row) -> ReadySetResult<Vec<readyset_data::DfValue>> {
    let mut noria_row = Vec::with_capacity(row.len());
    for idx in 0..row.len() {
        let val = value_to_value(row.as_ref(idx).unwrap());
        let col = row.columns_ref().get(idx).unwrap();
        let flags = col.flags();
        match col.column_type() {
            ColumnType::MYSQL_TYPE_STRING => {
                // ENUM and SET columns are stored as integers and retrieved as strings. We don't
                // need padding.
                let require_padding = val != mysql_common::value::Value::NULL
                    && !flags.contains(ColumnFlags::ENUM_FLAG)
                    && !flags.contains(ColumnFlags::SET_FLAG);
                match require_padding {
                    true => {
                        let bytes = match val.clone() {
                            mysql_common::value::Value::Bytes(b) => b,
                            _ => {
                                return Err(internal_err!(
                                    "Expected MYSQL_TYPE_STRING column to be of value Bytes, got {:?}",
                                    val
                                ));
                            }
                        };
                        match mysql_pad_collation_column(
                            &bytes,
                            col.column_type(),
                            col.character_set(),
                            col.column_length() as usize,
                        ) {
                            Ok(padded) => noria_row.push(padded),
                            Err(err) => return Err(internal_err!("Error padding column: {}", err)),
                        }
                    }
                    false => noria_row.push(readyset_data::DfValue::try_from(val)?),
                }
            }
            ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_TIMESTAMP2
            | ColumnType::MYSQL_TYPE_DATETIME
            | ColumnType::MYSQL_TYPE_DATETIME2 => {
                let df_val: DfValue = readyset_data::DfValue::try_from(val)
                    .map_err(|err| {
                        internal_err!("Error converting datetime/timestamp column: {}", err)
                    })
                    .and_then(|val| match val {
                        DfValue::TimestampTz(mut ts) => {
                            ts.set_subsecond_digits(col.decimals());
                            Ok(DfValue::TimestampTz(ts))
                        }
                        DfValue::None => Ok(DfValue::None), //NULL
                        _ => Err(internal_err!(
                            "Expected datetime/timestamp column to be of type TimestampTz, got {:?}",
                            val
                        )),
                    })?;
                noria_row.push(df_val);
            }
            ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
                let df_val: DfValue = readyset_data::DfValue::try_from(val)
                    .map_err(|err| internal_err!("Error converting date column: {}", err))
                    .and_then(|val| match val {
                        DfValue::TimestampTz(mut ts) => {
                            ts.set_date_only();
                            Ok(DfValue::TimestampTz(ts))
                        }
                        DfValue::None => Ok(DfValue::None), //NULL
                        _ => Err(internal_err!(
                            "Expected date column to be of type TimestampTz, got {:?}",
                            val
                        )),
                    })?;
                noria_row.push(df_val);
            }
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => match val {
                mysql_common::value::Value::Bytes(b) => {
                    let df_val = str::from_utf8(&b)
                        .ok()
                        .and_then(|s| Decimal::from_str_exact(s).ok())
                        .map(|d| DfValue::Numeric(Arc::new(d)))
                        .ok_or_else(|| internal_err!("Failed to parse decimal value"))?;
                    noria_row.push(df_val)
                }
                mysql_common::value::Value::NULL => noria_row.push(DfValue::None),
                _ => return Err(internal_err!("Expected a bytes value for decimal column")),
            },
            ColumnType::MYSQL_TYPE_JSON => {
                let df_val = match val {
                    mysql_common::value::Value::Bytes(b) => str::from_utf8(&b)
                        .ok()
                        .and_then(|s: &str| serde_json::from_str(s).ok())
                        .map(|j: Value| DfValue::from(j))
                        .ok_or_else(|| internal_err!("Failed to parse JSON value"))?,
                    mysql_common::value::Value::NULL => DfValue::None,
                    _ => {
                        return Err(internal_err!(
                            "Expected a bytes value for JSON column, got {:?}",
                            val
                        ));
                    }
                };
                noria_row.push(df_val);
            }
            _ => noria_row.push(readyset_data::DfValue::try_from(val)?),
        }
    }
    Ok(noria_row)
}

/// Although both are of the exact same type, there is a conflict between reexported versions
fn value_to_value(val: &mysql::Value) -> mysql_common::value::Value {
    match val {
        mysql::Value::NULL => mysql_common::value::Value::NULL,
        mysql::Value::Bytes(b) => mysql_common::value::Value::Bytes(b.clone()),
        mysql::Value::Int(i) => mysql_common::value::Value::Int(*i),
        mysql::Value::UInt(u) => mysql_common::value::Value::UInt(*u),
        mysql::Value::Float(f) => mysql_common::value::Value::Float(*f),
        mysql::Value::Double(d) => mysql_common::value::Value::Double(*d),
        mysql::Value::Date(y, m, d, hh, mm, ss, us) => {
            mysql_common::value::Value::Date(*y, *m, *d, *hh, *mm, *ss, *us)
        }
        mysql::Value::Time(is_neg, d, hh, mm, ss, us) => {
            mysql_common::value::Value::Time(*is_neg, *d, *hh, *mm, *ss, *us)
        }
    }
}

impl TableKind {
    pub fn kind(&self) -> &str {
        match self {
            TableKind::BaseTable => "TABLE",
            TableKind::View => "VIEW",
        }
    }
}

impl Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableKind::BaseTable => write!(f, "'BASE TABLE'"),
            TableKind::View => write!(f, "'VIEW'"),
        }
    }
}
