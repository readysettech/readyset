use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use std::future;
use std::time::Instant;

use futures::future::TryFutureExt;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use metrics::register_gauge;
use mysql::prelude::Queryable;
use mysql::{Transaction, TxOpts};
use mysql_async as mysql;
use nom_sql::Relation;
use readyset_client::metrics::recorded;
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::replication::{ReplicationOffset, ReplicationOffsets};
use readyset_client::ReadySetResult;
use readyset_data::Dialect;
use readyset_tracing::{debug, error, info, warn};
use tokio::task::JoinHandle;
use tracing::info_span;
use tracing_futures::Instrument;

use super::BinlogPosition;
use crate::db_util::DatabaseSchemas;
use crate::table_filter::TableFilter;

const BATCH_SIZE: usize = 1000; // How many queries to buffer before pushing to ReadySet

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

pub(crate) struct MySqlReplicator {
    /// This is the underlying (regular) MySQL connection
    pub(crate) pool: mysql::Pool,
    /// Filters out the desired tables to snapshot and replicate
    pub(crate) table_filter: TableFilter,
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
            Ok(r.ok_or("Empty response for SHOW CREATE VIEW")?.1)
        }
        TableKind::BaseTable => {
            // For SHOW CREATE TABLE format is the name of the table and the create DDL
            let r: Option<(String, String)> = q.query_first(query).await?;
            Ok(r.ok_or("Empty response for SHOW CREATE TABLE")?.1)
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

impl MySqlReplicator {
    /// Load all the `CREATE TABLE` statements for the tables in the MySQL database. Returns the the
    /// transaction that holds the DDL locks for the tables.
    ///
    /// If `install` is set to `true`, will also install the tables in ReadySet one-by-one, skipping
    /// over any tables that fail to install
    ///
    /// Returns a list of tables that should be dumped to readyset
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

        bad_tables
            .into_iter()
            .for_each(|(db, table)| self.table_filter.deny_replication(&db, &table));

        // We process all views, regardless of their schemas and the table filter, since a view can
        // exist that only selects from tables in other schemas.
        let all_views = get_table_list(&mut tx, TableKind::View).await?;

        // Process `CREATE VIEW` statements
        for (db, view) in all_views.iter() {
            let create_view = create_for_table(&mut tx, db, view, TableKind::View).await?;
            db_schemas.extend_create_schema_for_view(
                db.to_string(),
                view.to_string(),
                create_view.clone(),
                nom_sql::Dialect::MySQL,
            );
            if let Err(err) =
                future::ready(ChangeList::from_str(create_view, Dialect::DEFAULT_MYSQL))
                    .and_then(|changelist| async {
                        noria
                            .extend_recipe_no_leader_ready(
                                changelist.with_schema_search_path(vec![db.clone().into()]),
                            )
                            .await
                    })
                    .await
            {
                warn!(%view, %err, "Error extending CREATE VIEW, view will not be used");
            }
        }
        // Get the current binlog position, since at this point the tables are not locked, binlog
        // will advance while we are taking the snapshot. This is fine, we will catch up later.
        // We prefer to take the binlog position *after* the recipe is loaded in order to make sure
        // no ddl changes took place between the binlog position and the schema that we loaded
        let binlog_position = self.get_binlog_position().await?;

        noria
            .set_schema_replication_offset(Some(&binlog_position.try_into()?))
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

    /// Call `SELECT * FROM table` and convert all rows into a ReadySet row
    /// it may seem inefficient but apparently that is the correct way to
    /// replicate a table, and `mysqldump` and `debezium` do just that
    pub(crate) async fn dump_table(&self, table: &Relation) -> mysql::Result<TableDumper> {
        let mut tx = self
            .pool
            .start_transaction(tx_opts())
            .await
            .map_err(log_err)?;

        let _ = tx
            .query_drop("SET SESSION MAX_EXECUTION_TIME=0")
            .await
            .map_err(log_err);

        let query_count = format!("select count(*) from {table}");
        let query = format!("select * from {table}");
        Ok(TableDumper {
            query_count,
            query,
            tx,
        })
    }

    /// Use the SHOW MASTER STATUS statement to determine the current binary log
    /// file name and position.
    async fn get_binlog_position(&self) -> mysql::Result<BinlogPosition> {
        let mut conn = self.pool.get_conn().await?;
        let query = "SHOW MASTER STATUS";
        let pos: mysql::Row = conn.query_first(query).await?.ok_or(
            "Empty response for SHOW MASTER STATUS. \
             Ensure the binlog_format parameter is set to ROW and, if using RDS, backup retention \
             is greater than 0",
        )?;

        let file: String = pos.get(0).expect("Binlog file name");
        let offset: u32 = pos.get(1).expect("Binlog offset");

        Ok(BinlogPosition {
            binlog_file: file,
            position: offset,
        })
    }

    /// Issue a `LOCK TABLES tbl_name READ` for the table name provided
    async fn lock_table(&self, table: &Relation) -> mysql::Result<mysql::Conn> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("LOCK TABLES {table} READ");
        conn.query_drop(query).await?;
        Ok(conn)
    }

    /// Replicate a single table from the provided TableDumper and into ReadySet by
    /// converting every MySQL row into ReadySet row and calling `insert_many` in batches
    async fn replicate_table(
        mut dumper: TableDumper,
        mut table_mutator: readyset_client::Table,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;

        // Query for number of rows first
        let nrows: usize = dumper
            .tx
            .query_first(&dumper.query_count)
            .await
            .map_err(log_err)?
            .unwrap_or(0);

        let mut row_stream = dumper.stream().await.map_err(log_err)?;
        let mut rows = Vec::with_capacity(BATCH_SIZE);

        info!(rows = %nrows, "Replication started");

        table_mutator.set_snapshot_mode(true).await?;
        let progress_percentage_metric: metrics::Gauge = register_gauge!(
            recorded::REPLICATOR_SNAPSHOT_PERCENT,
            "name" => table_mutator.table_name().to_string(),
        );

        let start_time = Instant::now();
        let mut last_report_time = start_time;
        let snapshot_report_interval_secs = snapshot_report_interval_secs as u64;

        loop {
            let row = match row_stream.next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(err) if cnt == nrows => {
                    info!(error = %err, "Error encountered during snapshot, but all rows replicated succesfully");
                    break;
                }
                Err(err) => {
                    progress_percentage_metric.set(0.0);
                    return Err(err).map_err(log_err);
                }
            };

            rows.push(row);
            cnt += 1;

            if rows.len() == BATCH_SIZE {
                // We aggregate rows into batches and then send them all to noria
                let send_rows = std::mem::replace(&mut rows, Vec::with_capacity(BATCH_SIZE));
                table_mutator.insert_many(send_rows).await.map_err(|err| {
                    progress_percentage_metric.set(0.0);
                    log_err(err)
                })?;
            }

            if snapshot_report_interval_secs != 0
                && last_report_time.elapsed().as_secs() > snapshot_report_interval_secs
            {
                last_report_time = Instant::now();
                let estimate =
                    crate::estimate_remaining_time(start_time.elapsed(), cnt as f64, nrows as f64);
                let progress_percent = (cnt as f64 / nrows as f64) * 100.;
                let progress = format!("{:.2}%", progress_percent);
                info!(rows_replicated = %cnt, %progress, %estimate, "Snapshotting progress");
                progress_percentage_metric.set(progress_percent);
            }
        }

        if !rows.is_empty() {
            table_mutator.insert_many(rows).await.map_err(|err| {
                progress_percentage_metric.set(0.0);
                log_err(err)
            })?;
        }

        info!(rows_replicated = %cnt, "Replication finished");
        progress_percentage_metric.set(100.0);

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
    pub(crate) async fn snapshot_to_noria(
        mut self,
        noria: &mut readyset_client::ReadySetHandle,
        db_schemas: &mut DatabaseSchemas,
        snapshot_report_interval_secs: u16,
    ) -> ReadySetResult<()> {
        let result = self
            .replicate_to_noria_with_table_locks(noria, db_schemas, snapshot_report_interval_secs)
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
                    warn!(%err, "Failed to aquire instance lock, DDL changes may cause inconsistency");
                    None
                }
            }
        };

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
        let span = info_span!("Snapshotting table", %table);
        span.in_scope(|| info!("Acquiring read lock"));
        let mut read_lock = self.lock_table(&table).await?;
        // We acquire the position for each table individually, since it changes from
        // one lock to the other
        let repl_offset = ReplicationOffset::try_from(self.get_binlog_position().await?)?;
        span.in_scope(|| info!("Snapshotting table"));

        let dumper = self.dump_table(&table).instrument(span.clone()).await?;

        // At this point we have a transaction that will see *that* table at *this* binlog
        // position, so we can drop the read lock
        read_lock.query_drop("UNLOCK TABLES").await?;
        span.in_scope(|| info!("Read lock released"));

        let table_mutator = noria.table(table.clone()).instrument(span.clone()).await?;

        Ok(tokio::spawn(async move {
            (
                table,
                repl_offset,
                Self::replicate_table(dumper, table_mutator, snapshot_report_interval_secs)
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
                info!(%table, "Replication offset already exists for table, skipping snapshot");
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
                        let span = info_span!("Compacting table", %table);
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
                    error!(%table, error = %err, "Replication failed, retrying");
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
                    info!(%table, "Replication offset already exists for table, skipping snapshot");
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
}

/// An intermediary struct that can be used to get a stream of ReadySet rows
// This is required because mysql::QueryResult borrows from conn and then
// we have some hard to solve borrowing issues
pub(crate) struct TableDumper {
    query_count: String,
    query: String,
    tx: mysql::Transaction<'static>,
}

impl TableDumper {
    pub(crate) async fn stream(&mut self) -> mysql::Result<TableStream<'_>> {
        Ok(TableStream {
            query: self.tx.exec_iter(&self.query, ()).await?,
        })
    }
}

// Just another helper struct to make it streamable
pub(crate) struct TableStream<'a> {
    query: mysql::QueryResult<'a, 'static, mysql::BinaryProtocol>,
}

impl<'a> TableStream<'a> {
    /// Get the next row from the query response
    pub(crate) async fn next<'b>(
        &'b mut self,
    ) -> ReadySetResult<Option<Vec<readyset_data::DfValue>>> {
        let next_row = self.query.next().await?;
        next_row.map(mysql_row_to_noria_row).transpose()
    }
}

/// Convert each entry in a row to a ReadySet type that can be inserted into the base tables
fn mysql_row_to_noria_row(row: mysql::Row) -> ReadySetResult<Vec<readyset_data::DfValue>> {
    let mut noria_row = Vec::with_capacity(row.len());
    for idx in 0..row.len() {
        let val = value_to_value(row.as_ref(idx).unwrap());
        noria_row.push(readyset_data::DfValue::try_from(val)?);
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
