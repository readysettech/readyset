use futures::future::TryFutureExt;
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use mysql::{prelude::*, Transaction, TxOpts};
use mysql_async as mysql;
use noria::replication::{ReplicationOffset, ReplicationOffsets};
use noria::ReadySetResult;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;

use super::BinlogPosition;

const BATCH_SIZE: usize = 1000; // How many queries to buffer before pushing to Noria

/// Pass the error forward while logging it
fn log_err<E: Error>(err: E) -> E {
    error!(error = %err);
    err
}

enum TableKind {
    BaseTable,
    View,
}

pub struct MySqlReplicator {
    /// This is the underlying (regular) MySQL connection
    pub(crate) pool: mysql::Pool,
    /// If Some then only snapshot those tables, otherwise will snapshot all tables
    pub(crate) tables: Option<Vec<String>>,
}

/// Get the list of tables defined in the database
async fn load_table_list<Q: Queryable>(q: &mut Q, kind: TableKind) -> mysql::Result<Vec<String>> {
    let query = format!("show full tables where Table_Type={}", kind);
    let tables_entries: Vec<(String, String)> = q.query_map(query, std::convert::identity).await?;
    Ok(tables_entries.into_iter().map(|(name, _)| name).collect())
}

/// Get the `CREATE TABLE` or `CREATE VIEW` statement for the named table
async fn create_for_table<Q: Queryable>(
    q: &mut Q,
    table_name: &str,
    kind: TableKind,
) -> mysql::Result<String> {
    let query = format!("show create {} `{}`", kind.kind(), table_name);
    match kind {
        TableKind::View => {
            // For SHOW CREATE VIEW the format is the name of the view, the create DDL, character_set_client and collation_connection
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
    /// Load all the `CREATE TABLE` statements for the database and concatenate them
    /// into a single String that can be installed as a recipe in Noria. Return the stirng
    /// and the transaction that holds the DDL locks for the tables.
    pub async fn load_recipe_with_meta_lock(
        &mut self,
        noria: &mut noria::ControllerHandle,
        install: bool,
    ) -> ReadySetResult<Transaction<'static>> {
        let mut tx = self.pool.start_transaction(tx_opts()).await?;

        if self.tables.is_none() {
            self.tables = Some(load_table_list(&mut tx, TableKind::BaseTable).await?);
        }

        let tables = self.tables.clone().unwrap();

        // After we loaded the list of currently existing tables, we will acquire a metadata
        // lock on all of them to prevent DDL changes.
        // This is easy to do: simply use the tables from within a transaction:
        // https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html
        // >> To ensure transaction serializability, the server must not permit one session to perform
        // >> a data definition language (DDL) statement on a table that is used in an uncompleted explicitly
        // >> or implicitly started transaction in another session. The server achieves this by acquiring
        // >> metadata locks on tables used within a transaction and deferring release of those locks until
        // >> the transaction ends. A metadata lock on a table prevents changes to the table's structure.
        // >> This locking approach has the implication that a table that is being used by a transaction within
        // >> one session cannot be used in DDL statements by other sessions until the transaction ends.
        // >> This principle applies not only to transactional tables, but also to nontransactional tables.
        for tables in tables.chunks(20) {
            // There is a default limit of 61 tables per join, so we chunk into smaller joins just in case
            let metalock = format!("SELECT 1 FROM `{}` LIMIT 0", tables.iter().join("`,`"));
            tx.query_drop(metalock).await?;
        }

        if !install {
            info!("Not loading recipe as replication offset already exists for schema");
            return Ok(tx);
        }

        // Process `CREATE TABLE` statements
        for table in &tables {
            let create_table = create_for_table(&mut tx, table, TableKind::BaseTable).await?;
            debug!(%create_table, "Extending recipe");
            if let Err(err) = noria.extend_recipe_no_leader_ready(&create_table).await {
                self.tables.as_mut().unwrap().retain(|t| t != table); // Prevent the table from being snapshotted as well
                error!(%err, "Error extending CREATE TABLE, table will not be used");
            }
        }

        // Process `CREATE VIEW` statements
        for view in load_table_list(&mut tx, TableKind::View).await? {
            let create_view = create_for_table(&mut tx, &view, TableKind::View).await?;
            debug!(%create_view, "Extending recipe");

            if let Err(err) = noria.extend_recipe_no_leader_ready(&create_view).await {
                error!(%view, %err, "Error extending CREATE VIEW, view will not be used");
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

        Ok(tx)
    }

    /// Call `SELECT * FROM table` and convert all rows into a Noria row
    /// it may seem inefficient but apparently that is the correct way to
    /// replicate a table, and `mysqldump` and `debezium` do just that
    pub async fn dump_table(&self, table: &str) -> mysql::Result<TableDumper> {
        let tx = self
            .pool
            .start_transaction(tx_opts())
            .await
            .map_err(log_err)?;
        let query_count = format!("select count(*) from `{}`", table);
        let query = format!("select * from `{}`", table);
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
    async fn lock_table<N>(&self, name: N) -> mysql::Result<mysql::Conn>
    where
        N: Display,
    {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("LOCK TABLES `{}` READ", name);
        conn.query_drop(query).await.unwrap();
        Ok(conn)
    }

    /// Replicate a single table from the provided TableDumper and into Noria by
    /// converting every MySQL row into Noria row and calling `insert_many` in batches
    async fn replicate_table(
        mut dumper: TableDumper,
        mut table_mutator: noria::Table,
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

        loop {
            let row = match row_stream.next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(err) if cnt == nrows => {
                    info!(error = %err, "Error encountered during snapshot, but all rows replicated succesfully");
                    break;
                }
                Err(err) => return Err(err).map_err(log_err),
            };

            rows.push(row);
            cnt += 1;

            if rows.len() == BATCH_SIZE {
                // We aggregate rows into batches and then send them all to noria
                let send_rows = std::mem::replace(&mut rows, Vec::with_capacity(BATCH_SIZE));
                table_mutator
                    .insert_many(send_rows)
                    .await
                    .map_err(log_err)?;
            }

            if cnt % 1_000_000 == 0 && nrows > 0 {
                let progress = format!("{:.2}%", (cnt as f64 / nrows as f64) * 100.);
                info!(rows_replicated = %cnt, %progress, "Replication progress");
            }
        }

        if !rows.is_empty() {
            table_mutator.insert_many(rows).await.map_err(log_err)?;
        }

        info!(rows_replicated = %cnt, "Replication finished");

        Ok(())
    }

    /// This function replicates an entire MySQL database into a clean
    /// noria deployment.
    ///
    /// # Arguments
    ///
    /// * `noria`: The target Noria deployment
    /// * `replication_offsets`: The set of replication offsets for already-snapshotted tables and
    ///   the schema
    /// * `install_recipe`: Replicate and install the recipe (`CREATE TABLE` ...; `CREATE VIEW` ...;) in addition to the rows
    pub async fn snapshot_to_noria(
        mut self,
        noria: &mut noria::ControllerHandle,
        replication_offsets: &ReplicationOffsets,
        install_recipe: bool,
    ) -> ReadySetResult<()> {
        let result = self
            .replicate_to_noria_with_table_locks(noria, replication_offsets, install_recipe)
            .await;

        // Wait for all connections to finish, not strictly neccessary
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
        noria: &mut noria::ControllerHandle,
        replication_offsets: &ReplicationOffsets,
        install_recipe: bool,
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

        let _meta_lock = self
            .load_recipe_with_meta_lock(noria, install_recipe && !replication_offsets.has_schema())
            .await
            .map_err(log_err)?;

        self.dump_tables(noria, replication_offsets).await
    }

    /// Spawns a new tokio task that replicates a given table to noria, returning
    /// the join handle
    async fn dumper_task_for_table(
        &mut self,
        noria: &mut noria::ControllerHandle,
        table_name: String,
    ) -> ReadySetResult<JoinHandle<(String, ReplicationOffset, ReadySetResult<()>)>> {
        let span = info_span!("replicating table", table = %table_name);
        span.in_scope(|| info!("Acquiring read lock"));
        let mut read_lock = self.lock_table(&table_name).await?;
        // We acquire the position for each table individually, since it changes from
        // one lock to the other
        let repl_offset = ReplicationOffset::try_from(self.get_binlog_position().await?)?;
        span.in_scope(|| info!("Replicating table"));

        let dumper = self
            .dump_table(&table_name)
            .instrument(span.clone())
            .await?;

        // At this point we have a transaction that will see *that* table at *this* binlog
        // position, so we can drop the read lock
        read_lock.query_drop("UNLOCK TABLES").await?;
        span.in_scope(|| info!("Read lock released"));

        let table_mutator = noria.table(&table_name).instrument(span.clone()).await?;

        Ok(tokio::spawn(async {
            (
                table_name,
                repl_offset,
                Self::replicate_table(dumper, table_mutator)
                    .instrument(span)
                    .await,
            )
        }))
    }

    /// Copy all base tables into noria
    async fn dump_tables(
        &mut self,
        noria: &mut noria::ControllerHandle,
        replication_offsets: &ReplicationOffsets,
    ) -> ReadySetResult<()> {
        let mut replication_tasks = FuturesUnordered::new();
        let mut compacting_tasks = FuturesUnordered::new();

        // For each table we spawn a new task to parallelize the replication process
        for table_name in self.tables.clone().unwrap() {
            if replication_offsets.has_table(&table_name) {
                info!(%table_name, "Replication offset already exists for table, skipping snapshot");
            } else {
                replication_tasks.push(self.dumper_task_for_table(noria, table_name).await?);
            }
        }

        while let Some(task_result) = replication_tasks.next().await {
            // The unwrap is for the join handle in that case
            match task_result.unwrap() {
                (table_name, repl_offset, Ok(())) => {
                    let mut noria_table = noria.table(&table_name).await?;
                    compacting_tasks.push(tokio::spawn(async move {
                        let span = info_span!("Compacting table", table = %table_name);
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
                (table_name, _, Err(err)) => {
                    error!(table = %table_name, error = %err, "Replication failed, retrying");
                    replication_tasks.push(self.dumper_task_for_table(noria, table_name).await?);
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

/// An intermediary struct that can be used to get a stream of Noria rows
// This is required because mysql::QueryResult borrows from conn and then
// we have some hard to solve borrowing issues
pub struct TableDumper {
    query_count: String,
    query: String,
    tx: mysql::Transaction<'static>,
}

impl TableDumper {
    pub async fn stream(&mut self) -> mysql::Result<TableStream<'_>> {
        Ok(TableStream {
            query: self.tx.exec_iter(&self.query, ()).await?,
        })
    }
}

// Just another helper struct to make it streamable
pub struct TableStream<'a> {
    query: mysql::QueryResult<'a, 'static, mysql::BinaryProtocol>,
}

impl<'a> TableStream<'a> {
    /// Get the next row from the query response
    pub async fn next<'b>(&'b mut self) -> ReadySetResult<Option<Vec<noria_data::DataType>>> {
        let next_row = self.query.next().await?;
        next_row.map(mysql_row_to_noria_row).transpose()
    }
}

/// Convert each entry in a row to a Noria type that can be inserted into the base tables
fn mysql_row_to_noria_row(row: mysql::Row) -> ReadySetResult<Vec<noria_data::DataType>> {
    let mut noria_row = Vec::with_capacity(row.len());
    for idx in 0..row.len() {
        let val = value_to_value(row.as_ref(idx).unwrap());
        noria_row.push(noria_data::DataType::try_from(val)?);
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
    fn kind(&self) -> &str {
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
