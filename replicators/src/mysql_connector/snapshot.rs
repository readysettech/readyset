use futures::{stream::FuturesUnordered, StreamExt};
use mysql::prelude::*;
use mysql_async as mysql;
use noria::ReadySetResult;
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display};
use tracing::{debug, info, info_span};
use tracing_futures::Instrument;

use super::BinlogPosition;

const BATCH_SIZE: usize = 100; // How many queries to buffer before pushing to Noria

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

/// Issue a `LOCK TABLES tbl_name READ, ...` for the names of tables and views provided
async fn lock_tables<Q, N, I>(q: &mut Q, names: I) -> mysql::Result<()>
where
    Q: Queryable,
    N: Display,
    I: IntoIterator<Item = N>,
{
    let mut query = names.into_iter().fold("LOCK TABLES".to_string(), |q, t| {
        format!("{} {} READ,", q, t)
    });
    query.pop(); // Remove any trailing commas
    q.query_drop(query).await
}

/// Get the `CREATE TABLE` or `CREATE VIEW` statement for the named table
async fn create_for_table<Q: Queryable>(
    q: &mut Q,
    table_name: &str,
    kind: TableKind,
) -> mysql::Result<String> {
    let query = format!("show create {} {}", kind.kind(), table_name);
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

impl MySqlReplicator {
    /// Load all the `CREATE TABLE` statements for the database and concatenate them
    /// into a single String that can be installed as a recipe in Noria
    pub async fn load_recipe<Q: Queryable>(&mut self, q: &mut Q) -> mysql::Result<String> {
        if self.tables.is_none() {
            self.tables = Some(load_table_list(q, TableKind::BaseTable).await?);
        }

        let mut recipe = String::new();

        // Append `CREATE TABLE` statements
        for table in self.tables.as_ref().unwrap() {
            let create = create_for_table(q, table, TableKind::BaseTable).await?;
            recipe.push_str(&create);
            recipe.push_str(";\n");
        }

        // Append `CREATE VIEW` statements
        for view in load_table_list(q, TableKind::View).await? {
            let create = create_for_table(q, &view, TableKind::View).await?;
            recipe.push_str(&create);
            recipe.push_str(";\n");
        }

        Ok(recipe)
    }

    /// Call `SELECT * FROM table` and convert all rows into a Noria row
    /// it may seem inefficient but apparently that is the correct way to
    /// replicate a table, and `mysqldump` and `debezium` do just that
    pub async fn dump_table(&self, table: &str) -> mysql::Result<TableDumper> {
        let conn = self.pool.get_conn().await?;
        let query = format!("select * from {}", table);
        Ok(TableDumper { query, conn })
    }

    /// From MySQL docs:
    /// Start a session on the source by connecting to it with the command-line client,
    /// and flush all tables and block write statements by executing the
    /// FLUSH TABLES WITH READ LOCK statement
    /// Leave the client from which you issued the FLUSH TABLES statement running so that
    /// the read lock remains in effect. If you exit the client, the lock is released.
    /// (In our case we keep a reference to the connection alive)
    async fn flush_and_read_lock(&self) -> mysql::Result<mysql::Conn> {
        let mut conn = self.pool.get_conn().await?;
        let query = "FLUSH TABLES WITH READ LOCK";
        conn.query_drop(query).await?;
        Ok(conn)
    }

    /// From MySQL docs:
    /// In a different session on the source, use the SHOW MASTER STATUS
    /// statement to determine the current binary log file name and position.
    async fn get_binlog_position(&self) -> mysql::Result<BinlogPosition> {
        let mut conn = self.pool.get_conn().await?;
        let query = "SHOW MASTER STATUS";
        let pos: mysql::Row = conn
            .query_first(query)
            .await?
            .ok_or("Empty response for SHOW MASTER STATUS")?;

        let file: String = pos.get(0).expect("Binlog file name");
        let offset: u32 = pos.get(1).expect("Binlog offset");

        Ok(BinlogPosition {
            binlog_file: file,
            position: offset,
        })
    }

    /// Replicate a single table from the provided TableDumper and into Noria by
    /// converting every MySQL row into Noria row and calling `insert_many` in batches
    async fn replicate_table(
        mut dumper: TableDumper,
        mut table_mutator: noria::Table,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;
        let mut row_stream = dumper.stream().await?;
        let mut rows = Vec::with_capacity(BATCH_SIZE);

        info!("Replication started");

        while let Some(row) = row_stream.next().await? {
            rows.push(row);
            cnt += 1;

            if rows.len() == BATCH_SIZE {
                // We aggregate rows into batches and then send them all to noria
                let send_rows = std::mem::replace(&mut rows, Vec::with_capacity(BATCH_SIZE));
                table_mutator.insert_many(send_rows).await?;
            }
        }

        if !rows.is_empty() {
            table_mutator.insert_many(rows).await?;
        }

        info!(rows_replicated = %cnt, "Replication finished");

        Ok(())
    }

    /// This function replicates an entire MySQL database into a clean
    /// noria deployment.
    /// # Arguments
    /// * `noria`: The target Noria deployment
    /// * `install_recipe`: Replicate and install the recipe (`CREATE TABLE` ...; `CREATE VIEW` ...;) in addition to the rows
    pub async fn replicate_to_noria(
        mut self,
        noria: &mut noria::ControllerHandle,
        install_recipe: bool,
    ) -> ReadySetResult<BinlogPosition> {
        let result = match self
            .replicate_to_noria_with_global_lock(noria, install_recipe)
            .await
        {
            Err(err) if err.to_string().contains("Access denied for user") => {
                self.replicate_to_noria_with_table_locks(noria, install_recipe)
                    .await
            }
            result => result,
        };

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
        install_recipe: bool,
    ) -> ReadySetResult<BinlogPosition> {
        // Start a transaction with `REPEATABLE READ` isolation level
        let mut tx_opts = mysql::TxOpts::default();
        tx_opts.with_isolation_level(mysql::IsolationLevel::RepeatableRead);
        let mut tx = self.pool.start_transaction(tx_opts).await?;
        // Read the list of tables and views
        let tables = load_table_list(&mut tx, TableKind::BaseTable).await?;
        let views = load_table_list(&mut tx, TableKind::View).await?;
        if !tables.is_empty() || !views.is_empty() {
            lock_tables(&mut tx, tables.into_iter().chain(views)).await?;
        }
        debug!("Acquired table read locks");
        // Get current binlog position, since all table are locked no action can take place that would
        // advance the binlog *and* affect the tables
        let binlog_position = self.get_binlog_position().await?;

        // Even if we don't install the recipe, this will load the tables from the database
        let recipe = self.load_recipe(&mut tx).await?;
        debug!(%recipe, "Loaded recipe");

        if install_recipe {
            noria.install_recipe(&recipe).await?;
            debug!("Recipe installed");
        }

        // Although the table dumping happens on a connection pool, and not within our transaction,
        // it doesn't matter because we maintain a read lock on all the tables anyway
        self.dump_tables(noria).await?;
        noria
            .set_replication_offset(Some((&binlog_position).try_into()?))
            .await?;

        Ok(binlog_position)
    }

    /// This method aquires a global read lock using `FLUSH TABLES WITH READ LOCK`, which is
    /// the recommended MySQL method of obtaining a snapshot, however it is not available on
    /// Amazon RDS.
    async fn replicate_to_noria_with_global_lock(
        &mut self,
        noria: &mut noria::ControllerHandle,
        install_recipe: bool,
    ) -> ReadySetResult<BinlogPosition> {
        // We must hold the locking connection open until replication is finished,
        // if dropped, it would probably remain open in the pool, but we can't risk it
        let _lock = self.flush_and_read_lock().await?;
        debug!("Acquired read lock");

        let binlog_position = self.get_binlog_position().await?;

        // Even if we don't install the recipe, this will load the tables from the database
        let recipe = self.load_recipe(&mut self.pool.get_conn().await?).await?;
        debug!(%recipe, "Loaded recipe");

        if install_recipe {
            noria.install_recipe(&recipe).await?;
            debug!("Recipe installed");
        }

        self.dump_tables(noria).await?;
        noria
            .set_replication_offset(Some((&binlog_position).try_into()?))
            .await?;

        drop(_lock);

        Ok(binlog_position)
    }

    /// Copy all base tables into noria
    async fn dump_tables(&mut self, noria: &mut noria::ControllerHandle) -> ReadySetResult<()> {
        let mut replication_tasks = FuturesUnordered::new();

        // For each table we spawn a new task to parallelize the replication process somewhat
        for table_name in self.tables.as_ref().unwrap() {
            let dumper = self.dump_table(table_name).await?;
            let table_mutator = noria.table(table_name).await?;

            let span = info_span!("replicating table", table = %table_name);
            span.in_scope(|| debug!("Replicating table"));

            replication_tasks.push(tokio::spawn(
                Self::replicate_table(dumper, table_mutator).instrument(span),
            ));
        }

        while let Some(task_result) = replication_tasks.next().await {
            task_result.unwrap()?; // The unwrap is for the join handle in that case
        }

        Ok(())
    }
}

/// An intermediary struct that can be used to get a stream of Noria rows
// This is required because mysql::QueryResult borrows from conn and then
// we have some hard to solve borrowing issues
pub struct TableDumper {
    query: String,
    conn: mysql::Conn,
}

impl TableDumper {
    pub async fn stream(&mut self) -> mysql::Result<TableStream<'_>> {
        Ok(TableStream {
            query: self.conn.exec_iter(&self.query, ()).await?,
        })
    }
}

// Just another helper struct to make it streamable
pub struct TableStream<'a> {
    query: mysql::QueryResult<'a, 'static, mysql::BinaryProtocol>,
}

impl<'a> TableStream<'a> {
    /// Get the next row from the query response
    pub async fn next<'b>(&'b mut self) -> ReadySetResult<Option<Vec<noria::DataType>>> {
        let next_row = self.query.next().await?;
        next_row.map(mysql_row_to_noria_row).transpose()
    }
}

/// Convert each entry in a row to a Noria type that can be inserted into the base tables
fn mysql_row_to_noria_row(row: mysql::Row) -> ReadySetResult<Vec<noria::DataType>> {
    let mut noria_row = Vec::with_capacity(row.len());
    for idx in 0..row.len() {
        let val = value_to_value(row.as_ref(idx).unwrap());
        noria_row.push(noria::DataType::try_from(val)?);
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
