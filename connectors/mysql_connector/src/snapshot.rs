use std::convert::{TryFrom, TryInto};

use futures::{stream::FuturesUnordered, StreamExt};
use mysql::prelude::*;
use mysql_async as mysql;
use noria::{consensus::Authority, ReadySetResult, ReplicationOffset};
use slog::{info, Logger};

use crate::connector::BinlogPosition;

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
    pub(crate) log: Logger,
}

impl MySqlReplicator {
    /// Get the list of tables defined in the database
    async fn load_table_list(&self, kind: TableKind) -> mysql::Result<Vec<String>> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("show full tables where Table_Type={}", kind);
        let tables_entries: Vec<(String, String)> = conn.query_map(query, |r| r).await?;
        Ok(tables_entries.into_iter().map(|(name, _)| name).collect())
    }

    /// Get the `CREATE TABLE` statement for the named table
    async fn create_for_table(&self, table_name: &str) -> mysql::Result<String> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("show create table {}", table_name);
        // For SHOW CREATE TABLE format is the name of the table and the create DDL
        let create: Option<(String, String)> = conn.query_first(query).await?;
        Ok(create.ok_or("Empty response for SHOW CREATE TABLE")?.1)
    }

    /// Get the `CREATE VIEW` statement for the named view
    async fn create_for_view(&self, view_name: &str) -> mysql::Result<String> {
        let mut conn = self.pool.get_conn().await?;
        let query = format!("show create view {}", view_name);
        // For SHOW CREATE VIEW the is the name of the view, the create DDL, character_set_client and collation_connection
        let create: Option<(String, String, String, String)> = conn.query_first(query).await?;
        Ok(create.ok_or("Empty response for SHOW CREATE VIEW")?.1)
    }

    /// Load all the `CREATE TABLE` statements for the database and concatenate them
    /// into a single String that can be installed as a recipe in Noria
    pub async fn load_recipe(&mut self) -> mysql::Result<String> {
        if self.tables.is_none() {
            self.tables = Some(self.load_table_list(TableKind::BaseTable).await?);
        }

        let mut recipe = String::new();

        // Append `CREATE TABLE` statements
        for table in self.tables.as_ref().unwrap() {
            let create = self.create_for_table(&table).await?;
            recipe.push_str(&create);
            recipe.push_str(";\n");
        }

        // Append `CREATE VIEW` statements
        for view in self.load_table_list(TableKind::View).await? {
            let create = self.create_for_view(&view).await?;
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
        log: Logger,
    ) -> ReadySetResult<()> {
        let mut cnt = 0;
        let mut row_stream = dumper.stream().await?;
        let mut rows = Vec::with_capacity(BATCH_SIZE);

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

        info!(log, "Replication finished, rows replicated: {}", cnt);

        Ok(())
    }

    ///
    /// This function replicates an entire MySQL database into a clean
    /// noria deployment.
    /// # Arguments
    /// * `noria`: The target Noria deployment
    /// * `install_recipe`: Replicate and install the recipe (`CREATE TABLE` ...; `CREATE VIEW` ...;) in addition to the rows
    ///
    pub async fn replicate_to_noria<A: Authority>(
        mut self,
        noria: &mut noria::ControllerHandle<A>,
        install_recipe: bool,
    ) -> ReadySetResult<BinlogPosition> {
        // We must hold the locking connection open until replication is finished,
        // if dropped, it would probably remain open in the pool, but we can't risk it
        let _lock = self.flush_and_read_lock().await?;

        info!(self.log, "Acquired read lock");

        let binlog_position = self.get_binlog_position().await?;
        let repl_offset: ReplicationOffset = (&binlog_position).try_into()?;

        // Even if we don't install the recipe, this will load the tables from the database
        let recipe = self.load_recipe().await?;
        info!(self.log, "Loaded recipe:\n{}", recipe);

        if install_recipe {
            noria
                .install_recipe_with_offset(&recipe, Some(repl_offset.clone()))
                .await?;
            info!(self.log, "Recipe installed");
        }

        let mut replication_tasks = FuturesUnordered::new();

        // For each table we spawn a new task to parallelize the replication process somewhat
        for table_name in self.tables.as_ref().unwrap() {
            let dumper = self.dump_table(&table_name).await?;
            let mut table_mutator = noria.table(&table_name).await?;

            let log = self.log.new(slog::o!("table" => table_name.clone()));
            info!(log, "Replicating table");

            table_mutator
                .set_replication_offset(repl_offset.clone())
                .await?;

            replication_tasks.push(tokio::spawn(Self::replicate_table(
                dumper,
                table_mutator,
                log,
            )));
        }

        while let Some(task_result) = replication_tasks.next().await {
            task_result.unwrap()?; // The unwrap is for the join handle in that case
        }

        drop(_lock);

        // Wait for all connections to finish, not strictly neccessary
        self.pool.disconnect().await?;

        Ok(binlog_position)
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

impl std::fmt::Display for TableKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableKind::BaseTable => write!(f, "'BASE TABLE'"),
            TableKind::View => write!(f, "'VIEW'"),
        }
    }
}
