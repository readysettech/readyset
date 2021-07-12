use anyhow::{anyhow, bail, Context};
use colored::*;
use itertools::Itertools;
use mysql_async as mysql;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use noria_client::backend::Reader;
use slog::o;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::sleep;

use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria::consensus::LocalAuthority;
use noria::ControllerHandle;
use noria_client::backend::mysql_connector::MySqlConnector;
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::backend::BackendBuilder;
use noria_server::{Builder, ReuseConfigType};

use crate::ast::{Query, QueryResults, Record, SortMode, Statement, StatementResult, Value};
use crate::parser;

#[derive(Debug, Clone)]
pub struct TestScript {
    path: PathBuf,
    records: Vec<Record>,
}

impl From<Vec<Record>> for TestScript {
    fn from(records: Vec<Record>) -> Self {
        TestScript {
            path: "".into(),
            records,
        }
    }
}

impl FromIterator<Record> for TestScript {
    fn from_iter<T: IntoIterator<Item = Record>>(iter: T) -> Self {
        Self::from(iter.into_iter().collect::<Vec<_>>())
    }
}

impl Extend<Record> for TestScript {
    fn extend<T: IntoIterator<Item = Record>>(&mut self, iter: T) {
        self.records.extend(iter)
    }
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub deployment_name: String,
    pub use_mysql: bool,
    pub mysql_host: String,
    pub mysql_port: u16,
    pub mysql_user: String,
    pub mysql_db: String,
    pub disable_reuse: bool,
    pub verbose: bool,
    pub binlog_url: Option<String>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            deployment_name: "sqllogictest".to_string(),
            use_mysql: false,
            mysql_host: "localhost".to_string(),
            mysql_port: 3306,
            mysql_user: "root".to_string(),
            mysql_db: "sqllogictest".to_string(),
            disable_reuse: false,
            verbose: false,
            binlog_url: None,
        }
    }
}

impl RunOptions {
    fn logger(&self) -> slog::Logger {
        if self.verbose {
            noria_server::logger_pls()
        } else {
            slog::Logger::root(slog::Discard, o!())
        }
    }

    pub fn mysql_opts_no_db(&self) -> mysql::Opts {
        mysql::OptsBuilder::default()
            .ip_or_hostname(self.mysql_host.clone())
            .tcp_port(self.mysql_port)
            .user(Some(self.mysql_user.clone()))
            .into()
    }

    pub fn mysql_opts(&self) -> mysql::Opts {
        mysql::OptsBuilder::from_opts(self.mysql_opts_no_db())
            .db_name(Some(self.mysql_db.clone()))
            .into()
    }
}

fn compare_results(results: &[Value], expected: &[Value], type_sensitive: bool) -> bool {
    if type_sensitive {
        return results == expected;
    }

    results
        .iter()
        .zip(expected)
        .all(|(res, expected)| res.compare_type_insensitive(expected))
}

impl TestScript {
    pub fn read<R: io::Read>(path: PathBuf, input: R) -> anyhow::Result<Self> {
        let records = parser::read_records(input)?;
        Ok(Self { path, records })
    }

    pub fn open_file(path: PathBuf) -> anyhow::Result<Self> {
        let file = File::open(&path)?;
        Self::read(path, file)
    }

    pub fn name(&self) -> Cow<'_, str> {
        match self.path.file_name() {
            Some(n) => n.to_string_lossy(),
            None => Cow::Borrowed("unknown"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn run(&self, opts: RunOptions) -> anyhow::Result<()> {
        println!(
            "==> {} {}",
            "Running test script".bold(),
            self.path
                .canonicalize()
                .unwrap_or_else(|_| "".into())
                .to_string_lossy()
                .blue()
        );

        if opts.use_mysql {
            self.recreate_test_database(opts.mysql_opts().clone(), &opts.mysql_db)
                .await?;
            let mut conn = mysql::Conn::new(opts.mysql_opts())
                .await
                .with_context(|| "connecting to mysql")?;

            self.run_on_mysql(&mut conn, None).await?;
        } else {
            if let Some(binlog_url) = &opts.binlog_url {
                self.recreate_test_database(binlog_url.try_into().unwrap(), &opts.mysql_db)
                    .await?;
            }

            self.run_on_noria(&opts).await?;
        };

        println!(
            "{}",
            format!(
                "==> Successfully ran {} operations against {}",
                self.records.len(),
                if opts.use_mysql { "MySQL" } else { "Noria" }
            )
            .bold()
        );

        Ok(())
    }

    /// Establish a connection to MySQL and recreate the test database
    async fn recreate_test_database<S: AsRef<str>>(
        &self,
        opts: mysql::Opts,
        db_name: &S,
    ) -> anyhow::Result<()> {
        let mut create_db_conn = mysql::Conn::new(opts)
            .await
            .with_context(|| "connecting to mysql")?;

        create_db_conn
            .query_drop(format!("DROP DATABASE IF EXISTS {}", db_name.as_ref()))
            .await
            .with_context(|| "dropping database")?;

        create_db_conn
            .query_drop(format!("CREATE DATABASE {}", db_name.as_ref()))
            .await
            .with_context(|| "creating database")?;

        Ok(())
    }

    /// Run the test script on Noria server
    pub async fn run_on_noria(&self, opts: &RunOptions) -> anyhow::Result<()> {
        let authority = Arc::new(LocalAuthority::default());
        let mut noria_handle = self.start_noria_server(&opts, Arc::clone(&authority)).await;
        let (adapter_task, conn_opts) = self.setup_mysql_adapter(&opts, authority).await;

        let mut conn = mysql::Conn::new(conn_opts)
            .await
            .with_context(|| "connecting to noria-mysql")?;

        self.run_on_mysql(&mut conn, noria_handle.c.clone()).await?;

        // After all tests are done, stop the adapter
        adapter_task.abort();
        let _ = adapter_task.await;

        // Stop Noria
        noria_handle.shutdown();
        noria_handle.wait_done().await;

        Ok(())
    }

    pub async fn run_on_mysql(
        &self,
        conn: &mut mysql::Conn,
        mut noria: Option<ControllerHandle<LocalAuthority>>,
    ) -> anyhow::Result<()> {
        let mut prev_was_statement = false;

        for record in &self.records {
            match record {
                Record::Statement(stmt) => {
                    prev_was_statement = true;
                    self.run_statement(stmt, conn)
                        .await
                        .with_context(|| format!("Running statement {}", stmt.command))?
                }

                Record::Query(query) => {
                    if prev_was_statement {
                        prev_was_statement = false;
                        // we need to give the statements some time to propagate before we can issue
                        // the next query
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }

                    self.run_query(query, conn)
                        .await
                        .with_context(|| format!("Running query {}", query.query))?
                }
                Record::HashThreshold(_) => {}
                Record::Halt { .. } => break,
                Record::Sleep(msecs) => sleep(Duration::from_millis(*msecs)).await,
                Record::Graphviz => {
                    if let Some(noria) = &mut noria {
                        println!("{}", noria.graphviz().await?);
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_statement(&self, stmt: &Statement, conn: &mut mysql::Conn) -> anyhow::Result<()> {
        let res = conn.query_drop(&stmt.command).await;
        match stmt.result {
            StatementResult::Ok => {
                if let Err(e) = res {
                    bail!("Statement failed: {}", e);
                }
            }
            StatementResult::Error => {
                if res.is_ok() {
                    bail!("Statement should have failed, but succeeded");
                }
            }
        }
        Ok(())
    }

    async fn run_query(&self, query: &Query, conn: &mut mysql::Conn) -> anyhow::Result<()> {
        let results = if query.params.is_empty() {
            conn.query(&query.query).await?
        } else {
            conn.exec(&query.query, &query.params).await?
        };

        let mut rows = results
            .into_iter()
            .map(|mut row: Row| -> anyhow::Result<Vec<Value>> {
                match &query.column_types {
                    Some(column_types) => column_types
                        .iter()
                        .enumerate()
                        .map(|(col_idx, col_type)| -> anyhow::Result<Value> {
                            let val = row.take(col_idx).ok_or_else(|| {
                                anyhow!(
                                    "Row had the wrong number of columns: expected {}, but got {}",
                                    column_types.len(),
                                    row.len()
                                )
                            })?;
                            Value::from_mysql_value_with_type(val, col_type)
                                .with_context(|| format!("Converting value to {:?}", col_type))
                        })
                        .collect(),
                    None => row
                        .unwrap()
                        .into_iter()
                        .map(|val| {
                            Value::try_from(val).with_context(|| "Converting value".to_string())
                        })
                        .collect(),
                }
            });

        let vals: Vec<Value> = match query.sort_mode.unwrap_or_default() {
            SortMode::NoSort => rows.fold_ok(vec![], |mut acc, row| {
                acc.extend(row);
                acc
            })?,
            SortMode::RowSort => {
                let mut rows: Vec<_> = rows.try_collect()?;
                rows.sort();
                rows.into_iter().flatten().collect()
            }
            SortMode::ValueSort => {
                let mut vals = rows.fold_ok(vec![], |mut acc, row| {
                    acc.extend(row);
                    acc
                })?;
                vals.sort();
                vals
            }
        };

        match &query.results {
            QueryResults::Hash { count, digest } => {
                if *count != vals.len() {
                    bail!(
                        "Wrong number of results returned: expected {}, but got {}",
                        count,
                        vals.len(),
                    );
                }
                let actual_digest = Value::hash_results(&vals);
                if actual_digest != *digest {
                    bail!(
                        "Incorrect values returned from query, expected values hashing to {:x}, but got {:x}",
                        digest,
                        actual_digest
                    );
                }
            }
            QueryResults::Results(expected_vals) => {
                if vals.len() != expected_vals.len() {
                    bail!("The number of values returned does not match the number of values expected (left: expected, right: actual): \n {}, {}",expected_vals.len(), vals.len());
                }
                if !compare_results(&vals, expected_vals, query.column_types.is_some()) {
                    bail!(
                        "Incorrect values returned from query (left: expected, right: actual): \n{}",
                        pretty_assertions::Comparison::new(expected_vals, &vals)
                    )
                }
            }
        }
        Ok(())
    }

    async fn start_noria_server<A: 'static + noria::consensus::Authority>(
        &self,
        run_opts: &RunOptions,
        authority: Arc<A>,
    ) -> noria_server::Handle<A> {
        let mut retry: usize = 0;
        loop {
            retry += 1;

            let mut builder = Builder::default();
            builder.log_with(run_opts.logger());

            if run_opts.disable_reuse {
                builder.set_reuse(ReuseConfigType::NoReuse)
            }

            if let Some(binlog_url) = &run_opts.binlog_url {
                // Add the data base name to the mysql url, and set as binlog source
                builder.set_replicator_url(format!("{}/{}", binlog_url, run_opts.mysql_db));
            }

            match builder.start(Arc::clone(&authority)).await {
                Ok(builder) => return builder,
                Err(err) => {
                    // This can error out if there are too many open files, but if we wait a bit
                    // they will get closed (macOS problem)
                    if retry > 100 {
                        panic!("{:?}", err)
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    }

    async fn setup_mysql_adapter<A: 'static + noria::consensus::Authority>(
        &self,
        run_opts: &RunOptions,
        authority: Arc<A>,
    ) -> (tokio::task::JoinHandle<()>, mysql::Opts) {
        let binlog_url = run_opts.binlog_url.as_ref().map(|binlog_url| {
            // Append the database name to the binlog mysql url
            format!("{}/{}", binlog_url, run_opts.mysql_db)
        });

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
        let mut retry: usize = 0;
        let listener = loop {
            retry += 1;
            match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                Ok(listener) => break listener,
                Err(err) => {
                    if retry > 100 {
                        panic!("{:?}", err)
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await
                }
            }
        };
        let addr = listener.local_addr().unwrap();

        let ch = ControllerHandle::<A>::new(authority).await.unwrap();

        let task = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();

            let noria_connector = NoriaConnector::new(
                ch.clone(),
                auto_increments.clone(),
                query_cache.clone(),
                None,
            )
            .await;

            // cannot use .await inside map
            #[allow(clippy::manual_map)]
            let mysql_connector = match binlog_url.clone() {
                Some(url) => Some(MySqlConnector::new(url.clone()).await),
                None => None,
            };

            let backend_builder = BackendBuilder::new();

            let backend_builder = if let Some(url) = binlog_url {
                let writer = MySqlConnector::new(url);
                backend_builder.writer(writer.await)
            } else {
                let writer = NoriaConnector::new(ch, auto_increments, query_cache, None);
                backend_builder.writer(writer.await)
            };

            let backend = backend_builder
                .reader(Reader {
                    mysql_connector,
                    noria_connector,
                })
                .require_authentication(false)
                .build();

            MysqlIntermediary::run_on_tcp(backend, s).await.unwrap();
        });

        (
            task,
            mysql::OptsBuilder::default().tcp_port(addr.port()).into(),
        )
    }

    /// Get a reference to the test script's records.
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}
