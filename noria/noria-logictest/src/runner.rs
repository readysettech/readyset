use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::Write;
use std::iter::FromIterator;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;

use anyhow::{anyhow, bail, Context};
use colored::*;
use itertools::Itertools;
use msql_srv::MysqlIntermediary;
use mysql_async as mysql;
use tokio_postgres as pgsql;

use nom_sql::SelectStatement;
use noria::consensus::{Authority, LocalAuthorityStore};
use noria::ControllerHandle;
use noria_client::backend::{BackendBuilder, NoriaConnector};
use noria_client::UpstreamDatabase;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};
use noria_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use noria_server::{Builder, LocalAuthority, ReuseConfigType};

use crate::ast::{
    Conditional, Query, QueryResults, Record, SortMode, Statement, StatementResult, Value,
};
use crate::parser;
use crate::upstream::{DatabaseConnection, DatabaseType, DatabaseURL};

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

impl TestScript {
    pub fn write_to<W>(&self, w: &mut W) -> io::Result<()>
    where
        W: Write,
    {
        writeln!(w, "{}", self.records().iter().join("\n"))
    }
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub database_type: DatabaseType,
    pub upstream_database_url: Option<DatabaseURL>,
    pub replication_url: Option<String>,
    pub enable_reuse: bool,
    pub time: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            upstream_database_url: None,
            enable_reuse: false,
            time: false,
            replication_url: None,
            database_type: DatabaseType::MySQL,
        }
    }
}

impl RunOptions {
    fn db_name(&self) -> &str {
        self.upstream_database_url
            .as_ref()
            .and_then(|url| url.db_name())
            .unwrap_or("noria")
    }
}

pub struct NoriaOptions {
    pub authority: Arc<Authority>,
}

impl Default for NoriaOptions {
    fn default() -> Self {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        Self {
            authority: Arc::new(Authority::from(LocalAuthority::new_with_store(
                authority_store,
            ))),
        }
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

    pub async fn run(&mut self, opts: RunOptions, noria_opts: NoriaOptions) -> anyhow::Result<()> {
        println!(
            "==> {} {}",
            "Running test script".bold(),
            self.path
                .canonicalize()
                .unwrap_or_else(|_| "".into())
                .to_string_lossy()
                .blue()
        );

        let db_name = match &opts.upstream_database_url {
            Some(db_url) => db_url.upstream_type().to_string(),
            None => "Noria".to_owned(),
        };

        if let Some(upstream_url) = &opts.upstream_database_url {
            self.recreate_test_database(upstream_url).await?;
            let mut conn = upstream_url
                .connect()
                .await
                .with_context(|| "connecting to upstream database")?;

            self.run_on_database(&opts, &mut conn, None).await?;
        } else {
            if let Some(replication_url) = &opts.replication_url {
                self.recreate_test_database(&replication_url.parse()?)
                    .await?;
            }

            self.run_on_noria(&opts, &noria_opts).await?;
        };

        println!(
            "{}",
            format!(
                "==> Successfully ran {} operations against {}",
                self.records.len(),
                db_name
            )
            .bold()
        );

        Ok(())
    }

    /// Establish a connection to the upstream DB server and recreate the test database
    async fn recreate_test_database(&self, url: &DatabaseURL) -> anyhow::Result<()> {
        let db_name = url
            .db_name()
            .ok_or_else(|| anyhow!("Must specify database name as part of database URL"))?;
        let mut admin_url = url.clone();
        admin_url.set_db_name(match url.upstream_type() {
            DatabaseType::PostgreSQL => "postgres".to_owned(),
            DatabaseType::MySQL => "mysql".to_owned(),
        });
        let mut admin_conn = admin_url
            .connect()
            .await
            .with_context(|| "connecting to upstream")?;

        admin_conn
            .query_drop(format!("DROP DATABASE IF EXISTS {}", db_name))
            .await
            .with_context(|| "dropping database")?;

        admin_conn
            .query_drop(format!("CREATE DATABASE {}", db_name))
            .await
            .with_context(|| "creating database")?;

        Ok(())
    }

    /// Run the test script on Noria server
    pub async fn run_on_noria(
        &self,
        opts: &RunOptions,
        noria_opts: &NoriaOptions,
    ) -> anyhow::Result<()> {
        let mut noria_handle = self
            .start_noria_server(opts, noria_opts.authority.clone())
            .await;
        let (adapter_task, db_url) = self.setup_adapter(opts, noria_opts.authority.clone()).await;

        let mut conn = db_url
            .connect()
            .await
            .with_context(|| "connecting to adapter")?;

        self.run_on_database(opts, &mut conn, noria_handle.c.clone())
            .await?;

        // After all tests are done, stop the adapter
        adapter_task.abort();
        let _ = adapter_task.await;

        // Stop Noria
        noria_handle.shutdown();
        noria_handle.wait_done().await;

        Ok(())
    }

    pub async fn run_on_database(
        &self,
        opts: &RunOptions,
        conn: &mut DatabaseConnection,
        mut noria: Option<ControllerHandle>,
    ) -> anyhow::Result<()> {
        let mut prev_was_statement = false;

        let conditional_skip = |conditionals: &[Conditional]| {
            return conditionals.iter().any(|s| match s {
                Conditional::SkipIf(c) if c == &opts.database_type.to_string() => true,
                Conditional::OnlyIf(c) if c != &opts.database_type.to_string() => true,
                _ => false,
            });
        };

        for record in &self.records {
            match record {
                Record::Statement(stmt) => {
                    if conditional_skip(&stmt.conditionals) {
                        continue;
                    }
                    prev_was_statement = true;
                    self.run_statement(stmt, conn)
                        .await
                        .with_context(|| format!("Running statement {}", stmt.command))?
                }

                Record::Query(query) => {
                    if conditional_skip(&query.conditionals) {
                        continue;
                    }

                    if prev_was_statement {
                        prev_was_statement = false;
                        // we need to give the statements some time to propagate before we can issue
                        // the next query
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }

                    let timer = if opts.time {
                        query.label.clone().map(|label| (label, Instant::now()))
                    } else {
                        None
                    };

                    // Failure from noria on a FailNoUpstream query is considered a pass. Passing
                    // is considered a failure.
                    let invert_result = query.conditionals.contains(&Conditional::InvertNoUpstream)
                        && (opts.replication_url.is_none());

                    match self
                        .run_query(query, conn)
                        .await
                        .with_context(|| format!("Running query {}", query.query))
                    {
                        Ok(_) => {
                            if invert_result {
                                return Err(anyhow!("Expected failure: {}", query.query));
                            }
                        }
                        Err(e) => {
                            if !invert_result {
                                return Err(e);
                            }
                        }
                    }
                    if let Some((label, start_time)) = timer {
                        let duration = start_time.elapsed();
                        println!(
                            "{} {} {} {}",
                            "  > Query".bold(),
                            label.blue(),
                            "ran in".bold(),
                            humantime::format_duration(duration).to_string().blue()
                        );
                    }
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

    async fn run_statement(
        &self,
        stmt: &Statement,
        conn: &mut DatabaseConnection,
    ) -> anyhow::Result<()> {
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

    async fn run_query(&self, query: &Query, conn: &mut DatabaseConnection) -> anyhow::Result<()> {
        let results = if query.params.is_empty() {
            conn.query(&query.query).await?
        } else {
            conn.execute(&query.query, query.params.clone()).await?
        };

        let mut rows =
            results
                .into_iter()
                .map(|mut row: Vec<Value>| -> anyhow::Result<Vec<Value>> {
                    if let Some(column_types) = &query.column_types {
                        let row_len = row.len();
                        let wrong_columns = || {
                            anyhow!(
                                "Row had the wrong number of columns: expected {}, but got {}",
                                column_types.len(),
                                row_len
                            )
                        };

                        if row.len() > column_types.len() {
                            return Err(wrong_columns());
                        }

                        let mut vals = mem::take(&mut row).into_iter();
                        row = column_types
                            .iter()
                            .map(move |col_type| -> anyhow::Result<Value> {
                                let val = vals.next().ok_or_else(wrong_columns)?;
                                Ok(val
                                    .convert_type(col_type)
                                    .with_context(|| format!("Converting value to {:?}", col_type))?
                                    .into_owned())
                            })
                            .collect::<Result<_, _>>()?;
                    }
                    Ok(row)
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

    async fn start_noria_server(
        &self,
        run_opts: &RunOptions,
        authority: Arc<Authority>,
    ) -> noria_server::Handle {
        let mut retry: usize = 0;
        loop {
            retry += 1;

            let mut builder = Builder::for_tests();
            builder.set_allow_topk(true);

            if run_opts.enable_reuse {
                builder.set_reuse(Some(ReuseConfigType::Finkelstein))
            }

            if let Some(replication_url) = &run_opts.replication_url {
                // Add the data base name to the url, and set as replication source
                builder.set_replicator_url(format!("{}/{}", replication_url, run_opts.db_name()));
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

    async fn setup_adapter(
        &self,
        run_opts: &RunOptions,
        authority: Arc<Authority>,
    ) -> (tokio::task::JoinHandle<()>, DatabaseURL) {
        let database_type = run_opts.database_type;

        let replication_url = run_opts.replication_url.clone();
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

        let ch = ControllerHandle::new(authority).await;

        let task = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();

            let noria = NoriaConnector::new(ch, auto_increments, query_cache, None).await;

            macro_rules! make_backend {
                ($upstream:ty, $handler:ty) => {{
                    // cannot use .await inside map
                    #[allow(clippy::manual_map)]
                    let upstream = match replication_url.clone() {
                        Some(url) => Some(
                            <$upstream as UpstreamDatabase>::connect(url.clone())
                                .await
                                .unwrap(),
                        ),
                        None => None,
                    };

                    BackendBuilder::new()
                        .require_authentication(false)
                        .build::<_, $handler>(noria, upstream)
                }};
            }

            match database_type {
                DatabaseType::MySQL => MysqlIntermediary::run_on_tcp(
                    noria_mysql::Backend::new(make_backend!(MySqlUpstream, MySqlQueryHandler)),
                    s,
                )
                .await
                .unwrap(),
                DatabaseType::PostgreSQL => {
                    psql_srv::run_backend(
                        noria_psql::Backend(make_backend!(
                            PostgreSqlUpstream,
                            PostgreSqlQueryHandler
                        )),
                        s,
                    )
                    .await
                }
            }
        });

        (
            task,
            match database_type {
                DatabaseType::MySQL => mysql::OptsBuilder::default().tcp_port(addr.port()).into(),
                DatabaseType::PostgreSQL => {
                    let mut config = pgsql::Config::default();
                    config.host("localhost");
                    config.port(addr.port());
                    config.dbname("noria");
                    config.into()
                }
            },
        )
    }

    /// Get a reference to the test script's records.
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}
