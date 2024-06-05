use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::{io, mem};

use anyhow::{anyhow, bail, Context};
use console::style;
use database_utils::{DatabaseConnection, DatabaseType, DatabaseURL, QueryableConnection};
use itertools::Itertools;
use mysql_srv::MySqlIntermediary;
use nom_sql::{Dialect, Relation};
use readyset_adapter::backend::noria_connector::ReadBehavior;
use readyset_adapter::backend::{BackendBuilder, NoriaConnector};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_adapter::{ReadySetStatusReporter, UpstreamConfig, UpstreamDatabase};
use readyset_client::consensus::{Authority, LocalAuthorityStore};
use readyset_client::ReadySetHandle;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use readyset_server::{Builder, LocalAuthority, ReuseConfigType};
use readyset_util::shared_cache::SharedCache;
use readyset_util::shutdown::ShutdownSender;
use tokio::sync::RwLock;
use tokio::time::sleep;
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::ast::{
    Conditional, Query, QueryResults, Record, SortMode, Statement, StatementResult, Value,
};
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
    pub verbose: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            upstream_database_url: None,
            enable_reuse: false,
            time: false,
            replication_url: None,
            database_type: DatabaseType::MySQL,
            verbose: false,
        }
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

/// Establish a connection to the upstream DB server and recreate the test database
pub(crate) async fn recreate_test_database(url: &DatabaseURL) -> anyhow::Result<()> {
    let db_name = url
        .db_name()
        .ok_or_else(|| anyhow!("Must specify database name as part of database URL"))?;
    let mut admin_url = url.clone();
    admin_url.set_db_name(match url.database_type() {
        DatabaseType::PostgreSQL => "postgres".to_owned(),
        DatabaseType::MySQL => "mysql".to_owned(),
    });
    let mut admin_conn = admin_url
        .connect(None)
        .await
        .with_context(|| "connecting to upstream")?;

    admin_conn
        .query_drop(format!("DROP DATABASE IF EXISTS {}", db_name))
        .await
        .with_context(|| "dropping database")?;

    let mut create_database_query = format!("CREATE DATABASE {}", db_name);
    if url.is_mysql() {
        create_database_query.push_str(" CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_0900_bin'");
    }
    admin_conn
        .query_drop(create_database_query)
        .await
        .with_context(|| "creating database")?;

    Ok(())
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

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn run(&mut self, opts: RunOptions, noria_opts: NoriaOptions) -> anyhow::Result<()> {
        println!(
            "==> {} {}",
            style("Running test script").bold(),
            style(
                self.path
                    .canonicalize()
                    .unwrap_or_else(|_| "".into())
                    .to_string_lossy()
            )
            .blue()
        );

        if let Some(upstream_url) = &opts.upstream_database_url {
            recreate_test_database(upstream_url).await?;
            let mut conn = upstream_url
                .connect(None)
                .await
                .with_context(|| "connecting to upstream database")?;

            self.run_on_database(&opts, &mut conn, None).await?;
        } else {
            if let Some(replication_url) = &opts.replication_url {
                recreate_test_database(&replication_url.parse()?).await?;
            }

            self.run_on_noria(&opts, &noria_opts).await?;
        };

        Ok(())
    }

    /// Run the test script on ReadySet server
    pub async fn run_on_noria(
        &self,
        opts: &RunOptions,
        noria_opts: &NoriaOptions,
    ) -> anyhow::Result<()> {
        let (noria_handle, shutdown_tx) = self
            .start_noria_server(opts, noria_opts.authority.clone())
            .await;
        let (adapter_task, db_url) = self.setup_adapter(opts, noria_opts.authority.clone()).await;

        let mut conn = match db_url
            .connect(None)
            .await
            .with_context(|| "connecting to adapter")
        {
            Ok(conn) => conn,
            Err(e) => {
                shutdown_tx.shutdown().await;
                return Err(e);
            }
        };

        if let Err(e) = self
            .run_on_database(opts, &mut conn, noria_handle.c.clone())
            .await
        {
            shutdown_tx.shutdown().await;
            return Err(e);
        }

        // After all tests are done, stop the adapter
        adapter_task.abort();
        let _ = adapter_task.await;

        // Stop ReadySet
        shutdown_tx.shutdown().await;

        Ok(())
    }

    pub async fn run_on_database(
        &self,
        opts: &RunOptions,
        conn: &mut DatabaseConnection,
        mut noria: Option<ReadySetHandle>,
    ) -> anyhow::Result<()> {
        let mut prev_was_statement = false;

        let is_readyset = noria.is_some();
        let conditional_skip = |conditionals: &[Conditional]| {
            return conditionals.iter().any(|s| match s {
                Conditional::SkipIf(c) if c == "readyset" => is_readyset,
                Conditional::OnlyIf(c) if c == "readyset" => !is_readyset,
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
                    if opts.verbose {
                        eprintln!("     > {}", stmt.command);
                    }
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
                        tokio::time::sleep(Duration::from_millis(2000)).await;
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

                    if opts.verbose {
                        eprintln!("     > {}", query.query);
                    }

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
                            style("  > Query").bold(),
                            style(label).blue(),
                            style("ran in").bold(),
                            style(humantime::format_duration(duration).to_string()).blue()
                        );
                    }
                }
                Record::HashThreshold(_) => {}
                Record::Halt { .. } => break,
                Record::Sleep(msecs) => {
                    if opts.verbose {
                        eprintln!("     > sleep {msecs}ms");
                    }
                    sleep(Duration::from_millis(*msecs)).await
                }
                Record::Graphviz => {
                    if let Some(noria) = &mut noria {
                        if opts.verbose {
                            eprintln!("     > graphviz");
                        }
                        println!("{}", noria.graphviz(Default::default()).await?);
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

        let mut rows = <Vec<Vec<Value>>>::try_from(results)?.into_iter().map(
            |mut row: Vec<Value>| -> anyhow::Result<Vec<Value>> {
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
            },
        );

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
    ) -> (readyset_server::Handle, ShutdownSender) {
        let mut retry: usize = 0;
        loop {
            retry += 1;

            let mut builder = Builder::for_tests();
            builder.set_allow_mixed_comparisons(true);
            builder.set_allow_straddled_joins(true);
            builder.set_allow_post_lookup(true);

            if run_opts.enable_reuse {
                builder.set_reuse(Some(ReuseConfigType::Finkelstein))
            }

            if let Some(replication_url) = &run_opts.replication_url {
                builder.set_replication_url(replication_url.to_owned());
            }

            let persistence = readyset_server::PersistenceParameters {
                mode: readyset_server::DurabilityMode::DeleteOnExit,
                ..Default::default()
            };

            builder.set_persistence(persistence);

            let (mut noria, shutdown_tx) = match builder.start(Arc::clone(&authority)).await {
                Ok(builder) => builder,
                Err(err) => {
                    // This can error out if there are too many open files, but if we wait a bit
                    // they will get closed (macOS problem)
                    if retry > 100 {
                        panic!("{:?}", err)
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };
            noria.backend_ready().await;
            return (noria, shutdown_tx);
        }
    }

    async fn setup_adapter(
        &self,
        run_opts: &RunOptions,
        authority: Arc<Authority>,
    ) -> (tokio::task::JoinHandle<()>, DatabaseURL) {
        let database_type = run_opts.database_type;
        let replication_url = run_opts.replication_url.clone();
        let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
        let view_name_cache = SharedCache::new();
        let view_cache = SharedCache::new();
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

        let mut rh = ReadySetHandle::new(authority.clone()).await;

        let adapter_rewrite_params = rh.adapter_rewrite_params().await.unwrap();
        let adapter_start_time = SystemTime::now();

        let task = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();

            let noria = NoriaConnector::new(
                rh.clone(),
                auto_increments,
                view_name_cache.new_local(),
                view_cache.new_local(),
                ReadBehavior::Blocking,
                match database_type {
                    DatabaseType::MySQL => readyset_data::Dialect::DEFAULT_MYSQL,
                    DatabaseType::PostgreSQL => readyset_data::Dialect::DEFAULT_POSTGRESQL,
                },
                match database_type {
                    DatabaseType::MySQL => nom_sql::Dialect::MySQL,
                    DatabaseType::PostgreSQL => nom_sql::Dialect::PostgreSQL,
                },
                match database_type {
                    DatabaseType::MySQL if replication_url.is_some() => vec!["noria".into()],
                    DatabaseType::PostgreSQL if replication_url.is_some() => {
                        vec!["noria".into(), "public".into()]
                    }
                    _ => Default::default(),
                },
                adapter_rewrite_params,
            )
            .await;
            let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));

            macro_rules! make_backend {
                ($upstream:ty, $handler:ty, $dialect:expr $(,)?) => {{
                    // cannot use .await inside map
                    #[allow(clippy::manual_map)]
                    let upstream = match &replication_url {
                        Some(url) => Some(
                            <LazyUpstream<$upstream> as UpstreamDatabase>::connect(
                                UpstreamConfig::from_url(url),
                            )
                            .await
                            .unwrap(),
                        ),
                        None => None,
                    };

                    let status_reporter = ReadySetStatusReporter::new(
                        replication_url
                            .map(UpstreamConfig::from_url)
                            .unwrap_or_default(),
                        Some(rh),
                        Default::default(),
                        authority.clone(),
                    );
                    BackendBuilder::new()
                        .require_authentication(false)
                        .dialect($dialect)
                        .build::<_, $handler>(
                            noria,
                            upstream,
                            query_status_cache,
                            authority,
                            status_reporter,
                            adapter_start_time,
                        )
                }};
            }

            match database_type {
                DatabaseType::MySQL => MySqlIntermediary::run_on_tcp(
                    readyset_mysql::Backend {
                        noria: make_backend!(MySqlUpstream, MySqlQueryHandler, Dialect::MySQL,),
                        enable_statement_logging: false,
                    },
                    s,
                    false,
                )
                .await
                .unwrap(),
                DatabaseType::PostgreSQL => {
                    psql_srv::run_backend(
                        readyset_psql::Backend::new(make_backend!(
                            PostgreSqlUpstream,
                            PostgreSqlQueryHandler,
                            Dialect::PostgreSQL,
                        )),
                        s,
                        false,
                        None,
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
