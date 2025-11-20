use std::borrow::Cow;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{io, mem};

use anyhow::{anyhow, bail, Context};
use itertools::Itertools;
use tokio::time::sleep;
use tracing::{debug, info};

#[cfg(feature = "in-process-readyset")]
use tracing::error;

use database_utils::tls::ServerCertVerification;
use database_utils::{DatabaseConnection, DatabaseType, DatabaseURL, QueryableConnection};
use readyset_client_metrics::QueryDestination;
use readyset_data::{Collation, DfType, DfValue};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::retry_with_exponential_backoff;

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
        write!(w, "{self}")
    }
}

impl Display for TestScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.records().iter().join("\n"))
    }
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub database_type: DatabaseType,
    pub upstream_database_url: Option<DatabaseURL>,
    /// Should be set to true if `upstream_database_url` points to an existing Readyset instance, in
    /// which case we should not attempt to start a new in-process Readyset instance to test and
    /// should directly test the existing instance.
    pub upstream_database_is_readyset: bool,
    pub replication_url: Option<String>,
    pub parsing_preset: ParsingPreset,
    pub enable_reuse: bool,
    pub time: bool,
    pub verbose: bool,
}

impl RunOptions {
    pub fn default_for_database(database_type: DatabaseType) -> Self {
        Self {
            upstream_database_url: None,
            upstream_database_is_readyset: false,
            enable_reuse: false,
            time: false,
            replication_url: None,
            database_type,
            parsing_preset: ParsingPreset::for_tests(),
            verbose: false,
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
        .connect(&ServerCertVerification::Default)
        .await
        .with_context(|| "connecting to upstream")?;

    admin_conn
        .query_drop(format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .with_context(|| "dropping database")?;

    let mut create_database_query = format!("CREATE DATABASE {db_name}");
    if url.is_mysql() {
        create_database_query.push_str(" CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin'");
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

    pub async fn run(&mut self, opts: RunOptions) -> anyhow::Result<()> {
        info!(path = ?self.path, "Running test script");

        // Check filename to determine migration mode.
        // Check comments on MigrationMode for details.
        let out_of_band_migration = self.path.to_string_lossy().contains(".oob.");

        // Recreate the test database, unless this is a long-lived remote readyset instance (e.g.
        // running under Antithesis) in which case the state needs to be managed/reset externally;
        // we currently won't do the right thing if we drop and recreate an database out from under
        // the Readyset instance, and Postgres won't even let us do it. See REA-5958.
        if !opts.upstream_database_is_readyset {
            if let Some(upstream_url) = &opts.upstream_database_url {
                recreate_test_database(upstream_url).await?;
            } else if let Some(replication_url) = &opts.replication_url {
                recreate_test_database(&replication_url.parse()?).await?;
            }
        }

        if let Some(upstream_url) = &opts.upstream_database_url {
            let mut conn = upstream_url
                .connect(&ServerCertVerification::Default)
                .await
                .with_context(|| "connecting to upstream database")?;

            // We expect it's harmless to always enable the built-in citext extension, which fuzz
            // tests might generate.
            if upstream_url.is_postgres() {
                conn.query_drop("create extension if not exists citext")
                    .await?;
            }

            self.run_on_database(&opts, &mut conn, opts.upstream_database_is_readyset)
                .await?;
        } else {
            self.run_on_noria(&opts, out_of_band_migration).await?;
        };

        Ok(())
    }

    /// Something is misconfigured, and we are attempting to run in-process Readyset without that compiled in.
    #[cfg(not(feature = "in-process-readyset"))]
    pub async fn run_on_noria(
        &self,
        _opts: &RunOptions,
        _out_of_band_migration: bool,
    ) -> anyhow::Result<()> {
        panic!("in-process-readyset feature is not enabled, cannot start Readyset server");
    }

    /// Run the test script on in-process Readyset server
    #[cfg(feature = "in-process-readyset")]
    pub async fn run_on_noria(
        &self,
        opts: &RunOptions,
        out_of_band_migration: bool,
    ) -> anyhow::Result<()> {
        let (_noria_handle, shutdown_tx, adapter_task, db_url) =
            crate::in_process_readyset::start_readyset(opts, out_of_band_migration).await;
        let mut conn = match db_url
            .connect(&ServerCertVerification::Default)
            .await
            .with_context(|| "connecting to adapter")
        {
            Ok(conn) => conn,
            Err(e) => {
                shutdown_tx.shutdown().await;
                return Err(e);
            }
        };

        if let Err(e) = self.run_on_database(opts, &mut conn, true).await {
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
        is_readyset: bool,
    ) -> anyhow::Result<()> {
        let conditional_skip = |conditionals: &[Conditional]| {
            conditionals.iter().any(|s| match s {
                Conditional::SkipIf(c) if c == "readyset" => is_readyset,
                Conditional::OnlyIf(c) if c == "readyset" => !is_readyset,
                Conditional::SkipIf(c) => c == &opts.database_type.to_string(),
                Conditional::OnlyIf(c) => c != &opts.database_type.to_string(),
                _ => false,
            })
        };

        #[cfg(feature = "in-process-readyset")]
        let mut update_system_timezone = false;

        let retries = std::env::var("LOGICTEST_RETRIES")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(8);

        for record in &self.records {
            match record {
                Record::Statement(stmt) => {
                    if conditional_skip(&stmt.conditionals) {
                        continue;
                    }
                    #[cfg(feature = "in-process-readyset")]
                    if crate::in_process_readyset::might_be_timezone_changing_statement(
                        conn,
                        stmt.command.as_str(),
                    ) {
                        update_system_timezone = true;
                    }
                    debug!(command = stmt.command, "Running statement");
                    retry_with_exponential_backoff!(
                        {
                        self.run_statement(stmt, conn)
                            .await
                            .with_context(|| format!("Running statement {}", stmt.command))
                        },
                        retries: retries,
                        delay: 100,
                        backoff: 2,
                    )
                    .with_context(|| format!("Running statement with {retries} retries"))?;
                }

                Record::Query(query) => {
                    if conditional_skip(&query.conditionals) {
                        continue;
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

                    debug!(query = query.query, "Running query");

                    #[cfg(feature = "in-process-readyset")]
                    if update_system_timezone {
                        if let Err(err) =
                            crate::in_process_readyset::update_system_timezone(conn).await
                        {
                            error!(%err, "Failed to update system timezone")
                        }
                        update_system_timezone = false;
                    }

                    // 100 ms, 2x backoff
                    // 25.5 seconds total
                    match retry_with_exponential_backoff!(
                        {
                            {
                                let query_result = self
                                    .run_query(query, conn, is_readyset)
                                    .await
                                    .with_context(|| format!("Running query {}", query.query));

                                match (query_result, invert_result) {
                                    (Ok(_), true) => {
                                        Err(anyhow!("Expected failure: {}", query.query))
                                    }
                                    (Err(e), false) => Err(e),
                                    _ => Ok(()),
                                }
                            }
                        },
                        retries: retries,
                        delay: 100,
                        backoff: 2,
                    ) {
                        Ok(_) => {
                            if let Some((label, start)) = &timer {
                                let duration = start.elapsed();
                                debug!(label, "Query succeeded in {duration:?}");
                            };
                            Ok(())
                        }
                        Err(e) => Err(e.context(format!("Query failed after {retries} retries"))),
                    }?
                }
                Record::HashThreshold(_) => {}
                Record::Halt { .. } => break,
                Record::Sleep(msecs) => {
                    debug!(msecs, "sleep");
                    sleep(Duration::from_millis(*msecs)).await
                }
                Record::Graphviz => {
                    if is_readyset {
                        let graphviz: Vec<Vec<DfValue>> =
                            conn.simple_query("EXPLAIN GRAPHVIZ").await?.try_into()?;
                        let graphviz = graphviz[0][0]
                            .coerce_to(&DfType::Text(Collation::Utf8), &DfType::Unknown)?;
                        info!(graphviz = %graphviz.as_str().unwrap());
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
            StatementResult::Error { ref pattern } => match res {
                Err(e) => {
                    if let Some(pattern) = pattern {
                        if !pattern.is_empty()
                            && !regex::Regex::new(pattern).unwrap().is_match(&e.to_string())
                        {
                            bail!("Statement failed with unexpected error: {} (expected to match: {})", e, pattern);
                        }
                    }
                }
                Ok(_) => bail!("Statement should have failed, but succeeded"),
            },
        }
        Ok(())
    }

    async fn run_query(
        &self,
        query: &Query,
        conn: &mut DatabaseConnection,
        is_readyset: bool,
    ) -> anyhow::Result<()> {
        // If this is readyset, drop proxied queries, so that if we are retrying a SELECT and it was
        // previously unsupported (e.g. because a required table hadn't yet been replicated), we
        // will retry caching it instead of assuming it still can't be cached and just proxying it.
        //
        // TODO(REA-4799): remove this once the server tells the adapter about DDL and it
        // invalidates proxied queries
        if is_readyset {
            conn.query_drop("DROP ALL PROXIED QUERIES").await?;
        }

        let results = if query.params.is_empty() {
            conn.query(&query.query).await?
        } else {
            // We manually prepare and drop the statement, so that we can retry caching it if it was
            // previously unsupported.
            let stmt = conn.prepare(&query.query).await?;
            let results = conn.execute(&stmt, query.params.clone()).await?;
            conn.drop_prepared(stmt).await?;
            results
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
                                .with_context(|| {
                                    format!("Converting value {val:?} to {col_type:?}")
                                })?
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

        // If we are running against a remote readyset which could proxy, verify it didn't.
        if is_readyset {
            let explain_results = conn.simple_query("EXPLAIN LAST STATEMENT").await?;
            let explain_values: Vec<Vec<DfValue>> = explain_results.try_into()?;
            if let Some(explain) = explain_values.first() {
                let mut strings = explain.iter().map(|v| {
                    v.coerce_to(&DfType::Text(Collation::Utf8), &DfType::Unknown)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string()
                });
                let destination = strings.next().map(|s| s.try_into()).transpose()?;
                let status = strings.next().unwrap_or("no status".to_string());
                if let Some(destination) = destination {
                    if !matches!(destination, QueryDestination::Readyset(_)) {
                        bail!("Query destination should be readyset, was {destination}: {status}");
                    }
                } else {
                    bail!("Could not get destination");
                }
            }
        }

        Ok(())
    }

    /// Get a reference to the test script's records.
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}
