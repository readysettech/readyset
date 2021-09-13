//! Data structures for recording (user-facing) statistics about queries run during the execution of
//! a noria-client adapter

#![allow(dead_code)] // TODO: remove once this is used
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use noria::ReadySetError;

#[derive(Debug, Serialize)]
pub(crate) struct PrepareEvent {
    /// How long the prepare request took to run on the upstream database
    pub(crate) upstream_duration: Duration,

    /// How long the prepare request took to run on noria, if it was run on noria at all
    pub(crate) noria_duration: Option<Duration>,

    /// Error returned by noria, if any
    pub(crate) noria_error: Option<ReadySetError>,

    /// Whether this prepare event was run via fallback to an upstream database.
    ///
    /// This can be true if either the query was attempted to be run via noria and failed, or if
    /// execution rules (eg transactions) meant the query had to be run via fallback
    pub(crate) is_fallback: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExecuteEvent {
    /// How long the prepare request took to run on the upstream database
    pub(crate) upstream_duration: Duration,

    /// How long the prepare request took to run on noria, if it was run on noria at all
    pub(crate) noria_duration: Option<Duration>,

    /// Error returned by noria, if any
    pub(crate) noria_error: Option<ReadySetError>,

    /// True if the results returned by upstream and noria differed
    pub(crate) results_differed: bool,

    /// Whether this execute event was run via fallback to an upstream database.
    ///
    /// This can be true if either the query was attempted to be run via noria and failed, or if
    /// execution rules (eg transactions) meant the query had to be run via fallback
    pub(crate) is_fallback: bool,
}

/// Data structure representing information about a single, unique query run against an adapter
#[derive(Debug, Default, Serialize)]
pub(crate) struct QueryInfo {
    /// Information about times this query was prepared
    prepare_events: Vec<PrepareEvent>,

    /// Information about times this query was executed, either directly or via executing a prepared
    /// statement
    execute_events: Vec<ExecuteEvent>,
}

/// Data structure representing information about the queries that have been run during the
/// execution of an adapter, and statistics about those queries.
///
/// See [this design doc][design-doc] for more information
///
/// [design-doc]: https://docs.google.com/document/d/1i2HYLxANhJX4BxBnYeEzLO6sTecE4HkLoN31vXDlFCM/edit
#[derive(Debug, Serialize)]
struct QueryCoverageInfo {
    /// Path to which to write when save() is called
    #[serde(skip)]
    file: PathBuf,

    /// Queries that have been run during the execution of an adapter
    queries: HashMap<String, QueryInfo>,

    /// Full database schema for the upstream db. If none, schema has not been recorded yet.
    ///
    /// This may have to become a richer data type at some point in the future.
    schema: Option<String>,
}

impl QueryCoverageInfo {
    fn new(file: PathBuf) -> Self {
        Self {
            file,
            queries: HashMap::new(),
            schema: None,
        }
    }

    /// Record in this QueryCoverageInfo that a query was prepared
    fn query_prepared(&mut self, query: String, event: PrepareEvent) {
        self.queries
            .entry(query)
            .or_default()
            .prepare_events
            .push(event)
    }

    /// Record in this QueryCoverageInfo that a query was executed, either directly or via executing
    /// a prepared statement
    fn query_executed(&mut self, query: String, event: ExecuteEvent) {
        self.queries
            .entry(query)
            .or_default()
            .execute_events
            .push(event)
    }

    /// Serialize this QueryCoverageInfo to JSON and write it to self.file.  Takes care of
    /// opening/flushing/closing the file on its own.
    pub async fn save(&self) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .append(false)
            .truncate(true)
            .create(true)
            .open(&self.file)
            .await?;
        let data = serde_json::to_vec_pretty(&self)?;
        file.write_all(&data).await?;
        Ok(())
    }
}

/// A reference to a [`QueryCoverageInfo`] shared between multiple connections
// NOTE: see
// https://docs.rs/tokio/1.11.0/tokio/sync/struct.Mutex.html#which-kind-of-mutex-should-you-use for
// why this is a std::sync::Mutex rather than a tokio::sync::Mutex
#[derive(Clone, Copy, Debug)]
pub struct QueryCoverageInfoRef(&'static Mutex<QueryCoverageInfo>);

impl QueryCoverageInfoRef {
    /// Allocate a shared [`QueryCoverageInfo`] on the heap that lives for the lifetime of the
    /// program, and return a cloneable reference to it
    pub fn new(path: PathBuf) -> Self {
        Self(Box::leak(Box::new(Mutex::new(QueryCoverageInfo::new(
            path,
        )))))
    }

    /// Record that a query was prepared
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn query_prepared(&self, query: String, event: PrepareEvent) {
        self.0.lock().unwrap().query_prepared(query, event)
    }

    /// Record that a query was executed
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn query_executed(&self, query: String, event: ExecuteEvent) {
        self.0.lock().unwrap().query_executed(query, event)
    }

    /// Serialize to JSON and write to the file that was passed into new().
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub async fn save(&self) -> anyhow::Result<()> {
        self.0.lock().unwrap().save().await
    }
}
