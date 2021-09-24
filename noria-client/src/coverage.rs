//! Data structures for recording (user-facing) statistics about queries run during the execution of
//! a noria-client adapter

#![allow(dead_code)] // TODO: remove once this is used
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use serde::Serialize;

use noria::ReadySetError;

#[derive(Debug, Serialize, Default)]
pub(crate) struct PrepareEvent {
    /// How long the prepare request took to run on the upstream database
    pub(crate) upstream_duration: Duration,

    /// How long the prepare request took to run on noria, if it was run on noria at all
    pub(crate) noria_duration: Option<Duration>,

    /// Error returned by noria, if any
    pub(crate) noria_error: Option<String>,
}

pub(crate) struct MaybePrepareEvent(Option<PrepareEvent>);

impl MaybePrepareEvent {
    pub(crate) fn new(mirror_reads: bool) -> Self {
        let event = if mirror_reads {
            Some(Default::default())
        } else {
            None
        };
        MaybePrepareEvent(event)
    }

    pub(crate) fn start_timer(&mut self) -> PrepareTimerHandle {
        if let Some(ref mut e) = self.0 {
            PrepareTimerHandle::new(Some(e))
        } else {
            PrepareTimerHandle::new(None)
        }
    }

    pub(crate) fn set_noria_error(&mut self, error: &ReadySetError) -> &mut Self {
        if let Some(mut e) = self.0.take() {
            e.noria_error = Some(format!("{:?}", error));
            self.0 = Some(e)
        }
        self
    }
}

impl From<MaybePrepareEvent> for Option<PrepareEvent> {
    fn from(e: MaybePrepareEvent) -> Self {
        e.0
    }
}

pub(crate) struct PrepareTimerHandle<'a> {
    event: Option<&'a mut PrepareEvent>,
    timer: Option<Instant>,
}

impl<'a> PrepareTimerHandle<'a> {
    pub(crate) fn new(event: Option<&'a mut PrepareEvent>) -> PrepareTimerHandle {
        let timer = if event.is_some() {
            Some(Instant::now())
        } else {
            None
        };
        PrepareTimerHandle { event, timer }
    }

    pub(crate) fn set_upstream_duration(mut self) {
        if let (Some(mut e), Some(t)) = (self.event.take(), self.timer.take()) {
            e.upstream_duration = t.elapsed();
        }
    }

    pub(crate) fn set_noria_duration(mut self) {
        if let (Some(mut e), Some(t)) = (self.event.take(), self.timer.take()) {
            e.noria_duration = Some(t.elapsed());
        }
    }
}

#[derive(Debug, Serialize, Default)]
pub(crate) struct ExecuteEvent {
    /// How long the execute request took to run on the upstream database
    pub(crate) upstream_duration: Duration,

    /// How long the execute request took to run on noria, if it was run on noria at all
    pub(crate) noria_duration: Option<Duration>,

    /// Error returned by noria, if any
    pub(crate) noria_error: Option<String>,

    /// True if the results returned by upstream and noria differed
    pub(crate) results_differed: bool,
}

pub(crate) struct MaybeExecuteEvent(Option<ExecuteEvent>);

impl MaybeExecuteEvent {
    pub(crate) fn new(mirror_reads: bool) -> Self {
        let event = if mirror_reads {
            Some(Default::default())
        } else {
            None
        };
        MaybeExecuteEvent(event)
    }

    pub(crate) fn start_timer(&mut self) -> ExecuteTimerHandle {
        if let Some(ref mut e) = self.0 {
            ExecuteTimerHandle::new(Some(e))
        } else {
            ExecuteTimerHandle::new(None)
        }
    }

    pub(crate) fn set_noria_error(&mut self, error: &ReadySetError) -> &mut Self {
        if let Some(mut e) = self.0.take() {
            e.noria_error = Some(format!("{:?}", error));
            self.0 = Some(e)
        }
        self
    }

    pub(crate) fn set_results_differed(&mut self, differed: bool) -> &mut Self {
        if let Some(mut e) = self.0.take() {
            e.results_differed = differed;
            self.0 = Some(e)
        }
        self
    }
}

impl From<MaybeExecuteEvent> for Option<ExecuteEvent> {
    fn from(e: MaybeExecuteEvent) -> Self {
        e.0
    }
}

pub(crate) struct ExecuteTimerHandle<'a> {
    event: Option<&'a mut ExecuteEvent>,
    timer: Option<Instant>,
}

impl<'a> ExecuteTimerHandle<'a> {
    pub(crate) fn new(event: Option<&'a mut ExecuteEvent>) -> ExecuteTimerHandle {
        let timer = if event.is_some() {
            Some(Instant::now())
        } else {
            None
        };
        ExecuteTimerHandle { event, timer }
    }

    pub(crate) fn set_upstream_duration(mut self) {
        if let (Some(mut e), Some(t)) = (self.event.take(), self.timer.take()) {
            e.upstream_duration = t.elapsed();
        }
    }

    pub(crate) fn set_noria_duration(mut self) {
        if let (Some(mut e), Some(t)) = (self.event.take(), self.timer.take()) {
            e.noria_duration = Some(t.elapsed());
        }
    }
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
    /// Queries that have been run during the execution of an adapter
    queries: HashMap<String, QueryInfo>,

    /// Prepared statements that have been run associated with their prepared statement id. This is
    /// used to map prepared statements to their respective execute events.
    prepared: HashMap<u32, String>,

    /// Full database schema for the upstream db. If none, schema has not been recorded yet.
    ///
    /// This may have to become a richer data type at some point in the future.
    schema: Option<String>,
}

impl QueryCoverageInfo {
    fn new() -> Self {
        Self {
            queries: HashMap::new(),
            prepared: HashMap::new(),
            schema: None,
        }
    }

    /// Record in this QueryCoverageInfo that a query was prepared
    fn query_prepared(&mut self, query: String, event: PrepareEvent, statement_id: u32) {
        self.queries
            .entry(query.clone())
            .or_default()
            .prepare_events
            .push(event);
        self.prepared.entry(statement_id).or_insert(query);
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

    fn prepare_executed(&mut self, statement_id: u32, event: ExecuteEvent) {
        if let Some(query) = self.prepared.get(&statement_id) {
            self.queries
                .entry(query.to_owned())
                .or_default()
                .execute_events
                .push(event)
        }
    }

    /// Serialize this QueryCoverageInfo to JSON.
    fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(&self)?)
    }
}

/// A reference to a [`QueryCoverageInfo`] shared between multiple connections
// NOTE: see
// https://docs.rs/tokio/1.11.0/tokio/sync/struct.Mutex.html#which-kind-of-mutex-should-you-use for
// why this is a std::sync::Mutex rather than a tokio::sync::Mutex
#[derive(Clone, Copy, Debug)]
pub struct QueryCoverageInfoRef(&'static Mutex<QueryCoverageInfo>);

impl QueryCoverageInfoRef {
    /// Record that a query was prepared
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn query_prepared(&self, query: String, event: PrepareEvent, statement_id: u32) {
        self.0
            .lock()
            .unwrap()
            .query_prepared(query, event, statement_id)
    }

    /// Record that a query was executed.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn query_executed(&self, query: String, event: ExecuteEvent) {
        self.0.lock().unwrap().query_executed(query, event)
    }

    /// Record that a query was executed that was previously prepared.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn prepare_executed(&self, statement_id: u32, event: ExecuteEvent) {
        self.0.lock().unwrap().prepare_executed(statement_id, event)
    }

    /// Serialize to JSON and return the resulting blob.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        self.0.lock().unwrap().serialize()
    }
}

impl Default for QueryCoverageInfoRef {
    /// Allocate a shared [`QueryCoverageInfo`] on the heap that lives for the lifetime of the
    /// program, and return a cloneable reference to it
    fn default() -> Self {
        Self(Box::leak(Box::new(Mutex::new(QueryCoverageInfo::new()))))
    }
}
