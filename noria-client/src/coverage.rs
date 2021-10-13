//! Data structures for recording (user-facing) statistics about queries run during the execution of
//! a noria-client adapter

#![allow(dead_code)] // TODO: remove once this is used
use std::collections::HashMap;
use std::sync::Mutex;

use noria_client_metrics::QueryExecutionEvent;
use serde::Serialize;

/// Data structure representing information about a single, unique query run against an adapter
#[derive(Debug, Default, Serialize)]
pub(crate) struct QueryInfo {
    /// Information about times this query was prepared
    prepare_events: Vec<QueryExecutionEvent>,

    /// Information about times this query was executed, either directly or via executing a prepared
    /// statement
    execute_events: Vec<QueryExecutionEvent>,
}

type UpstreamStatementId = u32;
type NoriaStatementId = u32;

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
    prepared: HashMap<UpstreamStatementId, String>,

    /// Provides a mapping between upstream statement ids, and noria statement ids so we can map
    /// between the two, and ensure that when QCA is enabled, we always run executes against both
    /// upstream and noria.
    statement_id_map: HashMap<UpstreamStatementId, NoriaStatementId>,

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
            statement_id_map: HashMap::new(),
            schema: None,
        }
    }

    fn link_statement_ids(
        &mut self,
        upstream_statement_id: UpstreamStatementId,
        noria_statement_id: NoriaStatementId,
    ) {
        self.statement_id_map
            .insert(upstream_statement_id, noria_statement_id);
    }

    fn noria_statement_id(
        &mut self,
        upstream_statement_id: UpstreamStatementId,
    ) -> Option<&NoriaStatementId> {
        self.statement_id_map.get(&upstream_statement_id)
    }

    /// Record in this QueryCoverageInfo that a query was prepared
    fn query_prepared(&mut self, query: String, event: QueryExecutionEvent, statement_id: u32) {
        self.queries
            .entry(query.clone())
            .or_default()
            .prepare_events
            .push(event);
        self.prepared.entry(statement_id).or_insert(query);
    }

    /// Record in this QueryCoverageInfo that a query was executed, either directly or via executing
    /// a prepared statement
    fn query_executed(&mut self, query: String, event: QueryExecutionEvent) {
        self.queries
            .entry(query)
            .or_default()
            .execute_events
            .push(event)
    }

    fn prepare_executed(&mut self, statement_id: u32, event: QueryExecutionEvent) {
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
    pub(crate) fn query_prepared(
        &self,
        query: String,
        event: QueryExecutionEvent,
        statement_id: u32,
    ) {
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
    pub(crate) fn query_executed(&self, query: String, event: QueryExecutionEvent) {
        self.0.lock().unwrap().query_executed(query, event)
    }

    /// Record that a query was executed that was previously prepared.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn prepare_executed(&self, statement_id: u32, event: QueryExecutionEvent) {
        self.0.lock().unwrap().prepare_executed(statement_id, event)
    }

    /// Links two statement ids together so we can coordinate logging prepares and executes
    /// together between upstream and noria.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn link_statement_ids(&self, upstream_statement_id: u32, noria_statement_id: u32) {
        self.0
            .lock()
            .unwrap()
            .link_statement_ids(upstream_statement_id, noria_statement_id)
    }

    /// Retrieves the noria statement id that is linked to the provided upstream statement id.
    ///
    /// # Panics
    ///
    /// Panics if the backing mutex has been poisoned (this should generally only happen in
    /// exceptional cases)
    pub(crate) fn noria_statement_id(&self, upstream_statement_id: u32) -> Option<u32> {
        if let Some(id) = self
            .0
            .lock()
            .unwrap()
            .noria_statement_id(upstream_statement_id)
        {
            let noria_statement_id = *id;
            Some(noria_statement_id)
        } else {
            None
        }
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
