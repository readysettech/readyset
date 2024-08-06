use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use metrics::SharedString;
use nom_sql::{Relation, SqlIdentifier, SqlQuery};
use readyset_client::query::QueryId;
use readyset_errors::ReadySetError;
use serde::Serialize;

pub mod recorded;

/// Similar to logging levels, this enum allows control over how much data is
/// recorded about queries for reporting into metrics systems. Each enum value,
/// starting at `Disabled`, includes all of the preceeding (lower) level's metric
/// details. This gradation attempts to make a reasonable tradeoff of metrics
/// payload size vs. verbosity/debugability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum QueryLogMode {
    /// Do not capture query metrics at all.
    Disabled,
    /// Record query metrics for all queries, including ad-hoc (simple) queries
    /// as well as prepared statements.
    Enabled,
    /// Expert-mode. Captures the full suite of developer-level query metrics.
    /// Not advisable for a production environment, since it includes metric
    /// labels that may have very high cardinality.
    Verbose,
}

impl QueryLogMode {
    pub fn is_enabled(&self) -> bool {
        !matches!(self, QueryLogMode::Disabled)
    }

    pub fn is_verbose(&self) -> bool {
        matches!(self, QueryLogMode::Verbose)
    }
}

#[derive(Debug, Default, Clone)]
pub enum QueryIdWrapper {
    /// Caller is providing an already-calculated `QueryId`.
    Calculated(QueryId),

    /// Caller wants the `QueryId` to be recalculated by the logger engine
    /// (outside of the hot path). Must send along the schema search path
    /// for the `query` associated with this event.
    Uncalculated(Vec<SqlIdentifier>),

    #[default]
    None,
}

impl From<Option<QueryId>> for QueryIdWrapper {
    fn from(query_id: Option<QueryId>) -> Self {
        query_id.map(Self::Calculated).unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
/// Event logging for the execution of a single query in the adapter. Durations
/// logged should be mirrored by an update to `QueryExecutionTimerHandle`.
pub struct QueryExecutionEvent {
    pub event: EventType,
    pub sql_type: SqlQueryType,

    /// SqlQuery associated with this execution event.
    pub query: Option<Arc<SqlQuery>>,

    /// If query has an assigned readyset id
    pub query_id: QueryIdWrapper,

    /// How long the request spent in parsing.
    pub parse_duration: Option<Duration>,

    /// How long the execute request took to run on the upstream database
    pub upstream_duration: Option<Duration>,

    pub readyset_event: Option<ReadysetExecutionEvent>,

    /// Error returned by noria, if any.
    pub noria_error: Option<ReadySetError>,

    /// Where the query ended up executing
    pub destination: Option<QueryDestination>,
}

#[derive(Debug, Clone)]
pub enum ReadysetExecutionEvent {
    CacheRead {
        /// If query has a cache, the name of the cache
        cache_name: Relation,

        /// The number of keys that were read
        num_keys: u64,

        /// Number of cache misses which occurred as part of a query
        cache_misses: u64,

        /// How long the execute request took to run on ReadySet
        duration: Duration,
    },
    /// A SQL extension, prepare, write (in RYW mode)
    Other {
        /// How long the execute request took to run on ReadySet
        duration: Duration,
    },
}

impl ReadysetExecutionEvent {
    pub fn duration(&self) -> Duration {
        match self {
            Self::CacheRead { duration, .. } => *duration,
            Self::Other { duration, .. } => *duration,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Clone, Copy, Default)]
pub enum QueryDestination {
    #[default]
    Readyset,
    ReadysetThenUpstream,
    Upstream,
    Both,
}

impl TryFrom<&str> for QueryDestination {
    type Error = ReadySetError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "readyset" => Ok(QueryDestination::Readyset),
            "readyset_then_upstream" => Ok(QueryDestination::ReadysetThenUpstream),
            "upstream" => Ok(QueryDestination::Upstream),
            "both" => Ok(QueryDestination::Both),
            _ => Err(ReadySetError::Internal(
                "Invalid query destination".to_string(),
            )),
        }
    }
}

impl fmt::Display for QueryDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            QueryDestination::Readyset => "readyset",
            QueryDestination::ReadysetThenUpstream => "readyset_then_upstream",
            QueryDestination::Upstream => "upstream",
            QueryDestination::Both => "both",
        };
        write!(f, "{}", s)
    }
}

#[derive(Copy, Debug, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventType {
    Prepare,
    Execute,
    Query,
}

impl From<EventType> for SharedString {
    fn from(event: EventType) -> Self {
        match event {
            EventType::Prepare => SharedString::const_str("prepare"),
            EventType::Execute => SharedString::const_str("execute"),
            EventType::Query => SharedString::const_str("query"),
        }
    }
}

/// The type of a SQL query.
#[derive(Copy, Serialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SqlQueryType {
    Read,
    Write,
    Other,
}

// Implementing this so it can be used directly as a metric label.
impl From<SqlQueryType> for SharedString {
    fn from(query_type: SqlQueryType) -> Self {
        match query_type {
            SqlQueryType::Read => SharedString::const_str("read"),
            SqlQueryType::Write => SharedString::const_str("write"),
            SqlQueryType::Other => SharedString::const_str("other"),
        }
    }
}

/// Identifies the database that this metric corresponds to.
#[derive(Debug)]
pub enum DatabaseType {
    MySql,
    Psql,
    ReadySet,
}

impl From<DatabaseType> for SharedString {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::MySql => SharedString::const_str("mysql"),
            DatabaseType::Psql => SharedString::const_str("psql"),
            DatabaseType::ReadySet => SharedString::const_str("readyset"),
        }
    }
}

impl From<DatabaseType> for String {
    fn from(database_type: DatabaseType) -> Self {
        SharedString::from(database_type).into_owned()
    }
}

impl QueryExecutionEvent {
    pub fn new(t: EventType) -> Self {
        Self {
            event: t,
            sql_type: SqlQueryType::Other,
            query: None,
            query_id: QueryIdWrapper::None,
            parse_duration: None,
            upstream_duration: None,
            readyset_event: None,
            noria_error: None,
            destination: None,
        }
    }

    pub fn start_upstream_timer(&mut self) -> QueryExecutionTimerHandle {
        QueryExecutionTimerHandle::new(&mut self.upstream_duration)
    }

    pub fn start_parse_timer(&mut self) -> QueryExecutionTimerHandle {
        QueryExecutionTimerHandle::new(&mut self.parse_duration)
    }

    pub fn set_noria_error(&mut self, error: &ReadySetError) {
        self.noria_error = Some(error.clone());
    }
}

/// A handle to updating the durations in a `QueryExecutionEvent`. Once dropped,
/// updates the relevant timer.
pub struct QueryExecutionTimerHandle<'a> {
    duration: &'a mut Option<Duration>,
    start: Instant,
}

impl<'a> QueryExecutionTimerHandle<'a> {
    pub fn new(duration: &'a mut Option<Duration>) -> QueryExecutionTimerHandle<'a> {
        QueryExecutionTimerHandle {
            duration,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for QueryExecutionTimerHandle<'a> {
    fn drop(&mut self) {
        self.duration.replace(self.start.elapsed());
    }
}
