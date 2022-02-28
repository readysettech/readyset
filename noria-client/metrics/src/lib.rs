#![feature(derive_default_enum)]

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics::SharedString;
use nom_sql::SqlQuery;
use noria::ReadySetError;
use serde::Serialize;

pub mod recorded;

#[derive(Debug, Serialize, Clone)]
/// Event logging for the execution of a single query in the adapter. Durations
/// logged should be mirrored by an update to `QueryExecutionTimerHandle`.
pub struct QueryExecutionEvent {
    pub event: EventType,
    pub sql_type: SqlQueryType,

    /// SqlQuery associated with this execution event.
    pub query: Option<Arc<SqlQuery>>,

    /// How long the request spent in parsing.
    pub parse_duration: Option<Duration>,

    /// How long the execute request took to run on the upstream database
    pub upstream_duration: Option<Duration>,

    /// How long the execute request took to run on noria, if it was run on noria at all
    pub noria_duration: Option<Duration>,

    /// Error returned by noria, if any.
    pub noria_error: Option<ReadySetError>,

    /// Where the query ended up executing
    pub destination: Option<QueryDestination>,

    /// Number of cache misses which occurred as part of a query
    pub cache_misses: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize, Clone, Copy, Default)]
pub enum QueryDestination {
    #[default]
    Noria,
    NoriaThenFallback,
    Fallback,
    Both,
}

impl TryFrom<&str> for QueryDestination {
    type Error = ReadySetError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "readyset" => Ok(QueryDestination::Noria),
            "readyset_then_fallback" => Ok(QueryDestination::NoriaThenFallback),
            "fallback" => Ok(QueryDestination::Fallback),
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
            QueryDestination::Noria => "readyset",
            QueryDestination::NoriaThenFallback => "readyset_then_fallback",
            QueryDestination::Fallback => "fallback",
            QueryDestination::Both => "both",
        };
        write!(f, "{}", s)
    }
}

#[derive(Copy, Debug, Serialize, Clone)]
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
#[derive(Copy, Serialize, Clone, Debug)]
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
    Mysql,
    Psql,
    Noria,
}

impl From<DatabaseType> for String {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => "mysql".to_owned(),
            DatabaseType::Psql => "psql".to_owned(),
            DatabaseType::Noria => "noria".to_owned(),
        }
    }
}

impl QueryExecutionEvent {
    pub fn new(t: EventType) -> Self {
        Self {
            event: t,
            sql_type: SqlQueryType::Other,
            query: None,
            parse_duration: None,
            upstream_duration: None,
            noria_duration: None,
            noria_error: None,
            destination: None,
            cache_misses: None,
        }
    }

    pub fn start_noria_timer(&mut self) -> QueryExecutionTimerHandle {
        QueryExecutionTimerHandle::new(&mut self.noria_duration)
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
