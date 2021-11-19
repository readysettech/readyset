use metrics::SharedString;
use nom_sql::SqlQuery;
use noria::ReadySetError;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    pub noria_error: Option<String>,
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
        }
    }

    pub fn start_timer(&mut self) -> QueryExecutionTimerHandle {
        QueryExecutionTimerHandle::new(self)
    }

    pub fn set_noria_error(&mut self, error: &ReadySetError) {
        self.noria_error = Some(format!("{:?}", error));
    }
}

/// A handle to updating the durations in a `QueryExecutionEvent`. Each duration
/// should correspond to a call to `end_X`, where X is the
/// measuremnt we are timing.
pub struct QueryExecutionTimerHandle<'a> {
    event: &'a mut QueryExecutionEvent,
    timer: Instant,
}

impl<'a> QueryExecutionTimerHandle<'a> {
    pub fn new(event: &'a mut QueryExecutionEvent) -> QueryExecutionTimerHandle<'a> {
        QueryExecutionTimerHandle {
            event,
            timer: Instant::now(),
        }
    }

    pub fn set_upstream_duration(self) {
        self.event.upstream_duration = Some(self.timer.elapsed());
    }

    pub fn set_noria_duration(self) {
        self.event.noria_duration = Some(self.timer.elapsed());
    }

    pub fn set_parse_duration(self) {
        self.event.parse_duration = Some(self.timer.elapsed());
    }
}
