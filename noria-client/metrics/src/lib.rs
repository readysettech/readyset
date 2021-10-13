use nom_sql::SqlQuery;
use noria::ReadySetError;
use serde::Serialize;
use std::time::{Duration, Instant};

pub mod recorded;

#[derive(Debug, Serialize, Clone)]
/// Event logging for the execution of a single query in the adapter. Durations
/// logged should be mirrored by an update to `QueryExecutionTimerHandle`.
pub struct QueryExecutionEvent {
    pub event: EventType,

    /// SqlQuery associated with this execution event.
    pub query: Option<SqlQuery>,

    /// How long the execute request took to run on the upstream database
    pub upstream_duration: Option<Duration>,

    /// How long the execute request took to run on noria, if it was run on noria at all
    pub noria_duration: Option<Duration>,

    /// Error returned by noria, if any.
    pub noria_error: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub enum EventType {
    Prepare,
    Execute,
}

impl QueryExecutionEvent {
    pub fn new(t: EventType) -> Self {
        Self {
            event: t,
            query: None,
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
}
