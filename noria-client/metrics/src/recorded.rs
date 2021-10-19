//! Documents the set of metrics that are currently being recorded within
//! a noria-client.
use std::fmt::Formatter;

use metrics::SharedString;

/// Histogram: The time in microseconds that the Noria adapter spent
/// parsing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_type | The type of query that was being parsed. Must be a [`SqlQueryType`] |
pub const QUERY_PARSING_TIME: &str = "noria-client.parsing_time";

/// Histogram: The time in microseconds that the Noria adapter spent
/// executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_type | The type of query that was being executed. Must be a [`SqlQueryType`] |
pub const QUERY_EXECUTION_TIME: &str = "noria-client.execution_time";

/// Histogram: The time in microseconds that the database spent
/// executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | database_type | The database type being executed. Must be a ['DatabaseType'] |
pub const QUERY_LOG_EXECUTION_TIME: &str = "query-log.execution_time";

/// Counter: The total number of queries processing by the query reconciler.
/// Incremented on each loop of the reconciler.
pub const RECONCILER_PROCESSED: &str = "reconciler.processed";

/// Counter: The number of queries the reconciler has set to allowed.
/// Incremented on each loop of the reconciler.
/// TODO(justin): In the future it would be good to support gauges for the
/// counts of each query status in the reconciler cache. Requires optimization of
/// locking.
pub const RECONCILER_ALLOWED: &str = "reconciler.allowed";

/// Counter: The number of HTTP requests received at the noria-client.
pub const ADAPTER_EXTERNAL_REQUESTS: &str = "noria-client.external_requests";

/// A query log entry representing the time spent executing a single
/// query.
pub struct QueryLogEntry {
    pub query: String,
    pub database_type: DatabaseType,
    pub execution_time: f64,
}

impl std::fmt::Display for QueryLogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\t{:?}\t{}",
            self.query, self.database_type, self.execution_time
        )
    }
}

/// The type of a SQL query.
#[derive(Copy, Clone)]
pub enum SqlQueryType {
    /// Read query.
    Read,
    /// Write query.
    Write,
}

// Implementing this so it can be used directly as a metric label.
impl From<SqlQueryType> for SharedString {
    fn from(query_type: SqlQueryType) -> Self {
        match query_type {
            SqlQueryType::Read => SharedString::const_str("read"),
            SqlQueryType::Write => SharedString::const_str("write"),
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
