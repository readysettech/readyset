//! Documents the set of metrics that are currently being recorded within
//! a noria-client.
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

/// The type of a SQL query.
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

/// Identifies the database that is being adapted to
/// communicate with Noria.
pub enum DatabaseType {
    Mysql,
    Psql,
}

impl From<DatabaseType> for String {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => "mysql".to_owned(),
            DatabaseType::Psql => "psql".to_owned(),
        }
    }
}
