//! Documents the set of metrics that are currently being recorded within
//! a noria-client.

/// Histogram: The time in seconds that the database spent executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | database_type | The database type being executed. Must be a [`DatabaseType`] |
/// | query_type | SqlQueryType, whether the query was a read or write. |
/// | event_type | EventType, whether the query was a prepare, execute, or query.  |
///
/// [`DatabaseType`]: crate::DatabaseType
pub const QUERY_LOG_EXECUTION_TIME: &str = "readyset_query_log_execution_time";

/// Histogram: The time in seconds that the database spent executing a
/// query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | query_type | SqlQueryType, whether the query was a read or write. |
/// | event_type | EventType, whether the query was a prepare, execute, or query.  |
pub const QUERY_LOG_PARSE_TIME: &str = "readyset_query_log_parse_time";

/// Counter: The number of individual keys read for a query. This will be greater than the number of
/// times the query was executed in the case of `IN` queries.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_TOTAL_KEYS_READ: &str = "readyset_query_log_total_keys_read";

/// Counter: The number of cache misses which occurred, potentially multiple from a single query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_TOTAL_CACHE_MISSES: &str = "readyset_query_log_total_cache_misses";

/// Counter: The number of queries which encountered at least one cache miss.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_QUERY_CACHE_MISSED: &str = "readyset_query_log_query_cache_missed";

/// Counter: The number of successful queries (dry runs/real) processed by the migration handler.
pub const MIGRATION_HANDLER_SUCCESSES: &str = "readyset_migration_handler_successes";

/// Counter: The number of failed queries (dry runs/real) processed by the migration handler.
pub const MIGRATION_HANDLER_FAILURES: &str = "readyset_migration_handler_failures";

/// Counter: The number of queries the migration handler has set to allowed.  Incremented on each
/// loop of the migration handler.
/// TODO(justin): In the future it would be good to support gauges for the counts of each query
/// status in the query status cache. Requires optimization of locking.
pub const MIGRATION_HANDLER_ALLOWED: &str = "readyset_migration_handler_allowed";

/// Counter: The number of HTTP requests received at the noria-client.
pub const ADAPTER_EXTERNAL_REQUESTS: &str = "readyset_noria_client_external_requests";

/// Gauge: The number of currently connected SQL clients
pub const CONNECTED_CLIENTS: &str = "readyset_noria_client_connected_clients";

/// Counter: The number of queries that failed to parse.
pub const QUERY_LOG_PARSE_ERRORS: &str = "readyset_query_log_parse_errors";

/// Counter: The number of SET statements that were disallowed.
pub const QUERY_LOG_SET_DISALLOWED: &str = "readyset_query_log_set_disallowed";
