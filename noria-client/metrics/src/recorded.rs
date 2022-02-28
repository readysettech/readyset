//! Documents the set of metrics that are currently being recorded within
//! a noria-client.

/// Histogram: The time in seconds that the database spent
/// executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | database_type | The database type being executed. Must be a ['DatabaseType'] |
/// | query_type | SqlQueryType, whether the query was a read or write. |
/// | event_type | EventType, whether the query was a prepare, execute, or query.  |
pub const QUERY_LOG_EXECUTION_TIME: &str = "query-log.execution_time";

/// Histogram: The time in seconds that the database spent executing a
/// query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | query_type | SqlQueryType, whether the query was a read or write. |
/// | event_type | EventType, whether the query was a prepare, execute, or query.  |
pub const QUERY_LOG_PARSE_TIME: &str = "query-log.parse_time";

/// Counter: The number of cache misses which occurred, potentially multiple from a single query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_TOTAL_CACHE_MISSES: &str = "query-log.total_cache_misses";

/// Counter: The number of queries which encountered at least one cache miss.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_QUERY_CACHE_MISSED: &str = "query-log.query_cache_missed";

/// Counter: The total number of queries processing by the migration handler.
/// Incremented on each loop of the migration handler.
pub const MIGRATION_HANDLER_PROCESSED: &str = "migration-handler.processed";

/// Counter: The number of queries themigration handler has set to allowed.
/// Incremented on each loop of the migration handler.
/// TODO(justin): In the future it would be good to support gauges for the
/// counts of each query status in the query status cache. Requires
/// optimization of locking.
pub const MIGRATION_HANDLER_ALLOWED: &str = "migration-handler.allowed";

/// Counter: The number of HTTP requests received at the noria-client.
pub const ADAPTER_EXTERNAL_REQUESTS: &str = "noria-client.external_requests";

/// Gauge: The number of currently connected SQL clients
pub const CONNECTED_CLIENTS: &str = "noria-client.connected_clients";
