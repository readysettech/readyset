//! Documents the set of metrics that are currently being recorded within
//! a noria-client.

/// Histogram: The time in microseconds that the database spent executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | database_type | The database type being executed. Must be a [`DatabaseType`] |
/// | query_d | The hashed ID of the query. |
/// | cache_name | Optional. The cache name if the query is cached. |
///
/// [`DatabaseType`]: crate::DatabaseType
pub const QUERY_LOG_EXECUTION_TIME: &str = "readyset_query_log_execution_time_us";
pub const QUERY_LOG_EXECUTION_COUNT: &str = "readyset_query_log_execution_count";

/// Histogram: The time in microseconds that the database spent executing a
/// query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
/// | query_type | SqlQueryType, whether the query was a read or write. |
/// | event_type | EventType, whether the query was a prepare, execute, or query.  |
/// | query_d | The hashed ID of the query. |
pub const QUERY_LOG_PARSE_TIME: &str = "readyset_query_log_parse_time_us";

/// Counter: The number of individual keys read for a query. This will be greater than the number of
/// times the query was executed in the case of `IN` queries.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this key read. |
pub const QUERY_LOG_TOTAL_KEYS_READ: &str = "readyset_query_log_total_keys_read";

/// Counter: The number of cache misses which occurred, potentially multiple from a single query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this key read. |
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

/// Counter: The number of connections opened by clients
pub const CLIENT_CONNECTIONS_OPENED: &str = "readyset_noria_client_conns_opened";

/// Counter: The number of connections closed by clients
pub const CLIENT_CONNECTIONS_CLOSED: &str = "readyset_noria_client_conns_closed";

/// Gauge: The number of open connections to the upstream database,
/// on behalf of client connections.
pub const CLIENT_UPSTREAM_CONNECTIONS: &str = "readyset_client_upstream_connections";

/// Counter: The number of `EventType` operations received (query / prepare / execute).
pub const QUERY_LOG_EVENT_TYPE: &str = "readyset_query_log_event_type";

/// Counter: The number of queries that failed to parse.
pub const QUERY_LOG_PARSE_ERRORS: &str = "readyset_query_log_parse_errors";

/// Counter: The number of SET statements that were disallowed.
pub const QUERY_LOG_SET_DISALLOWED: &str = "readyset_query_log_set_disallowed";

/// Counter: The number of requests for views that were not found
pub const QUERY_LOG_VIEW_NOT_FOUND: &str = "readyset_query_log_view_not_found";

/// Counter: The number of errors due to RPC failures.
pub const QUERY_LOG_RPC_ERRORS: &str = "readyset_query_log_rpc_errors";

/// Gauge: The last seen size in bytes of a /metrics payload.
pub const METRICS_PAYLOAD_SIZE_BYTES: &str = "readyset_metrics_payload_size_bytes";

/// Gauge: The number of queries in the query logger backlog.
pub const QUERY_LOG_BACKLOG_SIZE: &str = "readyset_query_log_backlog_size";

/// Counter: The number of queries processed by the query logger.
pub const QUERY_LOG_PROCESSED_EVENTS: &str = "readyset_query_log_processed_events";

/// Counter: The number of queries checked for views.
pub const VIEWS_SYNCHRONIZER_QUERIES_CHECKED: &str = "readyset_views_synchronizer_queries_checked";

/// Gauge: The size of the query status cache's id-to-status mapping
pub const QUERY_STATUS_CACHE_ID_TO_STATUS_SIZE: &str =
    "readyset_query_status_cache_id_to_status_size";

/// Gauge: The size of the query status cache's statuses collection
pub const QUERY_STATUS_CACHE_STATUSES_SIZE: &str = "readyset_query_status_cache_statuses_size";

/// Gauge: The size of the views synchronizer's view name cache (local and shared variants)
pub const VIEWS_SYNCHRONIZER_VIEW_NAME_CACHE_SIZE: &str =
    "readyset_views_synchronizer_view_name_cache_size";

/// Gauge: The number of views that have been checked by the views synchronizer
pub const VIEWS_SYNCHRONIZER_VIEWS_CHECKED_SIZE: &str =
    "readyset_views_synchronizer_views_checked_size";

/// Gauge: The size of the query status cache's pending inlined migrations
pub const QUERY_STATUS_CACHE_PENDING_INLINE_MIGRATIONS: &str =
    "readyset_query_status_cache_pending_inline_migrations";
