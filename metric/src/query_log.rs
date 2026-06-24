/// Histogram: The time in microseconds that the database spent executing a query.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | database_type | The database type being executed. Must be a `readyset_client_metrics::DatabaseType` |
/// | query_id | The hashed ID of the query. Emitted whenever the adapter has an ID for the query (both default and verbose query-log modes). |
/// | query | The query text being executed. Verbose query-log mode only. |
/// | cache_name | Optional. The cache name if the query is cached. |
pub const QUERY_LOG_EXECUTION_TIME: &str = "readyset_query_log_execution_time_us";

/// Counter: The number of times the database executed a query.
///
/// See [`QUERY_LOG_EXECUTION_TIME`] for the set of labels.
pub const QUERY_LOG_EXECUTION_COUNT: &str = "readyset_query_log_execution_count";

/// Gauge: The last execution timestamp for a query, in seconds since the UTC epoch.
///
/// See [`QUERY_LOG_EXECUTION_TIME`] for the set of labels.
pub const QUERY_LOG_LAST_EXECUTION_EPOCH_S: &str = "readyset_query_log_last_execution_epoch_s";

/// Histogram: The time in microseconds that the database spent executing a query.
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

/// Counter: The number of queries that bypassed caches.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID of the cache associated with this query. |
/// | type | `deep` or `shallow` |
/// | reason | `hint` (SKIP CACHE directive), `trx` (inside a transaction), `unsupported_set` (unsupported SET proxying) |
pub const QUERY_LOG_TOTAL_SKIP_CACHE: &str = "readyset_query_log_total_skip_cache";

/// Counter: The number of queries which encountered at least one cache miss.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query | The query text being executed. |
pub const QUERY_LOG_QUERY_CACHE_MISSED: &str = "readyset_query_log_query_cache_missed";

/// Counter: The number of `EventType` operations received (query / prepare / execute).
pub const QUERY_LOG_EVENT_TYPE: &str = "readyset_query_log_event_type";

/// Counter: The number of queries that failed to parse.
pub const QUERY_LOG_PARSE_ERRORS: &str = "readyset_query_log_parse_errors";

/// Counter: The number of requests for views that were not found
pub const QUERY_LOG_VIEW_NOT_FOUND: &str = "readyset_query_log_view_not_found";

/// Counter: The number of errors due to RPC failures.
pub const QUERY_LOG_RPC_ERRORS: &str = "readyset_query_log_rpc_errors";

/// Gauge: The number of queries in the query logger backlog.
pub const QUERY_LOG_BACKLOG_SIZE: &str = "readyset_query_log_backlog_size";

/// Counter: The number of queries processed by the query logger.
pub const QUERY_LOG_PROCESSED_EVENTS: &str = "readyset_query_log_processed_events";
