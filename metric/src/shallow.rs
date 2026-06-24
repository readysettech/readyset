pub const SHALLOW_HIT: &str = "readyset_shallow.shallow_result_hit";
pub const SHALLOW_MISS: &str = "readyset_shallow.shallow_result_miss";
pub const SHALLOW_REFRESH: &str = "readyset_shallow.shallow_result_refresh";
pub const SHALLOW_EVICT_MEMORY: &str = "readyset_shallow.shallow_evict_memory";
pub const SHALLOW_SKIP_TOO_LARGE: &str = "readyset_shallow.shallow_skip_too_large";
pub const SHALLOW_REFRESH_QUEUE_EXCEEDED: &str = "readyset_shallow.shallow_refresh_queue_exceeded";

/// Histogram: The amount of time in microseconds spent executing the upstream query during a
/// shallow cache refresh. Only recorded for successful queries.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_QUERY_TIME: &str = "readyset_shallow.shallow_refresh_query_time_us";

/// Gauge: Number of QueryIds the in-request-path shallow-cache eligibility filter has rejected and
/// remembered, so subsequent requests for the same query short-circuit without re-walking the AST.
pub const SHALLOW_AUTO_CREATE_SKIP_SET_SIZE: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.size";

/// Counter: Number of times the shallow auto-create skip set crossed its soft cap and was
/// bulk-cleared. A non-zero value indicates either pathological client behaviour (queries with
/// literal-injected unique values) or that the cap is too low for the workload.
pub const SHALLOW_AUTO_CREATE_SKIP_OVERFLOW: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.overflow";
