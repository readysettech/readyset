pub const SHALLOW_HIT: &str = "readyset_shallow.shallow_result_hit";
pub const SHALLOW_MISS: &str = "readyset_shallow.shallow_result_miss";
pub const SHALLOW_REFRESH: &str = "readyset_shallow.shallow_result_refresh";
pub const SHALLOW_EVICT_MEMORY: &str = "readyset_shallow.shallow_evict_memory";
pub const SHALLOW_SKIP_TOO_LARGE: &str = "readyset_shallow.shallow_skip_too_large";
pub const SHALLOW_REFRESH_QUEUE_EXCEEDED: &str = "readyset_shallow.shallow_refresh_queue_exceeded";

/// Counter: Number of adaptive-refresh write-backs whose result differed from the entry it
/// replaced.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_CHANGED: &str = "readyset_shallow.shallow_refresh_changed";

/// Counter: Number of adaptive-refresh write-backs whose result matched the entry it replaced.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_UNCHANGED: &str = "readyset_shallow.shallow_refresh_unchanged";

/// Histogram: The amount of time in microseconds spent executing the upstream query during a
/// shallow cache refresh. Only recorded for successful queries.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_QUERY_TIME: &str = "readyset_shallow.shallow_refresh_query_time_us";

/// Gauge: Total refresh load an adaptive cache currently sends upstream, in parts-per-million of
/// upstream execution time per wall time, summed over the cache's entries at their current
/// refresh periods. Dropped caches leave the series at zero rather than removing it.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the adaptive cached query. |
pub const SHALLOW_ADAPTIVE_ACTUAL_LOAD_PPM: &str =
    "readyset_shallow.shallow_adaptive_actual_load_ppm";

/// Gauge: Refresh load an adaptive cache would send upstream if every entry refreshed at the
/// configured period, in parts-per-million of upstream execution time per wall time. Dropped
/// caches leave the series at zero rather than removing it.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the adaptive cached query. |
pub const SHALLOW_ADAPTIVE_BASELINE_LOAD_PPM: &str =
    "readyset_shallow.shallow_adaptive_baseline_load_ppm";

/// Gauge: 1 when an adaptive cache's actual refresh load is at or over its cap of allowed extra
/// load beyond the baseline, 0 otherwise. Dropped caches leave the series at zero rather than
/// removing it.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the adaptive cached query. |
pub const SHALLOW_ADAPTIVE_OVER_CAP: &str = "readyset_shallow.shallow_adaptive_over_cap";

/// Gauge: Number of refresh callbacks currently queued in a scheduled shallow cache's refresh
/// scheduler. Dropped caches leave the series at zero rather than removing it.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the scheduled cached query. |
pub const SHALLOW_SCHEDULER_QUEUE_DEPTH: &str = "readyset_shallow.shallow_scheduler_queue_depth";

/// Gauge: Number of QueryIds the in-request-path shallow-cache eligibility filter has rejected and
/// remembered, so subsequent requests for the same query short-circuit without re-walking the AST.
pub const SHALLOW_AUTO_CREATE_SKIP_SET_SIZE: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.size";

/// Counter: Number of times the shallow auto-create skip set crossed its soft cap and was
/// bulk-cleared. A non-zero value indicates either pathological client behaviour (queries with
/// literal-injected unique values) or that the cap is too low for the workload.
pub const SHALLOW_AUTO_CREATE_SKIP_OVERFLOW: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.overflow";
