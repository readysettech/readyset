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

/// Counter: Number of refresh write-backs whose data was never returned by a cache hit,
/// because the entry was replaced, the entry was evicted, or the refresh completed after
/// its entry was already gone. Explicit removals (flush, drop) of a live entry are not counted.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_WASTED: &str = "readyset_shallow.shallow_refresh_wasted";

/// Counter: Number of refreshes that never produced a result: the request was dropped with the
/// pool saturated, or the upstream connection or query failed.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query being refreshed. |
pub const SHALLOW_REFRESH_DROPPED: &str = "readyset_shallow.shallow_refresh_dropped";

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

/// Counter: Number of coalesces that were successfully served using the results from an already
/// in-flight fill.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_SUCCESS: &str = "readyset_shallow.shallow_coalesce_success";

/// Counter: Number of coalesce waits that reached the timeout with the fill still in flight.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_TIMEOUT: &str = "readyset_shallow.shallow_coalesce_timeout";

/// Counter: Number of coalesce waits that ended because the in-flight fill dropped without a
/// result.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_ABORT: &str = "readyset_shallow.shallow_coalesce_abort";

/// Histogram: The amount of time in microseconds a shallow cache miss spent waiting for a
/// successful result from an in-flight fill of the same key.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_SUCCESS_WAIT: &str = "readyset_shallow.shallow_coalesce_success_wait_us";

/// Histogram: The amount of time in microseconds a shallow cache miss spent waiting before a
/// coalesce timed out, and it went upstream.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_TIMEOUT_WAIT: &str = "readyset_shallow.shallow_coalesce_timeout_wait_us";

/// Histogram: The amount of time in microseconds a shallow cache miss spent waiting before a
/// coalesce aborted for reasons other than timing out, and it went upstream.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID for the cached query. |
pub const SHALLOW_COALESCE_ABORT_WAIT: &str = "readyset_shallow.shallow_coalesce_abort_wait_us";

/// Gauge: Total size in bytes of resident entries (keys plus values) in the entry store shared
/// by all shallow caches, as tracked by the store's own size accounting.
pub const SHALLOW_MEMORY_BYTES: &str = "readyset_shallow.shallow_memory_bytes";

/// Gauge: Number of resident entries in the entry store shared by all shallow caches.
pub const SHALLOW_ENTRIES: &str = "readyset_shallow.shallow_entries";

/// Gauge: Number of live workers in the shallow refresh pool. The pool is shared across all
/// shallow caches.
pub const SHALLOW_REFRESH_POOL_WORKERS: &str = "readyset_shallow.shallow_refresh_pool_workers";

/// Gauge: Number of shallow refresh pool workers currently idle. The pool is shared across all
/// shallow caches.
pub const SHALLOW_REFRESH_POOL_IDLE_WORKERS: &str =
    "readyset_shallow.shallow_refresh_pool_idle_workers";

/// Gauge: Number of refresh requests sitting in shallow refresh pool worker channels, updated as
/// requests are sent and workers finish. The pool is shared across all shallow caches.
pub const SHALLOW_REFRESH_POOL_QUEUED: &str = "readyset_shallow.shallow_refresh_pool_queued";

/// Gauge: Number of QueryIds the in-request-path shallow-cache eligibility filter has rejected and
/// remembered, so subsequent requests for the same query short-circuit without re-walking the AST.
pub const SHALLOW_AUTO_CREATE_SKIP_SET_SIZE: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.size";

/// Counter: Number of times the shallow auto-create skip set crossed its soft cap and was
/// bulk-cleared. A non-zero value indicates either pathological client behaviour (queries with
/// literal-injected unique values) or that the cap is too low for the workload.
pub const SHALLOW_AUTO_CREATE_SKIP_OVERFLOW: &str =
    "readyset_query_status_cache.shallow_auto_create_skip.overflow";

/// Counter: Number of times an in-request-path query was skipped for shallow auto-creation,
/// tagged by the reason it was ineligible. A query with several reasons increments the counter
/// once per distinct reason, so the tallies profile *why* auto-caching is skipped.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | reason | The eligibility reason (e.g. `"non-deterministic function"`). |
pub const SHALLOW_AUTO_CREATE_SKIPPED: &str =
    "readyset_query_status_cache.shallow_auto_create_skipped";
