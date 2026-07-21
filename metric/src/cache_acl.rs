/// Counter: Number of shallow-cache lookups declined because the session's
/// verdict was not `Allowed`. The direct signal of ACL-driven proxying; the
/// verdict tag separates a genuine denial from a pair the freshness worker
/// has not resolved yet.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | query_id | The query ID of the declined cache. |
/// | verdict | `denied`, `unknown`, or `untrusted` (session mirror not trusted). |
pub const CACHE_ACL_DECLINED: &str = "readyset_cache_acl.declined";

/// Counter: Freshness-worker runs, one per message processed.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | trigger | `periodic`, `flush_privileges`, `cache_created`, `cache_dropped`, `user_altered`, `user_dropped`, or `resolve_identity`. |
pub const CACHE_ACL_LOOP_RUNS: &str = "readyset_cache_acl.loop_runs";

/// Histogram: Time in microseconds the freshness worker spent processing one
/// message.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | trigger | Same values as `readyset_cache_acl.loop_runs`. |
pub const CACHE_ACL_LOOP_RUN_TIME: &str = "readyset_cache_acl.loop_run_time_us";

/// Counter: Verdict transitions recorded by the worker or the opportunistic
/// creator write, keyed by direction. `allowed -> denied` marks a revocation
/// taking effect.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | from | Prior verdict (`allowed`, `denied`, `unknown`). |
/// | to | New verdict. |
pub const CACHE_ACL_VERDICT_FLIPS: &str = "readyset_cache_acl.verdict_flips";

/// Counter: Per-identity grant-fingerprint changes detected by the worker,
/// each triggering a row re-probe. The leading indicator of an upstream
/// GRANT/REVOKE.
pub const CACHE_ACL_FINGERPRINT_FLIPS: &str = "readyset_cache_acl.fingerprint_flips";

/// Counter: Probe attempts that could not produce a verdict: the probe
/// session could not open or the probe errored transiently. Distinguishes a
/// systemic credential/host problem from a genuine deny.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | kind | `connect`, `probe`, or `fingerprint`. |
pub const CACHE_ACL_PROBE_FAILURES: &str = "readyset_cache_acl.probe_failures";

/// Gauge: Age in seconds of the oldest per-identity fingerprint check -- the
/// observable form of the stale-allow bound.
pub const CACHE_ACL_STALENESS: &str = "readyset_cache_acl.staleness_seconds";

/// Gauge: Number of (identity, cache) pairs with no stored verdict -- how
/// much of the matrix the worker has not resolved yet.
pub const CACHE_ACL_UNKNOWN_PAIRS: &str = "readyset_cache_acl.unknown_pairs";
