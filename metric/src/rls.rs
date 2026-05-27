/// Gauge: seconds since the last successful RLS catalog poll. Operators alert on this growing
/// past the configured poll interval, which means the cache may serve under a stale view of
/// policies / RLS flags / role attributes.
pub const RLS_POLL_AGE_SECONDS: &str = "readyset.rls.poll_age_seconds";

/// Counter: failed RLS catalog poll attempts. Paired with `RLS_POLL_AGE_SECONDS` for fail-open
/// alerting: a growing count alongside a growing age indicates the registry has fallen behind.
pub const RLS_POLL_ERRORS_TOTAL: &str = "readyset.rls.poll_errors_total";

/// Counter: times an RLS policy reload fired, labelled by `kind = relation | role | rls_flag`.
pub const RLS_POLICY_RELOADS_TOTAL: &str = "readyset.rls.policy_reloads_total";

/// Counter: bootstrap attempts against the Postgres upstream at adapter startup. Each retry and
/// the first successful attempt increment the counter, so operators see retry pressure before the
/// adapter comes up healthy or aborts.
pub const RLS_BOOTSTRAP_ATTEMPTS_TOTAL: &str = "readyset.rls.bootstrap_attempts_total";

/// Gauge: 1 once the first RLS catalog poll has completed successfully; absent (or 0) before that.
/// Gate `RLS_POLL_AGE_SECONDS` alerts on `AND RLS_POLL_INITIALISED == 1` to avoid firing on the
/// pre-first-poll window where the age is an uninitialised sentinel.
pub const RLS_POLL_INITIALISED: &str = "readyset.rls.poll_initialised";

/// Counter: shallow-cache lookups served off-cache because the session could not be partitioned
/// safely (untrusted state, or no usable session context). Distinct from a miss: nothing is
/// cached, so it never warms. A session stuck untrusted routes all its traffic here while the hit
/// rate still looks healthy.
pub const SHALLOW_RESULT_UNCACHEABLE: &str = "readyset_shallow.shallow_result_uncacheable";
