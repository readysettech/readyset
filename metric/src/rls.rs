/// Gauge: Seconds since the last successful RLS catalog poll.
/// Operators alert on this growing past the configured poll
/// interval, which means the cache may serve under a stale view
/// of policies / RLS flags / role attributes. Phase 1 of the
/// RLS shallow-cache project (D3a, D10).
pub const RLS_POLL_AGE_SECONDS: &str = "readyset.rls.poll_age_seconds";

/// Counter: Number of failed RLS catalog poll attempts. Paired
/// with `RLS_POLL_AGE_SECONDS` for fail-open alerting (D3a):
/// neither metric alone is a true SLO signal, but a growing
/// count combined with a growing age indicates the registry has
/// fallen behind.
pub const RLS_POLL_ERRORS_TOTAL: &str = "readyset.rls.poll_errors_total";

/// Counter: Number of times an RLS policy reload fired,
/// labelled by `kind = relation | role | rls_flag`.
pub const RLS_POLICY_RELOADS_TOTAL: &str = "readyset.rls.policy_reloads_total";

/// Counter: Number of bootstrap attempts made against the
/// Postgres upstream at adapter startup. Each retry within the
/// retry budget increments the counter; the first successful
/// attempt also increments. Operators see retry pressure (boot
/// takes 4 attempts -> 4 ticks) before the adapter either comes
/// up healthy or gives up and aborts.
pub const RLS_BOOTSTRAP_ATTEMPTS_TOTAL: &str = "readyset.rls.bootstrap_attempts_total";

/// Gauge: 1 once the first RLS catalog poll has completed
/// successfully; absent (or 0) before that. Operators should
/// gate alerts on `RLS_POLL_AGE_SECONDS` past a threshold with
/// `AND RLS_POLL_INITIALISED == 1` to avoid firing on the
/// pre-first-poll window where the age would otherwise be an
/// uninitialised sentinel.
pub const RLS_POLL_INITIALISED: &str = "readyset.rls.poll_initialised";
