//! Routing policy for deep SELECTs: whether a read is served from Readyset or proxied
//! upstream, plus the per-session state that decision reads.
//!
//! [`SelectRouter`] is the entry point. It borrows only what the decision needs -- the
//! session's [`ProxyState`] and [`SessionWriteTracker`], the query status cache, and the
//! connector's rewrite parameters -- so the routing rules stay independent of the upstream
//! database type.

use std::mem;
use std::time::{Duration, Instant};

use metrics::counter;
use readyset_client::ViewCreateRequest;
use readyset_client::query::{MigrationState, QueryId, QueryStatus};
use readyset_errors::ReadySetError;
use readyset_sql::ast::{CacheType, SelectStatement, TrxCachePolicy};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_passes::adapter_rewrites::{
    self, AdapterRewriteParams, DfQueryParameters, QueryParameters,
};
use readyset_util::redacted::Sensitive;
use schema_catalog::SchemaGeneration;
use tracing::{trace, warn};

use crate::query_status_cache::QueryStatusCache;

/// A state machine representing how statements are proxied upstream for a particular instance of a
/// backend.
///
/// The possible transitions of the state machine are modeled by the following graph:
///
/// ```dot
/// digraph ProxyState {
///     Never -> Never;
///
///     Fallback -> InTransaction   [label="BEGIN"];
///     InTransaction -> Fallback   [label="COMMIT/ROLLBACK"];
///     InTransaction -> Fallback   [label="SET autocommit=1"];
///
///     Fallback -> AutocommitOff   [label="SET autocommit=0"];
///     InTransaction -> AutocommitOff [label="SET autocommit=0"];
///     AutocommitOff -> Fallback   [label="SET autocommit=1"];
///     AutocommitOff -> AutocommitOff [label="COMMIT/ROLLBACK"];
///
///     Fallback -> ProxyAlways     [label="unsupported SET (Proxy mode)"];
///     InTransaction -> ProxyAlways [label="unsupported SET (Proxy mode)"];
///     AutocommitOff -> ProxyAlways [label="unsupported SET (Proxy mode)"];
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyState {
    /// Never proxy statements upstream. This is the behavior used when no upstream database is
    /// configured for a backend
    Never,

    /// Proxy writes upstream, and proxy reads upstream only after they fail when executed against
    /// ReadySet.
    ///
    /// This is the initial behavior used when an upstream database is configured for a backend
    Fallback,

    /// We are inside an explicit transaction (received a BEGIN or START TRANSACTION packet), so
    /// proxy all statements upstream, but return to [`ProxyState::Fallback`] when the transaction
    /// is finished. This state does not apply to transactions formed by `SET autocommit=0`.
    InTransaction,

    /// We are inside of an implicit transaction due to autocommit being turned off. This means
    /// that every time we get COMMIT or ROLLBACK, we instantly start a new transaction. All
    /// statements are proxied upstream unless we receive a `SET autocommit=1` statement, which
    /// would turn autocommit back on.
    AutocommitOff,

    /// Unconditionally proxy all statements upstream, and do not leave this state when leaving
    /// transactions. The backend enters this state when it receives an unsupported SQL `SET`
    /// statement and the [`unsupported_set_mode`] is set to [`Proxy`]
    ///
    /// [`unsupported_set_mode`]: Backend::unsupported_set_mode
    /// [`Proxy`]: UnsupportedSetMode::Proxy
    ProxyAlways,
}

impl ProxyState {
    /// Returns true if a query should be proxied upstream in most cases per this [`ProxyState`].
    /// The case in which we should not proxy a query upstream, is if the query in question has
    /// been manually migrated with the optional `ALWAYS` flag, such as `CREATE CACHE ALWAYS`.
    pub(super) fn should_proxy(&self) -> bool {
        matches!(
            self,
            Self::AutocommitOff | Self::InTransaction | Self::ProxyAlways
        )
    }

    /// Perform the appropriate state transition for this proxy state to begin a new transaction.
    pub(super) fn start_transaction(&mut self) {
        if self.is_fallback() {
            *self = ProxyState::InTransaction;
        }
    }

    /// Perform the appropriate state transition for this proxy state to end a transaction.
    /// Explicit `InTransaction` returns to `Fallback`; under `AutocommitOff`, COMMIT/ROLLBACK
    /// just begins a fresh implicit transaction and the state stays `AutocommitOff`.
    pub(super) fn end_transaction(&mut self) {
        if !matches!(self, Self::Never | Self::ProxyAlways | Self::AutocommitOff) {
            *self = ProxyState::Fallback;
        }
    }

    pub(super) fn in_transaction(&self) -> bool {
        *self == ProxyState::InTransaction
    }

    /// Returns true when autocommit is effectively on.
    /// True for all states except `AutocommitOff`.
    pub fn is_autocommit(&self) -> bool {
        !matches!(self, ProxyState::AutocommitOff)
    }

    /// Returns true when inside any transaction -- explicit (`BEGIN`) or
    /// implicit (`autocommit=0`).
    pub fn in_transaction_or_implicit(&self) -> bool {
        matches!(self, ProxyState::InTransaction | ProxyState::AutocommitOff)
    }

    /// Returns true when the proxy state is set to always proxy upstream,
    /// typically due to an unsupported SET statement.
    pub fn is_proxy_always(&self) -> bool {
        *self == ProxyState::ProxyAlways
    }

    /// Returns a reason tag for the skip-cache metric describing why queries
    /// are being proxied upstream.
    pub fn skip_reason(&self) -> &'static str {
        if self.in_transaction_or_implicit() {
            "trx"
        } else if self.is_proxy_always() {
            "unsupported_set"
        } else {
            "unknown"
        }
    }

    /// Returns true when a cache hit must be skipped given the cache's transaction policy,
    /// whether a write has been observed in the current transaction, and whether the
    /// session-level opportunistic read-your-writes window is active.
    ///
    /// Two regimes, decided by whether the session is inside a transaction:
    ///
    /// 1. Inside a transaction (or implicit transaction under `autocommit=0`):
    ///    only the per-cache [`TrxCachePolicy`] matters — `Never` always skips,
    ///    `UntilWrite` skips only after the first write (`had_write_in_txn`), and
    ///    `Always` never skips. The opportunistic window does not apply here, and
    ///    `opportunistic_ryw_active` is `false` by construction (cleared on `BEGIN`).
    ///
    /// 2. Outside any transaction: `opportunistic_ryw_active` overrides every per-cache
    ///    policy (including `Always`) because the window's whole purpose is to route
    ///    post-write reads upstream. The guarantee is opportunistic, not absolute: once
    ///    the window elapses the cache may still hold a pre-write value (e.g. a TTL that
    ///    has not expired, or a row Readyset has not yet refreshed), and reads can flip
    ///    back to a stale cached result. With the window inactive, only `ProxyAlways`
    ///    forces a skip.
    pub fn should_skip_cache_for(
        &self,
        trx_cache_policy: TrxCachePolicy,
        had_write_in_txn: bool,
        opportunistic_ryw_active: bool,
    ) -> bool {
        if opportunistic_ryw_active {
            return true;
        }
        if matches!(trx_cache_policy, TrxCachePolicy::Always) {
            return false;
        }
        match self {
            ProxyState::Never | ProxyState::Fallback => false,
            ProxyState::ProxyAlways => true,
            ProxyState::InTransaction | ProxyState::AutocommitOff => match trx_cache_policy {
                TrxCachePolicy::Never => true,
                TrxCachePolicy::UntilWrite => had_write_in_txn,
                TrxCachePolicy::Always => false,
            },
        }
    }

    /// Reason tag matched to [`Self::should_skip_cache_for`]. Used by `record_skip_cache`
    /// so dashboards distinguish `"trx"` (default per-cache rule), `"trx_after_write"`
    /// (`UntilWrite` cache that observed a write earlier in the transaction), and
    /// `"opportunistic_ryw"` (opportunistic read-your-writes window active outside any
    /// transaction).
    pub fn skip_reason_for(
        &self,
        trx_cache_policy: TrxCachePolicy,
        had_write_in_txn: bool,
        opportunistic_ryw_active: bool,
    ) -> &'static str {
        if opportunistic_ryw_active {
            return "opportunistic_ryw";
        }
        if self.is_proxy_always() {
            return "unsupported_set";
        }
        match (self, trx_cache_policy, had_write_in_txn) {
            (
                ProxyState::InTransaction | ProxyState::AutocommitOff,
                TrxCachePolicy::UntilWrite,
                true,
            ) => "trx_after_write",
            (ProxyState::InTransaction | ProxyState::AutocommitOff, _, _) => "trx",
            _ => "unknown",
        }
    }

    /// Sets the autocommit state accordingly. If turning autocommit on, will set ProxyState to
    /// Fallback as long as current state is AutocommitOff or InTransaction (the latter models
    /// MySQL's implicit COMMIT on `SET autocommit=1` during an active transaction).
    ///
    /// If turning autocommit off, will set state to AutocommitOff as long as state is not
    /// currently ProxyAlways or Never, as these states should not be overwritten.
    pub(super) fn set_autocommit(&mut self, on: bool) {
        if on {
            if matches!(self, Self::AutocommitOff | Self::InTransaction) {
                *self = ProxyState::Fallback;
            }
        } else if !matches!(self, Self::ProxyAlways | Self::Never) {
            *self = ProxyState::AutocommitOff;
        }
    }

    /// Returns `true` if the proxy state is [`Fallback`].
    ///
    /// [`Fallback`]: ProxyState::Fallback
    #[must_use]
    fn is_fallback(&self) -> bool {
        matches!(self, Self::Fallback)
    }
}

/// Session-level write tracking. Drives two separate rules from a single source of
/// truth (the wall-clock time of the most recent write the session has issued):
///
///   - In-transaction read-your-writes: governed entirely by the per-cache
///     [`TrxCachePolicy`]. `had_write_in_txn` is the input the policy consumes.
///   - Outside any transaction: the opportunistic read-your-writes window suppresses
///     the cache for the configured duration after a write.
///
/// The two regimes do not overlap. The opportunistic window is *only* consulted
/// outside transactions; inside a transaction, `TrxCachePolicy` decides routing and
/// the window is dormant (the deadline is cleared on `BEGIN`).
///
/// The window is opportunistic, not a consistency guarantee. It only suppresses the
/// cache for the configured duration; once it elapses, reads resume from the cache and
/// can serve a pre-write value if the cache has not refreshed yet.
///
/// State machine for `last_write_at`:
///
///   - `mark_write()` sets `last_write_at = Some(Instant::now())` and, if a window is
///     configured, precomputes `opportunistic_ryw_deadline = Some(now + window)`.
///   - `on_start_transaction()` (BEGIN / START TRANSACTION) clears both fields. Pre-txn
///     write history is intentionally dropped; once a transaction begins, only in-txn
///     writes are tracked, and the opportunistic window does not apply inside a
///     transaction (per-cache `TrxCachePolicy` governs in-txn read-your-writes).
///   - `on_commit()` (explicit COMMIT or implicit COMMIT under autocommit=0) refreshes
///     both fields if `last_write_at` was already `Some` (the txn's writes just landed
///     in the upstream, so the window must fire from now). If the field was `None`,
///     COMMIT leaves both fields `None`.
///   - `on_rollback()` always clears both fields. Rolled-back writes never landed.
///
/// `had_write_in_txn(state)` is a presence check on `last_write_at` while in a
/// transaction. `opportunistic_ryw_active()` checks the precomputed deadline (with
/// lazy-clear when stale), giving the read path a near-zero check once the window has
/// elapsed.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SessionWriteTracker {
    /// Configured opportunistic read-your-writes window. `None` disables the feature;
    /// the read path then never pays the `Instant::now()` comparison.
    opportunistic_ryw_window: Option<Duration>,
    /// Wall-clock time of the most recent write on this session, or `None` if no write
    /// has happened (or the most recent write was rolled back / cleared by `BEGIN`).
    pub last_write_at: Option<Instant>,
    /// Precomputed deadline `last_write_at + opportunistic_ryw_window`. Set in `mark_write()` so the
    /// read path does a single `Instant::now()` comparison and lazy-clears once stale.
    /// `None` either means there is no recent write or the configured window is `None`.
    opportunistic_ryw_deadline: Option<Instant>,
}

impl SessionWriteTracker {
    /// Construct a tracker with the configured opportunistic read-your-writes window.
    /// `None` disables the feature.
    pub(super) fn new(opportunistic_ryw_window: Option<Duration>) -> Self {
        Self {
            opportunistic_ryw_window,
            last_write_at: None,
            opportunistic_ryw_deadline: None,
        }
    }

    pub(super) fn mark_write(&mut self) {
        let now = Instant::now();
        self.last_write_at = Some(now);
        self.opportunistic_ryw_deadline = self.opportunistic_ryw_window.map(|w| now + w);
    }

    pub(super) fn on_start_transaction(&mut self) {
        self.last_write_at = None;
        self.opportunistic_ryw_deadline = None;
    }

    pub(super) fn on_commit(&mut self) {
        if self.last_write_at.is_some() {
            let now = Instant::now();
            self.last_write_at = Some(now);
            self.opportunistic_ryw_deadline = self.opportunistic_ryw_window.map(|w| now + w);
        }
    }

    pub(super) fn on_rollback(&mut self) {
        self.last_write_at = None;
        self.opportunistic_ryw_deadline = None;
    }

    /// Returns `true` when a write has been observed since the current transaction began.
    /// Always `false` outside any transaction.
    pub(super) fn had_write_in_txn(&self, proxy_state: ProxyState) -> bool {
        proxy_state.in_transaction_or_implicit() && self.last_write_at.is_some()
    }

    /// Returns `true` when the opportunistic read-your-writes window is currently active
    /// for this session, i.e. a write has been observed within the last configured window
    /// and we are not inside a transaction. The deadline is lazy-cleared when stale, so
    /// steady-state reads (no recent write) collapse back to a single branch on the read
    /// path.
    ///
    /// This is consulted only outside transactions. `BEGIN` clears the deadline, so the
    /// return value is `false` inside any transaction by construction; in-transaction
    /// read-your-writes is governed by the per-cache [`TrxCachePolicy`] via
    /// [`Self::had_write_in_txn`] instead.
    pub(super) fn opportunistic_ryw_active(&mut self) -> bool {
        match self.opportunistic_ryw_deadline {
            Some(d) if Instant::now() < d => true,
            Some(_) => {
                // Window elapsed; collapse back to the cheap path so subsequent reads
                // skip the clock check entirely. `last_write_at` is left intact for
                // diagnostics and is overwritten by the next `mark_write()`.
                self.opportunistic_ryw_deadline = None;
                false
            }
            None => false,
        }
    }
}

/// Records a skip-cache metric for a query that bypassed a cache.
pub(super) fn record_skip_cache(query_id: String, cache_type: &'static str, reason: &'static str) {
    counter!(
        metric::QUERY_LOG_TOTAL_SKIP_CACHE,
        "query_id" => query_id,
        "type" => cache_type,
        "reason" => reason
    )
    .increment(1);
}

/// The determination of whether we should attempt to select from ReadySet.
pub(super) enum ShouldTrySelect {
    /// We should attempt to select from ReadySet, with the given status and params.
    Yes {
        status: QueryStatus,
        params: DfQueryParameters,
        schema_generation: SchemaGeneration,
    },
    /// We should not attempt to select from ReadySet, and should proxy if there is an upstream. If
    /// there is no upstream, we should return an error. If there is no error, it is because we are
    /// in a proxying state (e.g. in transaction) and the cache was not marked `ALWAYS`.
    No { error: Option<ReadySetError> },
}

/// Decides whether a deep SELECT is served from Readyset or proxied upstream, rewriting the
/// statement in the process and recording the skip-cache metric when a cache is bypassed.
///
/// Construct one per decision: it holds a mutable borrow of the session's write tracker, and
/// [`Self::route`] returns an owned verdict so that borrow ends as soon as the call does.
pub(super) struct SelectRouter<'session> {
    /// Dialect used to render statements in diagnostics.
    dialect: Dialect,
    /// Rewrite parameters as the connector reports them. A TopK retry clears
    /// `server_supports_topk` on its own copy, so this stays the starting point rather than
    /// the value used for every attempt.
    rewrite_params: AdapterRewriteParams,
    query_status_cache: &'static QueryStatusCache,
    proxy_state: ProxyState,
    write_tracker: &'session mut SessionWriteTracker,
}

impl<'session> SelectRouter<'session> {
    pub(super) fn new(
        dialect: Dialect,
        rewrite_params: AdapterRewriteParams,
        query_status_cache: &'static QueryStatusCache,
        proxy_state: ProxyState,
        write_tracker: &'session mut SessionWriteTracker,
    ) -> Self {
        Self {
            dialect,
            rewrite_params,
            query_status_cache,
            proxy_state,
            write_tracker,
        }
    }

    /// Checks if noria should try to execute a given select and in the process mutates the
    /// supplied select statement by rewriting it.
    ///
    /// For TopK-eligible queries (ORDER BY + literal LIMIT), this function implements dual cache
    /// lookup based on which cache was actually created:
    /// 1. First checks if a TopK cache exists (created with literal LIMIT, e.g., CREATE CACHE
    ///    ... LIMIT 10)
    /// 2. If TopK cache exists, uses it (preferred as it's more efficient than letting the adapter
    ///    fetch all records then apply the limit)
    /// 3. Otherwise checks if parameterized cache exists (parameterized LIMITs are removed by the
    ///    adapter, since the server can't handle parameterized LIMITs).
    /// 4. If parameterized cache exists, uses it.
    /// 5. If neither cache exists, processes normally (go upstream if possible, else fail).
    ///
    /// All other query rewrites (autoparameterization, IN conditions, etc.) are applied
    /// consistently regardless of which path is taken.
    ///
    /// Returns whether noria should try the select, along with the query status if it was obtained
    /// during processing.
    pub(super) fn route(
        &mut self,
        q: &mut ViewCreateRequest,
        params: QueryParameters,
        schema_generation: SchemaGeneration,
        is_skip_cache: bool,
    ) -> ShouldTrySelect {
        let mut rewrite_params = self.rewrite_params;

        let is_topk_candidate =
            rewrite_params.server_supports_topk && has_topk_literal_limit(&q.statement);

        if is_topk_candidate {
            let mut original = q.clone();

            match self.lookup_topk_cache(
                q,
                rewrite_params,
                params.clone(),
                schema_generation,
                is_skip_cache,
            ) {
                yes @ ShouldTrySelect::Yes { .. } => return yes,
                ShouldTrySelect::No { .. } => {
                    trace!("No TopK cache for query, trying parameterized cache");
                    // We will try the query again, but this time without the LIMIT, to try and hit
                    // a cache that was created with a paramterized LIMIT (LIMIT ?)
                    rewrite_params.server_supports_topk = false;
                    mem::swap(q, &mut original);
                }
            }
        }

        match self.process_and_check(q, rewrite_params, params, schema_generation, is_skip_cache) {
            ShouldTrySelect::Yes { status, params, .. } => ShouldTrySelect::Yes {
                status,
                params,
                schema_generation,
            },
            no @ ShouldTrySelect::No { .. } => no,
        }
    }

    /// For TopK-eligible queries, attempts to use a cache that preserves the literal LIMIT.
    ///
    /// Returns [`ShouldTrySelect::Yes`] if a cache exists that can handle the query with TopK
    /// processing, [`ShouldTrySelect::No`] if no such cache exists and normal processing should be
    /// attempted.
    fn lookup_topk_cache(
        &mut self,
        q: &mut ViewCreateRequest,
        rewrite_params: AdapterRewriteParams,
        params: QueryParameters,
        schema_generation: SchemaGeneration,
        is_skip_cache: bool,
    ) -> ShouldTrySelect {
        // if the cache is not yet created, it's probably better
        // to let the adapter try the other path.
        match self.process_and_check(q, rewrite_params, params, schema_generation, is_skip_cache) {
            ShouldTrySelect::Yes {
                status:
                    status @ QueryStatus {
                        migration_state: MigrationState::Successful(_) | MigrationState::Inlined(_),
                        ..
                    },
                params,
                schema_generation,
            } => ShouldTrySelect::Yes {
                status,
                params,
                schema_generation,
            },
            ShouldTrySelect::Yes { .. } => ShouldTrySelect::No { error: None },
            no => no,
        }
    }

    /// Helper function to process a query and determine if Readyset should handle it.
    fn process_and_check(
        &mut self,
        q: &mut ViewCreateRequest,
        rewrite_params: AdapterRewriteParams,
        params: QueryParameters,
        schema_generation: SchemaGeneration,
        is_skip_cache: bool,
    ) -> ShouldTrySelect {
        match adapter_rewrites::rewrite_for_readyset(&mut q.statement, rewrite_params, params) {
            Ok(params) => {
                let status = self.query_status_cache.query_status(q);
                let has_deep_cache = matches!(
                    status.migration_state,
                    MigrationState::Successful(CacheType::Deep)
                );
                let should_try = if is_skip_cache {
                    if has_deep_cache {
                        record_skip_cache(QueryId::from(&*q).to_string(), "deep", "hint");
                    }
                    false
                } else {
                    let had_write_in_txn = self.write_tracker.had_write_in_txn(self.proxy_state);
                    let opportunistic_ryw_active = self.write_tracker.opportunistic_ryw_active();
                    if self.proxy_state.should_skip_cache_for(
                        status.trx_cache_policy,
                        had_write_in_txn,
                        opportunistic_ryw_active,
                    ) {
                        if has_deep_cache {
                            record_skip_cache(
                                QueryId::from(&*q).to_string(),
                                "deep",
                                self.proxy_state.skip_reason_for(
                                    status.trx_cache_policy,
                                    had_write_in_txn,
                                    opportunistic_ryw_active,
                                ),
                            );
                        }
                        false
                    } else {
                        true
                    }
                };
                if should_try {
                    ShouldTrySelect::Yes {
                        status,
                        params,
                        schema_generation,
                    }
                } else {
                    ShouldTrySelect::No { error: None }
                }
            }
            Err(error) => {
                warn!(
                    statement = %Sensitive(&q.statement.display(self.dialect)),
                    %error,
                    "This statement could not be rewritten by Readyset",
                );
                ShouldTrySelect::No { error: Some(error) }
            }
        }
    }
}

/// Helper function to check if a query has literal LIMIT values that could be TopK candidates
fn has_topk_literal_limit(statement: &SelectStatement) -> bool {
    statement.order.is_some()
        && statement.limit_clause.is_topk()
        && matches!(
            statement.limit_clause.limit(),
            Some(
                readyset_sql::ast::Literal::Integer(_)
                    | readyset_sql::ast::Literal::UnsignedInteger(_)
            )
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_autocommit_by_proxy_state() {
        assert!(ProxyState::Never.is_autocommit());
        assert!(ProxyState::Fallback.is_autocommit());
        assert!(ProxyState::InTransaction.is_autocommit());
        assert!(!ProxyState::AutocommitOff.is_autocommit());
        assert!(ProxyState::ProxyAlways.is_autocommit());
    }

    #[test]
    fn in_transaction_or_implicit_by_proxy_state() {
        assert!(!ProxyState::Never.in_transaction_or_implicit());
        assert!(!ProxyState::Fallback.in_transaction_or_implicit());
        assert!(ProxyState::InTransaction.in_transaction_or_implicit());
        assert!(ProxyState::AutocommitOff.in_transaction_or_implicit());
        assert!(!ProxyState::ProxyAlways.in_transaction_or_implicit());
    }

    /// Verify that existing in_transaction() is NOT affected -- it only covers
    /// explicit transactions, not AutocommitOff.
    #[test]
    fn in_transaction_only_covers_explicit() {
        assert!(!ProxyState::Never.in_transaction());
        assert!(!ProxyState::Fallback.in_transaction());
        assert!(ProxyState::InTransaction.in_transaction());
        assert!(!ProxyState::AutocommitOff.in_transaction());
        assert!(!ProxyState::ProxyAlways.in_transaction());
    }

    #[test]
    fn last_write_at_lifecycle() {
        // mark_write() bumps the timestamp; nothing else does.
        let mut tracker = SessionWriteTracker::default();
        assert!(tracker.last_write_at.is_none());
        tracker.mark_write();
        let first = tracker.last_write_at.expect("mark_write must set it");

        // Idempotent in the sense that mark_write() always produces a fresh `Some`.
        std::thread::sleep(Duration::from_millis(2));
        tracker.mark_write();
        let second = tracker.last_write_at.expect("still set");
        assert!(
            second >= first,
            "mark_write must produce a non-decreasing instant"
        );
    }

    #[test]
    fn begin_clears_last_write_at() {
        // INSERT (outside txn); BEGIN; -> field cleared.
        let mut tracker = SessionWriteTracker::default();
        tracker.mark_write();
        assert!(tracker.last_write_at.is_some());
        tracker.on_start_transaction();
        assert!(
            tracker.last_write_at.is_none(),
            "BEGIN must clear pre-txn writes per the design"
        );
    }

    #[test]
    fn commit_refreshes_when_txn_had_writes() {
        // BEGIN; INSERT; COMMIT; -> field refreshed (RYW window applies post-commit).
        let mut tracker = SessionWriteTracker::default();
        tracker.on_start_transaction();
        tracker.mark_write();
        let in_txn = tracker.last_write_at.expect("write inside txn");
        std::thread::sleep(Duration::from_millis(2));
        tracker.on_commit();
        let post_commit = tracker
            .last_write_at
            .expect("COMMIT must keep last_write_at when txn had writes");
        assert!(
            post_commit > in_txn,
            "COMMIT must refresh the timestamp so RYW fires from now"
        );

        // BEGIN; SELECT; COMMIT (no writes); -> field stays None.
        let mut tracker = SessionWriteTracker::default();
        tracker.on_start_transaction();
        tracker.on_commit();
        assert!(
            tracker.last_write_at.is_none(),
            "COMMIT with no writes must leave the field unset"
        );
    }

    #[test]
    fn rollback_always_clears() {
        // BEGIN; INSERT; ROLLBACK; -> field cleared (writes never landed).
        let mut tracker = SessionWriteTracker::default();
        tracker.on_start_transaction();
        tracker.mark_write();
        tracker.on_rollback();
        assert!(tracker.last_write_at.is_none());

        // ROLLBACK without an active txn is a no-op.
        let mut tracker = SessionWriteTracker::default();
        tracker.on_rollback();
        assert!(tracker.last_write_at.is_none());
    }

    #[test]
    fn had_write_in_txn_reflects_field_presence() {
        // Inside a txn, had_write_in_txn() returns true iff last_write_at is Some.
        // (The state machine guarantees the field was None at txn start.)
        let mut tracker = SessionWriteTracker::default();
        tracker.on_start_transaction();
        assert!(!tracker.had_write_in_txn(ProxyState::InTransaction));
        tracker.mark_write();
        assert!(tracker.had_write_in_txn(ProxyState::InTransaction));

        // Outside a txn, never true regardless of last_write_at.
        let mut tracker = SessionWriteTracker::default();
        tracker.mark_write();
        assert!(!tracker.had_write_in_txn(ProxyState::Fallback));
        assert!(!tracker.had_write_in_txn(ProxyState::ProxyAlways));
        assert!(!tracker.had_write_in_txn(ProxyState::Never));
    }

    #[test]
    fn opportunistic_ryw_disabled_by_default() {
        // No window configured: opportunistic_ryw_active() is always false, even after a write.
        let mut tracker = SessionWriteTracker::default();
        assert!(!tracker.opportunistic_ryw_active());
        tracker.mark_write();
        assert!(!tracker.opportunistic_ryw_active());
    }

    #[test]
    fn opportunistic_ryw_active_within_window() {
        // 1s window: a fresh write keeps opportunistic_ryw_active() true until the deadline elapses.
        let mut tracker = SessionWriteTracker::new(Some(Duration::from_secs(1)));
        assert!(!tracker.opportunistic_ryw_active());
        tracker.mark_write();
        assert!(tracker.opportunistic_ryw_active());
        // Idempotent on the read path: still active a moment later.
        assert!(tracker.opportunistic_ryw_active());
    }

    #[test]
    fn opportunistic_ryw_lazy_clears_when_stale() {
        // Tiny window so we can let it elapse in the test.
        let mut tracker = SessionWriteTracker::new(Some(Duration::from_millis(5)));
        tracker.mark_write();
        assert!(tracker.opportunistic_ryw_active());
        std::thread::sleep(Duration::from_millis(15));
        // First call after expiry returns false and clears the deadline.
        assert!(!tracker.opportunistic_ryw_active());
        // Subsequent calls take the cheap path (no clock read needed semantically).
        assert!(!tracker.opportunistic_ryw_active());
    }

    #[test]
    fn opportunistic_ryw_cleared_on_begin() {
        let mut tracker = SessionWriteTracker::new(Some(Duration::from_secs(60)));
        tracker.mark_write();
        assert!(tracker.opportunistic_ryw_active());
        tracker.on_start_transaction();
        assert!(
            !tracker.opportunistic_ryw_active(),
            "BEGIN must clear RYW so the in-txn rule takes over"
        );
    }

    #[test]
    fn opportunistic_ryw_refreshed_on_commit_when_txn_had_writes() {
        // BEGIN; INSERT; COMMIT -> RYW window fires from the commit.
        let mut tracker = SessionWriteTracker::new(Some(Duration::from_secs(60)));
        tracker.on_start_transaction();
        tracker.mark_write();
        // Inside the txn, RYW is suppressed (BEGIN cleared the deadline; mark_write
        // re-armed it, but in-txn the routing layer ignores it). Verify the deadline
        // is in fact set so on_commit() refreshes rather than clears.
        assert!(tracker.last_write_at.is_some());
        std::thread::sleep(Duration::from_millis(2));
        tracker.on_commit();
        assert!(
            tracker.opportunistic_ryw_active(),
            "COMMIT must keep RYW armed so post-COMMIT reads see their own writes"
        );
    }

    #[test]
    fn opportunistic_ryw_cleared_on_rollback() {
        let mut tracker = SessionWriteTracker::new(Some(Duration::from_secs(60)));
        tracker.on_start_transaction();
        tracker.mark_write();
        tracker.on_rollback();
        assert!(
            !tracker.opportunistic_ryw_active(),
            "ROLLBACK must drop RYW since the writes never landed"
        );
    }

    #[test]
    fn set_autocommit_state_transitions_only() {
        // The state-machine helper handles ProxyState transitions; the timestamp lives
        // separately and is unaffected by autocommit toggles.
        let mut s = ProxyState::InTransaction;
        s.set_autocommit(false);
        assert_eq!(s, ProxyState::AutocommitOff);

        let mut s = ProxyState::AutocommitOff;
        s.set_autocommit(true);
        assert_eq!(s, ProxyState::Fallback);

        let mut s = ProxyState::ProxyAlways;
        s.set_autocommit(false);
        assert_eq!(s, ProxyState::ProxyAlways);

        let mut s = ProxyState::Never;
        s.set_autocommit(false);
        assert_eq!(s, ProxyState::Never);
    }

    #[test]
    fn end_transaction_state_transitions_only() {
        let mut s = ProxyState::InTransaction;
        s.end_transaction();
        assert_eq!(s, ProxyState::Fallback);

        // AutocommitOff stays AutocommitOff (a fresh implicit txn).
        let mut s = ProxyState::AutocommitOff;
        s.end_transaction();
        assert_eq!(s, ProxyState::AutocommitOff);

        let mut s = ProxyState::ProxyAlways;
        s.end_transaction();
        assert_eq!(s, ProxyState::ProxyAlways);
    }

    #[test]
    fn should_skip_cache_for_always_never_skips() {
        for state in [
            ProxyState::Never,
            ProxyState::Fallback,
            ProxyState::ProxyAlways,
            ProxyState::InTransaction,
            ProxyState::AutocommitOff,
        ] {
            for had_write in [false, true] {
                assert!(
                    !state.should_skip_cache_for(TrxCachePolicy::Always, had_write, false),
                    "Always must never be skipped without RYW (state={state:?}, had_write={had_write})"
                );
            }
        }
    }

    #[test]
    fn should_skip_cache_for_outside_transaction() {
        // Fallback is the normal "no transaction" state. Cache should always serve.
        let state = ProxyState::Fallback;
        for had_write in [false, true] {
            assert!(!state.should_skip_cache_for(TrxCachePolicy::Never, had_write, false));
            assert!(!state.should_skip_cache_for(TrxCachePolicy::UntilWrite, had_write, false));
            assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, had_write, false));
        }
    }

    #[test]
    fn should_skip_cache_for_proxy_always_skips_unless_always_policy() {
        let state = ProxyState::ProxyAlways;
        assert!(state.should_skip_cache_for(TrxCachePolicy::Never, false, false));
        assert!(state.should_skip_cache_for(TrxCachePolicy::UntilWrite, false, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, false, false));
    }

    #[test]
    fn should_skip_cache_for_in_transaction_until_write() {
        let state = ProxyState::InTransaction;
        // Read-only-so-far transaction: UntilWrite serves from cache, Never skips.
        assert!(state.should_skip_cache_for(TrxCachePolicy::Never, false, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::UntilWrite, false, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, false, false));

        // After a write: UntilWrite reverts to upstream-only, like Never.
        assert!(state.should_skip_cache_for(TrxCachePolicy::Never, true, false));
        assert!(state.should_skip_cache_for(TrxCachePolicy::UntilWrite, true, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, true, false));
    }

    #[test]
    fn should_skip_cache_for_autocommit_off_until_write() {
        let state = ProxyState::AutocommitOff;
        // Implicit transaction with no write yet: UntilWrite serves from cache.
        assert!(state.should_skip_cache_for(TrxCachePolicy::Never, false, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::UntilWrite, false, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, false, false));

        // After a write in the implicit transaction: skip.
        assert!(state.should_skip_cache_for(TrxCachePolicy::Never, true, false));
        assert!(state.should_skip_cache_for(TrxCachePolicy::UntilWrite, true, false));
        assert!(!state.should_skip_cache_for(TrxCachePolicy::Always, true, false));
    }

    #[test]
    fn should_skip_cache_for_opportunistic_ryw_overrides_every_policy() {
        // RYW is a correctness property; it overrides every per-cache policy when active.
        // BEGIN clears the deadline, so by construction `opportunistic_ryw_active=true` only happens
        // outside any transaction.
        for state in [
            ProxyState::Never,
            ProxyState::Fallback,
            ProxyState::ProxyAlways,
        ] {
            for policy in [
                TrxCachePolicy::Never,
                TrxCachePolicy::UntilWrite,
                TrxCachePolicy::Always,
            ] {
                assert!(
                    state.should_skip_cache_for(policy, false, true),
                    "RYW must override {policy:?} in {state:?}"
                );
            }
        }
    }

    #[test]
    fn skip_reason_for_distinguishes_until_write_after_write() {
        // UntilWrite + had_write: dashboards see "trx_after_write" so the rollout impact on
        // the auto-cache configuration is observable.
        for state in [ProxyState::InTransaction, ProxyState::AutocommitOff] {
            assert_eq!(
                state.skip_reason_for(TrxCachePolicy::UntilWrite, true, false),
                "trx_after_write"
            );
            // No write yet: still "trx" (though this path doesn't actually skip the cache).
            assert_eq!(
                state.skip_reason_for(TrxCachePolicy::UntilWrite, false, false),
                "trx"
            );
            // Never-policy in any txn state stays "trx".
            assert_eq!(
                state.skip_reason_for(TrxCachePolicy::Never, false, false),
                "trx"
            );
            assert_eq!(
                state.skip_reason_for(TrxCachePolicy::Never, true, false),
                "trx"
            );
        }

        // ProxyAlways always wins (over both "trx*" tags).
        assert_eq!(
            ProxyState::ProxyAlways.skip_reason_for(TrxCachePolicy::Never, false, false),
            "unsupported_set"
        );
        assert_eq!(
            ProxyState::ProxyAlways.skip_reason_for(TrxCachePolicy::UntilWrite, false, false),
            "unsupported_set"
        );

        // RYW takes precedence over ProxyAlways and the trx tags.
        assert_eq!(
            ProxyState::Fallback.skip_reason_for(TrxCachePolicy::Always, false, true),
            "opportunistic_ryw"
        );
        assert_eq!(
            ProxyState::ProxyAlways.skip_reason_for(TrxCachePolicy::Never, false, true),
            "opportunistic_ryw"
        );
    }
}
