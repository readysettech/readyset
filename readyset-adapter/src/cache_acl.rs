//! Per-cache authorization state (the cache ACL, REA-6708).
//!
//! The verdict matrix maps `(identity, cache)` to upstream's authorization
//! answer for that pair. The serve path consults it for the session's
//! effective identity before any shallow cache lookup: `Allowed` serves from
//! cache, anything else declines and the query proxies to upstream, which
//! re-authorizes it (deny-means-proxy).
//!
//! Every verdict is established by a probe on the background freshness
//! worker; no probe ever runs inline on a client's turn, and nothing a
//! session does on its own behalf -- creating the cache included -- writes
//! itself one. An identity with no probed cell reads `Unknown` and is
//! declined, so the matrix can only ever be behind, never permissive.

use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use readyset_client::query::QueryId;
use readyset_sql::ast::SqlIdentifier;
use tokio::sync::mpsc;

/// Capacity of the freshness worker's demand queue, which the serve path
/// feeds at query rate with `try_send` and which drops messages when full;
/// the periodic pass converges anything lost. Lifecycle events do not share
/// it -- see [`AclHandle::send_lifecycle`].
pub const ACL_QUEUE_CAPACITY: usize = 64;

/// Upstream's authorization answer for one `(identity, cache)` cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Allowed,
    Denied,
    Unknown,
}

impl Verdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Allowed => "allowed",
            Verdict::Denied => "denied",
            Verdict::Unknown => "unknown",
        }
    }
}

/// One resolved cell of the verdict matrix.
#[derive(Clone, Copy, Debug)]
pub struct MatrixEntry {
    pub verdict: Verdict,
    /// Wall clock of the write that set this cell, for `readyset.cache_grants`.
    pub probed_at: SystemTime,
}

/// The verdict matrix: `(identity, cache) -> MatrixEntry`.
///
/// The row key is the session's effective identity: the login user, or the
/// role a session has assumed via `SET ROLE`. Only resolved cells are stored;
/// a missing entry reads as [`Verdict::Unknown`], which the serve path treats
/// exactly like [`Verdict::Denied`] -- decline the cache, proxy upstream.
///
/// Locking: the `DashMap` shards are the only synchronization. Readers copy
/// the entry out and drop the guard immediately; no guard is held across an
/// `.await`. Bulk discards run on the freshness worker via `retain`, and a
/// reader racing one sees either a discarded cell (`Unknown`, fail-safe) or
/// the prior verdict.
#[derive(Default)]
pub struct AclMatrix {
    cells: DashMap<(SqlIdentifier, QueryId), MatrixEntry, ahash::RandomState>,
}

impl AclMatrix {
    /// Hot-path read: one shard `get`, entry copied out. Missing means
    /// [`Verdict::Unknown`].
    pub fn verdict_for(&self, identity: &SqlIdentifier, cache: QueryId) -> Verdict {
        self.cells
            .get(&(identity.clone(), cache))
            .map(|e| e.verdict)
            .unwrap_or(Verdict::Unknown)
    }

    /// Store a probed verdict, returning the prior one so callers can count
    /// flips.
    pub fn record(
        &self,
        identity: SqlIdentifier,
        cache: QueryId,
        verdict: Verdict,
    ) -> Option<Verdict> {
        self.cells
            .insert(
                (identity, cache),
                MatrixEntry {
                    verdict,
                    probed_at: SystemTime::now(),
                },
            )
            .map(|e| e.verdict)
    }

    /// Whether the identity has at least one stored cell. The worker uses this
    /// to deduplicate resolve-identity requests: query-rate misses for an
    /// identity whose row is already being resolved are dropped.
    pub fn has_row(&self, identity: &SqlIdentifier) -> bool {
        self.cells.iter().any(|kv| kv.key().0 == *identity)
    }

    /// Discard an identity's row; its cells read `Unknown` until re-probed.
    pub fn discard_row(&self, identity: &SqlIdentifier) {
        self.cells.retain(|(id, _), _| id != identity);
    }

    /// Discard a cache's column, e.g. on `DROP CACHE`.
    pub fn discard_column(&self, cache: QueryId) {
        self.cells.retain(|(_, c), _| *c != cache);
    }

    /// Discard every cell, e.g. on `DROP ALL CACHES`.
    pub fn clear(&self) {
        self.cells.clear();
    }

    /// Snapshot the stored cells for observability consumers.
    pub fn snapshot(&self) -> Vec<(SqlIdentifier, QueryId, MatrixEntry)> {
        self.cells
            .iter()
            .map(|kv| (kv.key().0.clone(), kv.key().1, *kv.value()))
            .collect()
    }

    /// Derive the pairs with no stored cell: `identities x caches` minus the
    /// stored cells. The `Unknown` default is not materialized (A1), so the
    /// consumers that must enumerate it (the worker's fill pass, the
    /// unknown-pair metric, `readyset.cache_grants`) compute it on demand.
    pub fn unknown_pairs(
        &self,
        identities: &[SqlIdentifier],
        caches: &[QueryId],
    ) -> Vec<(SqlIdentifier, QueryId)> {
        let mut pairs = Vec::new();
        for identity in identities {
            for cache in caches {
                if !self.cells.contains_key(&(identity.clone(), *cache)) {
                    pairs.push((identity.clone(), *cache));
                }
            }
        }
        pairs
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

/// What prompted a full pass, for the loop-runs metric tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassTrigger {
    Periodic,
    FlushPrivileges,
}

impl PassTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            PassTrigger::Periodic => "periodic",
            PassTrigger::FlushPrivileges => "flush_privileges",
        }
    }
}

/// Who created a cache, as the session that issued the creation was judged:
/// its effective identity, plus the login user that identity was assumed
/// from. Upstream accepting the session's `SET ROLE` proved that membership,
/// so `via` is the connection the worker can probe an assumed role through.
#[derive(Clone, Debug)]
pub struct CacheCreator {
    pub identity: SqlIdentifier,
    pub via: Option<SqlIdentifier>,
}

/// Work items for the ACL freshness worker.
#[derive(Clone, Debug)]
pub enum AclMessage {
    /// Re-read every user's grant fingerprint, re-probe the rows that
    /// flipped, then probe any cell still `Unknown`.
    FullPass { trigger: PassTrigger },
    /// Probe this cache's column across all users, the creator first, so the
    /// identity most likely to read the new cache resolves before the rest.
    CacheCreated {
        cache: QueryId,
        creator: Option<CacheCreator>,
    },
    /// Discard the cache's column.
    CacheDropped { cache: QueryId },
    /// Re-probe the user's row across all caches (new user, changed
    /// password).
    UserAltered { user: SqlIdentifier },
    /// Discard the user's row.
    UserDropped { user: SqlIdentifier },
    /// The serve path saw an identity with no row -- a role assumed via
    /// `SET ROLE` -- and wants it resolved. `via` is the session's login
    /// user: upstream accepting their `SET ROLE` proved membership, so the
    /// worker can probe the role through that member's own connection.
    /// Deduplicated by the worker.
    ResolveIdentity {
        identity: SqlIdentifier,
        via: Option<SqlIdentifier>,
    },
}

/// Shared handle to the ACL: the matrix every `Backend` reads on the serve
/// path, plus the senders feeding the freshness worker.
///
/// The two senders separate the worker's inputs by rate and by consequence.
/// Lifecycle events are bounded in number by the caches and users that exist
/// and each one is the only thing that converges its row or column before the
/// next periodic pass, so they are never dropped. Demand resolution arrives
/// at query rate and is redundant with the periodic pass, so it is bounded
/// and droppable. Neither send ever blocks the client's turn.
#[derive(Clone)]
pub struct AclHandle {
    matrix: Arc<AclMatrix>,
    lifecycle: Option<mpsc::UnboundedSender<AclMessage>>,
    demand: Option<mpsc::Sender<AclMessage>>,
}

impl AclHandle {
    pub fn new(
        matrix: Arc<AclMatrix>,
        lifecycle: mpsc::UnboundedSender<AclMessage>,
        demand: mpsc::Sender<AclMessage>,
    ) -> Self {
        Self {
            matrix,
            lifecycle: Some(lifecycle),
            demand: Some(demand),
        }
    }

    /// A handle with no worker behind it. Used when authentication is off --
    /// where the enforcement seams never consult the matrix -- and as the
    /// default in tests.
    pub fn disabled() -> Self {
        Self {
            matrix: Arc::new(AclMatrix::default()),
            lifecycle: None,
            demand: None,
        }
    }

    pub fn matrix(&self) -> &AclMatrix {
        &self.matrix
    }

    /// Enqueue a cache or user lifecycle event. Never dropped: losing one
    /// leaves its row or column `Unknown` -- every session off-cache for that
    /// query -- until the next periodic pass.
    pub fn send_lifecycle(&self, msg: AclMessage) {
        if let Some(lifecycle) = &self.lifecycle {
            let _ = lifecycle.send(msg);
        }
    }

    /// Ask the worker to resolve an identity the serve path saw with no row.
    /// A full queue drops the message; the identity stays `Unknown` and is
    /// declined until the next periodic pass resolves it.
    pub fn send_demand(&self, msg: AclMessage) {
        if let Some(demand) = &self.demand {
            let _ = demand.try_send(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> SqlIdentifier {
        s.into()
    }

    fn qid(s: &str) -> QueryId {
        QueryId::from_unparsed_select(s)
    }

    fn allow(matrix: &AclMatrix, identity: SqlIdentifier, cache: QueryId) {
        let _ = matrix.record(identity, cache, Verdict::Allowed);
    }

    #[test]
    fn missing_cell_reads_unknown() {
        let matrix = AclMatrix::default();
        assert_eq!(
            matrix.verdict_for(&id("alice"), qid("select 1")),
            Verdict::Unknown
        );
    }

    #[test]
    fn record_returns_prior_verdict_for_flip_counting() {
        let matrix = AclMatrix::default();
        let cache = qid("select 1");
        assert_eq!(matrix.record(id("alice"), cache, Verdict::Allowed), None);
        assert_eq!(
            matrix.record(id("alice"), cache, Verdict::Denied),
            Some(Verdict::Allowed)
        );
        assert_eq!(matrix.verdict_for(&id("alice"), cache), Verdict::Denied);
    }

    #[test]
    fn a_probed_verdict_applies_only_to_its_own_identity() {
        let matrix = AclMatrix::default();
        let cache = qid("select 1");
        allow(&matrix, id("alice"), cache);
        assert_eq!(matrix.verdict_for(&id("alice"), cache), Verdict::Allowed);
        // Every other identity stays Unknown.
        assert_eq!(matrix.verdict_for(&id("bob"), cache), Verdict::Unknown);
    }

    #[test]
    fn discard_row_resets_to_unknown() {
        let matrix = AclMatrix::default();
        let (c1, c2) = (qid("select 1"), qid("select 2"));
        allow(&matrix, id("alice"), c1);
        allow(&matrix, id("alice"), c2);
        allow(&matrix, id("bob"), c1);
        matrix.discard_row(&id("alice"));
        assert_eq!(matrix.verdict_for(&id("alice"), c1), Verdict::Unknown);
        assert_eq!(matrix.verdict_for(&id("alice"), c2), Verdict::Unknown);
        assert_eq!(matrix.verdict_for(&id("bob"), c1), Verdict::Allowed);
    }

    #[test]
    fn discard_column_resets_to_unknown() {
        let matrix = AclMatrix::default();
        let (c1, c2) = (qid("select 1"), qid("select 2"));
        allow(&matrix, id("alice"), c1);
        allow(&matrix, id("alice"), c2);
        matrix.discard_column(c1);
        assert_eq!(matrix.verdict_for(&id("alice"), c1), Verdict::Unknown);
        assert_eq!(matrix.verdict_for(&id("alice"), c2), Verdict::Allowed);
    }

    #[test]
    fn unknown_pairs_derived_on_demand() {
        let matrix = AclMatrix::default();
        let (c1, c2) = (qid("select 1"), qid("select 2"));
        let users = [id("alice"), id("bob")];
        allow(&matrix, id("alice"), c1);
        let pairs = matrix.unknown_pairs(&users, &[c1, c2]);
        assert_eq!(pairs.len(), 3);
        assert!(!pairs.contains(&(id("alice"), c1)));
        assert!(pairs.contains(&(id("alice"), c2)));
        assert!(pairs.contains(&(id("bob"), c1)));
        assert!(pairs.contains(&(id("bob"), c2)));
    }

    #[test]
    fn disabled_handle_reads_unknown_and_drops_sends() {
        let handle = AclHandle::disabled();
        handle.send_lifecycle(AclMessage::FullPass {
            trigger: PassTrigger::Periodic,
        });
        handle.send_demand(AclMessage::ResolveIdentity {
            identity: id("alice"),
            via: None,
        });
        assert_eq!(
            handle.matrix().verdict_for(&id("alice"), qid("select 1")),
            Verdict::Unknown
        );
    }

    /// Lifecycle events survive a demand queue saturated by the serve path:
    /// losing one would leave its column Unknown for every session until the
    /// next periodic pass.
    #[test]
    fn a_saturated_demand_queue_does_not_drop_lifecycle_events() {
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
        let (demand_tx, _demand_rx) = mpsc::channel(ACL_QUEUE_CAPACITY);
        let handle = AclHandle::new(Arc::new(AclMatrix::default()), lifecycle_tx, demand_tx);

        for _ in 0..ACL_QUEUE_CAPACITY * 2 {
            handle.send_demand(AclMessage::ResolveIdentity {
                identity: id("limited"),
                via: None,
            });
        }
        for _ in 0..ACL_QUEUE_CAPACITY * 2 {
            handle.send_lifecycle(AclMessage::CacheDropped {
                cache: qid("select 1"),
            });
        }

        let mut delivered = 0;
        while lifecycle_rx.try_recv().is_ok() {
            delivered += 1;
        }
        assert_eq!(delivered, ACL_QUEUE_CAPACITY * 2);
    }
}
