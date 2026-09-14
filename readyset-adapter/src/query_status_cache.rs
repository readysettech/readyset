//! The query status cache provides a thread-safe window into an adapter's
//! knowledge about queries, currently the migration status of a query in
//! ReadySet.
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64, AtomicUsize};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use clap::ValueEnum;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lru::LruCache;
use metrics::{counter, gauge};
use parking_lot::{Mutex, RwLock};
use tracing::warn;

use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::*;
use readyset_client::{ShallowViewRequest, ViewCreateRequest};
use readyset_data::DfValue;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{CacheType, Relation, SqlIdentifier, TrxCachePolicy};
use readyset_sql_passes::adapter_rewrites::{
    AdapterRewriteParams, CacheLookupKey, DfQueryParameters, LiteralSlots, ValueSlots,
};

use readyset_sql_parsing::ParsingPreset;
use schema_catalog::{SchemaCatalogHandle, SchemaChangeHandler, SchemaGeneration};

use crate::table_extraction_visitor::{
    extract_from_view_create_request, extract_referenced_tables,
};

pub const DEFAULT_QUERY_STATUS_CAPACITY: usize = 100_000;

/// Soft cap on the number of [`QueryId`]s the in-request-path shallow auto-
/// cache filter will remember.  When this is exceeded the map is bulk-cleared
/// and the overflow counter increments.  A realistic workload's set of
/// ineligible queries is on the order of hundreds; crossing this cap is a
/// signal of pathological client behaviour rather than a normal condition.
/// Memory footprint at the cap is on the order of 100 MB (a `u64` key and a
/// short reason `String` per entry plus `hashbrown` slot + control-byte
/// overhead).
pub const SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP: usize = 1_000_000;

/// A metadata cache for all queries that have been processed by this
/// adapter. Thread-safe.
#[derive(Debug)]
pub struct QueryStatusCache {
    /// A thread-safe hash map that holds the query status of each query that
    /// has been sent to this adapter, keyed by the query's [`QueryId`].
    ///
    /// This map is used on the hot path to determine whether to route queries to upstream or to
    /// readyset.
    id_to_status: DashMap<QueryId, QueryStatus, ahash::RandomState>,

    /// A handle to a more detailed, persistent cache of Query information, which holds the full
    /// query strings. This structure is not used on the hot path, but rather for other auxiliary
    /// commands that seek more information about the queries we have processed so far.
    persistent_handle: PersistentStatusCacheHandle,

    /// Holds the current style of migration, whether async or explicit, which may change the
    /// behavior of some internal methods.
    style: MigrationStyle,

    /// Whether to store a list of pending inlined migrations. Inlined migrations are those with
    /// literal values inlined into certain placeholder positions in the query.
    ///
    /// Currently unused.
    placeholder_inlining: bool,

    /// Maps [`QueryId`]s the in-request-path shallow-cache eligibility filter
    /// has rejected to the reason for the rejection.  Consulted by
    /// `try_auto_create_shallow_cache` so the AST walk only runs once per
    /// ineligible query; the reason surfaces in `SHOW PROXIED QUERIES`.
    ///
    /// Strictly an implementation detail of the implicit auto-create path:
    /// explicit `CREATE SHALLOW CACHE` DDL and `/*rs+ CREATE SHALLOW CACHE */`
    /// hint flows do not consult it, so a stuck entry never blocks a user
    /// from creating a cache deliberately.  Bounded by
    /// [`SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP`]; on overflow the map is bulk-
    /// cleared and a warning is logged.
    shallow_auto_create_skip: DashMap<QueryId, String, ahash::RandomState>,

    /// The caches that keep some of their author's literals inline, indexed by the canonical
    /// shape a read hashes to. One shape can hold several, since two caches differing only in a
    /// literal share it.
    ///
    /// Such a cache takes a form no read produces on its own, so a read reaches it by matching
    /// what it carries at each parameter position against what the cache does.
    ///
    /// The shape keying an entry comes from the catalog the cache was created against, so a
    /// schema change can move it. Such a change also takes the status of every deep cache over
    /// the tables it names, and an entry whose cache has no status routes nothing, so the two are
    /// given up together -- see [`Self::remove_inline_literal_caches_referencing_tables`]. What
    /// files an entry again is the persisted DDL, once the cache is recreated.
    ///
    /// Only a deep cache is ever registered here, which is what makes giving them up safe: a
    /// shallow cache refreshes on its own TTL and nothing re-migrates it, so dropping its status
    /// strands it (REA-6692).
    inline_literal_caches: DashMap<QueryId, Vec<InlineLiteralCache>, ahash::RandomState>,

    /// How many are registered, so a read in a deployment that uses none skips working its shape
    /// out. Counted up before an entry appears and down after one is gone, so a reader seeing a
    /// non-zero count may find nothing -- costing it only the lookup -- while one seeing zero can
    /// be sure there is nothing to find.
    inline_literal_caches_live: AtomicUsize,

    /// Bumped whenever one is registered or forgotten, so a statement that planned against an
    /// earlier value can tell that the set has moved without comparing it.
    inline_literal_caches_generation: AtomicU64,

    /// Caches whose statement names a relation the catalog has not caught up to. Retried on the
    /// next catalog change, which is what a snapshot still filling in produces.
    inline_literal_deferred: Mutex<Vec<CacheDDLRequest>>,
}

/// A cache that keeps its author's literals inline, and what a read needs to reach it.
///
/// Such a cache takes a form no read produces on its own, so it is filed under the shape a read
/// does hash to, and a read that carries the literals in [`Self::slots`] is served from the form
/// and parameters here.
#[derive(Debug)]
pub struct InlineLiteralRegistration {
    /// The canonical shape a read hashes to.
    pub shape: QueryId,
    /// What the cache holds at each of that shape's parameter positions.
    pub slots: LiteralSlots,
    /// The cache's own form.
    pub request: ViewCreateRequest,
    /// Its parameters, whose values a matching read replaces with its own.
    pub params: DfQueryParameters,
}

/// A cache that keeps some of its author's literals inline, and what a read needs to reach it.
#[derive(Debug, Clone)]
pub(crate) struct InlineLiteralCache {
    /// What the cache holds at each canonical parameter position. An ad-hoc read whose own slots
    /// match belongs to this cache; see [`LiteralSlots::match_read`].
    pub(crate) slots: LiteralSlots,
    /// The same positions as values, which an execute is matched by; see [`ValueSlots::match_values`].
    pub(crate) values: ValueSlots,
    /// The cache's own form, which a matching read is served from. It is kept because no read
    /// produces this form, so there is nothing to derive it from later.
    ///
    /// Shared, so a lookup takes it while holding the map's guard and pays for a copy of it,
    /// if it needs one at all, once the guard is gone.
    pub(crate) request: Arc<ViewCreateRequest>,
    /// The id of that form, which is what a read served from it is logged under.
    pub(crate) query_id: QueryId,
    /// The cache's parameters. Everything but the values they carry is a property of its form, so
    /// a matching read substitutes only the values its own positions hold.
    pub(crate) params: Arc<DfQueryParameters>,
    /// The view an execute is sent to: one of the names claiming the entry.
    pub(crate) name: Relation,
    pub(crate) trx_cache_policy: TrxCachePolicy,
    /// The caches registered here, each with the DDL that created it. Two agreeing on every
    /// position share the entry, which lives as long as one of them does, so a DROP CACHE gives
    /// up only its own claim. The DDL is what files the cache again after a schema change.
    pub(crate) caches: HashMap<Relation, Option<CacheDDLRequest>>,
}

impl InlineLiteralCache {
    /// What a prepared statement keeps of this cache to match its executes against.
    fn candidate(&self) -> InlineLiteralCandidate {
        InlineLiteralCandidate {
            values: self.values.clone(),
            name: self.name.clone(),
            query_id: self.query_id,
            trx_cache_policy: self.trx_cache_policy,
            params: Arc::clone(&self.params),
        }
    }
}

/// A cache filed under one of a prepared statement's shapes, as the statement keeps it: enough to
/// match an execute and serve it.
#[derive(Debug, Clone)]
pub(crate) struct InlineLiteralCandidate {
    values: ValueSlots,
    name: Relation,
    query_id: QueryId,
    trx_cache_policy: TrxCachePolicy,
    params: Arc<DfQueryParameters>,
}

/// What serving a prepared statement's execute from a cache keeping its author's literals inline
/// needs: the cache's view and parameters, and the values this execute puts in them.
#[derive(Debug)]
pub struct InlineLiteralHit {
    pub name: Relation,
    pub query_id: QueryId,
    pub trx_cache_policy: TrxCachePolicy,
    pub params: Arc<DfQueryParameters>,
    pub values: Vec<DfValue>,
}

/// A miss is how a read whose values no cache kept reaches the upstream, so from outside it is
/// indistinguishable from the feature being broken.
fn record_inline_literal_lookup(hit: bool) {
    counter!(
        metric::INLINE_LITERAL_CACHE_LOOKUPS,
        "result" => if hit { "hit" } else { "miss" },
    )
    .increment(1);
}

/// The cache among `caches`, all filed under one shape, that an execute belongs to given the
/// values at its canonical positions, and what serving it needs. `values` is asked for only when
/// there is a cache to match.
pub(crate) fn match_inline_literal_values(
    caches: &[InlineLiteralCandidate],
    read: &LiteralSlots,
    values: impl FnOnce() -> Option<Vec<DfValue>>,
) -> Option<InlineLiteralHit> {
    if caches.is_empty() {
        return None;
    }
    let values = values()?;
    let matched = caches.iter().find_map(|cache| {
        let values = cache.values.match_values(read, &values)?;
        Some(InlineLiteralHit {
            name: cache.name.clone(),
            query_id: cache.query_id,
            trx_cache_policy: cache.trx_cache_policy,
            params: Arc::clone(&cache.params),
            values,
        })
    });
    record_inline_literal_lookup(matched.is_some());
    matched
}

#[derive(Debug)]
/// A handle to persistent metadata for all queries that have been processed by this adapter.
pub struct PersistentStatusCacheHandle {
    /// An [`LRUCache`] that holds the full [`Query`] as well as its associated
    /// [`QueryStatus`] for a fixed number of queries.
    statuses: RwLock<LruCache<QueryId, (Query, QueryStatus)>>,

    /// List of pending inlined migrations. Contains the query to be inlined, and the sets of
    /// parameters to use for inlining.
    pending_inlined_migrations: DashMap<ViewCreateRequest, HashSet<Vec<DfValue>>>,
}

pub struct ReportableMetrics {
    pub id_to_status_size: u64,
    pub statuses_size: u64,
    pub pending_inlined_migrations_size: u64,
}

impl Default for PersistentStatusCacheHandle {
    fn default() -> Self {
        Self {
            statuses: RwLock::new(LruCache::new(
                DEFAULT_QUERY_STATUS_CAPACITY
                    .try_into()
                    .expect("num persisted queries is not zero"),
            )),
            pending_inlined_migrations: Default::default(),
        }
    }
}

impl PersistentStatusCacheHandle {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            statuses: RwLock::new(LruCache::new(
                capacity.try_into().expect("capacity is not zero"),
            )),
            pending_inlined_migrations: Default::default(),
        }
    }

    fn insert_with_status(&self, q: Query, id: QueryId, status: QueryStatus) {
        // Deadlock avoidance: If `with_mut_status` is passed a Fn that tries to write the RwLock,
        // it will result in a deadlock.
        match self.statuses.try_write_for(Duration::from_millis(10)) {
            Some(mut status_guard) => {
                status_guard.put(id, (q, status));
                gauge!(metric::QUERY_STATUS_CACHE_PERSISTENT_CACHE_SIZE)
                    .set(status_guard.len() as f64);
            }
            None => {
                warn!(query_id=%id, "Avoiding deadlock when trying to insert")
            }
        }
    }

    fn cached_list(&self) -> Vec<(QueryId, Arc<ViewCreateRequest>, QueryStatus)> {
        let statuses = self.statuses.read();
        statuses
            .iter()
            .filter_map(|(query_id, (query, status))| match query {
                Query::Parsed(view) => {
                    if status.is_cached(None) {
                        Some((*query_id, view.clone(), status.clone()))
                    } else {
                        None
                    }
                }
                // ShallowParsed queries are not included in cached_list since they can't be
                // converted to ViewCreateRequest. They are handled separately.
                Query::ShallowParsed(..) => None,
                Query::ParseFailed(..) => None,
            })
            .collect::<Vec<_>>()
    }

    fn proxied_list(
        &self,
        style: MigrationStyle,
        cache_type: CacheType,
        shallow_auto_create_skip: &DashMap<QueryId, String, ahash::RandomState>,
    ) -> Vec<ProxiedQuery> {
        let statuses = self.statuses.read();
        statuses
            .iter()
            .filter_map(|(query_id, (query, status))| {
                if matches!(style, MigrationStyle::Async | MigrationStyle::InRequestPath)
                    && !status.is_unsupported()
                    && !(status.is_supported() && shallow_auto_create_skip.contains_key(query_id))
                {
                    return None;
                }
                if matches!(style, MigrationStyle::Explicit) && !status.is_proxied() {
                    return None;
                }
                if cache_type == CacheType::Shallow && !matches!(query, Query::ShallowParsed(..)) {
                    return None;
                }
                if cache_type == CacheType::Deep
                    && !matches!(query, Query::Parsed(..) | Query::ParseFailed(..))
                {
                    return None;
                }

                Some(ProxiedQuery {
                    id: *query_id,
                    query: query.clone(),
                    status: status.clone(),
                })
            })
            .collect()
    }
}

/// Keys into the queries stored in `QueryStatusCache`
///
/// This trait exists to allow us to overload the notion of "query" to include both successfully
/// parsed queries and queries that have failed to parse.
// The methods in this trait use closures because the reference types returned by DashMap include
// the key type, so methods that *return* lifetime-bound references would not be able to be generic
pub trait QueryStatusKey: Into<Query> + Hash + Clone {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R;

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R;

    fn query_id(&self) -> QueryId;
}

impl QueryStatusKey for Query {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        match self {
            Query::Parsed(k) => k.with_status(cache, f),
            Query::ShallowParsed(k) => k.with_status(cache, f),
            Query::ParseFailed(k, _) => k.with_status(cache, f),
        }
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        match self {
            Query::Parsed(k) => k.with_mut_status(cache, f),
            Query::ShallowParsed(k) => k.with_mut_status(cache, f),
            Query::ParseFailed(k, _) => k.with_mut_status(cache, f),
        }
    }

    fn query_id(&self) -> QueryId {
        self.into()
    }
}

impl QueryStatusKey for ShallowViewRequest {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        let id = QueryId::from(self);
        // Since this isn't mutating anything, we only need to access the in-memory map.
        f(cache.id_to_status.get(&id).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        let id = QueryId::from(self);
        // Since this is potentially mutating, we need to apply F to both the in-memory and the
        // persistent version of the status.
        f(cache.id_to_status.get_mut(&id).as_deref_mut());
        let mut statuses = cache.persistent_handle.statuses.write();
        let transformed_status = statuses.get_mut(&id).map(|(_, status)| status);
        f(transformed_status)
    }

    fn query_id(&self) -> QueryId {
        self.into()
    }
}

impl QueryStatusKey for ViewCreateRequest {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        let id = QueryId::from(self);
        // Since this isn't mutating anything, we only need to access the in-memory map.
        f(cache.id_to_status.get(&id).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        let id = QueryId::from(self);
        // Since this is potentially mutating, we need to apply F to both the in-memory and the
        // persistent version of the status.
        f(cache.id_to_status.get_mut(&id).as_deref_mut());
        let mut statuses = cache.persistent_handle.statuses.write();
        let transformed_status = statuses.get_mut(&id).map(|(_, status)| status);
        f(transformed_status)
    }

    fn query_id(&self) -> QueryId {
        self.into()
    }
}

impl QueryStatusKey for String {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        let id = QueryId::from_unparsed_select(self);
        // Since this isn't mutating anything, we only need to access the in-memory map.
        f(cache.id_to_status.get(&id).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        let id = QueryId::from_unparsed_select(self);
        // Since this is potentially mutating, we need to apply F to both the in-memory and the
        // persistent version of the status.
        f(cache.id_to_status.get_mut(&id).as_deref_mut());
        let mut statuses = cache.persistent_handle.statuses.write();
        let transformed_status = statuses.get_mut(&id).map(|(_, status)| status);
        f(transformed_status)
    }

    fn query_id(&self) -> QueryId {
        QueryId::from_unparsed_select(self)
    }
}

impl Default for QueryStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStatusCache {
    /// Constructs a new QueryStatusCache with the migration style set to InRequestPath and a
    /// default capacity of [`DEFAULT_QUERY_STATUS_CAPACITY`]
    pub fn new() -> QueryStatusCache {
        QueryStatusCache {
            id_to_status: Default::default(),
            persistent_handle: Default::default(),
            style: MigrationStyle::InRequestPath,
            placeholder_inlining: false,
            shallow_auto_create_skip: Default::default(),
            inline_literal_caches: Default::default(),
            inline_literal_caches_live: Default::default(),
            inline_literal_caches_generation: Default::default(),
            inline_literal_deferred: Default::default(),
        }
    }

    /// Constructs a new QueryStatusCache with the migration style set to InRequestPath and
    /// provided capacity that must be non-zero.
    pub fn with_capacity(capacity: usize) -> QueryStatusCache {
        QueryStatusCache {
            id_to_status: Default::default(),
            persistent_handle: PersistentStatusCacheHandle::with_capacity(capacity),
            style: MigrationStyle::InRequestPath,
            placeholder_inlining: false,
            shallow_auto_create_skip: Default::default(),
            inline_literal_caches: Default::default(),
            inline_literal_caches_live: Default::default(),
            inline_literal_caches_generation: Default::default(),
            inline_literal_deferred: Default::default(),
        }
    }

    /// Returns true if the in-request-path shallow auto-create filter has
    /// previously rejected this query; the caller should skip auto-creation
    /// without re-walking the AST.
    pub fn is_shallow_auto_create_skipped(&self, id: QueryId) -> bool {
        self.shallow_auto_create_skip.contains_key(&id)
    }

    /// The reason the in-request-path filter rejected this query, if any.
    pub fn shallow_auto_create_skip_reason(&self, id: QueryId) -> Option<String> {
        self.shallow_auto_create_skip
            .get(&id)
            .map(|reason| reason.value().clone())
    }

    /// Forget all remembered auto-create rejections, so previously-skipped
    /// queries are re-evaluated on their next execution. Called when the
    /// shallow-cache function allowlist changes (e.g.
    /// `ALTER READYSET ADD SHALLOW CACHE ALLOWED FUNCTION ...`): a query that
    /// was rejected for a now-allowed function must get another chance without
    /// requiring a restart.
    pub fn clear_shallow_auto_create_skips(&self) {
        self.shallow_auto_create_skip.clear();
        gauge!(metric::SHALLOW_AUTO_CREATE_SKIP_SET_SIZE)
            .set(self.shallow_auto_create_skip.len() as f64);
    }

    /// Whether any cache keeping literals inline is registered, so a read can skip working its
    /// own shape out where none is.
    pub fn may_have_inline_literal_caches(&self) -> bool {
        self.inline_literal_caches_live
            .load(atomic::Ordering::Acquire)
            > 0
    }

    /// How many times the set of caches keeping literals inline has changed. A statement holding
    /// an earlier value checked its shapes before some of them existed, so checking again can find
    /// a cache it missed.
    pub fn inline_literal_caches_generation(&self) -> u64 {
        self.inline_literal_caches_generation
            .load(atomic::Ordering::Acquire)
    }

    /// The caches keeping their literals inline filed under `shape`, for a statement to match its
    /// executes against until [`Self::inline_literal_caches_generation`] moves.
    pub(crate) fn inline_literal_caches_under(
        &self,
        shape: &QueryId,
    ) -> Vec<InlineLiteralCandidate> {
        self.inline_literal_caches
            .get(shape)
            .map(|caches| caches.iter().map(InlineLiteralCache::candidate).collect())
            .unwrap_or_default()
    }

    /// The form and parameters of the cache under `shape` that `read` belongs to, and what the
    /// read carried at the positions that cache parameterized.
    ///
    /// Caches are tried most-specific first, so a read that could belong to two of them reaches
    /// the same one every time. Only what serving the read needs is taken, leaving the matcher's
    /// own slots and the names claiming the entry behind.
    pub fn match_inline_literal_cache(
        &self,
        shape: &QueryId,
        read: &LiteralSlots,
    ) -> Option<(
        Arc<ViewCreateRequest>,
        Arc<DfQueryParameters>,
        CacheLookupKey,
    )> {
        let caches = self.inline_literal_caches.get(shape)?;
        let matched = caches.value().iter().find_map(|cache| {
            let matched = cache.slots.match_read(read)?;
            // Shared handles, so the copies a hit needs are made once the guard is gone.
            Some((
                Arc::clone(&cache.request),
                Arc::clone(&cache.params),
                matched,
            ))
        });
        record_inline_literal_lookup(matched.is_some());
        matched
    }

    /// [`match_inline_literal_values`] over the caches filed under `shape`.
    pub fn match_inline_literal_values(
        &self,
        shape: &QueryId,
        read: &LiteralSlots,
        values: impl FnOnce() -> Option<Vec<DfValue>>,
    ) -> Option<InlineLiteralHit> {
        match_inline_literal_values(&self.inline_literal_caches_under(shape), read, values)
    }

    /// Register a cache that keeps literals inline. A cache agreeing with one already registered
    /// on every position joins it rather than adding an entry of its own.
    pub fn register_inline_literal_cache(
        &self,
        registration: InlineLiteralRegistration,
        name: Relation,
        trx_cache_policy: TrxCachePolicy,
        ddl: Option<CacheDDLRequest>,
    ) -> ReadySetResult<()> {
        let InlineLiteralRegistration {
            shape,
            slots,
            request,
            params,
        } = registration;
        let values = slots.to_values(params.dialect())?;
        let mut caches = self.inline_literal_caches.entry(shape).or_default();
        match caches.iter_mut().find(|cache| cache.slots == slots) {
            Some(cache) => {
                cache.caches.insert(name, ddl);
            }
            None => {
                self.inline_literal_caches_live
                    .fetch_add(1, atomic::Ordering::Release);
                self.inline_literal_caches_generation
                    .fetch_add(1, atomic::Ordering::Release);
                caches.push(InlineLiteralCache {
                    slots,
                    values,
                    query_id: QueryId::from(&request),
                    request: Arc::new(request),
                    params: Arc::new(params),
                    name: name.clone(),
                    trx_cache_policy,
                    caches: HashMap::from([(name, ddl)]),
                });
                // A read tries these in order, so the one holding the most literals wins a read
                // that could belong to two of them. The slots break a tie, which keeps the winner
                // the same across restarts, where these are registered in recovery order.
                caches.sort_by(|a, b| {
                    b.slots
                        .inline_positions()
                        .cmp(&a.slots.inline_positions())
                        .then_with(|| a.slots.cmp(&b.slots))
                });
            }
        }
        Ok(())
    }

    /// Whether the cache called `name` keeps some of its author's literals inline.
    pub fn keeps_literals_inline(&self, name: &Relation) -> bool {
        self.may_have_inline_literal_caches()
            && self.inline_literal_caches.iter().any(|entry| {
                entry
                    .value()
                    .iter()
                    .any(|cache| cache.caches.contains_key(name))
            })
    }

    /// Give up `name`'s claim, forgetting a cache no name claims any more. Used by DROP CACHE.
    pub fn remove_inline_literal_cache_by_name(&self, name: &Relation) {
        if !self.may_have_inline_literal_caches() {
            return;
        }
        // `retain` can drop several entries in one pass, so count what actually went.
        let mut gone = 0;
        let mut claimed_by_name = false;
        self.inline_literal_caches.retain(|_, caches| {
            caches.retain_mut(|cache| {
                claimed_by_name |= cache.caches.remove(name).is_some();
                // An execute is sent to a view a name still claims.
                if cache.name == *name
                    && let Some(other) = cache.caches.keys().next()
                {
                    cache.name = other.clone();
                }
                let claimed = !cache.caches.is_empty();
                gone += usize::from(!claimed);
                claimed
            });
            !caches.is_empty()
        });
        if gone > 0 {
            self.inline_literal_caches_live
                .fetch_sub(gone, atomic::Ordering::Release);
        }
        // A statement holding the entry from before may still name the dropped view.
        if claimed_by_name {
            self.inline_literal_caches_generation
                .fetch_add(1, atomic::Ordering::Release);
        }
    }

    /// Keep the statements that could not be filed, to try again on the next catalog change.
    pub fn defer_inline_literal_caches(&self, deferred: Vec<CacheDDLRequest>) {
        *self.inline_literal_deferred.lock() = deferred;
    }

    /// Take them back.
    pub fn take_deferred_inline_literal_caches(&self) -> Vec<CacheDDLRequest> {
        mem::take(&mut *self.inline_literal_deferred.lock())
    }

    /// Forget every entry whose cache reads from one of `tables`.
    ///
    /// A schema change takes the statuses of the caches over those tables with it, and an entry
    /// whose cache has no status routes nothing, so the two are given up together. Matching is by
    /// name, as the status invalidation beside it is: an entry kept for a cache whose status is
    /// gone would hold a shape no read can be served from.
    pub fn remove_inline_literal_caches_referencing_tables(
        &self,
        tables: &[Relation],
    ) -> Vec<CacheDDLRequest> {
        let mut dropped_ddl = Vec::new();
        if tables.is_empty() || !self.may_have_inline_literal_caches() {
            return dropped_ddl;
        }
        let dropped: HashSet<&SqlIdentifier> = tables.iter().map(|table| &table.name).collect();
        let mut gone = 0;
        self.inline_literal_caches.retain(|_, caches| {
            caches.retain_mut(|cache| {
                // A statement whose tables cannot be read stays: it names none of them as far as
                // this can tell, and giving it up would strand a cache still being served.
                let Some(referenced) = extract_from_view_create_request(&cache.request) else {
                    return true;
                };
                let reads_dropped = referenced.iter().any(|table| dropped.contains(&table.name));
                if reads_dropped {
                    gone += 1;
                    // The entry is going, so its DDL moves out rather than being copied.
                    dropped_ddl.extend(mem::take(&mut cache.caches).into_values().flatten());
                }
                !reads_dropped
            });
            !caches.is_empty()
        });
        if gone > 0 {
            self.inline_literal_caches_live
                .fetch_sub(gone, atomic::Ordering::Release);
            self.inline_literal_caches_generation
                .fetch_add(1, atomic::Ordering::Release);
        }
        dropped_ddl
    }

    /// Forget every one of them, handing back the DDL that filed them.
    pub fn take_inline_literal_caches(&self) -> Vec<CacheDDLRequest> {
        let mut taken = Vec::new();
        self.inline_literal_caches.retain(|_, caches| {
            taken.extend(
                caches
                    .iter_mut()
                    .flat_map(|c| mem::take(&mut c.caches).into_values().flatten()),
            );
            false
        });
        self.clear_inline_literal_caches();
        taken
    }

    /// Forget every one of them. Used by DROP ALL CACHES.
    pub fn clear_inline_literal_caches(&self) {
        self.inline_literal_caches.clear();
        self.inline_literal_caches_live
            .store(0, atomic::Ordering::Release);
        self.inline_literal_caches_generation
            .fetch_add(1, atomic::Ordering::Release);
    }

    /// Record that the in-request-path filter has rejected this query, and
    /// why.  When the map crosses [`SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP`] it is
    /// bulk-cleared, the overflow counter increments, and a warning is logged.
    /// Reaching the cap indicates pathological client traffic (e.g. queries
    /// with literal-injected unique values) and warrants investigation.
    pub fn record_shallow_auto_create_skip(&self, id: QueryId, reason: String) {
        self.shallow_auto_create_skip.insert(id, reason);
        let len = self.shallow_auto_create_skip.len();
        if len >= SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP {
            // A small race here is benign: concurrent inserts that all
            // observe `len >= cap` will each clear-and-warn, but the next
            // refill takes ~SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP inserts, so
            // duplicate log lines and counter bumps are bounded by thread
            // count, not request rate.
            self.shallow_auto_create_skip.clear();
            counter!(metric::SHALLOW_AUTO_CREATE_SKIP_OVERFLOW).increment(1);
            warn!(
                cap = SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP,
                "Shallow auto-create skip set exceeded soft cap; bulk-cleared. \
                 Investigate whether the workload generates pathologically \
                 unique queries."
            );
        }
        gauge!(metric::SHALLOW_AUTO_CREATE_SKIP_SET_SIZE)
            .set(self.shallow_auto_create_skip.len() as f64);
    }

    /// Sets [`Self::style`]
    pub fn style(mut self, style: MigrationStyle) -> Self {
        self.style = style;
        self
    }

    /// Sets [`Self::placeholder_inlining`]
    pub fn set_placeholder_inlining(mut self, placeholder_inlining: bool) -> Self {
        self.placeholder_inlining = placeholder_inlining;
        self
    }

    /// Insert a query into the status cache with an initial status determined by the type of query
    /// that is being inserted. Parsed queries have initial status MigrationState::Pending, while
    /// queries that failed to parse have status MigrationState::Unsupported. Inserts into the
    /// statuses and ids hash maps.
    /// Only queries that are valid SQL should be inserted.
    /// Returns the QueryId and the MigrationState of the inserted Query
    /// self.statuses.insert() should not be called directly
    pub fn insert<Q>(&self, q: Q) -> (QueryId, MigrationState)
    where
        Q: Into<Query>,
    {
        let q = q.into();
        let status = QueryStatus::default_for_query(&q);
        let migration_state = status.migration_state.clone();
        let id = self.insert_with_status(q, status);
        (id, migration_state)
    }

    /// Inserts a query into the status cache with the provided QueryStatus
    /// Only queries that are valid SQL should be inserted.
    fn insert_with_status<Q>(&self, q: Q, status: QueryStatus) -> QueryId
    where
        Q: Into<Query>,
    {
        let q: Query = q.into();
        let status = match q {
            Query::Parsed { .. } => status,
            Query::ShallowParsed { .. } => status,
            Query::ParseFailed(_, ref reason) => QueryStatus {
                migration_state: MigrationState::Unsupported(reason.clone()),
                ..status
            },
        };
        let id = QueryId::from(&q);
        self.id_to_status.insert(id, status.clone());
        self.persistent_handle.insert_with_status(q, id, status);
        gauge!(metric::QUERY_STATUS_CACHE_SIZE).set(self.id_to_status.len() as f64);
        id
    }

    /// This function returns the id and query migration state of a query.
    ///
    /// Side Effects: If this is the first time we have seen this query, it also adds it to our
    /// mapping of queries.
    pub fn query_migration_state<Q>(&self, q: &Q) -> (QueryId, MigrationState)
    where
        Q: QueryStatusKey,
    {
        let id: QueryId = q.query_id();
        let query_state = self.id_to_status.get(&id);

        match query_state {
            Some(s) => (id, s.value().migration_state.clone()),
            None => self.insert(q.clone()),
        }
    }

    /// This function returns the id and query migration state of a query, if it exists. Unlike
    /// [`QueryStatusCache.query_migration_state`], it does not add the query to our mapping of
    /// queries if it is not present.
    pub fn try_query_migration_state<Q>(&self, q: &Q) -> (QueryId, Option<MigrationState>)
    where
        Q: QueryStatusKey,
    {
        let id = q.query_id();
        let query_state = self.id_to_status.get(&id);

        (id, query_state.map(|s| s.value().migration_state.clone()))
    }

    /// This function returns the query status of a query. If the query does not exist
    /// within the query status cache, an entry is created and the query is set to
    /// PendingMigration.
    ///
    /// `schema_generation` is the generation the caller rewrote `q` under, and is recorded
    /// alongside the entry. `CREATE CACHE FROM <query_id>` reads it back, so taking it as a
    /// parameter here leaves no way to put a query into the cache without it.
    pub fn query_status<Q>(&self, q: &Q, schema_generation: SchemaGeneration) -> QueryStatus
    where
        Q: QueryStatusKey,
    {
        let mut status = match q.with_status(self, |s| s.cloned()) {
            Some(s) => s,
            None => QueryStatus::with_migration_state(self.insert(q.clone()).1),
        };
        self.set_schema_generation(q, schema_generation);
        // Callers write whole statuses back, so the returned copy carries the generation too.
        status.schema_generation = Some(schema_generation);
        status
    }

    /// Try to return the query status of a query.  Does not modify the query status cache.
    pub fn try_query_status<Q>(&self, q: &Q) -> Option<QueryStatus>
    where
        Q: QueryStatusKey,
    {
        q.with_status(self, |s| s.cloned())
    }

    /// Updates the transition time in the execution info for the given query.
    pub fn update_transition_time<Q>(&self, q: &Q, transition: &std::time::Instant)
    where
        Q: QueryStatusKey,
    {
        q.with_mut_status(self, |s| {
            if let Some(s) = s
                && let Some(ref mut info) = s.execution_info
            {
                info.last_transition_time = *transition;
            }
        })
    }

    /// Resets the internal transition time to now. This should be used with extreme caution.
    pub fn reset_transition_time(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(s) = s
                && let Some(ref mut info) = s.execution_info
            {
                info.last_transition_time = Instant::now()
            }
        })
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to a networking problem.
    pub fn execute_network_failure(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(s) = s {
                match s.execution_info {
                    Some(ref mut info) => info.execute_network_failure(),
                    None => {
                        s.execution_info = Some(ExecutionInfo {
                            state: ExecutionState::NetworkFailure,
                            last_transition_time: Instant::now(),
                        });
                    }
                }
            }
        })
    }

    /// Update ExecutionInfo to indicate that a recent execute succeeded.
    pub fn execute_succeeded(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(s) = s {
                match s.execution_info {
                    Some(ref mut info) => info.execute_succeeded(),
                    None => {
                        s.execution_info = Some(ExecutionInfo {
                            state: ExecutionState::Successful,
                            last_transition_time: Instant::now(),
                        });
                    }
                }
            }
        })
    }

    /// Update ExecutionInfo to indicate that a recent execute failed.
    pub fn execute_failed(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(s) = s {
                match s.execution_info {
                    Some(ref mut info) => info.execute_failed(),
                    None => {
                        s.execution_info = Some(ExecutionInfo {
                            state: ExecutionState::Failed,
                            last_transition_time: Instant::now(),
                        });
                    }
                }
            }
        })
    }

    /// If the current ExecutionState is ExecutionState::NetworkFailure, then this method will
    /// return true if that state has persisted for longer than the supplied duration, otherwise,
    /// it will return false.
    pub fn execute_network_failure_exceeded(&self, q: &Query, duration: Duration) -> bool {
        q.with_mut_status(self, |s| {
            if let Some(s) = s
                && let Some(ref info) = s.execution_info
            {
                return info.execute_network_failure_exceeded(duration);
            }

            false
        })
    }

    /// The server does not have a view for this query, so set the query to pending.
    pub fn view_not_found_for_query<Q>(&self, q: &Q)
    where
        Q: QueryStatusKey,
    {
        let should_insert = q.with_mut_status(self, |s| {
            match s {
                Some(s) => {
                    // `Inlined` queries may only be changed from `Inlined` to `Unsupported`.
                    if !matches!(s.migration_state, MigrationState::Inlined(_)) {
                        s.migration_state = MigrationState::Pending
                    }
                    false
                }
                // If the query was not in the cache, make a new entry
                None => true,
            }
        });

        if should_insert {
            self.insert_with_status(
                q.clone(),
                QueryStatus {
                    migration_state: MigrationState::Pending,
                    execution_info: None,
                    trx_cache_policy: TrxCachePolicy::default(),
                    schema_generation: None,
                },
            );
        }
    }

    /// Updates the stored schema generation for a query that already exists in the cache.
    ///
    /// Private so that the generation cannot be stamped independently of the insert that
    /// [`Self::query_status`] pairs it with.
    fn set_schema_generation<Q>(&self, q: &Q, schema_generation: SchemaGeneration)
    where
        Q: QueryStatusKey,
    {
        q.with_mut_status(self, |status| {
            if let Some(status) = status {
                status.schema_generation = Some(schema_generation);
            }
        });
    }

    /// Updates a query's migration state to `m` unless the query's migration state was
    /// `MigrationState::Inlined`. An Inlined query can only transition to the Unsupported state.
    ///
    /// If provided, also updates this query's transaction cache policy.
    pub fn update_query_migration_state<Q>(
        &self,
        q: &Q,
        m: MigrationState,
        trx_cache_policy: Option<TrxCachePolicy>,
    ) where
        Q: QueryStatusKey,
    {
        let should_insert = q.with_mut_status(self, |s| {
            match s {
                Some(s) => {
                    match s.migration_state {
                        // A query with an Inlined state can only transition to Unsupported.
                        MigrationState::Inlined(_) => {
                            if matches!(m, MigrationState::Unsupported(_)) {
                                s.migration_state = m.clone()
                            }
                        }
                        // All other state transitions are allowed.
                        _ => s.migration_state = m.clone(),
                    }
                    if let Some(policy) = trx_cache_policy {
                        s.trx_cache_policy = policy;
                    }
                    false
                }
                None => true,
            }
        });
        if should_insert {
            self.insert_with_status(
                q.clone(),
                QueryStatus {
                    migration_state: m,
                    execution_info: None,
                    trx_cache_policy: trx_cache_policy.unwrap_or_default(),
                    schema_generation: None,
                },
            );
        }
    }

    /// Yields to the given function `f` a mutable reference to the migration state of the query
    /// `q`. The primary purpose of this method is allow for atomic reads and writes of the
    /// migration state of a query.
    pub fn with_mut_migration_state<Q, F>(&self, q: &Q, f: F) -> bool
    where
        Q: QueryStatusKey,
        F: Fn(&mut MigrationState),
    {
        q.with_mut_status(self, |maybe_query_status| {
            if let Some(query_status) = maybe_query_status {
                f(&mut query_status.migration_state);
                true
            } else {
                false
            }
        })
    }

    /// This function is called if we attempted to create an inlined migration but received an
    /// unsupported error. Updates the query status and removes pending inlined migrations.
    pub fn unsupported_inlined_migration(&self, q: &ViewCreateRequest) {
        let should_insert = q.with_mut_status(self, |s| match s {
            Some(s) => {
                s.migration_state =
                    MigrationState::Unsupported("Inlined migration not supported".into());
                false
            }
            None => true,
        });
        if should_insert {
            self.insert_with_status(
                q.clone(),
                QueryStatus {
                    migration_state: MigrationState::Unsupported(
                        "Inlined migration not supported".to_string(),
                    ),
                    execution_info: None,
                    trx_cache_policy: TrxCachePolicy::default(),
                    schema_generation: None,
                },
            );
        }
        self.persistent_handle.pending_inlined_migrations.remove(q);
    }

    /// Updates a queries status to `status` unless the queries migration state was
    pub fn update_query_status<Q>(&self, q: &Q, status: QueryStatus)
    where
        Q: QueryStatusKey,
    {
        let should_insert = q.with_mut_status(self, |s| match s {
            Some(s) => {
                s.migration_state.clone_from(&status.migration_state);
                s.execution_info.clone_from(&status.execution_info);
                s.schema_generation = status.schema_generation;
                false
            }
            None => true,
        });
        if should_insert {
            self.insert_with_status(q.clone(), status);
        }
    }

    /// Clear all queries currently marked as successful from the cache.
    pub fn clear(&self, cache_type: Option<CacheType>) {
        self.id_to_status
            .iter_mut()
            .filter(|v| v.is_cached(cache_type))
            .for_each(|mut v| {
                v.migration_state = MigrationState::Pending;
                v.trx_cache_policy = TrxCachePolicy::default();
            });
        let mut statuses = self.persistent_handle.statuses.write();
        statuses
            .iter_mut()
            .filter(|(_query_id, (_query, status))| status.is_cached(cache_type))
            .for_each(|(_query_id, (_query, status))| {
                status.migration_state = MigrationState::Pending;
                status.trx_cache_policy = TrxCachePolicy::default();
            });
    }

    /// Clear all queries not marked as successful from the cache.
    pub fn clear_proxied_queries(&self) {
        self.id_to_status
            .retain(|_query_id, status| status.is_cached(None));

        let mut statuses = self.persistent_handle.statuses.write();
        let keys_to_remove: Vec<QueryId> = statuses
            .iter()
            .filter(|(_, (_, status))| !status.is_cached(None))
            .map(|(query_id, _)| *query_id)
            .collect();

        for key in keys_to_remove {
            statuses.pop(&key);
        }
    }

    /// This method is called when a query is executed with the given params, but no inlined cache
    /// exists for the params. Adding the query to `Self::pending_inlined_migrations` indicates that
    /// it should be migrated by the MigrationHandler.
    pub fn inlined_cache_miss(&self, query: &ViewCreateRequest, params: Vec<DfValue>) {
        if self.placeholder_inlining {
            self.persistent_handle
                .pending_inlined_migrations
                .entry(query.clone())
                .or_default()
                .insert(params);
        }
    }

    /// Indicates that a migration has been completed for some set of literals for a query in
    /// `Self::pending_inlined_migrations`
    pub fn created_inlined_query(
        &self,
        query: &ViewCreateRequest,
        migrated_literals: Vec<&Vec<DfValue>>,
    ) {
        if let Entry::Occupied(mut entry) = self
            .persistent_handle
            .pending_inlined_migrations
            .entry(query.clone())
        {
            let pending_literals = entry.get_mut();
            for literals in migrated_literals {
                pending_literals.remove(literals);
            }
            // If we removed all the pending literals from the entry, we should remove the entry.
            if pending_literals.is_empty() {
                entry.remove();
            }
        }

        // Then update the inlined state epoch for the query
        query.with_mut_status(self, |s| {
            if let Some(QueryStatus {
                migration_state: MigrationState::Inlined(state),
                ..
            }) = s
            {
                state.epoch += 1;
            }
        })
    }

    /// Returns a list of queries that are pending an inlined migration, and a set of all literals
    /// to be used for inlining.
    pub fn pending_inlined_migration(&self) -> Vec<QueryInliningInstructions> {
        self.persistent_handle
            .pending_inlined_migrations
            .iter()
            .filter_map(|q| {
                // Get the placeholders that require inlining
                let placeholders =
                    q.key()
                        .with_status(self, |s| match s.map(|s| &s.migration_state) {
                            Some(MigrationState::Inlined(InlinedState {
                                inlined_placeholders,
                                ..
                            })) => Some(inlined_placeholders.clone()),
                            _ => None,
                        });

                // Generate QueryInliningInstructions
                placeholders.map(|p| {
                    QueryInliningInstructions::new(
                        q.key().clone(),
                        p,
                        q.value().iter().cloned().collect::<Vec<_>>(),
                    )
                })
            })
            .collect::<Vec<_>>()
    }

    /// Returns a list of queries that currently need the be processed to determine
    /// if they should be allowed (are supported by ReadySet).
    ///
    /// Does not include any queries that require inlining.
    pub fn pending_migration(&self) -> QueryList {
        let statuses = self.persistent_handle.statuses.read();
        statuses
            .iter()
            .filter_map(|(_query_id, (query, status))| {
                if status.is_pending() {
                    Some((query.clone(), status.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<(Query, QueryStatus)>>()
            .into()
    }

    /// Returns a list of queries whose migration states match `states`.
    pub fn queries_with_statuses(&self, states: &[MigrationState]) -> QueryList {
        let statuses = self.persistent_handle.statuses.read();
        statuses
            .iter()
            .filter_map(|(_query_id, (query, status))| {
                if states.contains(&status.migration_state) {
                    Some((query.clone(), status.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<(Query, QueryStatus)>>()
            .into()
    }

    /// Returns a list of queries that have a state of [`QueryState::Successful`].
    pub fn cached_list(&self) -> Vec<(QueryId, Arc<ViewCreateRequest>, QueryStatus)> {
        self.persistent_handle.cached_list()
    }

    /// Returns a list of queries that are proxied.
    pub fn proxied_list(&self, cache_type: CacheType) -> Vec<ProxiedQuery> {
        self.persistent_handle
            .proxied_list(self.style, cache_type, &self.shallow_auto_create_skip)
    }

    /// Returns a query given a query hash
    pub fn query(&self, id: &str) -> Option<Query> {
        let id = id.parse::<QueryId>().ok()?;
        let statuses = self.persistent_handle.statuses.read();
        statuses.peek(&id).map(|(query, _status)| query.clone())
    }

    /// Returns a query and its stored schema generation given a query hash.
    /// The schema generation reflects when the query was last rewritten by the adapter.
    pub fn query_with_schema_generation(
        &self,
        id: &str,
    ) -> Option<(Query, Option<SchemaGeneration>)> {
        let id = id.parse::<QueryId>().ok()?;
        let statuses = self.persistent_handle.statuses.read();
        statuses
            .peek(&id)
            .map(|(query, status)| (query.clone(), status.schema_generation))
    }

    /// Removes cache entries for queries that reference any of the specified tables.
    ///
    /// Uses a two-phase approach: read lock to collect query IDs to remove, then write lock to
    /// perform the removals. This minimizes write-lock hold time.
    pub fn invalidate_queries_referencing_tables(&self, dropped_tables: &[Relation]) {
        if dropped_tables.is_empty() {
            return;
        }

        // Build a name-based HashSet for O(1) lookup on the common path.
        let dropped_names: HashSet<&SqlIdentifier> =
            dropped_tables.iter().map(|r| &r.name).collect();

        // Phase 1: Read lock — collect IDs to remove
        let to_remove: Vec<QueryId> = {
            let statuses = self.persistent_handle.statuses.read();
            statuses
                .iter()
                .filter_map(|(query_id, (query, status))| {
                    // Shallow caches are TTL-governed; never invalidate them on
                    // a table change, whether replicator-driven or a client
                    // `DROP TABLE` (REA-6692).
                    if is_shallow_successful(status) {
                        return None;
                    }
                    if let Some(referenced_tables) = extract_referenced_tables(query)
                        && referenced_tables.iter().any(|table| {
                            if !dropped_names.contains(&table.name) {
                                return false;
                            }
                            // Name matches; verify schema qualification if both present.
                            dropped_tables.iter().any(|dropped| {
                                match (&table.schema, &dropped.schema) {
                                    (Some(t_schema), Some(d_schema)) => {
                                        t_schema == d_schema && table.name == dropped.name
                                    }
                                    // When one side has schema and the other doesn't, fall back
                                    // to name-only matching. Over-invalidation is acceptable;
                                    // under-invalidation causes stale EXPLAIN results.
                                    //
                                    // TODO (REA-5970): ideally, the queries stored in the cache
                                    // should have the tables resolved. However, that is a bit
                                    // difficult given that the search path can be a list of
                                    // schemas.
                                    _ => table.name == dropped.name,
                                }
                            })
                        })
                    {
                        Some(*query_id)
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Phase 2: Write lock — best-effort removal
        if !to_remove.is_empty() {
            let mut statuses = self.persistent_handle.statuses.write();
            for query_id in to_remove {
                statuses.pop(&query_id);
                self.id_to_status.remove(&query_id);
            }
        }
    }

    /// Remove a single query's status by id, unconditionally -- including a
    /// `Successful(Shallow)` entry that [`Self::invalidate_queries_referencing_tables`]
    /// deliberately preserves. Used by the RLS catalog poller when a table's RLS
    /// state makes an existing cache unsafe (e.g. a table turning RLS-active): a
    /// reliable, security-driven signal that must reset the query so it
    /// re-migrates instead of routing to the dropped cache. Distinct from the
    /// schema-catalog path, whose shallow-preservation (REA-6692) assumes an
    /// unreliable detector.
    pub fn invalidate_query(&self, query_id: &QueryId) {
        let mut statuses = self.persistent_handle.statuses.write();
        statuses.pop(query_id);
        self.id_to_status.remove(query_id);
    }

    pub fn reportable_metrics(&self) -> ReportableMetrics {
        ReportableMetrics {
            id_to_status_size: self.id_to_status.len() as u64,
            statuses_size: self.persistent_handle.statuses.read().len() as u64,
            pending_inlined_migrations_size: self.persistent_handle.pending_inlined_migrations.len()
                as u64,
        }
    }
}

impl QueryStatusCache {
    /// Drop every status a schema change takes, keeping the shallow ones.
    ///
    /// Shallow caches proxy to upstream and refresh on their own TTL, so dropping their status
    /// would orphan the cache (REA-6692).
    pub fn invalidate_all_statuses(&self) {
        // The write lock is held across both clears so a concurrent reader cannot re-insert
        // between them, and `pending_inlined_migrations` cannot be re-populated for a query
        // just removed.
        let mut statuses = self.persistent_handle.statuses.write();
        self.id_to_status
            .retain(|_, status| is_shallow_successful(status));
        let drop_ids: Vec<QueryId> = statuses
            .iter()
            .filter(|(_, (_, status))| !is_shallow_successful(status))
            .map(|(id, _)| *id)
            .collect();
        for id in drop_ids {
            statuses.pop(&id);
        }
        self.persistent_handle.pending_inlined_migrations.clear();
    }
}

impl SchemaChangeHandler for QueryStatusCache {
    fn invalidate_for_tables(&self, tables: &[Relation]) {
        self.invalidate_queries_referencing_tables(tables);
        self.remove_inline_literal_caches_referencing_tables(tables);
    }

    fn invalidate_all(&self) {
        self.invalidate_all_statuses();
        // Every entry belongs to a deep cache, and every deep status was just dropped.
        self.clear_inline_literal_caches();
    }
}

/// Whether a query's state is a successful shallow cache. Shallow caches proxy
/// to upstream and refresh on their own TTL, so they are excluded from all
/// schema-catalog-driven invalidation; their freshness is governed solely by
/// TTL/refresh (REA-6692).
fn is_shallow_successful(status: &QueryStatus) -> bool {
    matches!(
        status.migration_state,
        MigrationState::Successful(CacheType::Shallow)
    )
}

/// Bridges `&'static QueryStatusCache` to `Arc<dyn SchemaChangeHandler>`.
///
/// The QSC is `Box::leak`'d for `&'static` usage throughout the adapter, but the synchronizer
/// uses `Arc<dyn SchemaChangeHandler>`. This adapter bridges the two.
pub struct QscSchemaChangeAdapter {
    qsc: &'static QueryStatusCache,
    inline_literal_recovery: Option<InlineLiteralRecovery>,
}

/// What filing the caches that keep their literals inline takes.
#[derive(Clone)]
pub struct InlineLiteralRecovery {
    pub schema_catalog: SchemaCatalogHandle,
    pub parsing_preset: ParsingPreset,
    pub rewrite_params: AdapterRewriteParams,
}

impl QscSchemaChangeAdapter {
    pub fn new(qsc: &'static QueryStatusCache) -> Self {
        Self {
            qsc,
            inline_literal_recovery: None,
        }
    }

    /// File the caches that keep their literals inline again after each invalidation.
    pub fn recovering_inline_literal_caches(mut self, recovery: InlineLiteralRecovery) -> Self {
        self.inline_literal_recovery = Some(recovery);
        self
    }

    fn recover_inline_literal_caches(&self, ddl_requests: Vec<CacheDDLRequest>) {
        let Some(recovery) = self.inline_literal_recovery.clone() else {
            return;
        };
        let mut ddl_requests = ddl_requests;
        ddl_requests.extend(self.qsc.take_deferred_inline_literal_caches());
        if ddl_requests.is_empty() {
            return;
        }
        let qsc = self.qsc;
        tokio::spawn(async move {
            if let Err(error) = crate::backend::recreate_inline_literal_caches(
                qsc,
                recovery.schema_catalog,
                ddl_requests,
                recovery.parsing_preset,
                recovery.rewrite_params,
            )
            .await
            {
                warn!(%error, "could not file the inline-literal caches after a schema change");
            }
        });
    }
}

impl SchemaChangeHandler for QscSchemaChangeAdapter {
    fn invalidate_for_tables(&self, tables: &[Relation]) {
        self.qsc.invalidate_queries_referencing_tables(tables);
        let ddl = self
            .qsc
            .remove_inline_literal_caches_referencing_tables(tables);
        self.recover_inline_literal_caches(ddl);
    }

    fn invalidate_all(&self) {
        self.qsc.invalidate_all_statuses();
        let ddl = self.qsc.take_inline_literal_caches();
        self.recover_inline_literal_caches(ddl);
    }
}

/// MigrationStyle is used to communicate which style of managing migrations we have configured.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum MigrationStyle {
    /// Async migrations are enabled in the adapter by setting the --query-caching argument to
    /// async
    Async,
    /// Explicit migrations are enabled in the adapter by setting the --query-caching argument to
    /// explicit
    Explicit,
    /// InRequestPath is the style of managing migrations when neither async nor explicit
    /// migrations have been enabled.
    InRequestPath,
}

impl FromStr for MigrationStyle {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inrequestpath" => Ok(MigrationStyle::InRequestPath),
            "async" => Ok(MigrationStyle::Async),
            "explicit" => Ok(MigrationStyle::Explicit),
            other => Err(anyhow!("Invalid option specified: {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_client::ViewCreateRequest;
    use readyset_sql::ast::{CacheType, SelectStatement, ShallowCacheQuery, SqlQuery};
    use readyset_util::hash::hash;
    use vec1::Vec1;

    use super::*;

    fn select_statement(s: &str) -> anyhow::Result<SelectStatement> {
        match readyset_sql_parsing::parse_query(readyset_sql::Dialect::MySQL, s) {
            Ok(SqlQuery::Select(s)) => Ok(s),
            Ok(q) => Err(anyhow::anyhow!("Not a SELECT statement: {q:?}")),
            Err(e) => Err(anyhow::anyhow!("Parsing error: {e}")),
        }
    }

    #[test]
    fn query_hashes_eq_inner_hashes() {
        // This ensures that calling query_status on a &SelectStatement or &String will find the
        // corresponding Query in the DashMap
        let select = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let string = "SELECT * FROM t1".to_string();
        let q_select = Query::Parsed(Arc::new(select.clone()));
        let q_string = Query::ParseFailed(string.clone().into(), "Failed".to_string());
        assert_eq!(
            hash(&QueryId::from(&select)),
            hash(&QueryId::from(&q_select))
        );
        assert_eq!(
            hash(&QueryId::from_unparsed_select(string.as_str())),
            hash(&QueryId::from(&q_string))
        );
    }

    #[test]
    fn select_is_found_after_insert() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let status = QueryStatus::default_for_query(&q1.clone().into());
        let id = QueryId::from(&q1);

        cache.insert(q1.clone());

        let mut statuses = cache.persistent_handle.statuses.write();
        assert!(
            statuses
                .iter()
                .map(|(_, (q, _))| q.clone())
                .any(|q| q == q1.clone().into())
        );

        assert!(statuses.put(id, (q1.into(), status.clone())).is_some());

        assert_eq!(statuses.get(&id).unwrap().1, status);
    }

    #[test]
    fn string_is_found_after_insert() {
        let cache = QueryStatusCache::new();
        let q1 = "SELECT * FROM t1".to_string();
        let status = QueryStatus::default_for_query(&Query::ParseFailed(
            Arc::new(q1.clone()),
            "Failed".to_string(),
        ));
        let id = QueryId::from_unparsed_select(&q1);

        cache.insert(q1.clone());

        let mut statuses = cache.persistent_handle.statuses.write();
        assert!(
            statuses
                .iter()
                .map(|(_, (q, _))| q.clone())
                .any(|q| q == q1.clone().into())
        );

        assert!(statuses.put(id, (q1.into(), status.clone())).is_some());

        assert_eq!(statuses.get(&id).unwrap().1, status);
    }

    #[test]
    fn shallow_auto_create_skip_is_proxied_with_reason() {
        let cache = QueryStatusCache::new().style(MigrationStyle::InRequestPath);
        let query = ShallowCacheQuery::default();
        let query = ShallowViewRequest::new(query.clone(), vec![], query);

        let (id, state) = cache.query_migration_state(&query);
        assert_eq!(state, MigrationState::Pending);
        assert!(cache.proxied_list(CacheType::Shallow).is_empty());

        cache.record_shallow_auto_create_skip(id, "non-deterministic function: now".into());

        // A decline leaves the query pending, and pending queries stay out of the
        // listing whether or not they were declined. Running it upstream is what
        // moves it to Supported, and only then does the decline surface.
        assert!(cache.proxied_list(CacheType::Shallow).is_empty());
        cache.update_query_migration_state(&query, MigrationState::Supported, None);

        let proxied = cache.proxied_list(CacheType::Shallow);
        assert_eq!(proxied.len(), 1, "declined query should be listed");
        assert_eq!(proxied[0].id, id);
        assert_eq!(
            cache.shallow_auto_create_skip_reason(id).as_deref(),
            Some("non-deterministic function: now")
        );

        // Creating the cache manually removes the listing.
        cache.update_query_migration_state(
            &query,
            MigrationState::Successful(CacheType::Shallow),
            None,
        );
        assert!(cache.proxied_list(CacheType::Shallow).is_empty());

        // Re-allowing functions clears the decline, and the listing with it.
        cache.update_query_migration_state(&query, MigrationState::Supported, None);
        assert_eq!(cache.proxied_list(CacheType::Shallow).len(), 1);
        cache.clear_shallow_auto_create_skips();
        assert!(cache.proxied_list(CacheType::Shallow).is_empty());
        assert_eq!(cache.shallow_auto_create_skip_reason(id), None);
    }

    #[test]
    fn query_is_referenced_by_query_id() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.update_query_migration_state(&q1, MigrationState::Pending, None);
        cache.update_query_migration_state(&q2, MigrationState::Successful(CacheType::Deep), None);

        let h1 = QueryId::from(&q1);
        let h2 = QueryId::from(&q2);

        let r1 = cache.query(&h1.to_string()).unwrap();
        let r2 = cache.query(&h2.to_string()).unwrap();

        assert_eq!(r1, q1.into());
        assert_eq!(r2, q2.into());
    }

    #[test]
    fn query_is_allowed() {
        let cache = QueryStatusCache::new();
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).0,
            QueryId::from(&Into::<Query>::into(query.clone()))
        );

        // If we haven't explicitly updated it, we default to pending
        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );

        // Explicitly updating it also lets it be returned from pending_migration(), cached_list(),
        // and proxied_list()
        cache.update_query_migration_state(&query, MigrationState::Pending, None);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.cached_list().len(), 0);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 0);

        cache.update_query_migration_state(
            &query,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.cached_list().len(), 1);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 0);
    }

    #[test]
    fn query_is_denied() {
        let cache = QueryStatusCache::new();
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );
        cache.update_query_migration_state(&query, MigrationState::Pending, None);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.cached_list().len(), 0);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 0);

        cache.update_query_migration_state(&query, MigrationState::Unsupported("".into()), None);
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.cached_list().len(), 0);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 1);
    }

    #[test]
    fn query_is_inferred_denied_explicit() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );
        cache.update_query_migration_state(&query, MigrationState::Pending, None);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.cached_list().len(), 0);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 1);

        cache.update_query_migration_state(&query, MigrationState::Unsupported("".into()), None);
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.cached_list().len(), 0);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 1);
    }

    #[test]
    fn clear() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]),
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(
                select_statement("SELECT * FROM t1 WHERE id = ?").unwrap(),
                vec![],
            ),
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        assert_eq!(cache.cached_list().len(), 2);

        cache.clear(None);
        assert_eq!(cache.cached_list().len(), 0);
    }

    #[test]
    fn view_not_found_for_query() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.update_query_migration_state(&q1, MigrationState::Successful(CacheType::Deep), None);
        cache.update_query_migration_state(
            &q2,
            MigrationState::Inlined(InlinedState {
                inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
                epoch: 0,
            }),
            None,
        );
        // q1: supported -> pending
        cache.view_not_found_for_query(&q1);
        assert_eq!(cache.pending_migration().len(), 1);
        // q1: pending -> unsupported
        cache.update_query_migration_state(&q1, MigrationState::Unsupported("".to_string()), None);
        assert_eq!(cache.pending_migration().len(), 0);
        // q2: inlined -> inlined
        cache.view_not_found_for_query(&q2);
        assert_eq!(cache.pending_migration().len(), 0);
    }

    #[test]
    fn transition_from_unsupported() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        cache.update_query_migration_state(&q, MigrationState::Unsupported("Failed".into()), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Failed".into())
        );
        cache.update_query_migration_state(&q, MigrationState::Pending, None);
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Pending);
        cache.update_query_migration_state(&q, MigrationState::Unsupported("Failed".into()), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Failed".into())
        );
        cache.update_query_migration_state(
            &q,
            MigrationState::Successful(CacheType::Shallow),
            None,
        );
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Successful(CacheType::Shallow)
        );
        cache.update_query_migration_state(&q, MigrationState::Unsupported("Failed".into()), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Failed".into())
        );
        cache.update_query_migration_state(&q, MigrationState::Successful(CacheType::Deep), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Successful(CacheType::Deep)
        );
        cache.update_query_migration_state(&q, MigrationState::Unsupported("Failed".into()), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Failed".into())
        );
        cache.update_query_migration_state(
            &q,
            MigrationState::Inlined(InlinedState {
                inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
                epoch: 0,
            }),
            None,
        );
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Inlined(InlinedState {
                inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
                epoch: 0,
            }),
        );
        cache.update_query_migration_state(&q, MigrationState::Unsupported("Failed".into()), None);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Failed".into())
        );
        cache.update_query_migration_state(&q, MigrationState::Supported, None);
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Supported);
    }

    #[test]
    fn transition_from_inlined() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });

        cache.update_query_migration_state(&q, inlined_state.clone(), None);
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(&q, MigrationState::Pending, None);
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(&q, MigrationState::Successful(CacheType::Deep), None);
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(
            &q,
            MigrationState::Unsupported("Should fail".into()),
            None,
        );
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Should fail".into())
        );
    }

    #[test]
    fn inlined_cache_miss() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state, None);

        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::Max]);

        assert_eq!(
            cache
                .persistent_handle
                .pending_inlined_migrations
                .get(&q)
                .unwrap()
                .value()
                .len(),
            2
        );
    }

    #[test]
    fn unsupported_inlined_migration() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state, None);

        cache.inlined_cache_miss(&q, vec![DfValue::None]);

        cache.unsupported_inlined_migration(&q);

        assert!(
            cache
                .persistent_handle
                .pending_inlined_migrations
                .is_empty()
        );
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported("Inlined migration not supported".to_string())
        );
    }

    #[test]
    fn created_inlined_query() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state.clone(), None);

        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::Max]);
        cache.inlined_cache_miss(&q, vec![DfValue::Int(1)]);

        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.created_inlined_query(&q, vec![&vec![DfValue::Int(1)], &vec![DfValue::None]]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 1,
        });
        cache.update_query_migration_state(&q, inlined_state.clone(), None);
        let state = cache
            .query_status(&q, SchemaGeneration::INITIAL)
            .migration_state;
        assert_eq!(state, inlined_state);
        assert_eq!(
            cache
                .persistent_handle
                .pending_inlined_migrations
                .get(&q)
                .unwrap()
                .value()
                .len(),
            1
        );
        assert!(
            cache
                .persistent_handle
                .pending_inlined_migrations
                .get(&q)
                .unwrap()
                .value()
                .contains(&vec![DfValue::Max])
        )
    }

    #[test]
    fn pending_inlined_migration() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state, None);

        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::Max]);

        assert!(cache.pending_migration().is_empty());
        let pending = cache.pending_inlined_migration();
        assert_eq!(pending[0].query(), &q);
        assert_eq!(pending[0].placeholders(), &[1]);
        assert_eq!(pending[0].literals().len(), 2);
        assert!(pending[0].literals().contains(&vec![DfValue::Max]));
        assert!(pending[0].literals().contains(&vec![DfValue::None]));
    }

    #[test]
    fn avoid_insert_deadlock() {
        readyset_tracing::init_test_logging();
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q = Query::ParseFailed(Arc::new("foobar".to_string()), "Should Fail".to_string());

        q.with_mut_status(&cache, |_| {
            // Simulate it being removed by lru cache then inserted
            let query =
                Query::ParseFailed(Arc::new("foobar".to_string()), "Should Fail".to_string());
            let query_id = QueryId::from_unparsed_select("foobar");
            let query_status = QueryStatus::default_for_query(&query);
            cache
                .persistent_handle
                .insert_with_status(query, query_id, query_status);
        });
    }

    #[test]
    fn clear_proxied_queries() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]),
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(
                select_statement("SELECT * FROM t1 WHERE id = ?").unwrap(),
                vec![],
            ),
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(
                select_statement("SELECT * FROM t1 WHERE id > ?").unwrap(),
                vec![],
            ),
            MigrationState::Pending,
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT y FROM t2").unwrap(), vec![]),
            MigrationState::Unsupported("Should fail".to_string()),
            None,
        );
        assert_eq!(cache.cached_list().len(), 2);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 2);

        cache.clear_proxied_queries();
        assert_eq!(cache.cached_list().len(), 2);
        assert_eq!(cache.proxied_list(CacheType::Deep).len(), 0);
    }

    #[test]
    fn invalidate_queries_referencing_dropped_tables() {
        use readyset_sql::ast::*;

        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        // Create test queries with varying complexity
        let simple_t1 =
            ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let simple_t2 =
            ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);
        let join_query = ViewCreateRequest::new(
            select_statement("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id").unwrap(),
            vec![],
        );
        // Complex query with CTEs, subqueries, and EXISTS - references t1, t2, t3
        let complex_query = ViewCreateRequest::new(
            select_statement(
                "WITH cte1 AS (SELECT * FROM t1 WHERE id > 10) \
                 SELECT c.*, u.name FROM cte1 c \
                 JOIN (SELECT * FROM t2 WHERE active = 1) u ON c.id = u.id \
                 WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.ref_id = c.id)",
            )
            .unwrap(),
            vec![],
        );
        let unaffected_query =
            ViewCreateRequest::new(select_statement("SELECT * FROM t4").unwrap(), vec![]);

        // Add all queries to cache
        cache.update_query_migration_state(
            &simple_t1,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &simple_t2,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &join_query,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &complex_query,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &unaffected_query,
            MigrationState::Successful(CacheType::Deep),
            None,
        );

        assert_eq!(cache.cached_list().len(), 5);

        // Drop t1, should invalidate simple_t1, join_query, and complex_query
        let dropped_tables = vec![Relation::from("t1")];
        cache.invalidate_queries_referencing_tables(&dropped_tables);

        let remaining_queries = cache.cached_list();

        // simple_t2 (t2) and unaffected_query (t4) should remain
        assert_eq!(remaining_queries.len(), 2);
        let remaining_table_names: Vec<_> = remaining_queries
            .iter()
            .filter_map(|(_, vcr, _)| {
                vcr.statement
                    .tables
                    .first()
                    .and_then(|te| te.inner.as_table())
                    .map(|t| t.name.as_str())
            })
            .collect();

        assert!(remaining_table_names.contains(&"t2"));
        assert!(remaining_table_names.contains(&"t4"));

        // Add back a query and test nested reference invalidation
        cache.update_query_migration_state(
            &complex_query,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        assert_eq!(cache.cached_list().len(), 3);

        // should only invalidate the complex query (nested reference in EXISTS)
        let dropped_tables = vec![Relation::from("t3")];
        cache.invalidate_queries_referencing_tables(&dropped_tables);

        // Only simple_t2 and unaffected_query remain
        let final_queries = cache.cached_list();
        assert_eq!(final_queries.len(), 2);
        let final_table_names: Vec<_> = final_queries
            .iter()
            .filter_map(|(_, vcr, _)| {
                vcr.statement
                    .tables
                    .first()
                    .and_then(|te| te.inner.as_table())
                    .map(|t| t.name.as_str())
            })
            .collect();
        assert!(final_table_names.contains(&"t2"));
        assert!(final_table_names.contains(&"t4"));
    }

    #[test]
    fn invalidate_all_clears_both_caches() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        // Populate the cache with some queries
        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]),
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(
                select_statement("SELECT * FROM t1 WHERE id = ?").unwrap(),
                vec![],
            ),
            MigrationState::Pending,
            None,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]),
            MigrationState::Unsupported("nope".into()),
            None,
        );

        assert_eq!(cache.id_to_status.len(), 3);
        assert_eq!(cache.persistent_handle.statuses.read().len(), 3);

        // invalidate_all should clear everything
        cache.invalidate_all();

        assert_eq!(cache.id_to_status.len(), 0);
        assert_eq!(cache.persistent_handle.statuses.read().len(), 0);
        assert!(cache.cached_list().is_empty());
        assert!(cache.proxied_list(CacheType::Deep).is_empty());
    }

    #[test]
    fn invalidate_all_clears_pending_inlined_migrations() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .set_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });

        cache.update_query_migration_state(&q, inlined_state, None);
        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::Max]);

        // Verify pending_inlined_migrations is populated
        assert!(
            !cache
                .persistent_handle
                .pending_inlined_migrations
                .is_empty()
        );

        cache.invalidate_all();

        // Everything should be cleared
        assert_eq!(cache.id_to_status.len(), 0);
        assert_eq!(cache.persistent_handle.statuses.read().len(), 0);
        assert!(
            cache
                .persistent_handle
                .pending_inlined_migrations
                .is_empty()
        );
    }

    #[test]
    fn invalidate_all_then_queries_return_default() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        cache.update_query_migration_state(&q, MigrationState::Successful(CacheType::Deep), None);
        assert!(matches!(
            cache.query_migration_state(&q).1,
            MigrationState::Successful(_)
        ));

        cache.invalidate_all();

        // After invalidation, the query is no longer in the cache; querying it re-inserts with
        // default Pending state.
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Pending);
    }

    #[test]
    fn schema_catalog_invalidation_preserves_shallow_caches() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        let deep = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let shallow = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);
        cache.update_query_migration_state(
            &deep,
            MigrationState::Successful(CacheType::Deep),
            None,
        );
        cache.update_query_migration_state(
            &shallow,
            MigrationState::Successful(CacheType::Shallow),
            None,
        );

        // A full invalidation (SchemaChanges::All) clears deep state but leaves
        // the shallow cache routable.
        cache.invalidate_all();
        assert_eq!(
            cache.query_migration_state(&deep).1,
            MigrationState::Pending
        );
        assert_eq!(
            cache.query_migration_state(&shallow).1,
            MigrationState::Successful(CacheType::Shallow)
        );

        // A targeted invalidation referencing t2 must also leave the shallow
        // cache intact -- it is governed by its own TTL, not the schema catalog.
        cache.invalidate_queries_referencing_tables(&[Relation::from("t2")]);
        assert_eq!(
            cache.query_migration_state(&shallow).1,
            MigrationState::Successful(CacheType::Shallow)
        );
    }

    #[test]
    fn schema_generation_stored_and_retrieved() {
        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let generation = SchemaGeneration::INITIAL.next(); // generation 2

        // Insert via query_migration_state, then update generation separately
        cache.query_migration_state(&q);
        cache.set_schema_generation(&q, generation);

        // Retrieve via query_with_schema_generation
        let id = QueryId::from(&q);
        let result = cache.query_with_schema_generation(&id.to_string());
        assert!(result.is_some());
        let (_query, stored_gen) = result.unwrap();
        assert_eq!(stored_gen, Some(generation));
    }

    /// Every query `query_status` puts into the cache carries the generation it was rewritten
    /// under, so `CREATE CACHE FROM <query_id>` can read it back.
    #[test]
    fn query_status_records_the_generation() {
        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let generation = SchemaGeneration::INITIAL.next();

        let status = cache.query_status(&q, generation);
        assert_eq!(status.schema_generation, Some(generation));

        let id = QueryId::from(&q);
        let (_, stored) = cache
            .query_with_schema_generation(&id.to_string())
            .expect("query_status must insert the query");
        assert_eq!(stored, Some(generation));
    }

    #[test]
    fn schema_generation_none_for_queries_without_generation() {
        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        // Insert via update_query_migration_state (does not set schema_generation)
        cache.update_query_migration_state(&q, MigrationState::Pending, None);

        let id = QueryId::from(&q);
        let result = cache.query_with_schema_generation(&id.to_string());
        assert!(result.is_some());
        let (_query, stored_gen) = result.unwrap();
        assert_eq!(stored_gen, None);
    }

    #[test]
    fn try_query_migration_state_does_not_overwrite_generation() {
        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let generation = SchemaGeneration::INITIAL.next(); // generation 2

        // Store with generation 2
        cache.query_migration_state(&q);
        cache.set_schema_generation(&q, generation);

        // Read with try_query_migration_state (should not mutate)
        let (_, state) = cache.try_query_migration_state(&q);
        assert_eq!(state, Some(MigrationState::Pending));

        // Verify generation is still 2 (not overwritten)
        let id = QueryId::from(&q);
        let (_, stored_gen) = cache.query_with_schema_generation(&id.to_string()).unwrap();
        assert_eq!(stored_gen, Some(generation));
    }

    #[test]
    fn shallow_auto_create_skip_records_and_recalls() {
        let cache = QueryStatusCache::new();
        let q = ViewCreateRequest::new(select_statement("SELECT 1").unwrap(), vec![]);
        let id = QueryId::from(&q);

        assert!(!cache.is_shallow_auto_create_skipped(id));
        cache.record_shallow_auto_create_skip(id, "non-deterministic function".into());
        assert!(cache.is_shallow_auto_create_skipped(id));
        assert_eq!(
            cache.shallow_auto_create_skip_reason(id).as_deref(),
            Some("non-deterministic function")
        );

        // Distinct queries are tracked independently.
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM users").unwrap(), vec![]);
        let id2 = QueryId::from(&q2);
        assert!(!cache.is_shallow_auto_create_skipped(id2));
    }

    #[test]
    fn shallow_auto_create_skip_bulk_clears_at_soft_cap() {
        let cache = QueryStatusCache::new();
        // QueryId has no `From<u64>`, so synthesize ids via FromStr (`q_<hex>`).
        // Insert directly into the DashMap to avoid driving each one through
        // `record_shallow_auto_create_skip` and tripping the cap mid-fill.
        for i in 0..SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP as u64 - 1 {
            let id = QueryId::from_str(&format!("q_{i:x}")).unwrap();
            cache.shallow_auto_create_skip.insert(id, String::new());
        }
        assert_eq!(
            cache.shallow_auto_create_skip.len(),
            SHALLOW_AUTO_CREATE_SKIP_SOFT_CAP - 1
        );

        // Crossing the cap via the public API triggers a bulk clear.
        let trip = QueryId::from_str("q_ffffffffffffffff").unwrap();
        cache.record_shallow_auto_create_skip(trip, String::new());
        assert_eq!(
            cache.shallow_auto_create_skip.len(),
            0,
            "skip map should be bulk-cleared after crossing the soft cap"
        );
    }

    /// Rewrite `query`, either parameterizing every literal it can or keeping them inline.
    fn rewrite(query: &str, autoparameterize: bool) -> (SelectStatement, DfQueryParameters) {
        let mut catalog = schema_catalog::SchemaCatalog::default();
        catalog.view_schemas.insert(
            Relation {
                schema: None,
                name: "t".into(),
            },
            vec!["a".into(), "b".into(), "v".into()],
        );
        let context = schema_catalog::RewriteContext::new(
            readyset_data::Dialect::DEFAULT_MYSQL,
            std::sync::Arc::new(catalog),
            vec![],
        );
        let params = readyset_sql_passes::adapter_rewrites::AdapterRewriteParams {
            dialect: readyset_sql::Dialect::MySQL,
            server_supports_topk: false,
            server_supports_pagination: false,
            server_supports_mixed_comparisons: false,
            autoparameterize,
        };
        let mut stmt = select_statement(query).unwrap();
        let out = readyset_sql_passes::adapter_rewrites::rewrite_query(&mut stmt, params, &context)
            .unwrap();
        (stmt, out)
    }

    /// The slots a read carries, which is what it is matched by.
    fn read_slots(query: &str) -> LiteralSlots {
        rewrite(query, true).1.slots().clone()
    }

    /// File `query` as a cache keeping its literals inline, the way `CREATE CACHE` does: the shape
    /// and the slots come from the form that parameterizes everything, the served form from the
    /// one that keeps them.
    fn register(cache: &QueryStatusCache, name: &str, query: &str) -> QueryId {
        let (shape_stmt, canonical) = rewrite(query, true);
        let shape = QueryId::from_select(&shape_stmt, &[]);
        assert!(
            canonical.slots().inline_positions() > 0,
            "{query} keeps no literal inline, so it would need no entry"
        );
        let (form, params) = rewrite(query, false);
        cache
            .register_inline_literal_cache(
                InlineLiteralRegistration {
                    shape,
                    slots: canonical.slots().clone(),
                    request: ViewCreateRequest::new(form, vec![]),
                    params,
                },
                Relation::from(name),
                TrxCachePolicy::default(),
                Some(CacheDDLRequest {
                    unparsed_stmt: format!("CREATE CACHE {name} FROM {query}"),
                    schema_search_path: vec![],
                    dialect: readyset_data::Dialect::DEFAULT_MYSQL,
                    cache_name: Some(Relation::from(name)),
                }),
            )
            .unwrap();
        shape
    }

    /// A prepared statement binding a position the cache kept inline is matched by the value it
    /// binds, so one statement reaches a different cache per execute.
    #[test]
    fn an_execute_is_matched_by_its_bound_values() {
        let cache = QueryStatusCache::new();
        let shape = register(&cache, "two", "SELECT v FROM t WHERE a = ? AND b = 2");
        register(&cache, "three", "SELECT v FROM t WHERE a = ? AND b = 3");
        let read = read_slots("SELECT v FROM t WHERE a = ? AND b = ?");

        let hit = cache
            .match_inline_literal_values(&shape, &read, || Some(vec![1.into(), 2.into()]))
            .expect("binding the literal the cache kept reaches it");
        assert_eq!(hit.name, Relation::from("two"));
        assert_eq!(
            hit.values,
            vec![DfValue::from(1)],
            "the cache's own parameter"
        );

        let hit = cache
            .match_inline_literal_values(&shape, &read, || Some(vec![1.into(), 3.into()]))
            .expect("another value reaches the cache that kept it");
        assert_eq!(hit.name, Relation::from("three"));

        assert!(
            cache
                .match_inline_literal_values(&shape, &read, || Some(vec![1.into(), 4.into()]))
                .is_none(),
            "a value no cache kept belongs to none of them"
        );
    }

    /// A bound value reaches the cache keeping the same value, whatever type either side holds,
    /// and none keeping another. The bound value is converted to the kept literal's type, so a
    /// conversion that cannot hold it exactly is a miss.
    #[test]
    fn a_bound_value_reaches_the_cache_keeping_the_same_value() {
        // One cache, keeping `kept` at its second position. An execute binding that position
        // reaches the cache for each of `hits` and nothing for each of `misses`.
        fn keeping(kept: &str, hits: &[DfValue], misses: &[DfValue]) {
            let cache = QueryStatusCache::new();
            let shape = register(
                &cache,
                "kept",
                &format!("SELECT v FROM t WHERE a = ? AND b = {kept}"),
            );
            let read = read_slots("SELECT v FROM t WHERE a = ? AND b = ?");
            let matched = |bound: &DfValue| {
                cache.match_inline_literal_values(&shape, &read, || {
                    Some(vec![7.into(), bound.clone()])
                })
            };
            for bound in hits {
                let hit = matched(bound)
                    .unwrap_or_else(|| panic!("{bound:?} should reach the cache keeping {kept}"));
                assert_eq!(hit.values, vec![DfValue::from(7)]);
            }
            for bound in misses {
                assert!(
                    matched(bound).is_none(),
                    "{bound:?} should not reach the cache keeping {kept}"
                );
            }
        }
        let bytes = |s: &str| DfValue::from(s.as_bytes().to_vec());

        // A MySQL client binds a string as bytes and a Postgres client as text. The kept literal
        // carries its dialect's default collation, which is what decides case.
        keeping(
            "'x'",
            &[
                bytes("x"),
                DfValue::from("x"),
                bytes("X"),
                DfValue::from("X"),
            ],
            &[bytes("x ")],
        );
        keeping(
            "'a string longer than fourteen bytes'",
            &[bytes("a string longer than fourteen bytes")],
            &[bytes("a string longer than fourteen byte")],
        );
        // A binary literal is bytes on both sides.
        keeping("X'79'", &[bytes("y")], &[bytes("x")]);
        // Numbers compare by value, whichever variant carries them. 3.4 cannot be held as 3, so
        // it is a miss rather than a rounded match.
        keeping(
            "3",
            &[
                DfValue::Int(3),
                DfValue::UnsignedInt(3),
                DfValue::Double(3.0),
                DfValue::from("3"),
                bytes("3"),
            ],
            &[DfValue::Int(4), DfValue::Double(3.4), DfValue::Float(3.4)],
        );
        keeping(
            "2.5",
            &[DfValue::Double(2.5), DfValue::Float(2.5), bytes("2.5")],
            &[DfValue::Double(2.4)],
        );
        keeping("TRUE", &[DfValue::Int(1)], &[DfValue::Int(0)]);
        // A date kept as text takes the text a client binds for it; a timestamp renders with a
        // time of day, which the kept literal does not carry.
        let day = |d: u32| {
            DfValue::TimestampTz(
                chrono::NaiveDate::from_ymd_opt(2024, 1, d)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .into(),
            )
        };
        keeping(
            "'2024-01-01'",
            &[DfValue::from("2024-01-01")],
            &[day(1), day(2)],
        );
    }

    #[test]
    fn an_execute_over_a_shape_holding_no_cache_asks_for_no_values() {
        let cache = QueryStatusCache::new();
        register(&cache, "kept", "SELECT v FROM t WHERE a = 1");
        let other = QueryId::from_select(&rewrite("SELECT v FROM u WHERE a = 1", true).0, &[]);
        let read = read_slots("SELECT v FROM u WHERE a = ?");
        let asked = std::cell::Cell::new(false);
        assert!(
            cache
                .match_inline_literal_values(&other, &read, || {
                    asked.set(true);
                    None
                })
                .is_none()
        );
        assert!(
            !asked.get(),
            "no cache under the shape, so nothing to match against"
        );
    }

    /// The view an execute is sent to has to outlive the name it was registered under, and a
    /// statement holding the entry from before has to learn the view it names is gone.
    #[test]
    fn dropping_the_registering_name_hands_the_entry_to_the_other() {
        let cache = QueryStatusCache::new();
        let shape = register(&cache, "first", "SELECT v FROM t WHERE a = 1");
        register(&cache, "second", "SELECT v FROM t WHERE a = 1");
        let before = cache.inline_literal_caches_generation();
        cache.remove_inline_literal_cache_by_name(&Relation::from("first"));
        assert_ne!(cache.inline_literal_caches_generation(), before);
        let read = read_slots("SELECT v FROM t WHERE a = ?");
        let hit = cache
            .match_inline_literal_values(&shape, &read, || Some(vec![1.into()]))
            .expect("the entry stands while a name claims it");
        assert_eq!(hit.name, Relation::from("second"));
    }

    #[test]
    fn the_guard_answers_false_until_a_cache_is_registered() {
        let cache = QueryStatusCache::new();
        assert!(!cache.may_have_inline_literal_caches());
        register(&cache, "kept", "SELECT v FROM t WHERE a = 1");
        assert!(cache.may_have_inline_literal_caches());
    }

    #[test]
    fn a_read_reaches_the_cache_holding_more_of_its_literals() {
        let cache = QueryStatusCache::new();
        // Registered least-specific first, so order of arrival cannot be what decides.
        register(&cache, "one", "SELECT v FROM t WHERE a = 1 AND b = ?");
        let shape = register(&cache, "both", "SELECT v FROM t WHERE a = 1 AND b = 2");

        let (_, _, lookup) = cache
            .match_inline_literal_cache(
                &shape,
                &read_slots("SELECT v FROM t WHERE a = 1 AND b = 2"),
            )
            .expect("a read spelling both out belongs to a cache");
        // The cache holding both literals leaves nothing to look up, where the one holding only
        // `a` would leave `b`. An empty key is what says which of the two won.
        assert!(
            lookup.is_empty(),
            "expected the cache holding both literals, got a lookup of {lookup:?}"
        );
    }

    #[test]
    fn the_winner_does_not_depend_on_registration_order() {
        let forwards = QueryStatusCache::new();
        register(&forwards, "one", "SELECT v FROM t WHERE a = 1 AND b = ?");
        let shape = register(&forwards, "both", "SELECT v FROM t WHERE a = 1 AND b = 2");

        let backwards = QueryStatusCache::new();
        register(&backwards, "both", "SELECT v FROM t WHERE a = 1 AND b = 2");
        register(&backwards, "one", "SELECT v FROM t WHERE a = 1 AND b = ?");

        let read = read_slots("SELECT v FROM t WHERE a = 1 AND b = 2");
        let won = |c: &QueryStatusCache| {
            format!(
                "{:?}",
                c.match_inline_literal_cache(&shape, &read)
                    .expect("a read belongs to a cache")
                    .0
                    .statement
            )
        };
        assert_eq!(
            won(&forwards),
            won(&backwards),
            "recovery order must not change the winner"
        );
    }

    #[test]
    fn two_names_for_the_same_literals_share_one_entry() {
        let cache = QueryStatusCache::new();
        let shape = register(&cache, "first", "SELECT v FROM t WHERE a = 1");
        register(&cache, "second", "SELECT v FROM t WHERE a = 1");
        assert_eq!(
            cache.inline_literal_caches.get(&shape).unwrap().len(),
            1,
            "agreeing on every position joins the entry rather than adding one"
        );

        let read = read_slots("SELECT v FROM t WHERE a = 1");
        cache.remove_inline_literal_cache_by_name(&Relation::from("first"));
        assert!(
            cache.match_inline_literal_cache(&shape, &read).is_some(),
            "a name still claims it"
        );
        cache.remove_inline_literal_cache_by_name(&Relation::from("second"));
        assert!(
            cache.match_inline_literal_cache(&shape, &read).is_none(),
            "no name claims it any more"
        );
    }

    #[test]
    fn a_schema_change_gives_up_the_entries_over_the_tables_it_names() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new();
        let kept = register(&cache, "on_t", "SELECT v FROM t WHERE a = 1");
        register(&cache, "on_t2", "SELECT z FROM t2 WHERE x = 1");
        assert!(cache.may_have_inline_literal_caches());

        // A change to another table leaves both alone.
        cache.invalidate_for_tables(&[Relation::from("unrelated")]);
        assert_eq!(cache.inline_literal_caches.len(), 2);

        // One naming `t` takes only the entry reading from it.
        cache.invalidate_for_tables(&[Relation::from("t")]);
        assert!(
            cache.inline_literal_caches.get(&kept).is_none(),
            "the entry over `t` should be gone"
        );
        assert!(
            cache.may_have_inline_literal_caches(),
            "the entry over `t2` still holds literals inline"
        );

        // And one naming every table leaves nothing behind.
        cache.invalidate_for_tables(&[Relation::from("t2")]);
        assert!(!cache.may_have_inline_literal_caches());
    }

    #[test]
    fn invalidating_every_query_gives_up_every_entry() {
        use schema_catalog::SchemaChangeHandler;

        let cache = QueryStatusCache::new();
        register(&cache, "one", "SELECT v FROM t WHERE a = 1");
        register(&cache, "two", "SELECT z FROM t2 WHERE x = 1");
        assert!(cache.may_have_inline_literal_caches());

        cache.invalidate_all();
        assert!(
            !cache.may_have_inline_literal_caches(),
            "every entry belongs to a deep cache whose status was just dropped"
        );
    }

    #[test]
    fn a_forgotten_cache_hands_back_the_ddl_that_filed_it() {
        let cache = QueryStatusCache::new();
        register(&cache, "on_t", "SELECT v FROM t WHERE a = 1");
        register(&cache, "on_t2", "SELECT z FROM t2 WHERE x = 1");

        // A change to another table takes nothing, so there is nothing to file again.
        assert!(
            cache
                .remove_inline_literal_caches_referencing_tables(&[Relation::from("unrelated")])
                .is_empty()
        );

        let ddl = cache.remove_inline_literal_caches_referencing_tables(&[Relation::from("t")]);
        assert_eq!(ddl.len(), 1, "only the entry over `t` goes");
        assert_eq!(ddl[0].cache_name, Some(Relation::from("on_t")));
        assert!(ddl[0].unparsed_stmt.contains("SELECT v FROM t WHERE a = 1"));

        // And clearing hands back what is left.
        let rest = cache.take_inline_literal_caches();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].cache_name, Some(Relation::from("on_t2")));
        assert!(!cache.may_have_inline_literal_caches());
    }

    #[test]
    fn every_name_claiming_an_entry_keeps_its_own_ddl() {
        let cache = QueryStatusCache::new();
        // Same slots, so both names share one entry.
        register(&cache, "first", "SELECT v FROM t WHERE a = 1");
        register(&cache, "second", "SELECT v FROM t WHERE a = 1");
        assert_eq!(cache.inline_literal_caches.len(), 1);

        let ddl = cache.remove_inline_literal_caches_referencing_tables(&[Relation::from("t")]);
        let names: HashSet<_> = ddl.iter().filter_map(|d| d.cache_name.clone()).collect();
        assert_eq!(
            names,
            HashSet::from([Relation::from("first"), Relation::from("second")]),
            "a shared entry files every name again, not just the first"
        );
    }

    #[test]
    fn the_guard_falls_back_once_the_last_cache_is_dropped() {
        let cache = QueryStatusCache::new();
        register(&cache, "one", "SELECT v FROM t WHERE a = 1");
        register(&cache, "two", "SELECT v FROM t WHERE a = 1 AND b = 2");
        assert!(cache.may_have_inline_literal_caches());

        cache.remove_inline_literal_cache_by_name(&Relation::from("one"));
        assert!(
            cache.may_have_inline_literal_caches(),
            "one cache still holds literals inline"
        );
        cache.remove_inline_literal_cache_by_name(&Relation::from("two"));
        assert!(
            !cache.may_have_inline_literal_caches(),
            "a read should stop working its shape out once none is left"
        );
    }

    #[test]
    fn two_names_on_one_entry_keep_the_guard_up_until_both_go() {
        let cache = QueryStatusCache::new();
        register(&cache, "first", "SELECT v FROM t WHERE a = 1");
        register(&cache, "second", "SELECT v FROM t WHERE a = 1");
        cache.remove_inline_literal_cache_by_name(&Relation::from("first"));
        assert!(
            cache.may_have_inline_literal_caches(),
            "the entry is still claimed by a name"
        );
        cache.remove_inline_literal_cache_by_name(&Relation::from("second"));
        assert!(!cache.may_have_inline_literal_caches());
    }

    #[test]
    fn dropping_every_cache_forgets_them_all() {
        let cache = QueryStatusCache::new();
        let shape = register(&cache, "kept", "SELECT v FROM t WHERE a = 1");
        cache.clear_inline_literal_caches();
        assert!(
            cache
                .match_inline_literal_cache(&shape, &read_slots("SELECT v FROM t WHERE a = 1"))
                .is_none()
        );
        assert!(!cache.may_have_inline_literal_caches());
    }
}
