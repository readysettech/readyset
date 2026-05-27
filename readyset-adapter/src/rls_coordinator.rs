//! Adapter-side coordinator that bridges the generic shallow cache to the RLS catalog.
//!
//! `readyset-shallow` is generic over an opaque key `K` and has no notion of RLS. The adapter
//! instantiates it with [`ShallowKey`] and owns the policy-aware glue: the per-cache
//! [`ScopedDescriptor`], the relation reverse index the catalog poller walks on a change, and
//! [`RlsCoordinator::fill_rls_session_inputs`], the lookup hot path that contributes RLS-derived
//! values to the key (or refuses, routing the lookup off-cache).
//!
//! The coordinator implements [`readyset_rls::InvalidationSink`] so the catalog poller drives
//! cache re-keying / dropping through it. A bumped registry generation is folded into every
//! scoped key, so the sink callbacks cover only what the generation bump cannot: re-deriving a
//! cache's keyed input set when its policy changes, and dropping a pre-existing plain cache when
//! its relation turns RLS-active.
//!
//! Postgres-only. MySQL deployments construct no coordinator.

use std::collections::HashSet;
use std::sync::Arc;

use papaya::HashMap;
use parking_lot::Mutex;

use readyset_client::query::QueryId;
use readyset_rls::{Cacheability, InvalidationSink, Oid, PolicyRegistry, SessionInputType};
use readyset_shallow::CacheManager;

use crate::query_status_cache::QueryStatusCache;
use crate::session_context::SessionContext;
use crate::shallow_key::{SessionInputValue, SessionInputValues, ShallowKey};

/// The lookup cannot be keyed safely; the caller must serve it from upstream uncached rather than
/// risk a cross-tenant entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkipCache;

/// Per-cache RLS dependency descriptor for a scoped cache.
///
/// Present only for caches the analyzer found RLS-active. A cache with no descriptor is plain.
#[derive(Debug, Clone)]
pub struct ScopedDescriptor {
    /// Session inputs whose values are folded into the cache key, in analyzer-emitted order.
    /// Shared via `Arc` so cloning the descriptor on the lookup path stays cheap.
    pub session_rls_inputs: Arc<[SessionInputType]>,
    /// Relations whose policy / RLS state invalidates this cache.
    pub relations: Vec<Oid>,
}

/// Bridges the generic [`CacheManager`] to the RLS catalog.
///
/// `V` is the upstream cache-entry value type the manager stores.
pub struct RlsCoordinator<V>
where
    V: Send + Sync + 'static,
{
    registry: Arc<PolicyRegistry>,
    shallow: Arc<CacheManager<ShallowKey, V>>,
    /// Query-status cache, so an RLS-driven cache drop also resets the query's migration status --
    /// otherwise it keeps routing to the dropped shallow cache and the read errors
    /// `NoCacheForQuery`.
    query_status_cache: &'static QueryStatusCache,
    /// Scoped descriptors, keyed by query id. Absence means the cache is plain.
    descriptors: HashMap<QueryId, Arc<ScopedDescriptor>>,
    /// Relation OID -> the query ids of caches that reference it. Holds both scoped and plain
    /// caches, so `on_rls_flag_enabled` can find a plain cache to drop when its relation turns
    /// RLS-active.
    caches_by_relation: HashMap<Oid, Arc<Mutex<HashSet<QueryId>>>>,
}

impl<V> std::fmt::Debug for RlsCoordinator<V>
where
    V: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RlsCoordinator")
            .field("generation", &self.registry.generation())
            .field("descriptors", &self.descriptors.pin().len())
            .finish_non_exhaustive()
    }
}

impl<V> RlsCoordinator<V>
where
    V: Send + Sync + readyset_util::SizeOf + 'static,
{
    /// Build a coordinator over a shared registry and shallow manager.
    pub fn new(
        registry: Arc<PolicyRegistry>,
        shallow: Arc<CacheManager<ShallowKey, V>>,
        query_status_cache: &'static QueryStatusCache,
    ) -> Self {
        Self {
            registry,
            shallow,
            query_status_cache,
            descriptors: HashMap::new(),
            caches_by_relation: HashMap::new(),
        }
    }

    fn index_insert(
        index: &HashMap<Oid, Arc<Mutex<HashSet<QueryId>>>>,
        oid: Oid,
        query_id: QueryId,
    ) {
        let guard = index.pin();
        let set = guard
            .get_or_insert_with(oid, || Arc::new(Mutex::new(HashSet::new())))
            .clone();
        set.lock().insert(query_id);
    }

    fn index_remove(
        index: &HashMap<Oid, Arc<Mutex<HashSet<QueryId>>>>,
        oid: Oid,
        query_id: &QueryId,
    ) {
        let guard = index.pin();
        if let Some(set) = guard.get(&oid) {
            set.lock().remove(query_id);
        }
    }

    /// Register an RLS-active (scoped) cache. Records its descriptor and adds it to the relation
    /// reverse index.
    ///
    /// Role changes need no reverse index: a scoped key always folds in the effective role and
    /// session user, so a role-attribute change is invalidated by the registry generation bump.
    pub fn register_scoped(
        &self,
        query_id: QueryId,
        session_rls_inputs: Arc<[SessionInputType]>,
        relations: Vec<Oid>,
    ) {
        let descriptor = Arc::new(ScopedDescriptor {
            session_rls_inputs,
            relations: relations.clone(),
        });
        self.descriptors.pin().insert(query_id, descriptor);
        for relid in relations {
            Self::index_insert(&self.caches_by_relation, relid, query_id);
        }
    }

    /// Register a plain cache that references RLS-eligible relations. No descriptor is recorded,
    /// but the relation reverse index gains an entry so [`Self::on_rls_flag_enabled`] can find and
    /// drop the cache if its relation later turns RLS-active.
    pub fn register_relations(&self, query_id: QueryId, relations: Vec<Oid>) {
        for relid in relations {
            Self::index_insert(&self.caches_by_relation, relid, query_id);
        }
    }

    /// Forget a cache: drop its descriptor and remove it from the relation reverse index.
    pub fn unregister(&self, query_id: &QueryId) {
        let descriptor = self.descriptors.pin().remove(query_id).cloned();
        if let Some(descriptor) = descriptor {
            for relid in &descriptor.relations {
                Self::index_remove(&self.caches_by_relation, *relid, query_id);
            }
        } else {
            // Plain cache: its relations are not tracked here, so sweep every relation bucket.
            // The buckets are small and unregister is off the hot path.
            let guard = self.caches_by_relation.pin();
            for set in guard.values() {
                set.lock().remove(query_id);
            }
        }
    }

    /// Contribute the RLS-derived session values to the key the adapter is assembling for a lookup.
    ///
    /// A plain cache (no descriptor) contributes nothing. A scoped cache stamps the registry
    /// generation and the session's resolved values; without a session, or when the session
    /// cannot be keyed safely, the lookup must route off-cache and this returns `Err(SkipCache)`.
    pub fn fill_rls_session_inputs(
        &self,
        query_id: &QueryId,
        session: Option<&SessionContext>,
        values: &mut SessionInputValues,
    ) -> Result<(), SkipCache> {
        // Read the generation (Acquire) BEFORE the descriptor. The poller refreshes a cache's
        // descriptor and only then bumps the generation with a Release store, so reading the
        // generation first bounds a torn read to {old_gen, old_values} (consistent) or {old_gen,
        // new_values} (a fresh key that misses) -- never {new_gen, old_values}, which would
        // collide two sessions' keys under one entry and leak rows across tenants.
        let generation = self.registry.generation();
        let descriptor = self.descriptors.pin().get(query_id).cloned();
        let Some(descriptor) = descriptor else {
            return Ok(());
        };

        let Some(session) = session else {
            metrics::counter!(metric::SHALLOW_RESULT_UNCACHEABLE).increment(1);
            return Err(SkipCache);
        };

        let Some(resolved) = session.rls_input_values(&descriptor.session_rls_inputs) else {
            metrics::counter!(metric::SHALLOW_RESULT_UNCACHEABLE).increment(1);
            return Err(SkipCache);
        };

        // Fail closed when the session's login-time role-default snapshot was unavailable and a
        // keyed GUC is unset: the absent value could be masking an unresolved role default, so
        // keying on it could collapse distinct roles onto one entry. The snapshot is frozen at
        // connect, so the session's flag governs resolvability, not the registry's current one.
        if !session.role_defaults_available()
            && resolved
                .iter()
                .any(|v| matches!(v, SessionInputValue::Rls(_, None)))
        {
            metrics::counter!(metric::SHALLOW_RESULT_UNCACHEABLE).increment(1);
            metrics::counter!(
                metric::QUERY_LOG_TOTAL_SKIP_CACHE,
                "query_id" => query_id.to_string(),
                "type" => "shallow",
                "reason" => "rls_role_default_unverifiable",
            )
            .increment(1);
            return Err(SkipCache);
        }

        for value in resolved.iter() {
            values.add(value.clone());
        }
        values.add(SessionInputValue::RlsGeneration(generation));
        Ok(())
    }

    /// Forget every registered cache. Paired with the manager's `drop_all_caches` for
    /// `DROP ALL CACHES`.
    pub fn clear(&self) {
        self.descriptors.pin().clear();
        self.caches_by_relation.pin().clear();
    }

    /// `true` if the cache for `query_id` is scoped (has a descriptor). The lookup path uses this
    /// to keep a scoped cache's hits out of the session-less refresh pool, which would poison the
    /// partition.
    pub fn is_scoped(&self, query_id: &QueryId) -> bool {
        self.descriptors.pin().contains_key(query_id)
    }

    /// Snapshot the query ids registered against `relid`.
    fn query_ids_for_relation(&self, relid: Oid) -> Vec<QueryId> {
        let guard = self.caches_by_relation.pin();
        guard
            .get(&relid)
            .map(|set| set.lock().iter().copied().collect())
            .unwrap_or_default()
    }
}

impl<V> InvalidationSink for RlsCoordinator<V>
where
    V: Send + Sync + readyset_util::SizeOf + 'static,
{
    fn on_relation_changed(&self, relid: Oid) {
        // Re-analyze every scoped cache over this relation. A cache still in grammar gets its
        // keyed input set refreshed; one that left the grammar is dropped and unregistered so it
        // re-enters cleanly.
        for query_id in self.query_ids_for_relation(relid) {
            let descriptor = {
                let guard = self.descriptors.pin();
                match guard.get(&query_id) {
                    Some(d) => d.clone(),
                    None => continue,
                }
            };
            let analysis = readyset_rls::analyze_cache(&self.registry, &descriptor.relations);
            match analysis.cacheability {
                Cacheability::Cacheable => {
                    let refreshed = Arc::new(ScopedDescriptor {
                        session_rls_inputs: analysis.session_rls_inputs,
                        relations: descriptor.relations.clone(),
                    });
                    self.descriptors.pin().insert(query_id, refreshed);
                }
                Cacheability::Refuse(_) => {
                    let _ = self.shallow.drop_cache(None, Some(&query_id));
                    // Reset the migration status so the read re-routes instead of hitting the
                    // dropped shallow cache.
                    self.query_status_cache.invalidate_query(&query_id);
                    self.unregister(&query_id);
                }
            }
        }
    }

    fn on_role_changed(&self, _roleid: Oid) {
        // Generation-only invalidation: a role attribute change bumps the global registry
        // generation, invalidating every scoped key stamped under the prior generation. Scoped
        // partitions always key on the effective role + session user, so there is nothing
        // per-role to re-derive and no role reverse index to consult.
    }

    fn on_rls_flag_enabled(&self, relid: Oid) {
        // A relation turned RLS-active. Any pre-existing plain cache over it now serves rows
        // without per-tenant partitioning, so drop and unregister it; it re-enters on the next
        // lookup and re-analyzes as scoped. Scoped caches already partition correctly.
        for query_id in self.query_ids_for_relation(relid) {
            if self.is_scoped(&query_id) {
                continue;
            }
            let _ = self.shallow.drop_cache(None, Some(&query_id));
            // Reset the migration status so the next read re-migrates (re-analyzing the
            // now-RLS-active table as scoped) instead of routing to the dropped shallow cache.
            self.query_status_cache.invalidate_query(&query_id);
            self.unregister(&query_id);
        }
    }
}
