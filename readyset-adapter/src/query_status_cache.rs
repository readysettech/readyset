//! The query status cache provides a thread-safe window into an adapter's
//! knowledge about queries, currently the migration status of a query in
//! ReadySet.
use std::collections::HashSet;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use clap::ValueEnum;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::RwLock;
use readyset_client::query::*;
use readyset_client::ViewCreateRequest;
use readyset_data::DfValue;
use readyset_util::hash::hash;
use tracing::{error, warn};

pub const DEFAULT_QUERY_STATUS_CAPACITY: usize = 100_000;

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
    enable_experimental_placeholder_inlining: bool,
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
            }
            None => {
                warn!(query_id=%id, "Avoiding deadlock when trying to insert")
            }
        }
    }

    fn allow_list(&self) -> Vec<(QueryId, Arc<ViewCreateRequest>, QueryStatus)> {
        let statuses = self.statuses.read();
        statuses
            .iter()
            .filter_map(|(query_id, (query, status))| match query {
                Query::Parsed(view) => {
                    if status.is_successful() {
                        Some((*query_id, view.clone(), status.clone()))
                    } else {
                        None
                    }
                }
                Query::ParseFailed(_) => None,
            })
            .collect::<Vec<_>>()
    }

    fn deny_list(&self, style: MigrationStyle) -> Vec<DeniedQuery> {
        let statuses = self.statuses.read();
        match style {
            MigrationStyle::Async | MigrationStyle::InRequestPath => statuses
                .iter()
                .filter_map(|(query_id, (query, status))| {
                    if status.is_unsupported() || status.is_dropped() {
                        Some(DeniedQuery {
                            id: *query_id,
                            query: query.clone(),
                            status: status.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            MigrationStyle::Explicit => statuses
                .iter()
                .filter_map(|(query_id, (query, status))| {
                    if status.is_denied() {
                        Some(DeniedQuery {
                            id: *query_id,
                            query: query.clone(),
                            status: status.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        }
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
}

impl QueryStatusKey for Query {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        match self {
            Query::Parsed(k) => k.with_status(cache, f),
            Query::ParseFailed(k) => k.with_status(cache, f),
        }
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        match self {
            Query::Parsed(k) => k.with_mut_status(cache, f),
            Query::ParseFailed(k) => k.with_mut_status(cache, f),
        }
    }
}

impl QueryStatusKey for ViewCreateRequest {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        let id = QueryId::new(hash(self));
        // Since this isn't mutating anything, we only need to access the in-memory map.
        f(cache.id_to_status.get(&id).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        let id = QueryId::new(hash(self));
        // Since this is potentially mutating, we need to apply F to both the in-memory and the
        // persistent version of the status.
        f(cache.id_to_status.get_mut(&id).as_deref_mut());
        let mut statuses = cache.persistent_handle.statuses.write();
        let transformed_status = statuses.get_mut(&id).map(|(_, status)| status);
        f(transformed_status)
    }
}

impl QueryStatusKey for String {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        let id = QueryId::new(hash(self));
        // Since this isn't mutating anything, we only need to access the in-memory map.
        f(cache.id_to_status.get(&id).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: Fn(Option<&mut QueryStatus>) -> R,
    {
        let id = QueryId::new(hash(self));
        // Since this is potentially mutating, we need to apply F to both the in-memory and the
        // persistent version of the status.
        f(cache.id_to_status.get_mut(&id).as_deref_mut());

        let mut statuses = cache.persistent_handle.statuses.write();
        let transformed_status = statuses.get_mut(&id).map(|(_, status)| status);
        f(transformed_status)
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
            enable_experimental_placeholder_inlining: false,
        }
    }

    /// Constructs a new QueryStatusCache with the migration style set to InRequestPath and
    /// provided capacity that must be non-zero.
    pub fn with_capacity(capacity: usize) -> QueryStatusCache {
        QueryStatusCache {
            id_to_status: Default::default(),
            persistent_handle: PersistentStatusCacheHandle::with_capacity(capacity),
            style: MigrationStyle::InRequestPath,
            enable_experimental_placeholder_inlining: false,
        }
    }

    /// Sets [`Self::style`]
    pub fn style(mut self, style: MigrationStyle) -> Self {
        self.style = style;
        self
    }

    /// Sets [`Self::enable_experimental_placeholder_inlining`]
    pub fn enable_experimental_placeholder_inlining(
        mut self,
        enable_experimental_placeholder_inlining: bool,
    ) -> Self {
        self.enable_experimental_placeholder_inlining = enable_experimental_placeholder_inlining;
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
            Query::ParseFailed(_) => {
                let mut status = status;
                if status.migration_state != MigrationState::Unsupported {
                    error!("Cannot set migration state to anything other than Unsupported for a Query::ParseFailed");
                    status.migration_state = MigrationState::Unsupported
                }
                status
            }
        };
        let id = QueryId::new(hash(&q));
        self.id_to_status.insert(id, status.clone());
        self.persistent_handle.insert_with_status(q, id, status);
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
        let id = QueryId::new(hash(&q));
        let query_state = self.id_to_status.get(&id);

        match query_state {
            Some(s) => (id, s.value().migration_state.clone()),
            None => self.insert(q.clone()),
        }
    }

    /// This function returns the query status of a query. If the query does not exist
    /// within the query status cache, an entry is created and the query is set to
    /// PendingMigration.
    pub fn query_status<Q>(&self, q: &Q) -> QueryStatus
    where
        Q: QueryStatusKey,
    {
        match q.with_status(self, |s| s.cloned()) {
            Some(s) => s,
            None => QueryStatus::with_migration_state(self.insert(q.clone()).1),
        }
    }

    /// Updates the execution info for the given query.
    pub fn update_execution_info(&self, q: &Query, info: ExecutionInfo) {
        q.with_mut_status(self, |s| {
            if let Some(mut s) = s {
                s.execution_info = Some(info.clone());
            }
        })
    }

    /// Updates the transition time in the execution info for the given query.
    pub fn update_transition_time<Q>(&self, q: &Q, transition: &std::time::Instant)
    where
        Q: QueryStatusKey,
    {
        q.with_mut_status(self, |s| {
            if let Some(s) = s {
                if let Some(ref mut info) = s.execution_info {
                    info.last_transition_time = *transition;
                }
            }
        })
    }

    /// Resets the internal transition time to now. This should be used with extreme caution.
    pub fn reset_transition_time(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(s) = s {
                if let Some(ref mut info) = s.execution_info {
                    info.last_transition_time = Instant::now()
                }
            }
        })
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to a networking problem.
    pub fn execute_network_failure(&self, q: &Query) {
        q.with_mut_status(self, |s| {
            if let Some(mut s) = s {
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
            if let Some(mut s) = s {
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
            if let Some(mut s) = s {
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
            if let Some(s) = s {
                if let Some(ref info) = s.execution_info {
                    return info.execute_network_failure_exceeded(duration);
                }
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
                Some(mut s) => {
                    // We do not support transitions from the `Unsupported` state, as we assume
                    // any `Unsupported` query will remain `Unsupported` for the duration of
                    // this process.
                    //
                    // `Inlined` queries may only be changed from `Inlined` to `Unsupported`.
                    if !matches!(
                        s.migration_state,
                        MigrationState::Unsupported | MigrationState::Inlined(_)
                    ) {
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
                    always: false,
                },
            );
        }
    }

    /// Updates a query's migration state to `m` unless the query's migration state was
    /// `MigrationState::Unsupported` or `MigrationState::Inlined`. An unsupported query cannot
    /// currently become supported once again. An Inlined query can only transition to the
    /// Unsupported state.
    pub fn update_query_migration_state<Q>(&self, q: &Q, m: MigrationState)
    where
        Q: QueryStatusKey,
    {
        // Dropped should not be set manually
        debug_assert!(!matches!(m, MigrationState::Dropped));

        let should_insert = q.with_mut_status(self, |s| {
            match s {
                Some(mut s) => {
                    match s.migration_state {
                        // We do not support transitions from the `Unsupported` state, as we assume
                        // any `Unsupported` query will remain `Unsupported` for the duration of
                        // this process.
                        MigrationState::Unsupported => {}
                        // A query with an Inlined state can only transition to Unsupported.
                        MigrationState::Inlined(_) => {
                            if matches!(m, MigrationState::Unsupported) {
                                s.migration_state = MigrationState::Unsupported;
                            }
                        }
                        // All other state transitions are allowed.
                        _ => s.migration_state = m.clone(),
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
                    always: false,
                },
            );
        }
    }

    /// Marks a query as dropped by the user.
    ///
    /// NOTE: this should only be called after we successfully remove a View for this query. This is
    /// relevant because we report that dropped queries are supported by ReadySet.
    pub fn drop_query<Q>(&self, q: &Q)
    where
        Q: QueryStatusKey,
    {
        let should_insert = q.with_mut_status(self, |s| match s {
            Some(mut s) => {
                s.migration_state = MigrationState::Dropped;
                false
            }
            None => true,
        });
        if should_insert {
            self.insert_with_status(
                q.clone(),
                QueryStatus {
                    migration_state: MigrationState::Dropped,
                    execution_info: None,
                    always: false,
                },
            );
        }
    }

    /// This function is called if we attempted to create an inlined migration but received an
    /// unsupported error. Updates the query status and removes pending inlined migrations.
    pub fn unsupported_inlined_migration(&self, q: &ViewCreateRequest) {
        let should_insert = q.with_mut_status(self, |s| match s {
            Some(mut s) => {
                s.migration_state = MigrationState::Unsupported;
                false
            }
            None => true,
        });
        if should_insert {
            self.insert_with_status(
                q.clone(),
                QueryStatus {
                    migration_state: MigrationState::Unsupported,
                    execution_info: None,
                    always: false,
                },
            );
        }
        self.persistent_handle.pending_inlined_migrations.remove(q);
    }

    /// Updates the query's always flag, indicating whether the query should be served from
    /// ReadySet regardless of autocommit state.
    /// Will not apply the always flag to unsupported queries, or try to insert a query if it has
    /// not already been registered.
    pub fn always_attempt_readyset<Q>(&self, q: &Q, always: bool)
    where
        Q: QueryStatusKey,
    {
        q.with_mut_status(self, |s| match s {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                s.always = always;
            }
            _ => {}
        })
    }

    /// Updates a queries status to `status` unless the queries migration state was
    /// `MigrationState::Unsupported`. An unsupported query cannot currently become supported once
    /// again.
    pub fn update_query_status<Q>(&self, q: &Q, status: QueryStatus)
    where
        Q: QueryStatusKey,
    {
        let should_insert = q.with_mut_status(self, |s| match s {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                s.migration_state = status.migration_state.clone();
                s.execution_info = status.execution_info.clone();
                false
            }
            Some(mut s) => {
                s.execution_info = status.execution_info.clone();
                false
            }
            None => true,
        });
        if should_insert {
            self.insert_with_status(q.clone(), status);
        }
    }

    /// Clear all queries currently marked as successful from the cache.
    ///
    /// NOTE: We do not mark cleared queries as dropped, since we are not explicitly deny-listing
    /// cleared queries.
    pub fn clear(&self) {
        self.id_to_status
            .iter_mut()
            .filter(|v| v.is_successful())
            .for_each(|mut v| {
                v.migration_state = MigrationState::Pending;
                v.always = false;
            });
        let mut statuses = self.persistent_handle.statuses.write();
        statuses
            .iter_mut()
            .filter(|(_query_id, (_query, status))| status.is_successful())
            .for_each(|(_query_id, (_query, ref mut status))| {
                status.migration_state = MigrationState::Pending;
                status.always = false;
            });
    }

    /// This method is called when a query is executed with the given params, but no inlined cache
    /// exists for the params. Adding the query to `Self::pending_inlined_migrations` indicates that
    /// it should be migrated by the MigrationHandler.
    pub fn inlined_cache_miss(&self, query: &ViewCreateRequest, params: Vec<DfValue>) {
        if self.enable_experimental_placeholder_inlining {
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
                migration_state: MigrationState::Inlined(ref mut state),
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

    /// Returns a list of queries that have a state of [`QueryState::Successful`].
    pub fn allow_list(&self) -> Vec<(QueryId, Arc<ViewCreateRequest>, QueryStatus)> {
        self.persistent_handle.allow_list()
    }

    /// Returns a list of queries that are in the deny list.
    pub fn deny_list(&self) -> Vec<DeniedQuery> {
        self.persistent_handle.deny_list(self.style)
    }

    /// Returns a query given a query hash
    pub fn query(&self, id: &str) -> Option<Query> {
        let id = QueryId::new(u64::from_str_radix(id.strip_prefix("q_")?, 16).ok()?);
        let statuses = self.persistent_handle.statuses.read();
        statuses.peek(&id).map(|(query, _status)| query.clone())
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
    use nom_sql::{SelectStatement, SqlQuery};
    use readyset_client::ViewCreateRequest;
    use vec1::Vec1;

    use super::*;

    fn select_statement(s: &str) -> anyhow::Result<SelectStatement> {
        match nom_sql::parse_query(nom_sql::Dialect::MySQL, s) {
            Ok(SqlQuery::Select(s)) => Ok(s),
            _ => Err(anyhow::anyhow!("Invalid SELECT statement")),
        }
    }

    #[test]
    fn query_hashes_eq_inner_hashes() {
        // This ensures that calling query_status on a &SelectStatement or &String will find the
        // corresponding Query in the DashMap
        let select = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let string = "SELECT * FROM t1".to_string();
        let q_select: Query = select.clone().into();
        let q_string: Query = string.clone().into();
        assert_eq!(hash(&select), hash(&q_select));
        assert_eq!(hash(&string), hash(&q_string));
    }

    #[test]
    fn select_is_found_after_insert() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let status = QueryStatus::default_for_query(&q1.clone().into());
        let id = QueryId::new(hash(&q1));

        cache.insert(q1.clone());

        let mut statuses = cache.persistent_handle.statuses.write();
        assert!(statuses
            .iter()
            .map(|(_, (q, _))| q.clone())
            .any(|q| q == q1.clone().into()));

        assert!(statuses.put(id, (q1.into(), status.clone())).is_some());

        assert_eq!(statuses.get(&id).unwrap().1, status);
    }

    #[test]
    fn string_is_found_after_insert() {
        let cache = QueryStatusCache::new();
        let q1 = "SELECT * FROM t1".to_string();
        let status = QueryStatus::default_for_query(&q1.clone().into());
        let id = QueryId::new(hash(&q1));

        cache.insert(q1.clone());

        let mut statuses = cache.persistent_handle.statuses.write();
        assert!(statuses
            .iter()
            .map(|(_, (q, _))| q.clone())
            .any(|q| q == q1.clone().into()));

        assert!(statuses.put(id, (q1.into(), status.clone())).is_some());

        assert_eq!(statuses.get(&id).unwrap().1, status);
    }

    #[test]
    fn query_is_referenced_by_hash() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.update_query_migration_state(&q1, MigrationState::Pending);
        cache.update_query_migration_state(&q2, MigrationState::Successful);

        let h1 = QueryId::new(hash(&q1));
        let h2 = QueryId::new(hash(&q2));

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
            QueryId::new(hash(&Into::<Query>::into(query.clone())))
        );

        // If we haven't explicitly updated it, we default to pending
        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );

        // Explicitly updating it also lets it be returned from pending_migration(), allow_list(),
        // and deny_list()
        cache.update_query_migration_state(&query, MigrationState::Pending);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.allow_list().len(), 0);
        assert_eq!(cache.deny_list().len(), 0);

        cache.update_query_migration_state(&query, MigrationState::Successful);
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.allow_list().len(), 1);
        assert_eq!(cache.deny_list().len(), 0);
    }

    #[test]
    fn query_is_denied() {
        let cache = QueryStatusCache::new();
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );
        cache.update_query_migration_state(&query, MigrationState::Pending);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.allow_list().len(), 0);
        assert_eq!(cache.deny_list().len(), 0);

        cache.update_query_migration_state(&query, MigrationState::Unsupported);
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.allow_list().len(), 0);
        assert_eq!(cache.deny_list().len(), 1);
    }

    #[test]
    fn query_is_inferred_denied_explicit() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );
        cache.update_query_migration_state(&query, MigrationState::Pending);
        assert_eq!(cache.pending_migration().len(), 1);
        assert_eq!(cache.allow_list().len(), 0);
        assert_eq!(cache.deny_list().len(), 1);

        cache.update_query_migration_state(&query, MigrationState::Unsupported);
        assert_eq!(cache.pending_migration().len(), 0);
        assert_eq!(cache.allow_list().len(), 0);
        assert_eq!(cache.deny_list().len(), 1);
    }

    #[test]
    fn clear() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);

        cache.update_query_migration_state(
            &ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]),
            MigrationState::Successful,
        );
        cache.update_query_migration_state(
            &ViewCreateRequest::new(
                select_statement("SELECT * FROM t1 WHERE id = ?").unwrap(),
                vec![],
            ),
            MigrationState::Successful,
        );
        assert_eq!(cache.allow_list().len(), 2);

        cache.clear();
        assert_eq!(cache.allow_list().len(), 0);
    }

    #[test]
    fn view_not_found_for_query() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.update_query_migration_state(&q1, MigrationState::Successful);
        cache.update_query_migration_state(
            &q2,
            MigrationState::Inlined(InlinedState {
                inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
                epoch: 0,
            }),
        );
        // q1: supported -> pending
        cache.view_not_found_for_query(&q1);
        assert_eq!(cache.pending_migration().len(), 1);
        // q1: pending -> unsupported
        cache.update_query_migration_state(&q1, MigrationState::Unsupported);
        assert_eq!(cache.pending_migration().len(), 0);
        // q2: inlined -> inlined
        cache.view_not_found_for_query(&q2);
        assert_eq!(cache.pending_migration().len(), 0);
    }

    #[test]
    fn transition_from_unsupported() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        cache.update_query_migration_state(&q, MigrationState::Pending);
        cache.update_query_migration_state(&q, MigrationState::Unsupported);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
        cache.update_query_migration_state(&q, MigrationState::Pending);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
        cache.update_query_migration_state(
            &q,
            MigrationState::Inlined(InlinedState {
                inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
                epoch: 0,
            }),
        );
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
        cache.update_query_migration_state(&q, MigrationState::Successful);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
    }

    #[test]
    fn transition_from_inlined() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .enable_experimental_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });

        cache.update_query_migration_state(&q, inlined_state.clone());
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(&q, MigrationState::Pending);
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(&q, MigrationState::Successful);
        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.update_query_migration_state(&q, MigrationState::Unsupported);
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
    }

    #[test]
    fn inlined_cache_miss() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .enable_experimental_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state);

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
            .enable_experimental_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state);

        cache.inlined_cache_miss(&q, vec![DfValue::None]);

        cache.unsupported_inlined_migration(&q);

        assert!(cache
            .persistent_handle
            .pending_inlined_migrations
            .is_empty());
        assert_eq!(
            cache.query_migration_state(&q).1,
            MigrationState::Unsupported
        );
    }

    #[test]
    fn created_inlined_query() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .enable_experimental_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state.clone());

        cache.inlined_cache_miss(&q, vec![DfValue::None]);
        cache.inlined_cache_miss(&q, vec![DfValue::Max]);
        cache.inlined_cache_miss(&q, vec![DfValue::Int(1)]);

        assert_eq!(cache.query_migration_state(&q).1, inlined_state);
        cache.created_inlined_query(&q, vec![&vec![DfValue::Int(1)], &vec![DfValue::None]]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 1,
        });
        cache.update_query_migration_state(&q, inlined_state.clone());
        let state = cache.query_status(&q).migration_state;
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
        assert!(cache
            .persistent_handle
            .pending_inlined_migrations
            .get(&q)
            .unwrap()
            .value()
            .contains(&vec![DfValue::Max]))
    }

    #[test]
    fn pending_inlined_migration() {
        let cache = QueryStatusCache::new()
            .style(MigrationStyle::Explicit)
            .enable_experimental_placeholder_inlining(true);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let inlined_state = MigrationState::Inlined(InlinedState {
            inlined_placeholders: Vec1::try_from(vec![1]).unwrap(),
            epoch: 0,
        });
        cache.update_query_migration_state(&q, inlined_state);

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
    fn drop_query() {
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        // Assert that if we have not seen this query, the query is marked as dropped. This may be
        // relevant to multiple adapter configurations, or just after an adapter restart.
        cache.drop_query(&q);
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Dropped);

        // Assert that we can drop a Successful query
        cache.update_query_migration_state(&q, MigrationState::Successful);
        cache.drop_query(&q);
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Dropped);

        // Assert that cleared queries are not marked as Dropped.
        cache.update_query_migration_state(&q, MigrationState::Successful);
        cache.clear();
        assert_eq!(cache.query_migration_state(&q).1, MigrationState::Pending);
    }

    #[test]
    fn avoid_insert_deadlock() {
        readyset_tracing::init_test_logging();
        let cache = QueryStatusCache::new().style(MigrationStyle::Explicit);
        let q = Query::ParseFailed(Arc::new("foobar".to_string()));

        q.with_mut_status(&cache, |_| {
            // Simulate it being removed by lru cache then inserted
            let query = Query::ParseFailed(Arc::new("foobar".to_string()));
            let query_id = QueryId::new(0);
            let query_status = QueryStatus::default_for_query(&query);
            cache
                .persistent_handle
                .insert_with_status(query, query_id, query_status);
        });
    }
}
