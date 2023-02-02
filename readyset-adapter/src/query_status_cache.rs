//! The query status cache provides a thread-safe window into an adapter's
//! knowledge about queries, currently the migration status of a query in
//! ReadySet.
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use dashmap::DashMap;
use readyset_client::query::*;
use readyset_client::ViewCreateRequest;
use readyset_tracing::error;
use readyset_util::hash::hash;

/// A metadata cache for all queries that have been processed by this
/// adapter. Thread-safe.
#[derive(Debug)]
pub struct QueryStatusCache {
    /// A thread-safe hash map that holds the query status of each successfully parsed query that
    /// is cached.
    statuses: DashMap<Arc<ViewCreateRequest>, QueryStatus, ahash::RandomState>,

    // A thread-safe hash map that holds the query status of each query that has failed to parse.
    failed_parses: DashMap<Arc<String>, QueryStatus, ahash::RandomState>,

    /// A thread-safe hash map that maps a query's id to the query. The id is a string formatted as
    /// q_<16-digit-query-hash>. The id is stored as a string instead of a u64 to allow for
    /// different id formats in the future.
    ids: DashMap<QueryId, Query, ahash::RandomState>,

    /// Holds the current style of migration, whether async or explicit, which may change the
    /// behavior of some internal methods.
    style: MigrationStyle,

    /// Whether to store a list of pending inlined migrations. Inlined migrations are those with
    /// literal values inlined into certain placeholder positions in the query.
    ///
    /// Currently unused.
    automatic_placeholder_inlining: bool,
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
        F: FnOnce(Option<&mut QueryStatus>) -> R;
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
        F: FnOnce(Option<&mut QueryStatus>) -> R,
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
        f(cache.statuses.get(self).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&mut QueryStatus>) -> R,
    {
        f(cache.statuses.get_mut(self).as_deref_mut())
    }
}

impl QueryStatusKey for String {
    fn with_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&QueryStatus>) -> R,
    {
        f(cache.failed_parses.get(self).as_deref())
    }

    fn with_mut_status<F, R>(&self, cache: &QueryStatusCache, f: F) -> R
    where
        F: FnOnce(Option<&mut QueryStatus>) -> R,
    {
        f(cache.failed_parses.get_mut(self).as_deref_mut())
    }
}

impl Default for QueryStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStatusCache {
    /// Constructs a new QueryStatusCache with the migration style set to InRequestPath.
    pub fn new() -> QueryStatusCache {
        QueryStatusCache {
            statuses: DashMap::default(),
            failed_parses: DashMap::default(),
            ids: DashMap::default(),
            style: MigrationStyle::InRequestPath,
            automatic_placeholder_inlining: false,
        }
    }

    /// Sets [`Self::style`]
    pub fn style(mut self, style: MigrationStyle) -> Self {
        self.style = style;
        self
    }

    /// Sets [`Self::automatic_placeholder_inlining`]
    pub fn automatic_placeholder_inlining(mut self, automatic_placeholder_inlining: bool) -> Self {
        self.automatic_placeholder_inlining = automatic_placeholder_inlining;
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
        let migration_state = status.migration_state;
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
        self.ids.insert(id, q.clone());
        match q {
            Query::Parsed(q) => self.statuses.insert(q, status),
            Query::ParseFailed(q) => self.failed_parses.insert(q, status),
        };
        id
    }

    /// This function returns the id and query migration state of a query. If the query does not
    /// exist within the query status cache, an entry is created and the query is set to
    /// PendingMigration.
    pub fn query_migration_state<Q>(&self, q: &Q) -> (QueryId, MigrationState)
    where
        Q: QueryStatusKey,
    {
        let query_state = q.with_status(self, |m| m.map(|m| m.migration_state));
        let id = QueryId::new(hash(&q));

        match query_state {
            Some(s) => {
                debug_assert!(
                    *self.ids.get(&id).expect("query not found") == q.clone().into(),
                    "mismatch between calculated and cached id/query"
                );

                (id, s)
            }
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
                s.execution_info = Some(info);
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

    /// Updates a queries migration state to `m` unless the queries migration state was
    /// `MigrationState::Unsupported`. An unsupported query cannot currently become supported once
    /// again.
    pub fn update_query_migration_state<Q>(&self, q: &Q, m: MigrationState)
    where
        Q: QueryStatusKey,
    {
        q.with_mut_status(self, |s| {
            match s {
                Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                    // Once a query is determined to be unsupported, there is currently no going
                    // back. In the future when we can support this in the query
                    // path this check should change.
                    s.migration_state = m;
                }
                None => {
                    self.insert_with_status(
                        q.clone(),
                        QueryStatus {
                            migration_state: m,
                            execution_info: None,
                            always: false,
                        },
                    );
                }
                _ => {}
            }
        })
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
        q.with_mut_status(self, |s| match s {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                s.migration_state = status.migration_state;
                s.execution_info = status.execution_info;
            }
            Some(mut s) => {
                s.execution_info = status.execution_info;
            }
            None => {
                self.insert_with_status(q.clone(), status);
            }
        })
    }

    /// Clear all queries currently marked as successful from the cache.
    pub fn clear(&self) {
        self.statuses
            .iter_mut()
            .filter(|v| v.is_successful())
            .for_each(|mut v| {
                v.migration_state = MigrationState::Pending;
                v.always = false;
            });
    }

    /// Returns a list of queries that currently need the be processed to determine
    /// if they should be allowed (are supported by ReadySet).
    pub fn pending_migration(&self) -> QueryList {
        self.statuses
            .iter()
            .filter(|r| r.is_pending())
            .map(|r| ((*r.key()).clone().into(), r.value().clone()))
            .chain(
                self.failed_parses
                    .iter()
                    .filter(|r| r.is_pending())
                    .map(|r| ((*r.key()).clone().into(), r.value().clone())),
            )
            .collect::<Vec<(Query, QueryStatus)>>()
            .into()
    }

    /// Returns a list of queries that have a state of [`QueryState::Successful`].
    pub fn allow_list(&self) -> QueryList {
        self.statuses
            .iter()
            .filter(|r| r.is_successful())
            .map(|r| ((*(r.key())).clone().into(), r.value().clone()))
            .chain(
                self.failed_parses
                    .iter()
                    .filter(|r| r.is_successful())
                    .map(|r| ((*r.key()).clone().into(), r.value().clone())),
            )
            .collect::<Vec<(Query, QueryStatus)>>()
            .into()
    }

    /// Returns a list of queries that are in the deny list.
    pub fn deny_list(&self) -> Vec<DeniedQuery> {
        match self.style {
            MigrationStyle::Async | MigrationStyle::InRequestPath => self
                .ids
                .iter()
                .filter_map(|r| {
                    r.value().with_status(self, |s| {
                        s.and_then(|s| {
                            if s.is_unsupported() {
                                Some(DeniedQuery {
                                    id: *r.key(),
                                    query: r.value().clone(),
                                    status: s.clone(),
                                })
                            } else {
                                None
                            }
                        })
                    })
                })
                .collect::<Vec<_>>(),
            MigrationStyle::Explicit => self
                .ids
                .iter()
                .filter_map(|r| {
                    r.value().with_status(self, |s| {
                        s.and_then(|s| {
                            if s.is_denied() {
                                Some(DeniedQuery {
                                    id: *r.key(),
                                    query: r.value().clone(),
                                    status: s.clone(),
                                })
                            } else {
                                None
                            }
                        })
                    })
                })
                .collect::<Vec<_>>(),
        }
    }

    /// Returns a query given a query hash
    pub fn query(&self, id: &str) -> Option<Query> {
        let id = QueryId::new(u64::from_str_radix(id.strip_prefix("q_")?, 16).ok()?);
        self.ids.get(&id).map(|r| (*r.value()).clone())
    }
}

/// MigrationStyle is used to communicate which style of managing migrations we have configured.
#[derive(Debug, Clone, Copy)]
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
        cache.insert(q1.clone());
        assert!(cache
            .ids
            .iter()
            .map(|r| r.value().clone())
            .any(|q| q == q1.clone().into()));
        assert!(cache
            .statuses
            .insert(q1.clone().into(), status.clone())
            .is_some());
        assert_eq!(*cache.statuses.get(&q1).unwrap().value(), status);
    }

    #[test]
    fn string_is_found_after_insert() {
        let cache = QueryStatusCache::new();
        let q1 = "SELECT * FROM t1".to_string();
        let status = QueryStatus::default_for_query(&q1.clone().into());
        cache.insert(q1.clone());
        assert!(cache
            .ids
            .iter()
            .map(|r| r.value().clone())
            .any(|q| q == q1.clone().into()));
        assert!(cache
            .failed_parses
            .insert(q1.clone().into(), status.clone())
            .is_some());
        assert_eq!(*cache.failed_parses.get(&q1).unwrap().value(), status);
    }

    #[test]
    fn query_is_referenced_by_hash() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.query_migration_state(&q1);
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
        assert_eq!(
            cache.query_migration_state(&query).1,
            MigrationState::Pending
        );
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
}
