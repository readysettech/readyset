//! The query status cache provides a thread-safe window into an adapter's
//! knowledge about queries, currently the migration status of a query in
//! ReadySet.
use std::borrow::Borrow;
use std::hash::Hash;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use launchpad::hash::hash;
use readyset::query::*;
use tracing::error;

/// A metadata cache for all queries that have been processed by this
/// adapter. Thread-safe.
#[derive(Debug)]
pub struct QueryStatusCache {
    /// A thread-safe hash map that holds the query status of each successfully parsed query that
    /// is cached. Queries that are not successfully parsed will be present in `ids` but omitted
    /// from this hash map.
    statuses: DashMap<Query, QueryStatus>,

    /// A thread-safe hash map that maps a query's id to the query. The id is a string formatted as
    /// q_<16-digit-query-hash>. The id is stored as a string instead of a u64 to allow for
    /// different id formats in the future.
    ids: DashMap<QueryId, Query>,

    /// Holds the current style of migration, whether async or explicit, which may change the
    /// behavior of some internal methods.
    style: MigrationStyle,
}

impl Default for QueryStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStatusCache {
    /// Constructs a new QueryStatusCache with the migration style set to Async.
    pub fn new() -> QueryStatusCache {
        QueryStatusCache {
            statuses: DashMap::new(),
            ids: DashMap::new(),
            style: MigrationStyle::InRequestPath,
        }
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
    pub fn insert_with_status<Q>(&self, q: Q, status: QueryStatus) -> QueryId
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
        let id = hash_to_query_id(hash(&q));
        debug_assert!(
            id.len() <= format!("q_{}", u64::MAX).len(),
            "query id should be small enough that cloning isn't slow"
        );

        self.ids.insert(id.clone(), q.clone());
        self.statuses.insert(q, status);
        id
    }

    pub fn with_style(style: MigrationStyle) -> QueryStatusCache {
        QueryStatusCache {
            statuses: DashMap::new(),
            ids: DashMap::new(),
            style,
        }
    }

    /// This function returns the id and query migration state of a query. If the query does not
    /// exist within the query status cache, an entry is created and the query is set to
    /// PendingMigration.
    pub fn query_migration_state<Q>(&self, q: &Q) -> (String, MigrationState)
    where
        Q: Into<Query> + Hash + Eq + Clone,
        Query: Borrow<Q>,
    {
        let query_state = self.statuses.get(q).map(|m| m.migration_state);
        let id = hash_to_query_id(hash(&q));

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
        Q: Into<Query> + Hash + Eq + Clone,
        Query: Borrow<Q>,
    {
        match self.statuses.get(q).map(|s| s.clone()) {
            Some(s) => s,
            None => QueryStatus::with_migration_state(self.insert(q.clone()).1),
        }
    }

    /// Updates the execution info for the given query.
    pub fn update_execution_info(&self, q: &Query, info: ExecutionInfo) {
        if let Some(mut s) = self.statuses.get_mut(q) {
            s.execution_info = Some(info);
        }
    }

    /// Updates the transition time in the execution info for the given query.
    pub fn update_transition_time<Q>(&self, q: &Q, transition: &std::time::Instant)
    where
        Q: Into<Query> + Hash + Eq,
        Query: Borrow<Q>,
    {
        if let Some(mut s) = self.statuses.get_mut(q) {
            if let Some(ref mut info) = s.execution_info {
                info.last_transition_time = *transition;
            }
        }
    }

    /// Resets the internal transition time to now. This should be used with extreme caution.
    pub fn reset_transition_time(&self, q: &Query) {
        if let Some(mut s) = self.statuses.get_mut(q) {
            if let Some(ref mut info) = s.execution_info {
                info.last_transition_time = Instant::now()
            }
        }
    }

    /// Update ExecutionInfo to indicate that a recent execute failed due to a networking problem.
    pub fn execute_network_failure(&self, q: &Query) {
        if let Some(mut s) = self.statuses.get_mut(q) {
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
    }

    /// Update ExecutionInfo to indicate that a recent execute succeeded.
    pub fn execute_succeeded(&self, q: &Query) {
        if let Some(mut s) = self.statuses.get_mut(q) {
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
    }

    /// Update ExecutionInfo to indicate that a recent execute failed.
    pub fn execute_failed(&self, q: &Query) {
        if let Some(mut s) = self.statuses.get_mut(q) {
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
    }

    /// If the current ExecutionState is ExecutionState::NetworkFailure, then this method will
    /// return true if that state has persisted for longer than the supplied duration, otherwise,
    /// it will return false.
    pub fn execute_network_failure_exceeded(&self, q: &Query, duration: Duration) -> bool {
        if let Some(ref s) = self.statuses.get(q) {
            if let Some(ref info) = s.execution_info {
                return info.execute_network_failure_exceeded(duration);
            }
        }

        false
    }

    /// Updates a queries migration state to `m` unless the queries migration state was
    /// `MigrationState::Unsupported`. An unsupported query cannot currently become supported once
    /// again.
    pub fn update_query_migration_state<Q>(&self, q: &Q, m: MigrationState)
    where
        Q: Clone + Hash + Eq,
        Query: From<Q> + Borrow<Q>,
    {
        match self.statuses.get_mut(q) {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                // Once a query is determined to be unsupported, there is currently no going back.
                // In the future when we can support this in the query path this check should
                // change.
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
    }

    /// Updates the query's always flag, indicating whether the query should be served from
    /// ReadySet regardless of autocommit state.
    /// Will not apply the always flag to unsupported queries, or try to insert a query if it has
    /// not already been registered.
    pub fn always_attempt_readyset<Q>(&self, q: &Q, always: bool)
    where
        Q: Hash + Eq + Clone,
        Query: From<Q> + Borrow<Q>,
    {
        match self.statuses.get_mut(q) {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                s.always = always;
            }
            _ => {}
        }
    }

    /// Updates a queries status to `status` unless the queries migration state was
    /// `MigrationState::Unsupported`. An unsupported query cannot currently become supported once
    /// again.
    pub fn update_query_status<Q>(&self, q: &Q, status: QueryStatus)
    where
        Q: Hash + Eq + Clone,
        Query: From<Q> + Borrow<Q>,
    {
        match self.statuses.get_mut(q) {
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
        }
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
            .map(|r| ((*r.key()).clone(), r.value().clone()))
            .collect::<Vec<(Query, QueryStatus)>>()
            .into()
    }

    /// Returns a list of queries that have a state of [`QueryState::Successful`].
    pub fn allow_list(&self) -> QueryList {
        self.statuses
            .iter()
            .filter(|r| r.is_successful())
            .map(|r| ((*(r.key())).clone(), r.value().clone()))
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
                    self.statuses
                        .get(Borrow::<Query>::borrow(r.value()))
                        .and_then(|s| {
                            if s.is_unsupported() {
                                Some(DeniedQuery {
                                    id: r.key().clone(),
                                    query: r.value().clone(),
                                    status: s.value().clone(),
                                })
                            } else {
                                None
                            }
                        })
                })
                .collect::<Vec<_>>(),
            MigrationStyle::Explicit => self
                .ids
                .iter()
                .filter_map(|r| {
                    self.statuses
                        .get(Borrow::<Query>::borrow(r.value()))
                        .and_then(|s| {
                            if s.is_denied() {
                                Some(DeniedQuery {
                                    id: r.key().clone(),
                                    query: r.value().clone(),
                                    status: s.value().clone(),
                                })
                            } else {
                                None
                            }
                        })
                })
                .collect::<Vec<_>>(),
        }
    }

    /// Returns a query given a query hash
    pub fn query(&self, id: &str) -> Option<Query> {
        self.ids.get(id).map(|r| (*r.value()).clone())
    }
}

/// MigrationStyle is used to communicate which style of managing migrations we have configured.
#[derive(Debug)]
pub enum MigrationStyle {
    /// Async migrations are enabled in the adapter by passing the --async-migrations flag.
    Async,
    /// Explicit migrations are enabled in the adapter by passing the --explicit-migrations flag.
    Explicit,
    /// InRequestPath is the style of managing migrations when neither async nor explicit
    /// migrations have been enabled.
    InRequestPath,
}

#[cfg(test)]
mod tests {
    use nom_sql::{SelectStatement, SqlQuery};
    use readyset::ViewCreateRequest;

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
            .collect::<Vec<_>>()
            .contains(&q1.clone().into()));
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
            .collect::<Vec<_>>()
            .contains(&q1.clone().into()));
        assert!(cache
            .statuses
            .insert(q1.clone().into(), status.clone())
            .is_some());
        assert_eq!(*cache.statuses.get(&q1).unwrap().value(), status);
    }

    #[test]
    fn query_is_referenced_by_hash() {
        let cache = QueryStatusCache::new();
        let q1 = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);
        let q2 = ViewCreateRequest::new(select_statement("SELECT * FROM t2").unwrap(), vec![]);

        cache.query_migration_state(&q1);
        cache.update_query_migration_state(&q2, MigrationState::Successful);

        let h1 = hash_to_query_id(hash(&q1));
        let h2 = hash_to_query_id(hash(&q2));

        let r1 = cache.query(&h1).unwrap();
        let r2 = cache.query(&h2).unwrap();

        assert_eq!(r1, q1.into());
        assert_eq!(r2, q2.into());
    }

    #[test]
    fn query_is_allowed() {
        let cache = QueryStatusCache::new();
        let query = ViewCreateRequest::new(select_statement("SELECT * FROM t1").unwrap(), vec![]);

        assert_eq!(
            cache.query_migration_state(&query).0,
            hash_to_query_id(hash(&Into::<Query>::into(query.clone())))
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
        let cache = QueryStatusCache::with_style(MigrationStyle::Explicit);
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
        let cache = QueryStatusCache::with_style(MigrationStyle::Explicit);

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
