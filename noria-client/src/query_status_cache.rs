//! This implements the SeenCache component of [Readyset Query Handling][doc]
//!
//! [doc]: https://docs.google.com/document/d/1GUwLwklpwVlX0fuXSUspn_uFLC2jNEEHo4WUJX8yHbg/edit
//!
//! The QueryStatusCache maintains the QueryStatus for each parsed query
//! seen in the adapter. The QueryStatus assigned to each query influences
//! how the query is handled in the adapter.
//!
//! If the query:
//!   - PendingMigration: The query should be sent to the fallback database.
//!   - SuccessfulMigration: The query should be sent to noria.
//!   - FailedExecute: The query should be sent to fallback if failed enough times.
//!   - Is not in the cache: The queries status should be determined and
//!                          set to either NeedsProcesing or SuccessfulMigrationed.

use chrono::{DateTime, Utc};
use nom_sql::SelectStatement;
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::collections::HashMap;

use crate::rewrite::anonymize_literals;

// TODO(): Consider a more complex flaky failure deteciton method for
// failed executes.
const MAXIMUM_FAILED_EXECUTES: u32 = 5;

/// Holds metadata regarding when a query was first seen within the system,
/// along with its current state.
#[derive(Debug, Clone)]
struct QueryStatus {
    // Indicates when the state field was last changed
    last_state_change: DateTime<Utc>,
    state: QueryState,
}

impl QueryStatus {
    fn new(state: QueryState) -> Self {
        Self {
            last_state_change: Utc::now(),
            state,
        }
    }
}

/// Each query is uniquely identifier by its select statement
type Query = SelectStatement;

#[derive(Debug, PartialEq, Eq)]
pub struct QueryList {
    queries: Vec<Query>,
}

impl QueryList {
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

impl From<Vec<Query>> for QueryList {
    fn from(queries: Vec<Query>) -> Self {
        QueryList { queries }
    }
}

impl Serialize for QueryList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sanitized = self.queries.clone();
        sanitized.iter_mut().for_each(anonymize_literals);

        let mut seq = serializer.serialize_seq(Some(sanitized.len()))?;
        for q in sanitized {
            seq.serialize_element(&q.to_string())?;
        }
        seq.end()
    }
}

/// Represents the current state of any given query. Deny is an implicit state
/// that is derived from a combination of PendingMigration and other query
/// status metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryState {
    PendingMigration,
    SuccessfulMigration,
    FailedExecute(u32),
    Unsupported,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AdmitStatus {
    Allow,
    Deny,
    // Transient status, indicates that the query has not been successfully installed but we can
    // attempt to install again
    Pending,
}

// TODO(ENG-877): Refactor query status cache to perform state transition checks
/// Represents all queries that have been seen in the system, along with
/// metadata about when the query was first seen, and what state it's currently
/// in. QueryStatusCache is thread safe. It is intended that only one
/// QueryStatusCache is spun up per adapter.
pub struct QueryStatusCache {
    /// A thread-safe hash map that holds the query status of each query
    /// that is cached.
    inner: tokio::sync::RwLock<HashMap<Query, QueryStatus>>,
    /// Defines a maximum age that any query may stay in the QueryStatusCache
    /// with a state of QueryState::PendingMigration before it is inferred to be
    /// denied. If a query is denied it is sent exclusively to fallback.
    max_processing: chrono::Duration,
}

impl QueryStatusCache {
    /// Construct a new QueryStatusCache. Requires a duration for max processing
    /// time, which will be used to infer the deny list, as well as to cease
    /// processing queries past a given age.
    pub fn new(max_processing: chrono::Duration) -> QueryStatusCache {
        QueryStatusCache {
            inner: tokio::sync::RwLock::new(HashMap::new()),
            max_processing,
        }
    }

    /// Helper method to assist in computing a DateTime<Utc> of the oldest age we will
    /// currently process for any given query in the cache.
    fn max_age(&self) -> chrono::DateTime<Utc> {
        Utc::now() - self.max_processing
    }

    /// Registers a query with a default state of [`QueryState::PendingMigration`] if
    /// it doesn't already exist in the cache. If it already exists as pending in the cache this
    /// will no op.
    pub async fn set_pending_migration(&self, query: &Query) {
        self.inner
            .write()
            .await
            .entry(query.clone())
            .and_modify(|s| {
                if s.state != QueryState::PendingMigration {
                    s.state = QueryState::PendingMigration;
                    s.last_state_change = Utc::now();
                }
            })
            .or_insert_with(|| QueryStatus::new(QueryState::PendingMigration));
    }

    /// Sets the provided query to have a QueryState of SuccessfulMigration.
    /// If not found we no-op. This function should never be called with a query that
    /// isn't already registered in the cache.
    pub async fn set_successful_migration(&self, query: &Query) {
        // We can move to the successful migration state from any state other than failed execute
        self.inner
            .write()
            .await
            .entry(query.clone())
            .and_modify(|s| match s.state {
                QueryState::FailedExecute(_) | QueryState::SuccessfulMigration => {}
                _ => {
                    s.state = QueryState::SuccessfulMigration;
                    s.last_state_change = Utc::now();
                }
            })
            .or_insert_with(|| QueryStatus::new(QueryState::SuccessfulMigration));
    }

    /// Sets the provided query to have a QueryState of PendingMigration.
    /// If the query is not found this is a no-op.
    pub async fn set_failed_query(&self, query: &Query) {
        self.inner.write().await.get_mut(query).map(|s| {
            if s.state != QueryState::PendingMigration {
                s.state = QueryState::PendingMigration;
                s.last_state_change = Utc::now();
                s
            } else {
                s
            }
        });
    }

    pub async fn set_unsupported_query(&self, query: &Query) {
        self.inner
            .write()
            .await
            .entry(query.clone())
            .and_modify(|s| {
                if s.state != QueryState::Unsupported {
                    s.state = QueryState::Unsupported;
                    s.last_state_change = Utc::now();
                }
            })
            .or_insert_with(|| QueryStatus::new(QueryState::Unsupported));
    }

    /// Sets the provided query to have a QueryState of FailedExecute.
    /// If the query is not found this is a no-op.
    pub async fn set_failed_execute(&self, query: &Query) {
        self.inner.write().await.get_mut(query).map(|s| {
            s.state = match s.state {
                QueryState::FailedExecute(n) => QueryState::FailedExecute(n + 1),
                _ => QueryState::FailedExecute(1),
            };
            s.last_state_change = Utc::now();
            s
        });
    }

    /// Update the queries internal
    /// If the query is not found this is a no-op.
    pub async fn set_failed_prepare(&self, query: &Query) {
        self.inner.write().await.get_mut(query).map(|s| {
            if s.state != QueryState::PendingMigration {
                s.state = QueryState::PendingMigration;
                s.last_state_change = Utc::now();
            }
            s
        });
    }

    /// Returns a list of queries that currently need the be processed to determine
    /// if they should be allowed (are supported by Noria).
    pub async fn pending_migration(&self) -> Vec<Query> {
        self.inner
            .read()
            .await
            .iter()
            .filter(|(_, status)| matches!(status.state, QueryState::PendingMigration if status.last_state_change >= self.max_age()))
            .map(|(q, _)| q.clone())
            .collect()
    }

    /// Returns a list of queries that have a state of [`QueryState::SuccessfulMigration`].
    pub async fn allow_list(&self) -> QueryList {
        self.inner
            .read()
            .await
            .iter()
            .filter(|(_, status)| matches!(status.state, QueryState::SuccessfulMigration))
            .map(|(q, _)| q.clone())
            .collect::<Vec<Query>>()
            .into()
    }

    /// Returns a list of queries that are in the deny list.
    pub async fn deny_list(&self) -> QueryList {
        self.inner
            .read()
            .await
            .iter()
            .filter(|(_, status)| {
                (matches!(status.state, QueryState::PendingMigration) && status.last_state_change < self.max_age()) || matches!(status.state, QueryState::FailedExecute(n) if n >= MAXIMUM_FAILED_EXECUTES) || matches!(status.state, QueryState::Unsupported)
           })
            .map(|(q, _)| q.clone())
            .collect::<Vec<Query>>()
            .into()
    }

    /// Returns whether we should admit the query for execution in Noria.
    pub async fn admit(&self, query: &Query) -> Option<AdmitStatus> {
        self.inner.read().await.get(query).map(|s| {
            let QueryStatus {
                state,
                last_state_change,
            } = &s;
            match state {
                QueryState::PendingMigration => {
                    if last_state_change < &self.max_age() {
                        AdmitStatus::Deny
                    } else {
                        AdmitStatus::Pending
                    }
                }
                QueryState::SuccessfulMigration => AdmitStatus::Allow,
                QueryState::FailedExecute(count) if count < &MAXIMUM_FAILED_EXECUTES => {
                    AdmitStatus::Allow
                }
                _ => AdmitStatus::Deny,
            }
        })
    }

    /// Returns whether the given query exists in the query status cache.
    pub async fn exists(&self, query: &Query) -> bool {
        self.inner.read().await.contains_key(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use nom_sql::SqlQuery;

    fn test_cache() -> QueryStatusCache {
        QueryStatusCache::new(Duration::minutes(1))
    }

    fn select_statement(s: &str) -> anyhow::Result<SelectStatement> {
        match nom_sql::parse_query(nom_sql::Dialect::MySQL, s) {
            Ok(SqlQuery::Select(s)) => Ok(s),
            _ => Err(anyhow::anyhow!("Invalid SELECT statement")),
        }
    }

    #[tokio::test]
    async fn query_is_allowed() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 0);
        assert_eq!(cache.admit(&query).await, None);

        cache.set_pending_migration(&query).await;
        assert_eq!(cache.pending_migration().await, vec![query.clone()]);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 0);
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Pending));

        cache.set_successful_migration(&query).await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await, vec![query.clone()].into());
        assert_eq!(cache.deny_list().await.len(), 0);
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Allow));
    }

    #[tokio::test]
    async fn repeated_prepares() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();

        cache.set_pending_migration(&query).await;
        cache.set_pending_migration(&query).await;
        assert_eq!(cache.pending_migration().await.len(), 1);
    }

    #[tokio::test]
    async fn failed_executes() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();

        cache.set_pending_migration(&query).await;
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Pending));

        cache.set_successful_migration(&query).await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await, vec![query.clone()].into());
        assert_eq!(cache.deny_list().await.len(), 0);
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Allow));

        cache.set_failed_execute(&query).await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 0);
        // Allow until max_age has been hit.
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Allow));
    }

    #[tokio::test]
    async fn repeated_failed_execute() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();

        cache.set_pending_migration(&query).await;
        for _ in 0..4 {
            cache.set_failed_execute(&query).await;
        }
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Allow));
        cache.set_failed_execute(&query).await;
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Deny));
    }

    #[tokio::test]
    async fn add_as_successful() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();
        cache.set_successful_migration(&query).await;
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Allow));
    }

    #[tokio::test]
    async fn add_as_unsupported() {
        let cache = test_cache();
        let query = select_statement("SELECT * FROM t1").unwrap();
        cache.set_unsupported_query(&query).await;
        assert_eq!(cache.admit(&query).await, Some(AdmitStatus::Deny));
    }
}
