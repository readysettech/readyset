//! The query status cache provides a thread-safe window into an adapter's
//! knowledge about queries, currently the migration status of a query in
//! Noria.
//!
//! TODO(ENG-698): Track transient failure metadata to allow the adapter to
//! make decisions about when to mark failing queries as unsupported.
use crate::rewrite::anonymize_literals;
use dashmap::DashMap;
use nom_sql::SelectStatement;
use serde::{ser::SerializeSeq, Serialize, Serializer};

/// Each query is uniquely identifier by its select statement
type Query = SelectStatement;

#[derive(Debug, Clone)]
struct QueryStatus {
    migration_state: MigrationState,
}

impl QueryStatus {
    fn new() -> Self {
        Self {
            migration_state: MigrationState::Pending,
        }
    }
}

/// Represents the current migration state of a given query. This state should be updated any time
/// a migration is performed, or we learn that the migration state has changed, i.e. we receive a
/// ViewNotFound error indicating a query is not migrated.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationState {
    /// A migration has not been completed for this query. There may be one in progress depending
    /// on the adapters MigrationMode.
    Pending,
    /// This query has been migrated and a view exists.
    Successful,
    /// This query is not supported and should not be tried against Noria.
    Unsupported,
}

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

impl IntoIterator for QueryList {
    type Item = Query;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.queries.into_iter()
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

/// A metadata cache for all queries that have been processed by this
/// adapter. Thread-safe.
pub struct QueryStatusCache {
    /// A thread-safe hash map that holds the query status of each query
    /// that is cached.
    inner: DashMap<Query, QueryStatus>,

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
            inner: DashMap::new(),
            style: MigrationStyle::InRequestPath,
        }
    }

    pub fn with_style(style: MigrationStyle) -> QueryStatusCache {
        QueryStatusCache {
            inner: DashMap::new(),
            style,
        }
    }

    /// This function returns the query migration state of a query. If the query does not exist
    /// within the query status cache, an entry is created and the query is set to
    /// PendingMigration.
    pub async fn query_migration_state(&self, q: &Query) -> MigrationState {
        let query_state = self.inner.get(q).map(|m| m.migration_state.clone());
        match query_state {
            Some(s) => s,
            None => {
                self.inner.insert(q.clone(), QueryStatus::new());
                MigrationState::Pending
            }
        }
    }

    /// Updates a queries migration state to `m` unless the queries migration state was
    /// `MigrationState::Unsupported`. An unsupported query cannot currently become supported once
    /// again.
    pub async fn update_query_migration_state(&self, q: &Query, m: MigrationState) {
        match self.inner.get_mut(q) {
            Some(mut s) if s.migration_state != MigrationState::Unsupported => {
                // Once a query is determined to be unsupported, there is currently no going back.
                // In the future when we can support this in the query path this check should change.
                s.migration_state = m;
            }
            None => {
                let _ = self
                    .inner
                    .insert(q.clone(), QueryStatus { migration_state: m });
            }
            _ => {}
        }
    }

    /// Returns a list of queries that currently need the be processed to determine
    /// if they should be allowed (are supported by Noria).
    pub async fn pending_migration(&self) -> Vec<Query> {
        self.inner
            .iter()
            .filter(|r| matches!(r.value().migration_state, MigrationState::Pending))
            .map(|r| r.key().clone())
            .collect()
    }

    /// Returns a list of queries that have a state of [`QueryState::Successful`].
    pub async fn allow_list(&self) -> QueryList {
        self.inner
            .iter()
            .filter(|r| matches!(r.value().migration_state, MigrationState::Successful))
            .map(|r| r.key().clone())
            .collect::<Vec<Query>>()
            .into()
    }

    /// Returns a list of queries that are in the deny list.
    pub async fn deny_list(&self) -> QueryList {
        match self.style {
            MigrationStyle::Async | MigrationStyle::InRequestPath => self
                .inner
                .iter()
                .filter(|r| matches!(r.value().migration_state, MigrationState::Unsupported))
                .map(|r| r.key().clone())
                .collect::<Vec<Query>>()
                .into(),
            MigrationStyle::Explicit => self
                .inner
                .iter()
                .filter(|r| {
                    matches!(
                        r.value().migration_state,
                        MigrationState::Unsupported | MigrationState::Pending
                    )
                })
                .map(|r| r.key().clone())
                .collect::<Vec<Query>>()
                .into(),
        }
    }
}

/// MigrationStyle is used to communicate which style of managing migrations we have configured.
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
    use super::*;
    use nom_sql::SqlQuery;

    fn select_statement(s: &str) -> anyhow::Result<SelectStatement> {
        match nom_sql::parse_query(nom_sql::Dialect::MySQL, s) {
            Ok(SqlQuery::Select(s)) => Ok(s),
            _ => Err(anyhow::anyhow!("Invalid SELECT statement")),
        }
    }

    #[tokio::test]
    async fn query_is_allowed() {
        let cache = QueryStatusCache::new();
        let query = select_statement("SELECT * FROM t1").unwrap();

        assert_eq!(
            cache.query_migration_state(&query).await,
            MigrationState::Pending
        );
        assert_eq!(cache.pending_migration().await.len(), 1);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 0);

        cache
            .update_query_migration_state(&query, MigrationState::Successful)
            .await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await.len(), 1);
        assert_eq!(cache.deny_list().await.len(), 0);
    }

    #[tokio::test]
    async fn query_is_denied() {
        let cache = QueryStatusCache::new();
        let query = select_statement("SELECT * FROM t1").unwrap();

        assert_eq!(
            cache.query_migration_state(&query).await,
            MigrationState::Pending
        );
        assert_eq!(cache.pending_migration().await.len(), 1);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 0);

        cache
            .update_query_migration_state(&query, MigrationState::Unsupported)
            .await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 1);
    }

    #[tokio::test]
    async fn query_is_inferred_denied_explicit() {
        let cache = QueryStatusCache::with_style(MigrationStyle::Explicit);
        let query = select_statement("SELECT * FROM t1").unwrap();

        assert_eq!(
            cache.query_migration_state(&query).await,
            MigrationState::Pending
        );
        assert_eq!(cache.pending_migration().await.len(), 1);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 1);

        cache
            .update_query_migration_state(&query, MigrationState::Unsupported)
            .await;
        assert_eq!(cache.pending_migration().await.len(), 0);
        assert_eq!(cache.allow_list().await.len(), 0);
        assert_eq!(cache.deny_list().await.len(), 1);
    }
}
