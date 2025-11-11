use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use moka::sync::Cache as MokaCache;

use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, SelectStatement, SqlIdentifier,
};

use crate::{EvictionPolicy, QueryMetadata, QueryResult};

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug)]
struct CacheEntry<V> {
    values: Arc<Vec<V>>,
    metadata: Option<Arc<QueryMetadata>>,
    accessed_ms: AtomicU64,
    refreshed_ms: AtomicU64,
    refreshing: AtomicBool,
}

#[derive(Debug)]
pub struct CacheInfo {
    pub name: Option<Relation>,
    pub query_id: Option<QueryId>,
    pub query: SelectStatement,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub ttl_ms: Option<u64>,
}

impl From<CacheInfo> for CreateCacheStatement {
    fn from(info: CacheInfo) -> Self {
        Self {
            name: info.name,
            cache_type: Some(CacheType::Shallow),
            policy: info
                .ttl_ms
                .map(readyset_sql::ast::EvictionPolicy::from_ttl_ms),
            inner: Ok(CacheInner::Statement(Box::new(info.query))),
            unparsed_create_cache_statement: None,
            always: false,
            concurrently: false,
        }
    }
}

pub struct Cache<K, V> {
    results: MokaCache<K, Arc<CacheEntry<V>>>,
    cache_metadata: OnceLock<Arc<QueryMetadata>>,
    name: Option<Relation>,
    query_id: Option<QueryId>,
    query: SelectStatement,
    schema_search_path: Vec<SqlIdentifier>,
    ttl_ms: Option<u64>,
}

impl<K, V> Debug for Cache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("name", &self.name)
            .field("query_id", &self.query_id)
            .finish_non_exhaustive()
    }
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) fn new(
        policy: EvictionPolicy,
        name: Option<Relation>,
        query_id: Option<QueryId>,
        query: SelectStatement,
        schema_search_path: Vec<SqlIdentifier>,
    ) -> Self {
        let builder = MokaCache::builder();

        let (builder, ttl_ms) = match policy {
            EvictionPolicy::Ttl(ttl) => (builder.time_to_live(ttl), Some(ttl.as_millis() as _)),
        };

        Self {
            results: builder.build(),
            cache_metadata: Default::default(),
            name,
            query_id,
            query,
            schema_search_path,
            ttl_ms,
        }
    }

    pub(crate) fn insert(&self, k: K, v: Vec<V>, metadata: QueryMetadata) {
        let metadata = if let Some(existing) = self.cache_metadata.get() {
            if existing.as_ref() == &metadata {
                None
            } else {
                Some(Arc::new(metadata))
            }
        } else {
            let metadata = Arc::new(metadata);
            if let Err(existing) = self.cache_metadata.set(Arc::clone(&metadata)) {
                if existing.as_ref() == metadata.as_ref() {
                    None
                } else {
                    Some(metadata)
                }
            } else {
                None
            }
        };

        let now = current_timestamp_ms();
        let entry = Arc::new(CacheEntry {
            values: Arc::new(v),
            metadata,
            accessed_ms: now.into(),
            refreshed_ms: now.into(),
            refreshing: false.into(),
        });
        self.results.insert(k, entry);
    }

    pub fn get(&self, k: &K) -> Option<(QueryResult<V>, bool)> {
        self.results.get(k).map(|entry| {
            let now = current_timestamp_ms();
            entry.accessed_ms.store(now, Ordering::Relaxed);
            let refresh = if let Some(ttl_ms) = self.ttl_ms
                && now.saturating_sub(entry.refreshed_ms.load(Ordering::Relaxed)) >= ttl_ms / 2
                && entry
                    .refreshing
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                true
            } else {
                false
            };

            let metadata = entry
                .metadata
                .as_ref()
                .or_else(|| self.cache_metadata.get())
                .expect("No metadata available for cached result");

            (
                QueryResult {
                    values: Arc::clone(&entry.values),
                    metadata: Arc::clone(metadata),
                },
                refresh,
            )
        })
    }

    pub fn get_info(&self) -> CacheInfo {
        CacheInfo {
            name: self.name.clone(),
            query_id: self.query_id,
            query: self.query.clone(),
            schema_search_path: self.schema_search_path.clone(),
            ttl_ms: self.ttl_ms,
        }
    }

    pub fn name(&self) -> &Option<Relation> {
        &self.name
    }

    pub fn query_id(&self) -> &Option<QueryId> {
        &self.query_id
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use readyset_sql::ast::SelectStatement;

    use crate::{EvictionPolicy, QueryMetadata};

    use super::Cache;

    #[test]
    fn test_ttl_expiration() {
        let cache = Cache::new(
            EvictionPolicy::Ttl(Duration::from_secs(1)),
            None,
            None,
            SelectStatement::default(),
            vec![],
        );

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::empty();

        cache.insert(key.clone(), values.clone(), metadata);
        let result = cache.get(&key).unwrap();
        assert_eq!(result.0.values.as_ref(), &values);

        std::thread::sleep(Duration::from_secs(2));

        assert!(cache.get(&key).is_none());
    }
}
