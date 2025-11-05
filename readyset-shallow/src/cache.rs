use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moka::future::Cache as MokaCache;
use moka::policy::Expiry;

use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, SelectStatement, SqlIdentifier,
};

use crate::{EvictionPolicy, QueryMetadata, QueryResult};

pub(crate) type InnerCache<K, V> = Arc<MokaCache<(u64, K), Arc<CacheEntry<V>>>>;

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug)]
pub(crate) struct CacheEntry<V> {
    values: Arc<Vec<V>>,
    metadata: Option<Arc<QueryMetadata>>,
    accessed_ms: AtomicU64,
    refreshed_ms: AtomicU64,
    refreshing: AtomicBool,
    ttl_ms: Option<u64>,
}

pub(crate) struct CacheExpiration;

impl<K, V> Expiry<K, Arc<CacheEntry<V>>> for CacheExpiration {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &Arc<CacheEntry<V>>,
        _created: Instant,
    ) -> Option<Duration> {
        value.ttl_ms.map(Duration::from_millis)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &Arc<CacheEntry<V>>,
        _updated: Instant,
        _left: Option<Duration>,
    ) -> Option<Duration> {
        value.ttl_ms.map(Duration::from_millis)
    }
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
    id: u64,
    inner: InnerCache<K, V>,
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
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) fn new(
        id: u64,
        inner: InnerCache<K, V>,
        policy: EvictionPolicy,
        name: Option<Relation>,
        query_id: Option<QueryId>,
        query: SelectStatement,
        schema_search_path: Vec<SqlIdentifier>,
    ) -> Self {
        let ttl_ms = match policy {
            EvictionPolicy::Ttl(ttl) => Some(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
        };

        Self {
            id,
            inner,
            cache_metadata: Default::default(),
            name,
            query_id,
            query,
            schema_search_path,
            ttl_ms,
        }
    }

    pub(crate) async fn insert(&self, k: K, v: Vec<V>, metadata: QueryMetadata) {
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
            ttl_ms: self.ttl_ms,
        });
        self.inner.insert((self.id, k), entry).await;
    }

    pub async fn get(&self, k: K) -> (Option<(QueryResult<V>, bool)>, K) {
        let k = (self.id, k);
        let res = self.inner.get(&k).await.map(|entry| {
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
        });
        (res, k.1)
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

    use crate::{CacheManager, EvictionPolicy, QueryMetadata};

    use super::*;

    const ID: u64 = 0;

    fn new<K, V>(policy: EvictionPolicy) -> Cache<K, V>
    where
        K: Eq + Hash + Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        let inner = CacheManager::new_inner();
        Cache::new(
            ID,
            inner,
            policy,
            None,
            None,
            SelectStatement::default(),
            vec![],
        )
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache = new(EvictionPolicy::Ttl(Duration::from_secs(1)));

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::empty();

        cache.insert(key.clone(), values.clone(), metadata).await;
        let result = cache.get(key.clone()).await.0.unwrap();
        assert_eq!(result.0.values.as_ref(), &values);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.get(key).await.0.is_none());
    }

    #[tokio::test]
    async fn test_ttl_update() {
        let cache = new(EvictionPolicy::Ttl(Duration::from_secs(2)));

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::empty();

        for _ in 0..4 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            cache
                .insert(key.clone(), values.clone(), metadata.clone())
                .await;
            let result = cache.get(key.clone()).await.0.unwrap();
            assert_eq!(result.0.values.as_ref(), &values);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(cache.get(key).await.0.is_none());
    }

    #[tokio::test]
    async fn test_shared_inner_cache() {
        let inner = CacheManager::new_inner();
        let policy = EvictionPolicy::Ttl(Duration::from_secs(60));
        let stmt = SelectStatement::default();

        let cache_0 = Cache::new(
            0,
            Arc::clone(&inner),
            policy,
            None,
            None,
            stmt.clone(),
            vec![],
        );
        let cache_1 = Cache::new(
            1,
            Arc::clone(&inner),
            policy,
            None,
            None,
            stmt.clone(),
            vec![],
        );

        let key = vec!["shared_key"];
        let values_0 = vec![vec!["value_from_cache_0"]];
        let values_1 = vec![vec!["value_from_cache_1"]];
        let metadata = QueryMetadata::empty();

        cache_0
            .insert(key.clone(), values_0.clone(), metadata.clone())
            .await;
        cache_1
            .insert(key.clone(), values_1.clone(), metadata.clone())
            .await;

        let result_0 = cache_0.get(key.clone()).await.0.unwrap();
        assert_eq!(result_0.0.values.as_ref(), &values_0);

        let result_1 = cache_1.get(key.clone()).await.0.unwrap();
        assert_eq!(result_1.0.values.as_ref(), &values_1);
    }
}
