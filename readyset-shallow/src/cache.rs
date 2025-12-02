use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moka::future::Cache as MokaCache;
use moka::policy::Expiry;

use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, SelectStatement, SqlIdentifier,
};
use readyset_util::SizeOf;

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

impl<V> SizeOf for CacheEntry<V>
where
    V: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        let mut sz = size_of::<Self>() + self.values.deep_size_of();
        if let Some(ref meta) = self.metadata {
            sz += meta.deep_size_of();
        }
        sz
    }

    fn is_empty(&self) -> bool {
        false
    }
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
    pub ddl_req: CacheDDLRequest,
    pub always: bool,
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
            always: info.always,
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
    ddl_req: CacheDDLRequest,
    always: bool,
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: u64,
        inner: InnerCache<K, V>,
        policy: EvictionPolicy,
        name: Option<Relation>,
        query_id: Option<QueryId>,
        query: SelectStatement,
        schema_search_path: Vec<SqlIdentifier>,
        ddl_req: CacheDDLRequest,
        always: bool,
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
            ddl_req,
            always,
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

    pub(crate) async fn get(&self, k: K) -> (Option<(QueryResult<V>, bool)>, K) {
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
            ddl_req: self.ddl_req.clone(),
            always: self.always,
        }
    }

    pub fn name(&self) -> &Option<Relation> {
        &self.name
    }

    pub(crate) fn query_id(&self) -> &Option<QueryId> {
        &self.query_id
    }

    pub(crate) async fn count(&self) -> usize {
        self.inner.run_pending_tasks().await;
        self.inner.entry_count().try_into().unwrap_or(usize::MAX)
    }

    pub(crate) async fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use readyset_sql::ast::SelectStatement;

    use crate::{CacheManager, EvictionPolicy, QueryMetadata};

    use super::*;

    const ID: u64 = 0;

    fn test_ddl_req() -> CacheDDLRequest {
        CacheDDLRequest {
            unparsed_stmt: "CREATE SHALLOW CACHE test AS SELECT 1".to_string(),
            schema_search_path: vec![],
            dialect: readyset_sql::Dialect::PostgreSQL.into(),
        }
    }

    fn new<K, V>(max_capacity: Option<u64>, policy: EvictionPolicy) -> Cache<K, V>
    where
        K: Eq + Hash + Send + Sync + SizeOf + 'static,
        V: Send + Sync + SizeOf + 'static,
    {
        let inner = CacheManager::new_inner(max_capacity);
        Cache::new(
            ID,
            inner,
            policy,
            None,
            None,
            SelectStatement::default(),
            vec![],
            test_ddl_req(),
            false,
        )
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache = new(None, EvictionPolicy::Ttl(Duration::from_secs(1)));

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::Test;

        cache.insert(key.clone(), values.clone(), metadata).await;
        let result = cache.get(key.clone()).await.0.unwrap();
        assert_eq!(result.0.values.as_ref(), &values);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.get(key).await.0.is_none());
    }

    #[tokio::test]
    async fn test_ttl_update() {
        let cache = new(None, EvictionPolicy::Ttl(Duration::from_secs(1)));

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::Test;

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
        let inner = CacheManager::new_inner(None);
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
            test_ddl_req(),
            false,
        );
        let cache_1 = Cache::new(
            1,
            Arc::clone(&inner),
            policy,
            None,
            None,
            stmt.clone(),
            vec![],
            test_ddl_req(),
            false,
        );

        let key = vec!["shared_key"];
        let values_0 = vec![vec!["value_from_cache_0"]];
        let values_1 = vec![vec!["value_from_cache_1"]];
        let metadata = QueryMetadata::Test;

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

    #[test]
    fn test_size_of() {
        let now = current_timestamp_ms();
        let entry = CacheEntry {
            values: Arc::new(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            metadata: Some(Arc::new(QueryMetadata::Test)),
            accessed_ms: now.into(),
            refreshed_ms: now.into(),
            refreshing: false.into(),
            ttl_ms: None,
        };
        assert!(entry.deep_size_of() > 10 * 0u64.deep_size_of());
    }

    #[tokio::test]
    async fn test_max_capacity() {
        const BYTES: u64 = 1000;
        const COUNT: u64 = 100;

        let cache = new(Some(BYTES), EvictionPolicy::Ttl(Duration::from_secs(1)));
        for i in 0..COUNT {
            let v = vec!["xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()];
            cache.insert(i, v, QueryMetadata::Test).await;
        }
        cache.inner.run_pending_tasks().await;

        assert!(cache.inner.weighted_size() <= BYTES); // under limit?
        assert!(cache.inner.entry_count() < COUNT); // did we have to evict?
    }

    #[test]
    fn test_cache_debug() {
        let inner = CacheManager::new_inner(None);
        let relation = readyset_sql::ast::Relation {
            schema: None,
            name: "test_table".into(),
        };
        let query_id = readyset_client::query::QueryId::from_unparsed_select("SELECT * FROM test");

        let cache = Cache::<String, String>::new(
            42,
            inner,
            EvictionPolicy::Ttl(Duration::from_secs(60)),
            Some(relation.clone()),
            Some(query_id),
            SelectStatement::default(),
            vec![],
            test_ddl_req(),
            false,
        );

        let debug_str = format!("{:?}", cache);
        assert!(debug_str.contains("Cache"));
        assert!(debug_str.contains("name"));
        assert!(debug_str.contains("test_table"));
        assert!(debug_str.contains("query_id"));
        assert!(debug_str.contains("ttl_ms"));
        assert!(debug_str.contains("60000"));
    }
}
