use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use metrics::counter;
use moka::future::Cache as MokaCache;
use moka::notification::RemovalCause;
use papaya::HashMap;
use seize::Collector;
use tracing::info;

use readyset_client::consensus::CacheDDLRequest;
use readyset_client::metrics::recorded;
use readyset_client::query::QueryId;
use readyset_errors::{ReadySetError, ReadySetResult, internal};
use readyset_sql::ast::{Relation, ShallowCacheQuery, SqlIdentifier};
use readyset_util::SizeOf;

use crate::cache::{Cache, CacheEntry, CacheEntryInfo, CacheExpiration, CacheInfo, InnerCache};
use crate::{EvictionPolicy, QueryMetadata};
use readyset_util::hash::hash;

pub type RequestRefresh<K, V> = Arc<dyn Fn(CacheInsertGuard<K, V>) + Send + Sync>;

fn weight<K, V>(k: &K, v: &V) -> u32
where
    K: SizeOf,
    V: SizeOf,
{
    (k.deep_size_of() + v.deep_size_of())
        .try_into()
        .unwrap_or(u32::MAX)
}

pub struct CacheManager<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    caches: HashMap<u64, Arc<Cache<K, V>>>,
    names: HashMap<Relation, u64>,
    query_ids: HashMap<QueryId, u64>,
    inner: InnerCache<K, V>,
    // This lock also synchronizes inserts into the three HashMaps.
    next_id: Mutex<u64>,
}

fn new_table<K, V>() -> HashMap<K, V> {
    let gc = Collector::new().batch_size(1);
    HashMap::builder().collector(gc).build()
}

impl<K, V> CacheManager<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    pub(crate) fn new_inner(max_capacity: Option<u64>) -> InnerCache<K, V> {
        let mut builder = MokaCache::builder()
            .expire_after(CacheExpiration)
            .weigher(weight)
            .eviction_listener(|_, _, cause| {
                if cause == RemovalCause::Size {
                    counter!(recorded::SHALLOW_EVICT_MEMORY).increment(1);
                }
            });
        if let Some(capacity) = max_capacity {
            builder = builder.max_capacity(capacity);
        }
        Arc::new(builder.build())
    }

    pub fn new(max_capacity: Option<u64>) -> Self {
        Self {
            caches: new_table(),
            names: new_table(),
            query_ids: new_table(),
            inner: Self::new_inner(max_capacity),
            next_id: Default::default(),
        }
    }

    fn check_identifiers(
        name: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> ReadySetResult<()> {
        if name.is_some() || query_id.is_some() {
            Ok(())
        } else {
            internal!("no query id or name for cache");
        }
    }

    fn format_name(name: Option<&Relation>, query_id: Option<&QueryId>) -> String {
        name.map(|n| n.name.to_string())
            .or_else(|| query_id.map(|q| q.to_string()))
            .unwrap()
    }

    fn get_cache_id(&self, name: Option<&Relation>, query_id: Option<&QueryId>) -> Option<u64> {
        if let Some(name) = name {
            let guard = self.names.pin();
            if let Some(id) = guard.get(name) {
                return Some(*id);
            }
        }

        if let Some(query_id) = query_id {
            let guard = self.query_ids.pin();
            if let Some(id) = guard.get(query_id) {
                return Some(*id);
            }
        }

        None
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_cache(
        &self,
        name: Option<Relation>,
        query_id: Option<QueryId>,
        query: ShallowCacheQuery,
        schema_search_path: Vec<SqlIdentifier>,
        policy: EvictionPolicy,
        ddl_req: CacheDDLRequest,
        always: bool,
        coalesce_ms: Option<Duration>,
    ) -> ReadySetResult<()> {
        Self::check_identifiers(name.as_ref(), query_id.as_ref())?;
        let display_name = Self::format_name(name.as_ref(), query_id.as_ref());

        let mut next_id = self
            .next_id
            .lock()
            .expect("couldn't lock next_id to create");
        let id = *next_id;
        *next_id += 1;

        if self
            .get_cache_id(name.as_ref(), query_id.as_ref())
            .is_some()
        {
            return Err(ReadySetError::ViewAlreadyExists(display_name));
        }

        let inner = Arc::clone(&self.inner);
        let cache = Cache::new(
            id,
            inner,
            policy,
            name.clone(),
            query_id,
            query,
            schema_search_path,
            ddl_req,
            always,
            coalesce_ms,
        );

        if let Some(name) = name {
            let guard = self.names.pin();
            guard.insert(name, id);
        }
        if let Some(query_id) = query_id {
            let guard = self.query_ids.pin();
            guard.insert(query_id, id);
        }

        self.caches.pin().insert(id, cache);

        info!("created shallow cache {display_name}");
        Ok(())
    }

    pub fn drop_cache(
        &self,
        name: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> ReadySetResult<()> {
        Self::check_identifiers(name, query_id)?;
        let display_name = Self::format_name(name, query_id);

        let _lock = self.next_id.lock().expect("couldn't lock next_id to drop");

        let Some(id) = self.get_cache_id(name, query_id) else {
            return Err(ReadySetError::ViewNotFound(display_name));
        };

        let guard = self.caches.pin();
        let cache = guard
            .get(&id)
            .ok_or_else(|| ReadySetError::ViewNotFound(display_name.clone()))?;
        cache.stop();

        if let Some(name) = cache.name() {
            let names_guard = self.names.pin();
            names_guard.remove(name);
        }
        if let Some(query_id) = cache.query_id() {
            let queries_guard = self.query_ids.pin();
            queries_guard.remove(query_id);
        }

        guard.remove(&id);

        info!("dropped shallow cache {display_name}");
        Ok(())
    }

    pub fn drop_all_caches(&self) {
        let _lock = self
            .next_id
            .lock()
            .expect("couldn't lock next_id to drop all");

        let caches = self.caches.pin();
        for c in caches.values() {
            c.stop();
        }
        caches.clear();
        self.names.pin().clear();
        self.query_ids.pin().clear();
    }

    /// List the current shallow caches.
    ///
    /// Optionally, query_id and name can be passed to filter the results.  Passing both will
    /// include only results that match at least one.
    pub fn list_caches(
        &self,
        query_id: Option<QueryId>,
        name: Option<&Relation>,
    ) -> Vec<CacheInfo> {
        self.caches
            .pin()
            .values()
            .filter(|cache| {
                (query_id.is_none() && name.is_none())
                    || *cache.query_id() == query_id
                    || cache.name().as_ref() == name
            })
            .map(|cache| cache.get_info())
            .collect()
    }

    /// List all entries across shallow caches.
    ///
    /// Iterates the shared inner cache once (O(N) where N is total entries) instead of
    /// iterating per-cache which would be O(M*N) where M is the number of caches.
    ///
    /// Optionally, `query_id` filters results to a specific cache.
    /// Optionally, `limit` caps the number of results returned.
    pub fn list_entries(
        &self,
        query_id: Option<QueryId>,
        limit: Option<usize>,
    ) -> Vec<CacheEntryInfo> {
        // If filtering by query_id, resolve the cache_id directly via the query_ids map
        let filter_cache_id: Option<u64> = match query_id.as_ref() {
            Some(qid) => {
                let guard = self.query_ids.pin();
                match guard.get(qid) {
                    Some(id) => Some(*id),
                    None => return Vec::new(),
                }
            }
            None => None,
        };

        let caches = self.caches.pin();

        let iter = self.inner.iter().filter_map(|(key, entry)| {
            let cache_id = key.0;

            if let Some(target_id) = filter_cache_id
                && cache_id != target_id
            {
                return None;
            }

            // Look up the query_id from the Cache struct itself
            let cache = caches.get(&cache_id)?;
            let cache_query_id = *cache.query_id();

            match entry.as_ref() {
                CacheEntry::Present(values) => {
                    let entry_id = hash(&key.1);
                    Some(CacheEntryInfo {
                        query_id: cache_query_id,
                        entry_id,
                        last_accessed_ms: values.accessed_ms.load(Ordering::Relaxed),
                        last_refreshed_ms: values.refreshed_ms,
                        refresh_time_ms: values.execution_ms,
                    })
                }
                CacheEntry::Loading(_) => None,
            }
        });

        match limit {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    pub fn get(
        &self,
        name: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> Option<Arc<Cache<K, V>>> {
        let Ok(()) = Self::check_identifiers(name, query_id) else {
            return None;
        };
        let id = self.get_cache_id(name, query_id)?;
        self.caches.pin().get(&id).cloned()
    }

    pub fn exists(&self, relation: Option<&Relation>, query_id: Option<&QueryId>) -> bool {
        self.get(relation, query_id).is_some()
    }

    fn make_guard(cache: Arc<Cache<K, V>>, key: K) -> CacheInsertGuard<K, V> {
        CacheInsertGuard {
            cache,
            key: Some(key),
            results: Some(Vec::new()),
            metadata: None,
            filled: false,
            requested: Instant::now(),
        }
    }

    pub async fn get_or_start_insert<F>(
        &self,
        query_id: &QueryId,
        key: K,
        is_compatible: F,
    ) -> CacheResult<K, V>
    where
        F: Fn(&V) -> bool,
    {
        let Some(cache) = self.get(None, Some(query_id)) else {
            return CacheResult::NotCached;
        };
        let res = cache.get(key.clone()).await;

        match res {
            Some((res, needs_refresh)) if res.values.first().is_none_or(&is_compatible) => {
                cache.increment_hit();
                if needs_refresh && !cache.is_scheduled() {
                    let guard = Self::make_guard(cache, key);
                    CacheResult::HitAndRefresh(res, guard)
                } else {
                    CacheResult::Hit(res)
                }
            }
            Some(_) | None => {
                cache.increment_miss();
                let guard = Self::make_guard(cache, key);
                CacheResult::Miss(guard)
            }
        }
    }

    pub async fn count(&self, query_id: &QueryId) -> Option<usize> {
        Some(self.get(None, Some(query_id))?.count().await)
    }

    pub async fn run_pending_tasks(&self, query_id: &QueryId) {
        let cache = self.get(None, Some(query_id)).unwrap();
        cache.run_pending_tasks().await;
    }
}

pub enum CacheResult<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    NotCached,
    Miss(CacheInsertGuard<K, V>),
    Hit(crate::QueryResult<V>),
    HitAndRefresh(crate::QueryResult<V>, CacheInsertGuard<K, V>),
}

impl<K, V> Debug for CacheResult<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCached => f.debug_struct("NotCached").finish_non_exhaustive(),
            Self::Miss(..) => f.debug_struct("Miss").finish_non_exhaustive(),
            Self::Hit(..) => f.debug_struct("Hit").finish_non_exhaustive(),
            Self::HitAndRefresh(..) => f.debug_struct("HitAndRefresh").finish_non_exhaustive(),
        }
    }
}

impl<K, V> CacheResult<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn is_hit(&self) -> bool {
        matches!(self, Self::Hit(..) | Self::HitAndRefresh(..))
    }

    pub fn result(&self) -> &crate::QueryResult<V> {
        match self {
            Self::Hit(res) | Self::HitAndRefresh(res, _) => res,
            _ => panic!("no result in a non-hit"),
        }
    }

    pub fn guard(&mut self) -> &mut CacheInsertGuard<K, V> {
        match self {
            Self::NotCached | Self::Hit(_) => panic!("no guard"),
            Self::Miss(guard) | Self::HitAndRefresh(_, guard) => guard,
        }
    }
}

pub struct CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) cache: Arc<Cache<K, V>>,
    pub(crate) key: Option<K>,
    pub(crate) results: Option<Vec<V>>,
    pub(crate) metadata: Option<QueryMetadata>,
    pub(crate) filled: bool,
    pub(crate) requested: Instant,
}

impl<K, V> Debug for CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheInsertGuard")
            .field("results", &self.results.as_ref().unwrap().len())
            .field("filled", &self.filled)
            .finish_non_exhaustive()
    }
}

impl<K, V> CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Add a row to the result set.
    pub fn push(&mut self, row: V) {
        self.results.as_mut().unwrap().push(row);
    }

    /// Set the metadata for this result set.
    pub fn set_metadata(&mut self, metadata: QueryMetadata) {
        self.metadata = Some(metadata);
    }

    pub async fn schedule_refresh(&mut self, req: RequestRefresh<K, V>) {
        let Some(key) = self.key.clone() else {
            return;
        };
        self.cache.schedule_refresh(key, req).await;
    }

    pub fn is_scheduled(&self) -> bool {
        self.cache.is_scheduled()
    }

    /// Mark the guard as fully filled and ready to be inserted.
    ///
    /// When the insertion actually happens can be controlled by the caller.  If in async code,
    /// the caller may wait on the returned future, which will cause the result set to be
    /// inserted and become immediately visible.  Otherwise, when the guard is dropped, the
    /// insertion will be scheduled to happen asynchronously.
    pub fn filled(&mut self) -> impl Future<Output = ()> {
        self.filled = true;
        async {
            if self.filled {
                let (metadata, cache, key, results, execution) = self.take();
                cache.insert(key, results, metadata, execution).await;
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn take(&mut self) -> (QueryMetadata, Arc<Cache<K, V>>, K, Vec<V>, Duration) {
        let metadata = self.metadata.take().expect("no metadata for result set");
        let cache = Arc::clone(&self.cache);
        let key = self.key.take().unwrap();
        let results = self.results.take().unwrap();
        self.filled = false;
        (metadata, cache, key, results, self.requested.elapsed())
    }
}

impl<K, V> Drop for CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.filled {
            let (metadata, cache, key, results, execution) = self.take();
            tokio::spawn(async move {
                cache.insert(key, results, metadata, execution).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use readyset_client::consensus::CacheDDLRequest;
    use readyset_client::query::QueryId;
    use readyset_sql::ast::ShallowCacheQuery;

    use super::*;
    use crate::EvictionPolicy;

    fn test_ddl_req() -> CacheDDLRequest {
        CacheDDLRequest {
            unparsed_stmt: "CREATE SHALLOW CACHE test AS SELECT 1".to_string(),
            schema_search_path: vec![],
            dialect: readyset_sql::Dialect::PostgreSQL.into(),
        }
    }

    fn default_policy() -> EvictionPolicy {
        EvictionPolicy::Ttl {
            ttl: Duration::from_secs(60),
        }
    }

    #[tokio::test]
    async fn test_list_entries_empty_manager() {
        let manager: CacheManager<String, String> = CacheManager::new(None);
        let entries = manager.list_entries(None, None);
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_list_entries_returns_all_entries() {
        let manager: CacheManager<Vec<&str>, Vec<&str>> = CacheManager::new(None);

        let query_id_1 = QueryId::from_unparsed_select("SELECT 1");
        let query_id_2 = QueryId::from_unparsed_select("SELECT 2");

        // Create two caches
        manager
            .create_cache(
                None,
                Some(query_id_1),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        manager
            .create_cache(
                None,
                Some(query_id_2),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        // Insert entries into both caches
        let cache_1 = manager.get(None, Some(&query_id_1)).unwrap();
        let cache_2 = manager.get(None, Some(&query_id_2)).unwrap();

        // Mark intent and insert for cache 1
        let _ = cache_1.get(vec!["key1"]).await;
        cache_1
            .insert(
                vec!["key1"],
                vec![vec!["value1"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        // Mark intent and insert for cache 2
        let _ = cache_2.get(vec!["key2"]).await;
        cache_2
            .insert(
                vec!["key2"],
                vec![vec!["value2"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        // List all entries without filter
        let entries = manager.list_entries(None, None);
        assert_eq!(entries.len(), 2);

        // Verify both query_ids are present
        let query_ids: Vec<_> = entries.iter().filter_map(|e| e.query_id).collect();
        assert!(query_ids.contains(&query_id_1));
        assert!(query_ids.contains(&query_id_2));
    }

    #[tokio::test]
    async fn test_list_entries_filters_by_query_id() {
        let manager: CacheManager<Vec<&str>, Vec<&str>> = CacheManager::new(None);

        let query_id_1 = QueryId::from_unparsed_select("SELECT 1");
        let query_id_2 = QueryId::from_unparsed_select("SELECT 2");

        // Create two caches
        manager
            .create_cache(
                None,
                Some(query_id_1),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        manager
            .create_cache(
                None,
                Some(query_id_2),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        // Insert entries into both caches
        let cache_1 = manager.get(None, Some(&query_id_1)).unwrap();
        let cache_2 = manager.get(None, Some(&query_id_2)).unwrap();

        let _ = cache_1.get(vec!["key1"]).await;
        cache_1
            .insert(
                vec!["key1"],
                vec![vec!["value1"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        let _ = cache_2.get(vec!["key2"]).await;
        cache_2
            .insert(
                vec!["key2"],
                vec![vec!["value2"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        // Filter by query_id_1
        let entries = manager.list_entries(Some(query_id_1), None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].query_id, Some(query_id_1));

        // Filter by query_id_2
        let entries = manager.list_entries(Some(query_id_2), None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].query_id, Some(query_id_2));
    }

    #[tokio::test]
    async fn test_list_entries_nonexistent_query_id() {
        let manager: CacheManager<Vec<&str>, Vec<&str>> = CacheManager::new(None);

        let query_id_1 = QueryId::from_unparsed_select("SELECT 1");
        let nonexistent = QueryId::from_unparsed_select("SELECT nonexistent");

        // Create one cache
        manager
            .create_cache(
                None,
                Some(query_id_1),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        let cache = manager.get(None, Some(&query_id_1)).unwrap();
        let _ = cache.get(vec!["key"]).await;
        cache
            .insert(
                vec!["key"],
                vec![vec!["value"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        // Filter by non-existent query_id should return empty
        let entries = manager.list_entries(Some(nonexistent), None);
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_list_entries_respects_limit() {
        let manager: CacheManager<String, String> = CacheManager::new(None);

        let query_id = QueryId::from_unparsed_select("SELECT 1");

        manager
            .create_cache(
                None,
                Some(query_id),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        let cache = manager.get(None, Some(&query_id)).unwrap();

        // Insert multiple entries with different keys
        for i in 0..5 {
            let key = format!("key{}", i);
            let _ = cache.get(key.clone()).await;
            cache
                .insert(
                    key,
                    vec!["value".to_string()],
                    crate::QueryMetadata::Test,
                    Duration::ZERO,
                )
                .await;
        }

        // Without limit, should return all 5
        let entries = manager.list_entries(None, None);
        assert_eq!(entries.len(), 5);

        // With limit 2, should return only 2
        let entries = manager.list_entries(None, Some(2));
        assert_eq!(entries.len(), 2);

        // With limit 0, should return empty
        let entries = manager.list_entries(None, Some(0));
        assert!(entries.is_empty());

        // With limit larger than count, should return all
        let entries = manager.list_entries(None, Some(100));
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn test_list_entries_skips_loading_entries() {
        let manager: CacheManager<Vec<&str>, Vec<&str>> = CacheManager::new(None);

        let query_id = QueryId::from_unparsed_select("SELECT 1");

        manager
            .create_cache(
                None,
                Some(query_id),
                ShallowCacheQuery::default(),
                vec![],
                default_policy(),
                test_ddl_req(),
                false,
                None,
            )
            .unwrap();

        let cache = manager.get(None, Some(&query_id)).unwrap();

        // Create a Loading entry by calling get without inserting
        let _ = cache.get(vec!["loading_key"]).await;

        // The Loading entry should not appear in list_entries
        let entries = manager.list_entries(None, None);
        assert!(entries.is_empty());

        // Now insert a real entry
        let _ = cache.get(vec!["real_key"]).await;
        cache
            .insert(
                vec!["real_key"],
                vec![vec!["value"]],
                crate::QueryMetadata::Test,
                Duration::ZERO,
            )
            .await;

        // Should only see the real entry
        let entries = manager.list_entries(None, None);
        assert_eq!(entries.len(), 1);
    }
}
