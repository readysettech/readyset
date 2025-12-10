use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use metrics::counter;
use moka::future::Cache as MokaCache;
use papaya::HashMap;
use tracing::info;

use readyset_client::consensus::CacheDDLRequest;
use readyset_client::metrics::recorded;
use readyset_client::query::QueryId;
use readyset_errors::{ReadySetError, ReadySetResult, internal};
use readyset_sql::ast::{Relation, SelectStatement, SqlIdentifier};
use readyset_util::SizeOf;

use crate::cache::{Cache, CacheExpiration, CacheInfo, InnerCache};
use crate::{EvictionPolicy, QueryMetadata};

fn weight<K, V>(k: &K, v: &V) -> u32
where
    K: SizeOf,
    V: SizeOf,
{
    (k.deep_size_of() + v.deep_size_of())
        .try_into()
        .unwrap_or(u32::MAX)
}

pub struct CacheManager<K, V> {
    caches: HashMap<u64, Arc<Cache<K, V>>>,
    names: HashMap<Relation, u64>,
    query_ids: HashMap<QueryId, u64>,
    inner: InnerCache<K, V>,
    // This lock also synchronizes inserts into the three HashMaps.
    next_id: Mutex<u64>,
}

impl<K, V> CacheManager<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    pub(crate) fn new_inner(max_capacity: Option<u64>) -> InnerCache<K, V> {
        let mut builder = MokaCache::builder()
            .expire_after(CacheExpiration)
            .weigher(weight);
        if let Some(capacity) = max_capacity {
            builder = builder.max_capacity(capacity);
        }
        Arc::new(builder.build())
    }

    pub fn new(max_capacity: Option<u64>) -> Self {
        Self {
            caches: Default::default(),
            names: Default::default(),
            query_ids: Default::default(),
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
        query: SelectStatement,
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
        let cache = Arc::new(Cache::new(
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
        ));

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

        self.caches.pin().clear();
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
        }
    }

    pub async fn get_or_start_insert(&self, query_id: &QueryId, key: K) -> CacheResult<K, V> {
        let Some(cache) = self.get(None, Some(query_id)) else {
            return CacheResult::NotCached;
        };
        let res = cache.get(key.clone()).await;
        let guard = Self::make_guard(cache, key);
        let query = query_id.to_string();
        match res {
            Some((res, false)) => {
                counter!(recorded::SHALLOW_HIT, "query" => query).increment(1);
                CacheResult::Hit(res, guard)
            }
            Some((res, true)) => {
                counter!(recorded::SHALLOW_HIT, "query" => query.clone()).increment(1);
                counter!(recorded::SHALLOW_REFRESH, "query" => query).increment(1);
                CacheResult::HitAndRefresh(res, guard)
            }
            None => {
                counter!(recorded::SHALLOW_MISS, "query" => query).increment(1);
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
    Hit(crate::QueryResult<V>, CacheInsertGuard<K, V>),
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
            Self::Hit(res, _) | Self::HitAndRefresh(res, _) => res,
            _ => panic!("no result in a non-hit"),
        }
    }

    pub fn guard(&mut self) -> &mut CacheInsertGuard<K, V> {
        match self {
            Self::NotCached => panic!("NotCached has no guard"),
            Self::Miss(guard) | Self::Hit(_, guard) | Self::HitAndRefresh(_, guard) => guard,
        }
    }
}

pub struct CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    cache: Arc<Cache<K, V>>,
    key: Option<K>,
    results: Option<Vec<V>>,
    metadata: Option<QueryMetadata>,
    filled: bool,
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
                let (metadata, cache, key, results) = self.take();
                cache.insert(key, results, metadata).await;
            }
        }
    }

    fn take(&mut self) -> (QueryMetadata, Arc<Cache<K, V>>, K, Vec<V>) {
        let metadata = self.metadata.take().expect("no metadata for result set");
        let cache = Arc::clone(&self.cache);
        let key = self.key.take().unwrap();
        let results = self.results.take().unwrap();
        self.filled = false;
        (metadata, cache, key, results)
    }
}

impl<K, V> Drop for CacheInsertGuard<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.filled {
            let (metadata, cache, key, results) = self.take();
            tokio::spawn(async move {
                cache.insert(key, results, metadata).await;
            });
        }
    }
}
