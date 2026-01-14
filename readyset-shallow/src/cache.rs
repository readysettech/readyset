use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::future;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use moka::policy::Expiry;
use tokio::sync::{Mutex, oneshot};
use tokio::time::{interval, timeout};

use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, SelectStatement, SqlIdentifier,
};
use readyset_util::SizeOf;

use crate::{CacheInsertGuard, EvictionPolicy, QueryMetadata, QueryResult, RequestRefresh};

pub(crate) type InnerCache<K, V> = Arc<MokaCache<(u64, K), Arc<CacheEntry<V>>>>;
type ScheduledRefresh<K, V> = (K, RequestRefresh<K, V>);
type Scheduler<K, V> = Arc<Mutex<BTreeMap<Instant, Vec<ScheduledRefresh<K, V>>>>>;

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug)]
pub(crate) struct CacheValues<V> {
    values: Arc<Vec<V>>,
    metadata: Option<Arc<QueryMetadata>>,
    accessed_ms: AtomicU64,
    refreshed_ms: u64,
    refreshing: AtomicBool,
    ttl_ms: Option<u64>,
    execution_ms: u64,
}

#[derive(Debug, Default)]
pub(crate) struct CacheStubInner {
    loaded: bool,
    waiters: Vec<oneshot::Sender<()>>,
}

#[derive(Debug)]
pub(crate) struct CacheStub {
    inserted: Instant,
    inner: Arc<Mutex<CacheStubInner>>,
}

impl Default for CacheStub {
    fn default() -> Self {
        Self {
            inserted: Instant::now(),
            inner: Default::default(),
        }
    }
}

impl CacheStub {
    async fn coalesce(&self, coalesce_ms: u64) {
        let wait = Duration::from_millis(coalesce_ms).saturating_sub(self.inserted.elapsed());
        if wait.is_zero() {
            return; // stub is stale, maybe previous load failed, return a miss
        }

        let mut inner = self.inner.lock().await;
        if inner.loaded {
            return;
        }
        let (tx, rx) = oneshot::channel();
        inner.waiters.push(tx);
        drop(inner);
        let _ = timeout(wait, rx).await;
    }
}

#[derive(Debug)]
pub(crate) enum CacheEntry<V> {
    Present(CacheValues<V>),
    Loading(CacheStub),
}

impl<V> Default for CacheEntry<V> {
    fn default() -> Self {
        Self::Loading(Default::default())
    }
}

impl<V> SizeOf for CacheEntry<V>
where
    V: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        match self {
            CacheEntry::Present(values) => {
                let mut sz = size_of::<Self>() + values.values.deep_size_of();
                if let Some(ref meta) = values.metadata {
                    sz += meta.deep_size_of();
                }
                sz
            }
            CacheEntry::Loading(_) => 0,
        }
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
        match value.as_ref() {
            CacheEntry::Present(values) => values.ttl_ms.map(Duration::from_millis),
            CacheEntry::Loading(_) => None,
        }
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &Arc<CacheEntry<V>>,
        _updated: Instant,
        _left: Option<Duration>,
    ) -> Option<Duration> {
        match value.as_ref() {
            CacheEntry::Present(values) => values.ttl_ms.map(|ttl| {
                Duration::from_millis(
                    ttl.saturating_sub(
                        current_timestamp_ms()
                            .saturating_sub(values.accessed_ms.load(Ordering::Relaxed)),
                    ),
                )
            }),
            CacheEntry::Loading(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct CacheInfo {
    pub name: Option<Relation>,
    pub query_id: Option<QueryId>,
    pub query: SelectStatement,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub ttl_ms: Option<u64>,
    pub refresh_ms: Option<u64>,
    pub coalesce_ms: Option<u64>,
    pub ddl_req: CacheDDLRequest,
    pub always: bool,
    pub schedule: bool,
}

impl From<CacheInfo> for CreateCacheStatement {
    fn from(info: CacheInfo) -> Self {
        let policy = match (info.ttl_ms, info.refresh_ms) {
            (Some(ttl_ms), Some(refresh_ms)) if refresh_ms != ttl_ms / 2 => {
                Some(readyset_sql::ast::EvictionPolicy::TtlAndPeriod {
                    ttl: Duration::from_millis(ttl_ms),
                    refresh: Duration::from_millis(refresh_ms),
                    schedule: info.schedule,
                })
            }
            (Some(ttl_ms), _) => Some(readyset_sql::ast::EvictionPolicy::from_ttl_ms(ttl_ms)),
            _ => None,
        };

        Self {
            name: info.name,
            cache_type: Some(CacheType::Shallow),
            policy,
            coalesce_ms: info.coalesce_ms.map(Duration::from_millis),
            inner: CacheInner::Statement {
                deep: Ok(Box::new(info.query)),
                shallow: Err("shallow".to_string()),
            },
            unparsed_create_cache_statement: None,
            always: info.always,
            concurrently: false,
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct Cache<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    id: u64,
    inner: InnerCache<K, V>,
    cache_metadata: OnceLock<Arc<QueryMetadata>>,
    name: Option<Relation>,
    query_id: Option<QueryId>,
    query: SelectStatement,
    schema_search_path: Vec<SqlIdentifier>,
    ttl_ms: Option<u64>,
    refresh_ms: Option<u64>,
    coalesce_ms: Option<u64>,
    ddl_req: CacheDDLRequest,
    always: bool,
    scheduler: Option<Scheduler<K, V>>,
    shutdown_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl<K, V> Debug for Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("name", &self.name)
            .field("query_id", &self.query_id)
            .field("ttl_ms", &self.ttl_ms)
            .field("refresh_ms", &self.refresh_ms)
            .finish_non_exhaustive()
    }
}

impl<K, V> Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
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
        coalesce_ms: Option<Duration>,
    ) -> Arc<Self> {
        let (ttl_ms, refresh_ms, schedule) = match policy {
            EvictionPolicy::Ttl { ttl } => {
                let ttl_ms = ttl.as_millis().try_into().unwrap_or(u64::MAX);
                (Some(ttl_ms), Some(ttl_ms / 2), false)
            }
            EvictionPolicy::TtlAndPeriod {
                ttl,
                refresh,
                schedule,
            } => {
                let ttl_ms = ttl.as_millis().try_into().unwrap_or(u64::MAX);
                let refresh_ms = refresh.as_millis().try_into().unwrap_or(u64::MAX);
                (Some(ttl_ms), Some(refresh_ms), schedule)
            }
        };

        let (scheduler, shutdown_tx, shutdown_rx) = if schedule {
            let (tx, rx) = oneshot::channel();
            (Some(Default::default()), Some(tx), Some(rx))
        } else {
            (None, None, None)
        };

        let cache = Arc::new(Self {
            id,
            inner,
            cache_metadata: Default::default(),
            name,
            query_id,
            query,
            schema_search_path,
            ttl_ms,
            refresh_ms,
            coalesce_ms: coalesce_ms.map(|d| d.as_millis().try_into().unwrap_or_default()),
            ddl_req,
            always,
            scheduler: scheduler.clone(),
            shutdown_tx: std::sync::Mutex::new(shutdown_tx),
        });

        if let Some(scheduler) = scheduler {
            let weak = Arc::downgrade(&cache);
            let shutdown_rx = shutdown_rx.unwrap();
            tokio::spawn(Self::scheduler_loop(weak, scheduler, shutdown_rx));
        }

        cache
    }

    fn make_guard(
        cache: Arc<Cache<K, V>>,
        key: K,
        refresh: RequestRefresh<K, V>,
    ) -> CacheInsertGuard<K, V> {
        CacheInsertGuard {
            cache,
            key: Some(key),
            results: Some(Vec::new()),
            metadata: None,
            filled: false,
            requested: Instant::now(),
            refresh: Some(refresh),
        }
    }

    async fn process_due_callbacks(cache: Arc<Cache<K, V>>, scheduler: &Scheduler<K, V>) {
        let now = Instant::now();

        let due = {
            let mut sched = scheduler.lock().await;
            let times: Vec<_> = sched.range(..=now).map(|(instant, _)| *instant).collect();

            times
                .into_iter()
                .filter_map(|instant| sched.remove(&instant))
                .flatten()
                .collect::<Vec<_>>()
        };

        for (key, callback) in due {
            if let Some(e) = cache.inner.get(&(cache.id, key.clone())).await
                && let CacheEntry::Present(ref e) = *e
                && let Some(ttl_ms) = e.ttl_ms
            {
                let now = current_timestamp_ms();
                if e.accessed_ms.load(Ordering::Relaxed) > now - ttl_ms {
                    cache
                        .schedule_refresh(key.clone(), Arc::clone(&callback), scheduler)
                        .await;

                    let guard = Self::make_guard(Arc::clone(&cache), key, Arc::clone(&callback));
                    callback(guard);
                }
            }
        }
    }

    async fn scheduler_loop(
        cache: Weak<Cache<K, V>>,
        scheduler: Scheduler<K, V>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let mut ticker = interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }

            ticker.tick().await;

            let Some(cache) = cache.upgrade() else {
                break;
            };

            Self::process_due_callbacks(cache, &scheduler).await;
        }
    }

    fn dedupe_metadata(&self, metadata: QueryMetadata) -> Option<Arc<QueryMetadata>> {
        if let Some(existing) = self.cache_metadata.get() {
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
        }
    }

    fn make_entry(
        &self,
        v: Vec<V>,
        metadata: Option<Arc<QueryMetadata>>,
        execution: Duration,
    ) -> CacheValues<V> {
        let now = current_timestamp_ms();
        let execution_ms = execution.as_millis().try_into().unwrap_or_default();
        CacheValues {
            values: Arc::new(v),
            metadata,
            accessed_ms: now.into(),
            refreshed_ms: now,
            refreshing: false.into(),
            ttl_ms: self.ttl_ms.map(|ttl| ttl.saturating_sub(execution_ms)),
            execution_ms,
        }
    }

    async fn notify_waiters(waiters: Option<Arc<Mutex<CacheStubInner>>>) {
        let Some(waiters) = waiters else {
            return;
        };
        let mut inner = waiters.lock().await;
        inner.loaded = true;
        for tx in inner.waiters.drain(..) {
            let _ = tx.send(());
        }
    }

    async fn insert_entry(
        &self,
        k: K,
        new: CacheValues<V>,
    ) -> (bool, Option<Arc<Mutex<CacheStubInner>>>) {
        let mut waiters = None;
        let mut was_present = false;
        self.inner
            .entry((self.id, k))
            .and_compute_with(|e| {
                if let Some(e) = e {
                    match &**e.value() {
                        CacheEntry::Present(e) => {
                            was_present = true;
                            let acc = e.accessed_ms.load(Ordering::Relaxed);
                            new.accessed_ms.store(acc, Ordering::Relaxed);
                        }
                        CacheEntry::Loading(stub) => {
                            waiters = Some(Arc::clone(&stub.inner));
                        }
                    }
                }
                let new = Arc::new(CacheEntry::Present(new));
                future::ready(Op::Put(new))
            })
            .await;
        (was_present, waiters)
    }

    async fn schedule_refresh(
        &self,
        k: K,
        refresh: RequestRefresh<K, V>,
        scheduler: &Scheduler<K, V>,
    ) {
        let Some(ms) = self.refresh_ms else {
            return;
        };
        let mut sched = scheduler.lock().await;
        sched
            .entry(Instant::now() + Duration::from_millis(ms))
            .or_insert_with(Vec::new)
            .push((k, refresh));
    }

    pub(crate) async fn insert(
        &self,
        k: K,
        v: Vec<V>,
        metadata: QueryMetadata,
        execution: Duration,
        refresh: Option<RequestRefresh<K, V>>,
    ) {
        let metadata = self.dedupe_metadata(metadata);
        let entry = self.make_entry(v, metadata, execution);
        let (was_present, waiters) = self.insert_entry(k.clone(), entry).await;
        Self::notify_waiters(waiters).await;

        if let Some(refresh) = refresh
            && let Some(ref sched) = self.scheduler
            && !was_present
        {
            // for scheduled refresh, schedule the first refresh on first insertion
            self.schedule_refresh(k, refresh, sched).await;
        }
    }

    fn get_hit(&self, values: &CacheValues<V>) -> Option<(QueryResult<V>, bool)> {
        let now = current_timestamp_ms();
        values.accessed_ms.store(now, Ordering::Relaxed);
        let refresh = if let Some(refresh_ms) = self.refresh_ms
            && now.saturating_sub(values.refreshed_ms + values.execution_ms) >= refresh_ms
            && values
                .refreshing
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            true
        } else {
            false
        };

        let metadata = values
            .metadata
            .as_ref()
            .or_else(|| self.cache_metadata.get())
            .expect("No metadata available for cached result");

        Some((
            QueryResult {
                values: Arc::clone(&values.values),
                metadata: Arc::clone(metadata),
            },
            refresh,
        ))
    }

    pub(crate) async fn get(&self, k: K) -> Option<(QueryResult<V>, bool)> {
        const MAX_RETRIES: usize = 1;

        let k = (self.id, k);
        if let Some(coalesce_ms) = self.coalesce_ms {
            for i in 0..=MAX_RETRIES {
                let e = self.inner.entry(k.clone()).or_default().await;
                match &**e.value() {
                    CacheEntry::Loading(_) if i == MAX_RETRIES => break, // bad luck?
                    CacheEntry::Loading(_) if e.is_fresh() => break,     // first miss
                    CacheEntry::Loading(stub) => stub.coalesce(coalesce_ms).await,
                    CacheEntry::Present(values) => return self.get_hit(values),
                }
            }
            None
        } else {
            self.inner.get(&k).await.map(|entry| match entry.as_ref() {
                CacheEntry::Present(values) => self.get_hit(values),
                CacheEntry::Loading(_) => None, // unreachable, but ignore and return miss
            })?
        }
    }

    pub fn get_info(&self) -> CacheInfo {
        CacheInfo {
            name: self.name.clone(),
            query_id: self.query_id,
            query: self.query.clone(),
            schema_search_path: self.schema_search_path.clone(),
            ttl_ms: self.ttl_ms,
            refresh_ms: self.refresh_ms,
            coalesce_ms: self.coalesce_ms,
            ddl_req: self.ddl_req.clone(),
            always: self.always,
            schedule: self.is_scheduled(),
        }
    }

    pub fn name(&self) -> &Option<Relation> {
        &self.name
    }

    pub(crate) fn query_id(&self) -> &Option<QueryId> {
        &self.query_id
    }

    pub(crate) fn is_scheduled(&self) -> bool {
        self.scheduler.is_some()
    }

    pub(crate) async fn count(&self) -> usize {
        self.inner.run_pending_tasks().await;
        self.inner.entry_count().try_into().unwrap_or(usize::MAX)
    }

    pub(crate) async fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks().await;
    }

    pub(crate) fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use readyset_sql::ast::SelectStatement;

    use crate::{CacheManager, EvictionPolicy, QueryMetadata};

    use super::*;

    const ID: u64 = 0;
    const ZERO_DURATION: Duration = Duration::from_secs(0);

    fn test_ddl_req() -> CacheDDLRequest {
        CacheDDLRequest {
            unparsed_stmt: "CREATE SHALLOW CACHE test AS SELECT 1".to_string(),
            schema_search_path: vec![],
            dialect: readyset_sql::Dialect::PostgreSQL.into(),
        }
    }

    fn new<K, V>(max_capacity: Option<u64>, policy: EvictionPolicy) -> Arc<Cache<K, V>>
    where
        K: Clone + Eq + Hash + Send + Sync + SizeOf + 'static,
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
            None,
        )
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache = new(
            None,
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(1),
            },
        );

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::Test;

        cache
            .insert(key.clone(), values.clone(), metadata, ZERO_DURATION, None)
            .await;
        let result = cache.get(key.clone()).await.unwrap();
        assert_eq!(result.0.values.as_ref(), &values);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.get(key).await.is_none());
    }

    #[tokio::test]
    async fn test_ttl_update() {
        let cache = new(
            None,
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(1),
            },
        );

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::Test;

        for _ in 0..4 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            cache
                .insert(
                    key.clone(),
                    values.clone(),
                    metadata.clone(),
                    ZERO_DURATION,
                    None,
                )
                .await;
            let result = cache.get(key.clone()).await.unwrap();
            assert_eq!(result.0.values.as_ref(), &values);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(cache.get(key).await.is_none());
    }

    #[tokio::test]
    async fn test_shared_inner_cache() {
        let inner = CacheManager::new_inner(None);
        let policy = EvictionPolicy::Ttl {
            ttl: Duration::from_secs(60),
        };
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
            None,
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
            None,
        );

        let key = vec!["shared_key"];
        let values_0 = vec![vec!["value_from_cache_0"]];
        let values_1 = vec![vec!["value_from_cache_1"]];
        let metadata = QueryMetadata::Test;

        cache_0
            .insert(
                key.clone(),
                values_0.clone(),
                metadata.clone(),
                ZERO_DURATION,
                None,
            )
            .await;
        cache_1
            .insert(
                key.clone(),
                values_1.clone(),
                metadata.clone(),
                ZERO_DURATION,
                None,
            )
            .await;

        let result_0 = cache_0.get(key.clone()).await.unwrap();
        assert_eq!(result_0.0.values.as_ref(), &values_0);

        let result_1 = cache_1.get(key.clone()).await.unwrap();
        assert_eq!(result_1.0.values.as_ref(), &values_1);
    }

    #[test]
    fn test_size_of() {
        let now = current_timestamp_ms();
        let entry = CacheEntry::Present(CacheValues {
            values: Arc::new(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            metadata: Some(Arc::new(QueryMetadata::Test)),
            accessed_ms: now.into(),
            refreshed_ms: now,
            refreshing: false.into(),
            ttl_ms: None,
            execution_ms: 0,
        });
        assert!(entry.deep_size_of() > 10 * 0u64.deep_size_of());
    }

    #[tokio::test]
    async fn test_max_capacity() {
        const BYTES: u64 = 1000;
        const COUNT: u64 = 100;

        let cache = new(
            Some(BYTES),
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(1),
            },
        );
        for i in 0..COUNT {
            let v = vec!["xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()];
            cache
                .insert(i, v, QueryMetadata::Test, ZERO_DURATION, None)
                .await;
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
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(60),
            },
            Some(relation.clone()),
            Some(query_id),
            SelectStatement::default(),
            vec![],
            test_ddl_req(),
            false,
            None,
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
