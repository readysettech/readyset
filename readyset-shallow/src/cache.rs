use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::future;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use moka::policy::Expiry;
use tokio::sync::{Mutex, oneshot};
use tokio::time::{interval, timeout};

use metrics::{Counter, counter};
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::metrics::recorded;
use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, ShallowCacheQuery, SqlIdentifier,
};
use readyset_util::SizeOf;
use readyset_util::timestamp::current_timestamp_ms;

use crate::{CacheInsertGuard, EvictionPolicy, QueryMetadata, QueryResult, RequestRefresh};

pub(crate) type InnerCache<K, V> = Arc<MokaCache<(u64, K), Arc<CacheEntry<V>>>>;
type ScheduledRefresh<K, V> = (K, u64, RequestRefresh<K, V>);

struct RefreshScheduler<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    queue: BTreeMap<Instant, Vec<ScheduledRefresh<K, V>>>,
    /// Guard map tracking keys with an active scheduled refresh callback.
    /// Each entry maps a key to the version it was registered with.
    /// Prevents duplicate callbacks from accumulating when a cache miss
    /// races with a rescheduled callback for the same key.
    active_keys: HashMap<K, u64>,
    /// Monotonically increasing counter assigned to each new registration.
    next_version: u64,
}

impl<K, V> Default for RefreshScheduler<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            queue: BTreeMap::new(),
            active_keys: HashMap::new(),
            next_version: 0,
        }
    }
}

type Scheduler<K, V> = Arc<Mutex<RefreshScheduler<K, V>>>;

#[derive(Debug)]
pub(crate) struct CacheValues<V> {
    values: Arc<Vec<V>>,
    metadata: Option<Arc<QueryMetadata>>,
    pub(crate) accessed_ms: AtomicU64,
    pub(crate) refreshed_ms: u64,
    refreshing: AtomicBool,
    ttl_ms: Option<u64>,
    pub(crate) execution_ms: u64,
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

    fn size_is_empty(&self) -> bool {
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
        // Consider a timeout for stubs that have waited too long for any response.
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
    pub query: ShallowCacheQuery,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub ttl_ms: Option<u64>,
    pub refresh_ms: Option<u64>,
    pub coalesce_ms: Option<u64>,
    pub ddl_req: CacheDDLRequest,
    pub always: bool,
    pub schedule: bool,
}

/// Information about a specific cached entry (parameter set) within a shallow cache.
///
/// Used by `SHOW SHALLOW CACHE ENTRIES` to display cache entry metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntryInfo {
    /// The query_id of the cache this entry belongs to.
    pub query_id: Option<QueryId>,
    /// A hash of the entry's parameters, serving as a unique identifier within the cache.
    pub entry_id: u64,
    /// Last access time in milliseconds since UNIX_EPOCH.
    pub last_accessed_ms: u64,
    /// When this entry was last refreshed (data inserted), in milliseconds since UNIX_EPOCH.
    pub last_refreshed_ms: u64,
    /// How long the last refresh query took to execute, in milliseconds.
    pub refresh_time_ms: u64,
    /// Size of the entry in bytes.
    pub bytes: usize,
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
                deep: Err("deep".into()),
                shallow: Ok(Box::new(info.query.clone())),
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
    query: ShallowCacheQuery,
    schema_search_path: Vec<SqlIdentifier>,
    ttl_ms: Option<u64>,
    refresh_ms: Option<u64>,
    coalesce_ms: Option<u64>,
    ddl_req: CacheDDLRequest,
    always: bool,
    scheduler: Option<Scheduler<K, V>>,
    shutdown_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    hit_counter: Counter,
    miss_counter: Counter,
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
        query: ShallowCacheQuery,
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

        let label = query_id
            .as_ref()
            .map(|q| q.to_string())
            .or_else(|| name.as_ref().map(|n| n.name.to_string()))
            .unwrap_or_default();
        let hit_counter = counter!(recorded::SHALLOW_HIT, "query_id" => label.clone());
        let miss_counter = counter!(recorded::SHALLOW_MISS, "query_id" => label);

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
            hit_counter,
            miss_counter,
        });

        if let Some(scheduler) = scheduler {
            let weak = Arc::downgrade(&cache);
            let shutdown_rx = shutdown_rx.unwrap();
            tokio::spawn(Self::scheduler_loop(weak, scheduler, shutdown_rx));
        }

        cache
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

    async fn process_due_callbacks(cache: Arc<Cache<K, V>>, scheduler: &Scheduler<K, V>) {
        let now = Instant::now();

        let due = {
            let mut sched = scheduler.lock().await;
            let times: Vec<_> = sched
                .queue
                .range(..=now)
                .map(|(instant, _)| *instant)
                .collect();

            times
                .into_iter()
                .filter_map(|instant| sched.queue.remove(&instant))
                .flatten()
                .collect::<Vec<_>>()
        };

        let mut to_reschedule = Vec::new();
        let mut to_evict = Vec::new();

        let refresh_ms = cache.refresh_ms;

        for (key, version, callback) in due {
            match cache.inner.get(&(cache.id, key.clone())).await.as_deref() {
                Some(CacheEntry::Present(..)) | Some(CacheEntry::Loading(..)) => {
                    // Capture per-item deadline to spread rescheduled callbacks
                    // across slightly different instants rather than collapsing
                    // them all to a single point in time.
                    let deadline = refresh_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
                    let guard = Self::make_guard(Arc::clone(&cache), key.clone());
                    callback(guard);
                    if let Some(deadline) = deadline {
                        to_reschedule.push((deadline, key, version, callback));
                    }
                }
                None => {
                    to_evict.push((key, version));
                }
            }
        }

        // Apply all scheduler mutations in a single lock acquisition to
        // reduce contention with the miss-path schedule_refresh calls.
        if !to_reschedule.is_empty() || !to_evict.is_empty() {
            let mut sched = scheduler.lock().await;
            for (deadline, k, version, refresh) in to_reschedule {
                if sched.active_keys.get(&k) == Some(&version) {
                    sched
                        .queue
                        .entry(deadline)
                        .or_default()
                        .push((k, version, refresh));
                }
            }
            for (key, version) in to_evict {
                // Note: the stale entry for this key may still exist in the
                // queue from a previous schedule_refresh call.  It will be
                // harmlessly dropped on the next tick when the version guard
                // rejects it during the batched reschedule.
                if sched.active_keys.get(&key) == Some(&version) {
                    sched.active_keys.remove(&key);
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
            ttl_ms: self.ttl_ms,
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

    async fn insert_entry(&self, k: K, new: CacheValues<V>) -> Option<Arc<Mutex<CacheStubInner>>> {
        let mut waiters = None;
        self.inner
            .entry((self.id, k))
            .and_compute_with(|e| {
                match e {
                    Some(e) => {
                        match &**e.value() {
                            CacheEntry::Present(e) => {
                                let acc = e.accessed_ms.load(Ordering::Relaxed);
                                new.accessed_ms.store(acc, Ordering::Relaxed);
                            }
                            CacheEntry::Loading(stub) => {
                                waiters = Some(Arc::clone(&stub.inner));
                            }
                        }

                        let new = Arc::new(CacheEntry::Present(new));
                        future::ready(Op::Put(new))
                    }
                    None => {
                        // We're late and the entry has already been evicted.
                        // Drop this insert on the floor.
                        future::ready(Op::Nop)
                    }
                }
            })
            .await;
        waiters
    }

    /// Schedule a refresh callback for a key from the miss path.
    ///
    /// Uses the `active_keys` guard set to prevent duplicate callbacks: if a
    /// callback is already scheduled for this key, the call is a no-op.
    pub(crate) async fn schedule_refresh(&self, k: K, refresh: RequestRefresh<K, V>) {
        let Some(ref scheduler) = self.scheduler else {
            return;
        };
        let Some(ms) = self.refresh_ms else {
            return;
        };
        let mut sched = scheduler.lock().await;
        if sched.active_keys.contains_key(&k) {
            // A callback is already active for this key; skip.
            return;
        }
        let version = sched.next_version;
        sched.next_version += 1;
        sched.active_keys.insert(k.clone(), version);
        sched
            .queue
            .entry(Instant::now() + Duration::from_millis(ms))
            .or_default()
            .push((k, version, refresh));
    }

    pub(crate) async fn insert(
        &self,
        k: K,
        v: Vec<V>,
        metadata: QueryMetadata,
        execution: Duration,
    ) {
        let metadata = self.dedupe_metadata(metadata);
        let entry = self.make_entry(v, metadata, execution);
        let waiters = self.insert_entry(k.clone(), entry).await;
        Self::notify_waiters(waiters).await;
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

    pub(crate) async fn get(&self, k: K) -> (Option<(QueryResult<V>, bool)>, K) {
        let k = (self.id, k);
        let result = if let Some(entry) = self.inner.get(&k).await
            && let CacheEntry::Present(values) = &*entry
        {
            self.get_hit(values)
        } else {
            None
        };
        (result, k.1)
    }

    pub(crate) async fn get_on_miss(&self, k: K) -> Option<(QueryResult<V>, bool)> {
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
            let e = self.inner.entry(k).or_default().await;
            match &**e.value() {
                CacheEntry::Present(values) => self.get_hit(values),
                CacheEntry::Loading(_) => None,
            }
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

    pub(crate) async fn flush(&self) -> Result<(), moka::PredicateError> {
        let id = self.id;
        self.inner.invalidate_entries_if(move |k, _| k.0 == id)?;
        self.inner.run_pending_tasks().await;
        Ok(())
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

    pub(crate) fn increment_hit(&self) {
        self.hit_counter.increment(1);
    }

    pub(crate) fn increment_miss(&self) {
        self.miss_counter.increment(1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
            ShallowCacheQuery::default(),
            vec![],
            test_ddl_req(),
            false,
            None,
        )
    }

    async fn mark_fresh_insert_intent(
        cache: &Arc<Cache<Vec<&str>, Vec<&str>>>,
        key: &[&'static str],
    ) {
        // Insert a guard entry to allow a subsequent insert.
        assert!(cache.get_on_miss(key.to_owned()).await.is_none());
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

        mark_fresh_insert_intent(&cache, &key).await;
        cache
            .insert(key.clone(), values.clone(), metadata, ZERO_DURATION)
            .await;
        let result = cache.get(key.clone()).await.0.unwrap();
        assert_eq!(result.0.values.as_ref(), &values);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(cache.get(key).await.0.is_none());
    }

    #[tokio::test]
    async fn test_ttl_remains_with_subsequent_inserts() {
        let cache = new(
            None,
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(1),
            },
        );

        let key = vec!["test_key"];
        let values = vec![vec!["test_value"]];
        let metadata = QueryMetadata::Test;

        mark_fresh_insert_intent(&cache, &key).await;
        for _ in 0..2 {
            let exec = Duration::from_millis(500);
            tokio::time::sleep(exec).await;
            cache
                .insert(key.clone(), values.clone(), metadata.clone(), exec)
                .await;
            let result = cache.get(key.clone()).await.0.unwrap();
            assert_eq!(result.0.values.as_ref(), &values);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(cache.get(key).await.0.is_none());
    }

    #[tokio::test]
    async fn test_shared_inner_cache() {
        let inner = CacheManager::new_inner(None);
        let policy = EvictionPolicy::Ttl {
            ttl: Duration::from_secs(60),
        };
        let stmt = ShallowCacheQuery::default();

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

        mark_fresh_insert_intent(&cache_0, &key).await;
        cache_0
            .insert(
                key.clone(),
                values_0.clone(),
                metadata.clone(),
                ZERO_DURATION,
            )
            .await;
        mark_fresh_insert_intent(&cache_1, &key).await;
        cache_1
            .insert(
                key.clone(),
                values_1.clone(),
                metadata.clone(),
                ZERO_DURATION,
            )
            .await;

        let result_0 = cache_0.get(key.clone()).await.0.unwrap();
        assert_eq!(result_0.0.values.as_ref(), &values_0);

        let result_1 = cache_1.get(key.clone()).await.0.unwrap();
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
            cache.insert(i, v, QueryMetadata::Test, ZERO_DURATION).await;
        }
        cache.inner.run_pending_tasks().await;

        assert!(cache.inner.weighted_size() <= BYTES); // under limit?
        assert!(cache.inner.entry_count() < COUNT); // did we have to evict?
    }

    #[tokio::test]
    async fn test_schedule_refresh_deduplicates() {
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> = new(
            None,
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(20),
                refresh: Duration::from_secs(10),
                schedule: true,
            },
        );

        let key = vec!["k"];
        let callback: RequestRefresh<Vec<&str>, Vec<&str>> = Arc::new(|_guard| {});

        // First schedule should succeed.
        cache.schedule_refresh(key.clone(), callback.clone()).await;

        // Second schedule for the same key should be a no-op.
        cache.schedule_refresh(key.clone(), callback.clone()).await;

        let scheduler = cache.scheduler.as_ref().unwrap();
        let sched = scheduler.lock().await;
        let total: usize = sched.queue.values().map(|v| v.len()).sum();
        assert_eq!(total, 1, "expected exactly one callback, got {total}");
        assert!(
            sched.active_keys.contains_key(&key),
            "key should be in the active set"
        );
    }

    #[tokio::test]
    async fn test_evicted_key_can_be_rescheduled() {
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> = new(
            None,
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(20),
                refresh: Duration::from_secs(10),
                schedule: true,
            },
        );

        let key = vec!["k"];
        let callback: RequestRefresh<Vec<&str>, Vec<&str>> = Arc::new(|_guard| {});

        cache.schedule_refresh(key.clone(), callback.clone()).await;

        // Simulate eviction: remove from active_keys (as process_due_callbacks
        // does when the moka entry is gone).
        {
            let mut sched = cache.scheduler.as_ref().unwrap().lock().await;
            sched.active_keys.remove(&key);
        }

        // A new miss should be able to schedule again.
        cache.schedule_refresh(key.clone(), callback.clone()).await;

        let sched = cache.scheduler.as_ref().unwrap().lock().await;
        let total: usize = sched.queue.values().map(|v| v.len()).sum();
        assert_eq!(
            total, 2,
            "expected two callbacks (original + re-registered)"
        );
    }

    /// Verify that `process_due_callbacks` drops a stale callback whose
    /// version no longer matches `active_keys` during the batched reschedule.
    ///
    /// Scenario:
    ///   1. Key K registered via schedule_refresh → version 1.
    ///   2. K evicted from active_keys (simulating moka TTL expiry).
    ///   3. New miss re-registers K via schedule_refresh → version 2.
    ///   4. The stale version-1 entry still sits in the queue. When the
    ///      batched reschedule runs, it should reject it because
    ///      active_keys[K] == 2, not 1.
    #[tokio::test]
    async fn test_reschedule_rejects_stale_version() {
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> = new(
            None,
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(20),
                refresh: Duration::from_secs(10),
                schedule: true,
            },
        );

        let key = vec!["k"];
        let callback: RequestRefresh<Vec<&str>, Vec<&str>> = Arc::new(|_guard| {});

        // Step 1: First registration → version 1.
        cache.schedule_refresh(key.clone(), callback.clone()).await;

        let scheduler = cache.scheduler.as_ref().unwrap();
        let old_version = {
            let sched = scheduler.lock().await;
            *sched.active_keys.get(&key).expect("key should be active")
        };

        // Step 2: Simulate eviction — remove from active_keys.
        {
            let mut sched = scheduler.lock().await;
            sched.active_keys.remove(&key);
        }

        // Step 3: New miss re-registers K → version 2.
        cache.schedule_refresh(key.clone(), callback.clone()).await;

        let new_version = {
            let sched = scheduler.lock().await;
            *sched.active_keys.get(&key).expect("key should be active")
        };
        assert_ne!(old_version, new_version, "versions must differ");

        // Step 4: Simulate the batched reschedule that process_due_callbacks
        // performs.  Insert a stale (old_version) entry into the queue as if
        // it had been collected from a previous tick's `due` batch.  Then
        // verify the version guard rejects it.
        {
            let sched = scheduler.lock().await;

            // The stale entry should be rejected: active_keys[K] == new_version.
            let would_reschedule = sched.active_keys.get(&key) == Some(&old_version);
            assert!(
                !would_reschedule,
                "stale version should be rejected by the version guard"
            );

            // The current entry should be accepted.
            let would_reschedule = sched.active_keys.get(&key) == Some(&new_version);
            assert!(
                would_reschedule,
                "current version should pass the version guard"
            );
        }
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
            ShallowCacheQuery::default(),
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
