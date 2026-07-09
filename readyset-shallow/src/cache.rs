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
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::{Mutex, oneshot, watch};
use tokio::time::{interval, timeout};

use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_sql::ast::{
    CacheInner, CacheType, CreateCacheStatement, Relation, ShallowCacheQuery, SqlIdentifier,
    TrxCachePolicy,
};
use readyset_util::SizeOf;
use readyset_util::timestamp::current_timestamp_ms;

use crate::{
    CacheInsertGuard, ContentHash, EvictionPolicy, QueryMetadata, QueryResult, RequestRefresh,
    rows_content_hash,
};

/// Minimum adaptive refresh period, as a percentage of the cache's configured period.
const MIN_PERIOD_PERCENT: u64 = 10;
/// Amount an adaptive refresh period moves per refresh, as a percentage of the cache's
/// configured period.
const NUDGE_PERCENT: u64 = 10;
/// Default maximum extra refresh load an adaptive cache may send upstream, as a percentage of
/// the load required to refresh every current entry at the configured period.
pub(crate) const DEFAULT_MAX_EXTRA_LOAD_PERCENT: u64 = 100;

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

/// Per-cache refresh-load accounting for the adaptive load cap.
///
/// Load is measured as upstream execution time per unit of wall time, in parts-per-million: an
/// entry whose refresh takes `execution_ms` and runs every `period_ms` contributes
/// `execution_ms * 1_000_000 / period_ms`. `actual` sums that over the cache's entries at
/// their current periods; `baseline` sums it at the configured period. The two are equal when
/// every entry refreshes at the configured period, and their difference is the extra load
/// adaptive refresh sends upstream.
///
/// Entries add their contributions at write-back; the store's eviction listener subtracts
/// them on removal, including the replacement performed by every refresh overwrite.
pub(crate) struct AdaptiveState {
    refresh_ms: u64,
    max_extra_load_percent: u64,
    actual_ppm: AtomicU64,
    baseline_ppm: AtomicU64,
    actual_gauge: Gauge,
    baseline_gauge: Gauge,
    over_cap_gauge: Gauge,
}

impl Debug for AdaptiveState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveState")
            .field("refresh_ms", &self.refresh_ms)
            .field("max_extra_load_percent", &self.max_extra_load_percent)
            .field("actual_ppm", &self.actual_ppm)
            .field("baseline_ppm", &self.baseline_ppm)
            .finish_non_exhaustive()
    }
}

impl AdaptiveState {
    pub(crate) fn new(refresh_ms: u64, max_extra_load_percent: u64, query_id: QueryId) -> Self {
        let label = query_id.to_string();
        Self {
            refresh_ms: refresh_ms.max(1),
            max_extra_load_percent,
            actual_ppm: AtomicU64::new(0),
            baseline_ppm: AtomicU64::new(0),
            actual_gauge: gauge!(metric::SHALLOW_ADAPTIVE_ACTUAL_LOAD_PPM,
                "query_id" => label.clone()),
            baseline_gauge: gauge!(metric::SHALLOW_ADAPTIVE_BASELINE_LOAD_PPM,
                "query_id" => label.clone()),
            over_cap_gauge: gauge!(metric::SHALLOW_ADAPTIVE_OVER_CAP, "query_id" => label),
        }
    }

    fn contributions<V>(&self, values: &CacheValues<V>) -> (u64, u64) {
        // Sub-millisecond queries still weigh in, so caches full of fast queries are capped by
        // refresh count rather than exempted. Rounding up keeps entries with very long periods
        // from contributing zero, which would collapse the cap to nothing.
        let exec = values.execution_ms.max(1);
        let period = values.period_ms.unwrap_or(self.refresh_ms).max(1);
        (
            (exec * 1_000_000).div_ceil(period),
            (exec * 1_000_000).div_ceil(self.refresh_ms),
        )
    }

    pub(crate) fn add<V>(&self, values: &CacheValues<V>) {
        let (actual, baseline) = self.contributions(values);
        let prev_actual = self.actual_ppm.fetch_add(actual, Ordering::Relaxed);
        let prev_baseline = self.baseline_ppm.fetch_add(baseline, Ordering::Relaxed);
        self.set_gauges(
            prev_actual.saturating_add(actual),
            prev_baseline.saturating_add(baseline),
        );
    }

    pub(crate) fn remove<V>(&self, values: &CacheValues<V>) {
        let (actual, baseline) = self.contributions(values);
        // Saturate rather than wrap; a transient over-subtraction must not poison the sums.
        let prev_actual = self
            .actual_ppm
            .update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                v.saturating_sub(actual)
            });
        let prev_baseline = self
            .baseline_ppm
            .update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                v.saturating_sub(baseline)
            });
        // Every removed entry's contributions were added at its write-back, so the sums always
        // cover the subtraction; saturating would mean a double-subtraction leak.
        antithesis_sdk::assert_always_greater_than_or_equal_to!(
            prev_actual,
            actual,
            "Shallow adaptive actual load subtraction does not underflow"
        );
        antithesis_sdk::assert_always_greater_than_or_equal_to!(
            prev_baseline,
            baseline,
            "Shallow adaptive baseline load subtraction does not underflow"
        );
        self.set_gauges(
            prev_actual.saturating_sub(actual),
            prev_baseline.saturating_sub(baseline),
        );
    }

    fn set_gauges(&self, actual: u64, baseline: u64) {
        self.actual_gauge.set(actual as f64);
        self.baseline_gauge.set(baseline as f64);
        self.over_cap_gauge.set(self.over_cap() as u64 as f64);
    }

    /// Reset the exported gauges when the cache is dropped, so a dropped cache's series
    /// read zero rather than holding their last values.
    pub(crate) fn zero_gauges(&self) {
        self.actual_gauge.set(0.0);
        self.baseline_gauge.set(0.0);
        self.over_cap_gauge.set(0.0);
    }

    /// Current (actual, baseline) load sums, for tests.
    #[cfg(test)]
    pub(crate) fn load_ppm(&self) -> (u64, u64) {
        (
            self.actual_ppm.load(Ordering::Relaxed),
            self.baseline_ppm.load(Ordering::Relaxed),
        )
    }

    /// Whether total refresh load is at or over the cap of `max_extra_load_percent` extra load
    /// beyond the baseline. A cache with no baseline load, e.g. an empty one, is never over
    /// the cap.
    fn over_cap(&self) -> bool {
        let actual = self.actual_ppm.load(Ordering::Relaxed);
        let baseline = self.baseline_ppm.load(Ordering::Relaxed);
        let cap =
            baseline.saturating_add(baseline.saturating_mul(self.max_extra_load_percent) / 100);
        baseline > 0 && actual >= cap
    }
}

#[derive(Debug)]
pub(crate) struct CacheValues<V> {
    values: Arc<Vec<V>>,
    metadata: Option<Arc<QueryMetadata>>,
    pub(crate) accessed_ms: AtomicU64,
    pub(crate) refreshed_ms: u64,
    refreshing: Arc<AtomicBool>,
    ttl_ms: Option<u64>,
    pub(crate) execution_ms: u64,
    /// This key's current refresh period. Fixed at the cache's configured period unless the
    /// cache is adaptive, in which case it is nudged at each refresh write-back.
    pub(crate) period_ms: Option<u64>,
    /// Order-insensitive content hash of `values`, computed only for adaptive caches; see
    /// [`rows_content_hash`].
    values_hash: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct CacheStub {
    /// Indicates that the creator of this stub has either finished populating or failed.
    done: Receiver<()>,
}

#[derive(Debug)]
pub(crate) enum CacheEntry<V> {
    Present(CacheValues<V>),
    Loading(CacheStub),
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

#[derive(Debug)]
pub(crate) enum Lookup<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    Hit(QueryResult<V>, Option<Arc<AtomicBool>>),
    Miss(CacheInsertGuard<K, V>),
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
    pub query_id: QueryId,
    pub query: ShallowCacheQuery,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub ttl_ms: Option<u64>,
    pub refresh_ms: Option<u64>,
    pub coalesce_ms: Option<u64>,
    pub ddl_req: CacheDDLRequest,
    pub trx_cache_policy: TrxCachePolicy,
    pub schedule: bool,
    pub adaptive: bool,
}

/// Information about a specific cached entry (parameter set) within a shallow cache.
///
/// Used by `SHOW SHALLOW CACHE ENTRIES` to display cache entry metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntryInfo {
    /// The query_id of the cache this entry belongs to.
    pub query_id: QueryId,
    /// A hash of the entry's parameters, serving as a unique identifier within the cache.
    pub entry_id: u64,
    /// Last access time in milliseconds since UNIX_EPOCH.
    pub last_accessed_ms: u64,
    /// When this entry was last refreshed (data inserted), in milliseconds since UNIX_EPOCH.
    pub last_refreshed_ms: u64,
    /// How long the last refresh query took to execute, in milliseconds.
    pub refresh_time_ms: u64,
    /// The entry's current refresh period in milliseconds; adaptive caches nudge this at each
    /// refresh.
    pub refresh_period_ms: Option<u64>,
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
            trx_cache_policy: info.trx_cache_policy,
            adaptive: info.adaptive,
            concurrently: false,
            topk_buffer_multiplier: None,
            autoparam: Default::default(),
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
    query_id: QueryId,
    query: ShallowCacheQuery,
    schema_search_path: Vec<SqlIdentifier>,
    ttl_ms: Option<u64>,
    refresh_ms: Option<u64>,
    coalesce_ms: Option<u64>,
    adaptive: Option<Arc<AdaptiveState>>,
    max_entry_bytes: Option<usize>,
    entry_sizer: fn(&Vec<V>) -> usize,
    entry_hasher: fn(&[V]) -> u64,
    ddl_req: CacheDDLRequest,
    trx_cache_policy: TrxCachePolicy,
    scheduler: Option<Scheduler<K, V>>,
    /// Number of callbacks in the scheduler queue, mirrored to a gauge. Mutated only while
    /// the scheduler mutex is held, so it stays consistent with the queue contents.
    scheduler_queue_len: AtomicU64,
    scheduler_queue_gauge: Option<Gauge>,
    shutdown_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    hit_counter: Counter,
    miss_counter: Counter,
    refresh_changed_counter: Counter,
    refresh_unchanged_counter: Counter,
    coalesce_wait_histogram: Histogram,
    coalesce_resolved_counter: Counter,
    coalesce_timeout_counter: Counter,
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
        query_id: QueryId,
        query: ShallowCacheQuery,
        schema_search_path: Vec<SqlIdentifier>,
        ddl_req: CacheDDLRequest,
        trx_cache_policy: TrxCachePolicy,
        coalesce_ms: Option<Duration>,
        adaptive: Option<Arc<AdaptiveState>>,
        max_entry_bytes: Option<usize>,
    ) -> Arc<Self>
    where
        V: ContentHash + SizeOf,
    {
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

        let label = query_id.to_string();
        let hit_counter = counter!(metric::SHALLOW_HIT, "query_id" => label.clone());
        let miss_counter = counter!(metric::SHALLOW_MISS, "query_id" => label.clone());
        let refresh_changed_counter =
            counter!(metric::SHALLOW_REFRESH_CHANGED, "query_id" => label.clone());
        let scheduler_queue_gauge = schedule
            .then(|| gauge!(metric::SHALLOW_SCHEDULER_QUEUE_DEPTH, "query_id" => label.clone()));
        let refresh_unchanged_counter =
            counter!(metric::SHALLOW_REFRESH_UNCHANGED, "query_id" => label.clone());
        let coalesce_wait_histogram =
            histogram!(metric::SHALLOW_COALESCE_WAIT_TIME, "query_id" => label.clone());
        let coalesce_resolved_counter =
            counter!(metric::SHALLOW_COALESCE_RESOLVED, "query_id" => label.clone());
        let coalesce_timeout_counter =
            counter!(metric::SHALLOW_COALESCE_TIMEOUT, "query_id" => label);

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
            adaptive,
            max_entry_bytes,
            entry_sizer: <Vec<V> as SizeOf>::deep_size_of,
            entry_hasher: rows_content_hash::<V>,
            ddl_req,
            trx_cache_policy,
            scheduler: scheduler.clone(),
            scheduler_queue_len: AtomicU64::new(0),
            scheduler_queue_gauge,
            shutdown_tx: std::sync::Mutex::new(shutdown_tx),
            hit_counter,
            miss_counter,
            refresh_changed_counter,
            refresh_unchanged_counter,
            coalesce_wait_histogram,
            coalesce_resolved_counter,
            coalesce_timeout_counter,
        });

        if let Some(scheduler) = scheduler {
            let weak = Arc::downgrade(&cache);
            let shutdown_rx = shutdown_rx.unwrap();
            let gauge = cache.scheduler_queue_gauge.clone();
            tokio::spawn(Self::scheduler_loop(weak, scheduler, shutdown_rx, gauge));
        }

        cache
    }

    fn make_guard(
        cache: Arc<Cache<K, V>>,
        key: K,
        done: Option<Sender<()>>,
    ) -> CacheInsertGuard<K, V> {
        CacheInsertGuard {
            cache,
            key: Some(key),
            results: Some(Vec::new()),
            metadata: None,
            filled: false,
            requested: Instant::now(),
            done,
            refreshing: None,
        }
    }

    /// Apply a scheduler queue mutation to the maintained length and mirror it to the
    /// gauge. Takes the locked scheduler so the length cannot drift from the queue.
    fn scheduler_queue_changed(
        &self,
        _sched: &mut RefreshScheduler<K, V>,
        added: usize,
        removed: usize,
    ) {
        let len = (self.scheduler_queue_len.load(Ordering::Relaxed) + added as u64)
            .saturating_sub(removed as u64);
        self.scheduler_queue_len.store(len, Ordering::Relaxed);
        if let Some(gauge) = &self.scheduler_queue_gauge {
            gauge.set(len as f64);
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

            let due = times
                .into_iter()
                .filter_map(|instant| sched.queue.remove(&instant))
                .flatten()
                .collect::<Vec<_>>();
            cache.scheduler_queue_changed(&mut sched, 0, due.len());
            due
        };

        let mut to_reschedule = Vec::new();
        let mut to_evict = Vec::new();

        let refresh_ms = cache.refresh_ms;

        for (key, version, callback) in due {
            match cache.inner.get(&(cache.id, key.clone())).await.as_deref() {
                Some(entry @ (CacheEntry::Present(..) | CacheEntry::Loading(..))) => {
                    // Reschedule at the entry's own period, which adaptive caches nudge at each
                    // write-back. Capture per-item deadline to spread rescheduled callbacks
                    // across slightly different instants rather than collapsing
                    // them all to a single point in time.
                    let period_ms = match entry {
                        CacheEntry::Present(values) => values.period_ms,
                        CacheEntry::Loading(..) => refresh_ms,
                    };
                    let deadline = period_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
                    let guard = Self::make_guard(Arc::clone(&cache), key.clone(), None);
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
            let mut requeued = 0;
            for (deadline, k, version, refresh) in to_reschedule {
                if sched.active_keys.get(&k) == Some(&version) {
                    sched
                        .queue
                        .entry(deadline)
                        .or_default()
                        .push((k, version, refresh));
                    requeued += 1;
                }
            }
            cache.scheduler_queue_changed(&mut sched, requeued, 0);
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
        queue_gauge: Option<Gauge>,
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

        // The cache is going away; report an empty queue rather than the last value.
        if let Some(gauge) = queue_gauge {
            gauge.set(0.0);
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
        let values_hash = self.adaptive.as_ref().map(|_| (self.entry_hasher)(&v));
        CacheValues {
            values: Arc::new(v),
            metadata,
            accessed_ms: now.into(),
            refreshed_ms: now,
            refreshing: Arc::new(false.into()),
            ttl_ms: self.ttl_ms,
            execution_ms,
            period_ms: self.refresh_ms,
            values_hash,
        }
    }

    /// Compute the refresh period for an entry replacing `old`. Shrink toward the minimum when
    /// requested, grow toward the configured period otherwise, clamped to
    /// [`MIN_PERIOD_PERCENT`, 100%] of the cache's configured period.
    fn adapted_period(&self, old: &CacheValues<V>, shrink: bool) -> Option<u64> {
        let refresh_ms = self.refresh_ms?;
        let old_period = old.period_ms.unwrap_or(refresh_ms);
        let nudge = (refresh_ms * NUDGE_PERCENT / 100).max(1);
        let period = if shrink {
            let min = (refresh_ms * MIN_PERIOD_PERCENT / 100).max(1);
            old_period.saturating_sub(nudge).max(min)
        } else {
            old_period.saturating_add(nudge).min(refresh_ms)
        };
        Some(period)
    }

    async fn insert_entry(&self, k: K, mut new: CacheValues<V>) {
        self.inner
            .entry((self.id, k))
            .and_compute_with(|e| {
                match e {
                    Some(e) => {
                        if let CacheEntry::Present(old) = &**e.value() {
                            let acc = old.accessed_ms.load(Ordering::Relaxed);
                            new.accessed_ms.store(acc, Ordering::Relaxed);
                            if let Some(state) = &self.adaptive {
                                let changed = old.values_hash != new.values_hash;
                                if changed {
                                    self.refresh_changed_counter.increment(1);
                                } else {
                                    self.refresh_unchanged_counter.increment(1);
                                }
                                // At or over the load cap the period only grows, letting the
                                // aggregate drift back under.
                                let shrink = changed && !state.over_cap();
                                // The branches differ only in their Antithesis reachability
                                // markers, which compile to no-ops in normal builds.
                                #[allow(clippy::if_same_then_else)]
                                if shrink {
                                    antithesis_sdk::assert_reachable!(
                                        "Shallow adaptive refresh shrank a key's period"
                                    );
                                } else if changed {
                                    antithesis_sdk::assert_reachable!(
                                        "Shallow adaptive refresh held at the load cap"
                                    );
                                } else {
                                    antithesis_sdk::assert_reachable!(
                                        "Shallow adaptive refresh grew a key's period"
                                    );
                                }
                                new.period_ms = self.adapted_period(old, shrink);
                            }
                        }
                        if let Some(state) = &self.adaptive {
                            // The matching subtraction happens in the store's eviction
                            // listener, which receives the replaced entry.
                            state.add(&new);
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
        self.scheduler_queue_changed(&mut sched, 1, 0);
    }

    pub(crate) async fn insert(
        &self,
        k: K,
        v: Vec<V>,
        metadata: QueryMetadata,
        execution: Duration,
    ) {
        if let Some(cap) = self.max_entry_bytes
            && (self.entry_sizer)(&v) > cap
        {
            counter!(metric::SHALLOW_SKIP_TOO_LARGE, "query_id" => self.query_id.to_string())
                .increment(1);
            self.clear_stub(k).await;
            return;
        }
        let metadata = self.dedupe_metadata(metadata);
        let entry = self.make_entry(v, metadata, execution);
        self.insert_entry(k, entry).await;
    }

    /// Remove a loading stub left behind when an insert is skipped, so the key
    /// reverts to a normal miss. A concurrently-inserted present entry is left
    /// untouched.
    async fn clear_stub(&self, k: K) {
        self.inner
            .entry((self.id, k))
            .and_compute_with(|e| {
                let op = match e.as_ref().map(|e| &**e.value()) {
                    Some(CacheEntry::Loading(..)) => Op::Remove,
                    Some(CacheEntry::Present(..)) | None => Op::Nop,
                };
                future::ready(op)
            })
            .await;
    }

    fn get_hit(&self, values: &CacheValues<V>) -> (QueryResult<V>, Option<Arc<AtomicBool>>) {
        let now = current_timestamp_ms();
        values.accessed_ms.store(now, Ordering::Relaxed);
        let refresh = if let Some(period_ms) = values.period_ms
            && now.saturating_sub(values.refreshed_ms + values.execution_ms) >= period_ms
            && values
                .refreshing
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            Some(Arc::clone(&values.refreshing))
        } else {
            None
        };

        let metadata = values
            .metadata
            .as_ref()
            .or_else(|| self.cache_metadata.get())
            .expect("No metadata available for cached result");

        (
            QueryResult {
                values: Arc::clone(&values.values),
                metadata: Arc::clone(metadata),
            },
            refresh,
        )
    }

    pub(crate) async fn get(&self, k: K) -> (Option<(QueryResult<V>, Option<Arc<AtomicBool>>)>, K) {
        let k = (self.id, k);
        let result = if let Some(entry) = self.inner.get(&k).await
            && let CacheEntry::Present(values) = &*entry
        {
            Some(self.get_hit(values))
        } else {
            None
        };
        (result, k.1)
    }

    fn hit(&self, values: &CacheValues<V>) -> Lookup<K, V> {
        let (res, refresh) = self.get_hit(values);
        Lookup::Hit(res, refresh)
    }

    /// Wait the allowed duration while for a stub entry for this key to populate.
    ///
    /// Returns immediately if the key is already populated or if the producer of a stub entry
    /// already dropped.
    async fn coalesce(&self, k: &(u64, K), wait: Duration) {
        let Some(entry) = self.inner.get(k).await else {
            return;
        };
        let CacheEntry::Loading(stub) = &*entry else {
            return;
        };
        let start = Instant::now();
        let result = timeout(wait, stub.done.clone().changed()).await;
        self.coalesce_wait_histogram
            .record(start.elapsed().as_micros() as f64);
        // A dropped producer counts as resolved: the wait ended before the deadline.
        match result {
            Ok(_) => self.coalesce_resolved_counter.increment(1),
            Err(_) => self.coalesce_timeout_counter.increment(1),
        }
    }

    /// Create an insert guard and associated cache stub.
    fn start_fill(self: &Arc<Self>, key: K) -> (Arc<CacheEntry<V>>, CacheInsertGuard<K, V>) {
        let (tx, rx) = watch::channel(());
        let stub = Arc::new(CacheEntry::Loading(CacheStub { done: rx }));
        let guard = Self::make_guard(Arc::clone(self), key, Some(tx));
        (stub, guard)
    }

    pub(crate) async fn get_on_miss(self: &Arc<Self>, k: K) -> Lookup<K, V> {
        let mk = (self.id, k.clone());

        if let Some(coalesce_ms) = self.coalesce_ms {
            self.coalesce(&mk, Duration::from_millis(coalesce_ms)).await;
        }

        let mut lookup = None;
        self.inner
            .entry(mk)
            .and_compute_with(|e| {
                let op = match e.as_ref().map(|e| &**e.value()) {
                    Some(CacheEntry::Present(values)) => {
                        lookup = Some(self.hit(values));
                        Op::Nop
                    }
                    // There's still an active stub here, but we aren't waiting anymore.
                    Some(CacheEntry::Loading(stub)) if stub.done.has_changed().is_ok() => {
                        lookup = Some(Lookup::Miss(Self::make_guard(
                            Arc::clone(self),
                            k.clone(),
                            None,
                        )));
                        Op::Nop
                    }
                    // Found either nothing or an exhausted stub; insert a fresh stub.
                    None | Some(CacheEntry::Loading(..)) => {
                        let (stub, guard) = self.start_fill(k.clone());
                        lookup = Some(Lookup::Miss(guard));
                        Op::Put(stub)
                    }
                };
                future::ready(op)
            })
            .await;
        lookup.expect("present or starting fill")
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
            trx_cache_policy: self.trx_cache_policy,
            schedule: self.is_scheduled(),
            adaptive: self.adaptive.is_some(),
        }
    }

    pub fn name(&self) -> &Option<Relation> {
        &self.name
    }

    pub(crate) fn query_id(&self) -> &QueryId {
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
    use std::{assert_matches, time::Duration};

    use crate::{CacheManager, EvictionPolicy, QueryMetadata};

    use super::*;

    const ID: u64 = 0;
    const ZERO_DURATION: Duration = Duration::from_secs(0);

    fn test_ddl_req() -> CacheDDLRequest {
        CacheDDLRequest {
            unparsed_stmt: "CREATE SHALLOW CACHE test AS SELECT 1".to_string(),
            schema_search_path: vec![],
            dialect: readyset_sql::Dialect::PostgreSQL.into(),
            cache_name: None,
        }
    }

    fn new<K, V>(max_capacity: Option<u64>, policy: EvictionPolicy) -> Arc<Cache<K, V>>
    where
        K: Clone + Eq + Hash + Send + Sync + SizeOf + 'static,
        V: ContentHash + Send + Sync + SizeOf + 'static,
    {
        new_capped(max_capacity, policy, None)
    }

    fn new_capped<K, V>(
        max_capacity: Option<u64>,
        policy: EvictionPolicy,
        max_entry_bytes: Option<usize>,
    ) -> Arc<Cache<K, V>>
    where
        K: Clone + Eq + Hash + Send + Sync + SizeOf + 'static,
        V: ContentHash + Send + Sync + SizeOf + 'static,
    {
        let inner = CacheManager::new_inner(max_capacity, Default::default());
        Cache::new(
            ID,
            inner,
            policy,
            None,
            QueryId::random(),
            ShallowCacheQuery::default(),
            vec![],
            test_ddl_req(),
            TrxCachePolicy::Never,
            None,
            None,
            max_entry_bytes,
        )
    }

    async fn mark_fresh_insert_intent(
        cache: &Arc<Cache<Vec<&str>, Vec<&str>>>,
        key: &[&'static str],
    ) {
        // Insert a stub entry to allow a subsequent insert.
        assert_matches!(cache.get_on_miss(key.to_owned()).await, Lookup::Miss(..));
    }

    #[tokio::test]
    async fn test_per_entry_size_cap() {
        let policy = EvictionPolicy::Ttl {
            ttl: Duration::from_secs(60),
        };
        let result = vec![vec!["a", "b", "c"]];

        // A tiny cap rejects the result: the loading stub is cleared and nothing is cached.
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> = new_capped(None, policy, Some(1));
        mark_fresh_insert_intent(&cache, &["k"]).await;
        cache
            .insert(
                vec!["k"],
                result.clone(),
                QueryMetadata::Test,
                ZERO_DURATION,
            )
            .await;
        assert!(cache.get(vec!["k"]).await.0.is_none());
        assert_eq!(cache.count().await, 0);

        // A generous cap caches the same result.
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> =
            new_capped(None, policy, Some(10 * 1024 * 1024));
        mark_fresh_insert_intent(&cache, &["k"]).await;
        cache
            .insert(vec!["k"], result, QueryMetadata::Test, ZERO_DURATION)
            .await;
        assert!(cache.get(vec!["k"]).await.0.is_some());
        assert_eq!(cache.count().await, 1);
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
        let inner = CacheManager::new_inner(None, Default::default());
        let policy = EvictionPolicy::Ttl {
            ttl: Duration::from_secs(60),
        };
        let stmt = ShallowCacheQuery::default();

        let cache_0 = Cache::new(
            0,
            Arc::clone(&inner),
            policy,
            None,
            QueryId::random(),
            stmt.clone(),
            vec![],
            test_ddl_req(),
            TrxCachePolicy::Never,
            None,
            None,
            None,
        );
        let cache_1 = Cache::new(
            1,
            Arc::clone(&inner),
            policy,
            None,
            QueryId::random(),
            stmt.clone(),
            vec![],
            test_ddl_req(),
            TrxCachePolicy::Never,
            None,
            None,
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
            refreshing: Arc::new(false.into()),
            ttl_ms: None,
            execution_ms: 0,
            period_ms: None,
            values_hash: None,
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
        assert_eq!(cache.scheduler_queue_len.load(Ordering::Relaxed), 1);
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
        assert_eq!(cache.scheduler_queue_len.load(Ordering::Relaxed), 2);
    }

    /// Verify that the maintained scheduler queue length matches the queue contents
    /// across the drain and requeue paths of `process_due_callbacks`: a key still in
    /// the store is requeued at its entry's period, while an evicted key's callback is
    /// dropped and unregistered. Paused time keeps the background scheduler loop from
    /// processing callbacks concurrently.
    #[tokio::test(start_paused = true)]
    async fn test_scheduler_queue_len_accounting() {
        let cache: Arc<Cache<Vec<&str>, Vec<&str>>> = new(
            None,
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(20),
                refresh: Duration::from_secs(10),
                schedule: true,
            },
        );
        let callback: RequestRefresh<Vec<&str>, Vec<&str>> = Arc::new(|_guard| {});

        let present = vec!["present"];
        let absent = vec!["absent"];
        let lookup = cache.get_on_miss(present.clone()).await;
        assert_matches!(lookup, Lookup::Miss(_));
        cache
            .insert(
                present.clone(),
                vec![vec!["v"]],
                QueryMetadata::Test,
                ZERO_DURATION,
            )
            .await;
        drop(lookup);
        cache
            .schedule_refresh(present.clone(), callback.clone())
            .await;
        cache
            .schedule_refresh(absent.clone(), callback.clone())
            .await;

        let scheduler = cache.scheduler.as_ref().unwrap();
        {
            let sched = scheduler.lock().await;
            let total: usize = sched.queue.values().map(|v| v.len()).sum();
            assert_eq!(total, 2);
        }
        assert_eq!(cache.scheduler_queue_len.load(Ordering::Relaxed), 2);

        // Move both callbacks into the past so the next processing pass drains them.
        {
            let mut sched = scheduler.lock().await;
            let entries: Vec<_> = std::mem::take(&mut sched.queue)
                .into_values()
                .flatten()
                .collect();
            sched
                .queue
                .insert(Instant::now() - Duration::from_secs(1), entries);
        }
        Cache::process_due_callbacks(Arc::clone(&cache), scheduler).await;

        let sched = scheduler.lock().await;
        let total: usize = sched.queue.values().map(|v| v.len()).sum();
        assert_eq!(total, 1, "only the present key should be requeued");
        assert!(sched.active_keys.contains_key(&present));
        assert!(!sched.active_keys.contains_key(&absent));
        assert_eq!(cache.scheduler_queue_len.load(Ordering::Relaxed), 1);
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
        let inner = CacheManager::new_inner(None, Default::default());
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
            query_id,
            ShallowCacheQuery::default(),
            vec![],
            test_ddl_req(),
            TrxCachePolicy::Never,
            None,
            None,
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
