use std::assert_matches;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt;
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_errors::ReadySetError;
use readyset_sql::ast::{Relation, ShallowCacheQuery, TrxCachePolicy};
use readyset_util::SizeOf;
use readyset_util::hash::hash;
use tokio::test;
use tokio::time::sleep;

use crate::{
    CacheEntryInfo, CacheInfo, CacheInsertGuard, CacheManager, CacheResult, ContentHash,
    EvictionPolicy, QueryMetadata, rows_content_hash,
};

fn test_manager() -> CacheManager<String, String> {
    CacheManager::new(None, None)
}

fn test_relation(name: &str) -> Relation {
    Relation {
        schema: None,
        name: name.into(),
    }
}

fn test_policy() -> EvictionPolicy {
    EvictionPolicy::Ttl {
        ttl: Duration::from_secs(60),
    }
}

fn test_stmt() -> ShallowCacheQuery {
    ShallowCacheQuery::default()
}

fn test_ddl_req() -> CacheDDLRequest {
    CacheDDLRequest {
        unparsed_stmt: "CREATE SHALLOW CACHE test AS SELECT 1".to_string(),
        schema_search_path: vec![],
        dialect: readyset_sql::Dialect::PostgreSQL.into(),
        cache_name: None,
    }
}

fn create_test_cache<K, V>(
    manager: &CacheManager<K, V>,
    name: Option<Relation>,
    query_id: QueryId,
    policy: EvictionPolicy,
) -> Result<(), ReadySetError>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: ContentHash + SizeOf + Send + Sync + 'static,
{
    manager.create_cache(
        name,
        query_id,
        test_stmt(),
        vec![],
        policy,
        test_ddl_req(),
        TrxCachePolicy::Never,
        None,
        false,
    )
}

fn keys_iter(count: usize) -> impl Iterator<Item = String> {
    (0..count).map(|i| format!("key_{i}"))
}

/// One shallow cache on a manager, with helpers for the common data-path operations. Cloning
/// shares the underlying manager, so clones can be moved into spawned tasks.
#[derive(Clone)]
struct TestCache<V = String>
where
    V: Send + Sync + 'static,
{
    manager: Arc<CacheManager<String, V>>,
    query_id: QueryId,
}

struct TestCacheBuilder {
    capacity: Option<u64>,
    name: Option<Relation>,
    policy: EvictionPolicy,
    coalesce: Option<Duration>,
    adaptive: bool,
    max_extra_load_percent: Option<u64>,
}

impl TestCache {
    fn new() -> Self {
        Self::builder().build()
    }

    fn builder() -> TestCacheBuilder {
        TestCacheBuilder {
            capacity: None,
            name: None,
            policy: test_policy(),
            coalesce: None,
            adaptive: false,
            max_extra_load_percent: None,
        }
    }
}

impl TestCacheBuilder {
    fn capacity(mut self, capacity: u64) -> Self {
        self.capacity = Some(capacity);
        self
    }

    fn name(mut self, name: Relation) -> Self {
        self.name = Some(name);
        self
    }

    fn policy(mut self, policy: EvictionPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn ttl(self, ttl: Duration) -> Self {
        self.policy(EvictionPolicy::Ttl { ttl })
    }

    /// TtlAndPeriod with a long TTL, refreshing on stale reads.
    fn period(self, refresh: Duration) -> Self {
        self.policy(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(60),
            refresh,
            schedule: false,
        })
    }

    /// TtlAndPeriod with a long TTL, refreshing on a schedule.
    fn scheduled_period(self, refresh: Duration) -> Self {
        self.policy(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(60),
            refresh,
            schedule: true,
        })
    }

    fn coalesce(mut self, window: Duration) -> Self {
        self.coalesce = Some(window);
        self
    }

    fn adaptive(mut self) -> Self {
        self.adaptive = true;
        self
    }

    fn max_extra_load_percent(mut self, percent: u64) -> Self {
        self.max_extra_load_percent = Some(percent);
        self
    }

    fn build(self) -> TestCache {
        self.build_typed()
    }

    fn build_typed<V>(self) -> TestCache<V>
    where
        V: ContentHash + SizeOf + Send + Sync + 'static,
    {
        let mut manager = CacheManager::new(self.capacity, None);
        if let Some(percent) = self.max_extra_load_percent {
            manager.set_adaptive_max_extra_load_percent(percent);
        }
        let query_id = QueryId::random();
        manager
            .create_cache(
                self.name,
                query_id,
                test_stmt(),
                vec![],
                self.policy,
                test_ddl_req(),
                TrxCachePolicy::Never,
                self.coalesce,
                self.adaptive,
            )
            .unwrap();
        TestCache {
            manager: Arc::new(manager),
            query_id,
        }
    }
}

impl<V> TestCache<V>
where
    V: ContentHash + SizeOf + Send + Sync + 'static,
{
    /// A second cache with default settings on the same manager.
    fn sibling(&self) -> Self {
        let query_id = QueryId::random();
        create_test_cache(&*self.manager, None, query_id, test_policy()).unwrap();
        Self {
            manager: Arc::clone(&self.manager),
            query_id,
        }
    }

    async fn get(&self, key: &str) -> CacheResult<String, V> {
        self.manager
            .get_or_start_insert(&self.query_id, key.to_string(), |_| true)
            .await
    }

    async fn expect_miss(&self, key: &str) -> CacheInsertGuard<String, V> {
        let CacheResult::Miss(guard) = self.get(key).await else {
            panic!("expected miss");
        };
        guard
    }

    async fn populate(&self, key: &str, values: impl IntoIterator<Item = impl Into<V>>) {
        let mut guard = self.expect_miss(key).await;
        values.into_iter().for_each(|v| guard.push(v.into()));
        guard.set_metadata(QueryMetadata::Test);
        guard.filled().await;
    }

    async fn check_miss(&self, key: &str) {
        assert_matches!(self.get(key).await, CacheResult::Miss(_));
    }

    async fn check_hit(&self, key: &str) {
        assert_matches!(
            self.get(key).await,
            CacheResult::Hit(..) | CacheResult::HitAndRefresh(..)
        );
    }

    async fn check_hit_value(&self, key: &str, expected: impl IntoIterator<Item = impl Into<V>>)
    where
        V: PartialEq + Debug,
    {
        match self.get(key).await {
            CacheResult::Hit(results) | CacheResult::HitAndRefresh(results, _) => {
                let expected: Vec<V> = expected.into_iter().map(Into::into).collect();
                assert_eq!(results.values, expected.into());
            }
            _ => panic!("expected Hit or HitAndRefresh"),
        }
    }

    async fn count_hits(&self, keys: impl Iterator<Item = String>) -> usize {
        let mut hits = 0;
        for key in keys {
            if self.get(&key).await.is_hit() {
                hits += 1;
            }
        }
        hits
    }

    /// Insert values directly, as a completed refresh would.
    async fn refresh_insert_with_exec(
        &self,
        key: &str,
        values: impl IntoIterator<Item = impl Into<V>>,
        exec: Duration,
    ) {
        let cache = self.manager.get(None, Some(&self.query_id)).unwrap();
        let values = values.into_iter().map(Into::into).collect();
        cache
            .insert(key.to_string(), values, QueryMetadata::Test, exec, true)
            .await;
        // Run the eviction listener for the replaced entry so load accounting settles.
        self.run_pending_tasks().await;
    }

    async fn refresh_insert(&self, key: &str, values: impl IntoIterator<Item = impl Into<V>>) {
        self.refresh_insert_with_exec(key, values, Duration::ZERO)
            .await;
    }

    async fn run_pending_tasks(&self) {
        self.manager.run_pending_tasks(&self.query_id).await;
    }

    async fn flush(&self) {
        self.manager
            .flush_cache(None, Some(&self.query_id))
            .await
            .unwrap();
    }

    fn wasted_refresh_count(&self) -> Option<u64> {
        self.manager.wasted_refresh_count(&self.query_id)
    }

    fn adaptive_load(&self) -> Option<(u64, u64)> {
        self.manager.adaptive_load(&self.query_id)
    }

    fn cache_info(&self) -> CacheInfo {
        self.manager
            .list_caches(Some(self.query_id), None)
            .into_iter()
            .next()
            .expect("no cache info")
    }

    /// The cache's sole entry.
    fn sole_entry(&self) -> CacheEntryInfo {
        let mut entries = self.manager.list_entries(Some(self.query_id), None);
        assert_eq!(entries.len(), 1);
        entries.remove(0)
    }

    fn entry_period(&self) -> Option<u64> {
        self.sole_entry().refresh_period_ms
    }

    fn entry_period_for(&self, key: &str) -> Option<u64> {
        let entry_id = hash(&key.to_string());
        self.manager
            .list_entries(Some(self.query_id), None)
            .into_iter()
            .find(|e| e.entry_id == entry_id)
            .expect("no entry for key")
            .refresh_period_ms
    }
}

#[test]
async fn test_create_cache_with_relation() {
    let manager = test_manager();
    let relation = test_relation("test_table");

    create_test_cache(
        &manager,
        Some(relation.clone()),
        QueryId::random(),
        test_policy(),
    )
    .unwrap();
    assert!(manager.exists(Some(&relation), None));
}

#[test]
async fn test_list_caches_and_drop_all() {
    let manager = test_manager();

    let relation1 = test_relation("table1");
    let relation2 = test_relation("table2");
    let query_id1 = QueryId::from_unparsed_select("SELECT * FROM test1");
    let query_id2 = QueryId::from_unparsed_select("SELECT * FROM test2");

    create_test_cache(
        &manager,
        Some(relation1.clone()),
        QueryId::random(),
        test_policy(),
    )
    .unwrap();
    create_test_cache(&manager, None, query_id1, test_policy()).unwrap();
    create_test_cache(&manager, Some(relation2.clone()), query_id2, test_policy()).unwrap();

    let all_caches = manager.list_caches(None, None);
    assert_eq!(all_caches.len(), 3);

    let filtered = manager.list_caches(None, Some(&relation1));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, Some(relation1.clone()));

    let filtered = manager.list_caches(Some(query_id1), None);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].query_id, query_id1);

    let filtered = manager.list_caches(Some(query_id1), Some(&relation1));
    assert_eq!(filtered.len(), 2);

    manager.drop_all_caches();

    assert!(!manager.exists(Some(&relation1), None));
    assert!(!manager.exists(Some(&relation2), None));
    assert!(!manager.exists(None, Some(&query_id1)));
    assert!(!manager.exists(None, Some(&query_id2)));

    let caches = manager.list_caches(None, None);
    assert_eq!(caches.len(), 0);
}

#[test]
async fn test_create_cache_with_query_id() {
    let manager = test_manager();
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, query_id, test_policy()).unwrap();
    assert!(manager.exists(None, Some(&query_id)));
}

#[test]
async fn test_create_cache_with_both_identifiers() {
    let manager = test_manager();
    let relation = test_relation("test_table");
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, Some(relation.clone()), query_id, test_policy()).unwrap();

    assert!(manager.exists(Some(&relation), None));
    assert!(manager.exists(None, Some(&query_id)));
    assert!(manager.exists(Some(&relation), Some(&query_id)));
}

#[test]
async fn test_duplicate_cache_creation() {
    let manager = test_manager();
    let relation = test_relation("test_table");

    create_test_cache(
        &manager,
        Some(relation.clone()),
        QueryId::random(),
        test_policy(),
    )
    .unwrap();

    let result = create_test_cache(
        &manager,
        Some(relation.clone()),
        QueryId::random(),
        test_policy(),
    );
    assert_matches!(result, Err(ReadySetError::ViewAlreadyExists(_)));
}

#[test]
async fn test_drop_cache_by_relation() {
    let manager = test_manager();
    let relation = test_relation("test_table");

    create_test_cache(
        &manager,
        Some(relation.clone()),
        QueryId::random(),
        test_policy(),
    )
    .unwrap();
    assert!(manager.exists(Some(&relation), None));

    manager.drop_cache(Some(&relation), None).unwrap();
    assert!(!manager.exists(Some(&relation), None));
}

#[test]
async fn test_drop_cache_by_query_id() {
    let manager = test_manager();
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, query_id, test_policy()).unwrap();
    assert!(manager.exists(None, Some(&query_id)));

    manager.drop_cache(None, Some(&query_id)).unwrap();
    assert!(!manager.exists(None, Some(&query_id)));
}

#[test]
async fn test_drop_nonexistent_cache() {
    let manager = test_manager();
    let relation = test_relation("nonexistent");

    let result = manager.drop_cache(Some(&relation), None);
    assert_matches!(result, Err(ReadySetError::ViewNotFound(_)));
}

#[test]
async fn test_drop_cache_without_identifiers() {
    let manager = test_manager();

    let result = manager.drop_cache(None, None);
    assert_matches!(result, Err(ReadySetError::Internal(_)));
}

#[test]
async fn test_get_or_start_insert_not_cached() {
    let manager = test_manager();
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string(), |_| true)
        .await;
    assert_matches!(result, CacheResult::NotCached);
}

#[test]
async fn test_get_or_start_insert_miss() {
    let t = TestCache::new();
    t.check_miss("key1").await;
}

#[test]
async fn test_get_or_start_insert_hit() {
    let t = TestCache::new();
    t.populate("key1", ["value1"]).await;
    t.check_hit_value("key1", ["value1"]).await;
}

#[test]
async fn test_cache_insert_guard_not_filled() {
    let t = TestCache::new();

    let mut guard = t.expect_miss("key1").await;
    guard.push("value1".to_string());
    guard.set_metadata(QueryMetadata::Test);
    drop(guard);

    t.check_miss("key1").await;
}

#[test]
#[should_panic(expected = "no metadata for result set")]
async fn test_cache_insert_guard_filled_without_metadata() {
    let t = TestCache::new();

    let mut guard = t.expect_miss("key1").await;
    guard.push("value1".to_string());
    drop(guard.filled());
}

#[test]
async fn test_cache_isolation() {
    let t1 = TestCache::new();
    let t2 = t1.sibling();

    t1.populate("key1", ["value1"]).await;

    t2.check_miss("key1").await;
    t1.check_hit("key1").await;
}

#[test]
async fn test_concurrent_cache_creation() {
    const COUNT: usize = 10;

    let manager = Arc::new(test_manager());

    let handles = (0..COUNT).map(|i| {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let relation = test_relation(&format!("table_{i}"));
            let query_id = QueryId::from_unparsed_select(format!("SELECT {i}"));
            create_test_cache(&manager, Some(relation), query_id, test_policy())
        })
    });

    let successes = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(successes, COUNT);
}

#[test]
async fn test_concurrent_cache_creation_same_name() {
    const COUNT: usize = 10;

    let manager = Arc::new(test_manager());
    let relation = test_relation("test_table");

    let handles = (0..COUNT).map(|_| {
        let manager = Arc::clone(&manager);
        let relation = relation.clone();
        tokio::spawn(async move {
            create_test_cache(&manager, Some(relation), QueryId::random(), test_policy())
        })
    });

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let errors = results
        .iter()
        .filter(|r| matches!(r, Err(ReadySetError::ViewAlreadyExists(_))))
        .count();

    assert_eq!(successes, 1);
    assert_eq!(errors, COUNT - 1);
}

#[test]
async fn test_concurrent_inserts_different_keys() {
    const COUNT: usize = 20;

    let t = TestCache::new();

    let handles = keys_iter(COUNT).enumerate().map(|(i, key)| {
        let t = t.clone();
        tokio::spawn(async move {
            t.populate(&key, [format!("value_{i}")]).await;
        })
    });

    futures::future::join_all(handles).await;

    for (i, key) in keys_iter(COUNT).enumerate() {
        t.check_hit_value(&key, [format!("value_{i}")]).await;
    }
}

#[test]
async fn test_concurrent_reads_and_writes() {
    const PRE_POPULATE: usize = 5;
    const TOTAL_KEYS: usize = 10;

    let t = TestCache::new();

    for (i, key) in keys_iter(PRE_POPULATE).enumerate() {
        t.populate(&key, [format!("value_{i}")]).await;
    }

    let mut handles = Vec::new();

    for _ in 0..TOTAL_KEYS {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for key in keys_iter(PRE_POPULATE) {
                let _result = t.get(&key).await;
            }
        }));
    }

    for (i, key) in keys_iter(TOTAL_KEYS).enumerate().skip(PRE_POPULATE) {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            t.populate(&key, [format!("value_{i}")]).await;
        }));
    }

    futures::future::join_all(handles).await;

    for key in keys_iter(TOTAL_KEYS) {
        t.check_hit(&key).await;
    }
}

#[test]
async fn test_concurrent_create_and_drop() {
    const COUNT: usize = 10;

    let manager = Arc::new(test_manager());

    let handles = (0..COUNT).map(|i| {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let relation = test_relation(&format!("table_{i}"));
            let query_id = QueryId::from_unparsed_select(format!("SELECT {i}"));
            create_test_cache(&manager, Some(relation.clone()), query_id, test_policy()).unwrap();

            sleep(Duration::from_millis(10)).await;
            manager.drop_cache(Some(&relation), None).unwrap();
        })
    });

    futures::future::join_all(handles).await;

    for i in 0..COUNT {
        let relation = test_relation(&format!("table_{i}"));
        assert!(!manager.exists(Some(&relation), None));
    }
}

#[test]
async fn test_ttl_refresh_ahead() {
    let t = TestCache::builder().ttl(Duration::from_secs(5)).build();

    t.populate("key1", ["value1"]).await;
    assert_matches!(t.get("key1").await, CacheResult::Hit(..));

    sleep(Duration::from_secs(3)).await;

    assert_matches!(t.get("key1").await, CacheResult::HitAndRefresh(..));
}

#[test]
async fn test_refreshing_state_cleared_when_guard_dropped() {
    let t = TestCache::builder().ttl(Duration::from_secs(5)).build();

    t.populate("key1", ["value1"]).await;

    sleep(Duration::from_secs(3)).await;

    // The first stale access starts a refresh.
    let CacheResult::HitAndRefresh(_, guard) = t.get("key1").await else {
        panic!("stale access should start a refresh");
    };

    // A subsequent stale access should not start a new refresh.
    assert_matches!(t.get("key1").await, CacheResult::Hit(..));

    // The refresh is dropped without inserting.
    drop(guard);

    // The next access starts a new refresh.
    assert_matches!(t.get("key1").await, CacheResult::HitAndRefresh(..));
}

#[test]
async fn test_ttl_expiration() {
    let t = TestCache::builder().ttl(Duration::from_secs(5)).build();

    t.populate("key1", ["value1"]).await;
    t.check_hit("key1").await;

    sleep(Duration::from_secs(6)).await;

    t.check_miss("key1").await;
}

#[test]
async fn test_max_capacity_enforcement() {
    const COUNT: usize = 15;

    let t = TestCache::builder().capacity(1024).build();

    for key in keys_iter(COUNT) {
        t.populate(&key, ["x".repeat(100)]).await;
    }

    let count = t.manager.count(&t.query_id).await.unwrap();
    assert!(count < COUNT, "expected {count} < {COUNT}");

    let hits = t.count_hits(keys_iter(COUNT)).await;
    assert!(hits < COUNT, "expected {hits} < {COUNT}");
}

#[test]
async fn test_multi_cache_capacity_sharing() {
    const COUNT: usize = 10;

    let t1 = TestCache::builder().capacity(2048).build();
    let t2 = t1.sibling();

    for key in keys_iter(COUNT) {
        let large_value = "x".repeat(100);
        t1.populate(&key, [large_value.clone()]).await;
        t2.populate(&key, [large_value]).await;
    }

    t1.run_pending_tasks().await;
    t2.run_pending_tasks().await;

    let hits_1 = t1.count_hits(keys_iter(COUNT)).await;
    let hits_2 = t2.count_hits(keys_iter(COUNT)).await;
    let total_hits = hits_1 + hits_2;

    assert!(
        total_hits < 20,
        "Expected some evictions, got {total_hits} hits",
    );
}

#[test]
async fn test_cache_result_debug() {
    let t = TestCache::new();

    let not_cached = t
        .manager
        .get_or_start_insert(&QueryId::random(), "key1".to_string(), |_| true)
        .await;
    let debug_str = format!("{not_cached:?}");
    assert!(debug_str.contains("NotCached"));

    let miss = t.get("key1").await;
    let debug_str = format!("{miss:?}");
    assert!(debug_str.contains("Miss"));

    let CacheResult::Miss(mut guard) = miss else {
        panic!("expected miss");
    };
    guard.push("value1".to_string());
    guard.set_metadata(QueryMetadata::Test);
    guard.filled().await;

    let hit = t.get("key1").await;
    let debug_str = format!("{hit:?}");
    assert!(debug_str.contains("Hit"));
}

#[test]
async fn test_cache_insert_guard_debug() {
    let t = TestCache::new();

    let mut guard = t.expect_miss("key1").await;
    guard.push("value1".to_string());
    guard.push("value2".to_string());

    let debug_str = format!("{guard:?}");
    assert!(debug_str.contains("CacheInsertGuard"));
    assert!(debug_str.contains("results"));
    assert!(debug_str.contains("filled"));
}

#[test]
async fn test_ttl_and_period_refresh() {
    let t = TestCache::builder()
        .policy(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(10),
            refresh: Duration::from_secs(2),
            schedule: false,
        })
        .build();

    t.populate("key1", ["value1"]).await;
    assert_matches!(t.get("key1").await, CacheResult::Hit(..));

    sleep(Duration::from_millis(2100)).await;

    assert_matches!(t.get("key1").await, CacheResult::HitAndRefresh(..));
}

#[test]
async fn test_coalesce_concurrent_requests() {
    let t = TestCache::builder()
        .coalesce(Duration::from_millis(5000))
        .build();

    let handle_1 = {
        let t = t.clone();
        tokio::spawn(async move {
            let start = Instant::now();

            let mut guard = t.expect_miss("key1").await;
            guard.push("value1".to_string());
            guard.set_metadata(QueryMetadata::Test);
            sleep(Duration::from_millis(2000)).await;
            guard.filled().await;

            start.elapsed().as_millis() as u64
        })
    };

    let handle_2 = {
        let t = t.clone();
        tokio::spawn(async move {
            let start = Instant::now();

            sleep(Duration::from_millis(1000)).await;
            let CacheResult::Hit(..) = t.get("key1").await else {
                panic!("should hit");
            };

            start.elapsed().as_millis() as u64
        })
    };

    let (end_1_ms, end_2_ms) = tokio::join!(handle_1, handle_2);
    let (end_1_ms, end_2_ms) = (end_1_ms.unwrap(), end_2_ms.unwrap());
    let (end_1_ms, end_2_ms) = (end_1_ms.min(end_2_ms), end_1_ms.max(end_2_ms));
    let diff = end_2_ms - end_1_ms;

    assert!(
        diff < 500,
        "Expected completion within 500 ms, but got {diff} ms"
    );
}

#[test]
async fn test_coalesce_initial_insert_failure() {
    let t = TestCache::builder()
        .coalesce(Duration::from_millis(5000))
        .build();

    // First get misses and creates the loading stub.
    let guard = t.expect_miss("key1").await;

    // While the first insert guard is still held, a subsequent get must block for coalescing.
    assert_matches!(
        t.get("key1").now_or_never(),
        None,
        "coalescing get should block while the stub is still loading",
    );

    // Fail to insert anything before dropping the guard.
    drop(guard);

    // A subsequent get must not block on coalescing after the failed insert.
    let result = t
        .get("key1")
        .now_or_never()
        .expect("coalescing get should not block after the initial insert failed");
    let CacheResult::Miss(mut guard) = result else {
        panic!("get should reclaim the dead stub as a miss");
    };
    guard.push("value1".to_string());
    guard.set_metadata(QueryMetadata::Test);
    guard.filled().await;

    // Should now hit.
    assert_matches!(t.get("key1").await, CacheResult::Hit(..));
}

#[test]
async fn test_periodic_refresh_callback() {
    let t = TestCache::builder()
        .policy(EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(5),
            refresh: Duration::from_secs(1),
            schedule: true,
        })
        .build();

    let refresh_count = Arc::new(AtomicU32::new(0));

    let mut guard = t.expect_miss("key1").await;
    guard.push("value_0".to_string());
    guard.set_metadata(QueryMetadata::Test);

    let count = Arc::clone(&refresh_count);
    let refresh_callback = Arc::new(move |mut guard: CacheInsertGuard<String, String>| {
        let current = count.fetch_add(1, Ordering::SeqCst) + 1;
        guard.push(format!("value_{current}"));
        guard.set_metadata(QueryMetadata::Test);
        tokio::spawn(async move {
            guard.filled().await;
        });
    });

    guard.schedule_refresh(refresh_callback).await;
    guard.filled().await;
    t.check_hit_value("key1", ["value_0"]).await;

    // get a little out of phase
    sleep(Duration::from_millis(200)).await;

    for i in 1..=3 {
        sleep(Duration::from_millis(1000)).await;

        t.check_hit_value("key1", [format!("value_{i}")]).await;
    }

    assert_eq!(refresh_count.load(Ordering::SeqCst), 3);

    sleep(Duration::from_secs(10)).await;
    t.check_miss("key1").await;
}

#[test(flavor = "multi_thread")]
async fn test_slow_refresh_serves_stale_data() {
    let t = TestCache::builder()
        .scheduled_period(Duration::from_secs(1))
        .build_typed::<u32>();

    let current = Arc::new(AtomicU32::new(0));

    let updater = {
        let current = Arc::clone(&current);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                current.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    let mut guard = t.expect_miss("key1").await;

    sleep(Duration::from_millis(200)).await;

    let value = current.load(Ordering::SeqCst);
    guard.push(value);
    guard.set_metadata(QueryMetadata::Test);

    let refresh = {
        let current = Arc::clone(&current);
        Arc::new(move |mut guard: CacheInsertGuard<String, u32>| {
            let value = current.load(Ordering::SeqCst);
            tokio::spawn(async move {
                sleep(Duration::from_secs(2)).await;
                guard.push(value);
                guard.set_metadata(QueryMetadata::Test);
                guard.filled().await;
                println!("inserted {value}");
            });
        })
    };

    guard.schedule_refresh(refresh).await;
    guard.filled().await;

    sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        sleep(Duration::from_secs(1)).await;

        let CacheResult::Hit(result) = t.get("key1").await else {
            panic!("expected hit");
        };

        let value = result.values[0];
        let current = current.load(Ordering::SeqCst);
        println!("test {value} {current}");

        assert!(
            value == current.saturating_sub(2),
            "cached value {value} should be <= current value {current}"
        );
    }

    updater.abort();
}

#[test]
async fn test_flush_all_caches_clears_entries_preserves_definitions() {
    let t1 = TestCache::new();
    let t2 = t1.sibling();

    t1.populate("key1", ["value1"]).await;
    t2.populate("key2", ["value2"]).await;

    assert_eq!(t1.manager.list_entries(None, None).len(), 2);

    t1.manager.flush_all_caches().await;

    assert!(t1.manager.list_entries(None, None).is_empty());
    assert_eq!(t1.manager.list_caches(None, None).len(), 2);
    t1.check_miss("key1").await;
    t2.check_miss("key2").await;
}

#[test]
async fn test_flush_cache_clears_entries_preserves_definition() {
    let relation = test_relation("my_cache");
    let t = TestCache::builder().name(relation.clone()).build();

    t.populate("key1", ["value1"]).await;

    t.check_hit("key1").await;
    assert_eq!(t.manager.list_entries(None, None).len(), 1);

    t.manager.flush_cache(Some(&relation), None).await.unwrap();

    assert!(t.manager.list_entries(None, None).is_empty());
    assert!(t.manager.exists(Some(&relation), None));
    t.check_miss("key1").await;
}

#[test]
async fn test_flush_cache_only_affects_target() {
    let relation_1 = test_relation("cache_1");
    let t1 = TestCache::builder().name(relation_1.clone()).build();
    let t2 = t1.sibling();

    t1.populate("key1", ["value1"]).await;
    t2.populate("key2", ["value2"]).await;

    t1.manager
        .flush_cache(Some(&relation_1), None)
        .await
        .unwrap();

    t1.check_miss("key1").await;
    t2.check_hit("key2").await;
    assert!(t1.manager.exists(Some(&relation_1), None));
    assert!(t2.manager.exists(None, Some(&t2.query_id)));
}

#[test]
async fn rows_content_hash_order_insensitive() {
    struct Row(u64);
    impl ContentHash for Row {
        fn content_hash(&self) -> u64 {
            hash(&self.0)
        }
    }

    let ab = rows_content_hash(&[Row(1), Row(2)]);
    let ba = rows_content_hash(&[Row(2), Row(1)]);
    assert_eq!(ab, ba);

    assert_ne!(ab, rows_content_hash(&[Row(1)]));
    assert_ne!(ab, rows_content_hash(&[Row(1), Row(3)]));
    // Duplicate rows are distinct from a single occurrence.
    assert_ne!(ab, rows_content_hash(&[Row(1), Row(1), Row(2)]));
    assert_eq!(rows_content_hash::<Row>(&[]), rows_content_hash::<Row>(&[]));
}

#[test]
async fn test_adaptive_period_nudging() {
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;
    assert_eq!(t.entry_period(), Some(1000));

    // A changed value shrinks the period by 10% of the configured period.
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.entry_period(), Some(900));

    // An unchanged value grows it back, clamped at the configured period.
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.entry_period(), Some(1000));
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.entry_period(), Some(1000));

    // The same rows in a different order do not count as a change.
    t.refresh_insert("key", ["a", "b"]).await;
    assert_eq!(t.entry_period(), Some(900));
    t.refresh_insert("key", ["b", "a"]).await;
    assert_eq!(t.entry_period(), Some(1000));
}

#[test]
async fn test_non_adaptive_period_fixed() {
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .build();

    t.populate("key", ["v0"]).await;
    assert_eq!(t.entry_period(), Some(1000));

    for i in 1..5 {
        t.refresh_insert("key", [format!("v{i}")]).await;
        assert_eq!(t.entry_period(), Some(1000));
    }
}

#[test]
async fn test_adaptive_stale_check_uses_period() {
    let t = TestCache::builder()
        .period(Duration::from_millis(2000))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;

    // A changed write-back drops the period to 1800ms, so a read at ~1900ms is stale even
    // though the configured period has not yet elapsed.
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.entry_period(), Some(1800));

    sleep(Duration::from_millis(1900)).await;
    assert_matches!(t.get("key").await, CacheResult::HitAndRefresh(..));
}

#[test]
async fn test_adaptive_scheduled_refresh_periods() {
    let t = TestCache::builder()
        .scheduled_period(Duration::from_millis(500))
        .adaptive()
        .build();

    let mut guard = t.expect_miss("key").await;
    guard.push("value_0".to_string());
    guard.set_metadata(QueryMetadata::Test);

    let counter = Arc::new(AtomicU32::new(0));
    let changing = Arc::new(AtomicBool::new(true));
    let refresh_callback = {
        let counter = Arc::clone(&counter);
        let changing = Arc::clone(&changing);
        Arc::new(move |mut guard: CacheInsertGuard<String, String>| {
            let value = if changing.load(Ordering::SeqCst) {
                format!("value_{}", counter.fetch_add(1, Ordering::SeqCst) + 1)
            } else {
                "steady".to_string()
            };
            guard.push(value);
            guard.set_metadata(QueryMetadata::Test);
            tokio::spawn(async move {
                guard.filled().await;
            });
        })
    };
    guard.schedule_refresh(refresh_callback).await;
    guard.filled().await;

    // While the value keeps changing, scheduled refreshes shrink the period.
    sleep(Duration::from_millis(3500)).await;
    assert!(t.entry_period().unwrap() <= 400);

    // Once the value stops changing, the period climbs back to the configured one.
    changing.store(false, Ordering::SeqCst);
    sleep(Duration::from_millis(5000)).await;
    assert_eq!(t.entry_period(), Some(500));
}

#[test]
async fn test_adaptive_load_cap_single_key() {
    // With one entry the cap (100% extra load) binds when the period halves: shrinking stops
    // at 50% of the configured period, and further changed refreshes oscillate between
    // growing back under the boundary and shrinking down to it.
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;

    let mut min_seen = u64::MAX;
    for i in 1..=12 {
        t.refresh_insert("key", [format!("v{i}")]).await;
        let period = t.entry_period().unwrap();
        assert!(period >= 500, "cap breached at iteration {i}: {period}");
        min_seen = min_seen.min(period);
    }
    assert_eq!(min_seen, 500);
    assert_matches!(t.entry_period(), Some(500 | 600));
}

#[test]
async fn test_adaptive_load_cap_small_baseline() {
    // A cheap query on a long period contributes well under 100 ppm; the cap must scale
    // proportionally at that magnitude so the entry can still shrink to the minimum period.
    let t = TestCache::builder()
        .period(Duration::from_secs(15))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;

    let mut min_seen = u64::MAX;
    for i in 1..=8 {
        t.refresh_insert("key", [format!("v{i}")]).await;
        min_seen = min_seen.min(t.entry_period().unwrap());
    }
    assert_eq!(min_seen, 7500);
    assert_matches!(t.entry_period(), Some(7500 | 9000));
}

#[test]
async fn test_adaptive_load_cap_shares_budget() {
    // A stable second entry adds budget headroom, letting the churning key shrink below half
    // of the configured period before the cap binds (at 300ms here).
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    for key in ["stable", "churn"] {
        t.populate(key, [format!("{key}_v")]).await;
        // Force execution time to zero.
        t.refresh_insert(key, [format!("{key}_v")]).await;
    }

    for i in 1..=20 {
        t.refresh_insert("churn", [format!("v{i}")]).await;
        let period = t.entry_period_for("churn").unwrap();
        assert!(period >= 300, "cap breached at iteration {i}: {period}");
    }
    assert_matches!(t.entry_period_for("churn"), Some(300 | 400));
    assert_eq!(t.entry_period_for("stable"), Some(1000));
}

#[test]
async fn test_adaptive_load_cap_weighs_execution_time() {
    // An expensive stable entry dominates the load budget, so a cheap churning key has room
    // to reach the minimum period.
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    for key in ["stable", "churn"] {
        t.populate(key, [format!("{key}_v")]).await;
    }
    t.refresh_insert_with_exec("stable", ["stable_v"], Duration::from_millis(100))
        .await;

    for i in 1..=15 {
        t.refresh_insert("churn", [format!("v{i}")]).await;
    }
    assert_eq!(t.entry_period_for("churn"), Some(100));
}

#[test]
async fn test_adaptive_load_accounting_balances() {
    let t = TestCache::builder()
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;
    // Force execution time to zero.
    t.refresh_insert("key", ["v0"]).await;
    assert_eq!(t.adaptive_load(), Some((1000, 1000)));

    // CacheInfo reports the same load accounting; the cap (100% extra) is not yet hit.
    let info = t.cache_info();
    assert_eq!(info.load_actual_ppm, Some(1000));
    assert_eq!(info.load_baseline_ppm, Some(1000));
    assert_eq!(info.over_cap, Some(false));
    assert_eq!(info.scheduler_queue_len, None);

    // Shrink to the 500ms floor; the entry now contributes twice its baseline.
    for i in 1..=5 {
        t.refresh_insert("key", [format!("v{i}")]).await;
    }
    assert_eq!(t.adaptive_load(), Some((2000, 1000)));

    let info = t.cache_info();
    assert_eq!(info.load_actual_ppm, Some(2000));
    assert_eq!(info.load_baseline_ppm, Some(1000));
    assert_eq!(info.over_cap, Some(true));

    // Flushing evicts the entry, which gives its contributions back. An empty cache is not
    // over the cap.
    t.flush().await;
    assert_eq!(t.adaptive_load(), Some((0, 0)));
    assert_eq!(t.cache_info().over_cap, Some(false));

    // A refilled key starts fresh and may shrink again.
    t.populate("key", ["w0"]).await;
    t.refresh_insert("key", ["w0"]).await;
    assert_eq!(t.adaptive_load(), Some((1000, 1000)));
    t.refresh_insert("key", ["w1"]).await;
    assert_eq!(t.entry_period(), Some(900));
}

#[test]
async fn test_adaptive_load_cap_configurable() {
    // With a 300% extra-load allowance, a lone churning key may quadruple its baseline load:
    // shrinking continues past half the configured period and stalls around a quarter of it.
    let t = TestCache::builder()
        .max_extra_load_percent(300)
        .period(Duration::from_millis(1000))
        .adaptive()
        .build();

    t.populate("key", ["v0"]).await;

    let mut min_seen = u64::MAX;
    for i in 1..=12 {
        t.refresh_insert("key", [format!("v{i}")]).await;
        let period = t.entry_period().unwrap();
        assert!(period >= 200, "cap breached at iteration {i}: {period}");
        min_seen = min_seen.min(period);
    }
    assert_eq!(min_seen, 200);
    assert_matches!(t.entry_period(), Some(200 | 300));
}

#[test]
async fn test_wasted_refresh_replaced_unserved() {
    let t = TestCache::new();

    t.populate("key", ["v0"]).await;

    // The first refresh replaces the miss fill, whose data was served as the miss response.
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.wasted_refresh_count(), Some(0));

    // The second refresh replaces the first, whose data was never served.
    t.refresh_insert("key", ["v2"]).await;
    assert_eq!(t.wasted_refresh_count(), Some(1));
}

#[test]
async fn test_served_refresh_not_wasted() {
    let t = TestCache::new();

    t.populate("key", ["v0"]).await;

    t.refresh_insert("key", ["v1"]).await;
    t.check_hit("key").await;
    t.refresh_insert("key", ["v2"]).await;
    assert_eq!(t.wasted_refresh_count(), Some(0));

    t.refresh_insert("key", ["v3"]).await;
    assert_eq!(t.wasted_refresh_count(), Some(1));
}

#[test]
async fn test_wasted_refresh_ttl_expiration() {
    let t = TestCache::builder().ttl(Duration::from_secs(1)).build();

    for key in ["refreshed", "fill_only"] {
        t.populate(key, ["v0"]).await;
    }
    t.refresh_insert("refreshed", ["v1"]).await;

    sleep(Duration::from_secs(2)).await;
    t.run_pending_tasks().await;

    // Only the unserved refresh counts; the expired miss fill was served as its miss response.
    assert_eq!(t.wasted_refresh_count(), Some(1));
}

#[test]
async fn test_wasted_refresh_late_after_eviction() {
    let t = TestCache::builder().ttl(Duration::from_secs(1)).build();

    t.populate("key", ["v0"]).await;

    sleep(Duration::from_secs(2)).await;
    t.run_pending_tasks().await;
    assert_eq!(t.wasted_refresh_count(), Some(0));

    // A refresh that completes only after its entry was evicted is wasted.
    t.refresh_insert("key", ["v1"]).await;
    assert_eq!(t.wasted_refresh_count(), Some(1));
}

#[test]
async fn test_flush_not_counted_as_wasted() {
    let t = TestCache::new();

    t.populate("key", ["v0"]).await;
    t.refresh_insert("key", ["v1"]).await;

    t.flush().await;
    assert_eq!(t.wasted_refresh_count(), Some(0));
}

#[test]
async fn test_wasted_refresh_size_eviction() {
    let t = TestCache::builder().capacity(1024).build();

    t.populate("key", ["v0"]).await;

    // Refresh with a value larger than the store's capacity: the replacement is evicted for
    // size before it can ever be served.
    t.refresh_insert("key", ["x".repeat(2000)]).await;
    assert_eq!(t.wasted_refresh_count(), Some(1));
}

#[test]
async fn test_list_entries_served_flag() {
    let t = TestCache::new();

    t.populate("key", ["v0"]).await;
    assert!(t.sole_entry().served);

    t.refresh_insert("key", ["v1"]).await;
    assert!(!t.sole_entry().served);

    t.check_hit("key").await;
    assert!(t.sole_entry().served);
}
