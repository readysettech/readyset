use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use assert_matches::assert_matches;
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_errors::ReadySetError;
use readyset_sql::ast::{Relation, ShallowCacheQuery};
use readyset_util::SizeOf;
use tokio::time::sleep;

use crate::{CacheManager, CacheResult, EvictionPolicy, QueryMetadata};

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
    }
}

fn create_test_cache<K, V>(
    manager: &CacheManager<K, V>,
    name: Option<Relation>,
    query_id: Option<QueryId>,
    policy: EvictionPolicy,
) -> Result<(), ReadySetError>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: SizeOf + Send + Sync + 'static,
{
    manager.create_cache(
        name,
        query_id,
        test_stmt(),
        vec![],
        policy,
        test_ddl_req(),
        false,
        None,
    )
}

async fn insert_value<K, V>(result: CacheResult<K, V>, values: Vec<V>)
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let CacheResult::Miss(mut guard) = result else {
        panic!("expected miss");
    };
    values.into_iter().for_each(|v| guard.push(v));
    guard.set_metadata(QueryMetadata::Test);
    guard.filled().await;
}

fn keys_iter(count: usize) -> impl Iterator<Item = String> {
    (0..count).map(|i| format!("key_{i}"))
}

async fn check_miss<K, V>(manager: &CacheManager<K, V>, query_id: &QueryId, key: K)
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    let result = manager.get_or_start_insert(query_id, key).await;
    assert_matches!(result, CacheResult::Miss(_));
}

async fn check_hit<K, V>(manager: &CacheManager<K, V>, query_id: &QueryId, key: K)
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    let result = manager.get_or_start_insert(query_id, key).await;
    assert_matches!(
        result,
        CacheResult::Hit(..) | CacheResult::HitAndRefresh(..)
    );
}

async fn check_hit_value<K, V>(
    manager: &CacheManager<K, V>,
    query_id: &QueryId,
    key: K,
    expected: Vec<V>,
) where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + PartialEq + std::fmt::Debug + 'static,
{
    let result = manager.get_or_start_insert(query_id, key).await;
    match result {
        CacheResult::Hit(results, _) | CacheResult::HitAndRefresh(results, _) => {
            assert_eq!(results.values, expected.into());
        }
        _ => panic!("expected Hit or HitAndRefresh"),
    }
}

async fn count_hits<K, V>(
    manager: &CacheManager<K, V>,
    query_id: &QueryId,
    keys: impl Iterator<Item = K>,
) -> usize
where
    K: Hash + Eq + Send + Sync + Clone + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    let mut hits = 0;
    for key in keys {
        let result = manager.get_or_start_insert(query_id, key).await;
        if result.is_hit() {
            hits += 1;
        }
    }
    hits
}

#[tokio::test]
async fn test_create_cache_with_relation() {
    let manager = CacheManager::<String, String>::new(None);
    let relation = test_relation("test_table");

    let result = create_test_cache(&manager, Some(relation.clone()), None, test_policy());
    assert!(result.is_ok());
    assert!(manager.exists(Some(&relation), None));
}

#[tokio::test]
async fn test_list_caches_and_drop_all() {
    let manager = CacheManager::<String, String>::new(None);

    let relation1 = test_relation("table1");
    let relation2 = test_relation("table2");
    let query_id1 = QueryId::from_unparsed_select("SELECT * FROM test1");
    let query_id2 = QueryId::from_unparsed_select("SELECT * FROM test2");

    create_test_cache(&manager, Some(relation1.clone()), None, test_policy()).unwrap();
    create_test_cache(&manager, None, Some(query_id1), test_policy()).unwrap();
    create_test_cache(
        &manager,
        Some(relation2.clone()),
        Some(query_id2),
        test_policy(),
    )
    .unwrap();

    let all_caches = manager.list_caches(None, None);
    assert_eq!(all_caches.len(), 3);

    let filtered = manager.list_caches(None, Some(&relation1));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, Some(relation1.clone()));

    let filtered = manager.list_caches(Some(query_id1), None);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].query_id, Some(query_id1));

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

#[tokio::test]
async fn test_create_cache_with_query_id() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    let result = create_test_cache(&manager, None, Some(query_id), test_policy());
    assert!(result.is_ok());
    assert!(manager.exists(None, Some(&query_id)));
}

#[tokio::test]
async fn test_create_cache_with_both_identifiers() {
    let manager = CacheManager::<String, String>::new(None);
    let relation = test_relation("test_table");
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    let result = create_test_cache(
        &manager,
        Some(relation.clone()),
        Some(query_id),
        test_policy(),
    );
    assert!(result.is_ok());

    assert!(manager.exists(Some(&relation), None));
    assert!(manager.exists(None, Some(&query_id)));
    assert!(manager.exists(Some(&relation), Some(&query_id)));
}

#[tokio::test]
async fn test_create_cache_without_identifiers() {
    let manager = CacheManager::<String, String>::new(None);

    let result = create_test_cache(&manager, None, None, test_policy());
    assert_matches!(result, Err(ReadySetError::Internal(_)));
}

#[tokio::test]
async fn test_duplicate_cache_creation() {
    let manager = CacheManager::<String, String>::new(None);
    let relation = test_relation("test_table");

    create_test_cache(&manager, Some(relation.clone()), None, test_policy()).unwrap();

    let result = create_test_cache(&manager, Some(relation.clone()), None, test_policy());
    assert_matches!(result, Err(ReadySetError::ViewAlreadyExists(_)));
}

#[tokio::test]
async fn test_drop_cache_by_relation() {
    let manager = CacheManager::<String, String>::new(None);
    let relation = test_relation("test_table");

    create_test_cache(&manager, Some(relation.clone()), None, test_policy()).unwrap();
    assert!(manager.exists(Some(&relation), None));

    let result = manager.drop_cache(Some(&relation), None);
    assert!(result.is_ok());
    assert!(!manager.exists(Some(&relation), None));
}

#[tokio::test]
async fn test_drop_cache_by_query_id() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();
    assert!(manager.exists(None, Some(&query_id)));

    let result = manager.drop_cache(None, Some(&query_id));
    assert!(result.is_ok());
    assert!(!manager.exists(None, Some(&query_id)));
}

#[tokio::test]
async fn test_drop_nonexistent_cache() {
    let manager = CacheManager::<String, String>::new(None);
    let relation = test_relation("nonexistent");

    let result = manager.drop_cache(Some(&relation), None);
    assert_matches!(result, Err(ReadySetError::ViewNotFound(_)));
}

#[tokio::test]
async fn test_drop_cache_without_identifiers() {
    let manager = CacheManager::<String, String>::new(None);

    let result = manager.drop_cache(None, None);
    assert_matches!(result, Err(ReadySetError::Internal(_)));
}

#[tokio::test]
async fn test_get_or_start_insert_not_cached() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    assert_matches!(result, CacheResult::NotCached);
}

#[tokio::test]
async fn test_get_or_start_insert_miss() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    check_miss(&manager, &query_id, "key1".to_string()).await;
}

#[tokio::test]
async fn test_get_or_start_insert_hit() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    insert_value(result, vec!["value1".to_string()]).await;

    check_hit_value(
        &manager,
        &query_id,
        "key1".to_string(),
        vec!["value1".to_string()],
    )
    .await;
}

#[tokio::test]
async fn test_cache_insert_guard_not_filled() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let CacheResult::Miss(mut guard) = result else {
        panic!("Expected Miss");
    };
    guard.push("value1".to_string());
    guard.set_metadata(QueryMetadata::Test);
    drop(guard);

    check_miss(&manager, &query_id, "key1".to_string()).await;
}

#[tokio::test]
#[should_panic(expected = "no metadata for result set")]
async fn test_cache_insert_guard_filled_without_metadata() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let CacheResult::Miss(mut guard) = result else {
        panic!("expected miss");
    };
    guard.push("value1".to_string());
    drop(guard.filled());
}

#[tokio::test]
async fn test_cache_isolation() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id_1 = QueryId::from_unparsed_select("SELECT * FROM table1");
    let query_id_2 = QueryId::from_unparsed_select("SELECT * FROM table2");

    create_test_cache(&manager, None, Some(query_id_1), test_policy()).unwrap();
    create_test_cache(&manager, None, Some(query_id_2), test_policy()).unwrap();

    let result = manager
        .get_or_start_insert(&query_id_1, "key1".to_string())
        .await;
    insert_value(result, vec!["value1".to_string()]).await;

    check_miss(&manager, &query_id_2, "key1".to_string()).await;
    check_hit(&manager, &query_id_1, "key1".to_string()).await;
}

#[tokio::test]
async fn test_concurrent_cache_creation() {
    const COUNT: usize = 10;

    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let mut handles = Vec::new();

    for i in 0..COUNT {
        let manager_clone = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            let relation = test_relation(&format!("table_{}", i));
            create_test_cache(&manager_clone, Some(relation), None, test_policy())
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let successes = results
        .iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    assert_eq!(successes, COUNT);
}

#[tokio::test]
async fn test_concurrent_cache_creation_same_name() {
    const COUNT: usize = 10;

    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let relation = test_relation("test_table");
    let mut handles = Vec::new();

    for _ in 0..COUNT {
        let manager_clone = Arc::clone(&manager);
        let relation_clone = relation.clone();
        let handle = tokio::spawn(async move {
            create_test_cache(&manager_clone, Some(relation_clone), None, test_policy())
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let successes = results
        .iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    let errors = results
        .iter()
        .filter(|r| {
            matches!(
                r.as_ref().unwrap(),
                Err(ReadySetError::ViewAlreadyExists(_))
            )
        })
        .count();

    assert_eq!(successes, 1);
    assert_eq!(errors, COUNT - 1);
}

#[tokio::test]
async fn test_concurrent_inserts_different_keys() {
    const COUNT: usize = 20;

    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let mut handles = Vec::new();

    for (i, key) in keys_iter(COUNT).enumerate() {
        let manager_clone = Arc::clone(&manager);
        let query_id_clone = query_id;
        let handle = tokio::spawn(async move {
            let result = manager_clone
                .get_or_start_insert(&query_id_clone, key)
                .await;
            insert_value(result, vec![format!("value_{}", i)]).await;
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    for (i, key) in keys_iter(COUNT).enumerate() {
        check_hit_value(&manager, &query_id, key, vec![format!("value_{i}")]).await;
    }
}

#[tokio::test]
async fn test_concurrent_reads_and_writes() {
    const PRE_POPULATE: usize = 5;
    const TOTAL_KEYS: usize = 10;

    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    for (i, key) in keys_iter(PRE_POPULATE).enumerate() {
        let result = manager.get_or_start_insert(&query_id, key).await;
        insert_value(result, vec![format!("value_{}", i)]).await;
    }

    let mut handles = Vec::new();

    for _ in 0..TOTAL_KEYS {
        let manager_clone = Arc::clone(&manager);
        let query_id_clone = query_id;
        let handle = tokio::spawn(async move {
            for key in keys_iter(PRE_POPULATE) {
                let _result = manager_clone
                    .get_or_start_insert(&query_id_clone, key)
                    .await;
            }
        });
        handles.push(handle);
    }

    for (i, key) in keys_iter(TOTAL_KEYS).enumerate().skip(PRE_POPULATE) {
        let manager_clone = Arc::clone(&manager);
        let query_id_clone = query_id;
        let handle = tokio::spawn(async move {
            let result = manager_clone
                .get_or_start_insert(&query_id_clone, key)
                .await;
            insert_value(result, vec![format!("value_{}", i)]).await;
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    for key in keys_iter(TOTAL_KEYS) {
        check_hit(&manager, &query_id, key).await;
    }
}

#[tokio::test]
async fn test_concurrent_create_and_drop() {
    const COUNT: usize = 10;

    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let mut handles = Vec::new();

    for i in 0..COUNT {
        let manager = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            let relation = test_relation(&format!("table_{}", i));
            create_test_cache(&manager, Some(relation.clone()), None, test_policy()).unwrap();

            sleep(Duration::from_millis(10)).await;
            manager.drop_cache(Some(&relation), None).unwrap();
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    for i in 0..COUNT {
        let relation = test_relation(&format!("table_{}", i));
        assert!(!manager.exists(Some(&relation), None));
    }
}

#[tokio::test]
async fn test_ttl_refresh_ahead() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(
        &manager,
        None,
        Some(query_id),
        EvictionPolicy::Ttl {
            ttl: Duration::from_secs(5),
        },
    )
    .unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    insert_value(result, vec!["value1".to_string()]).await;

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    assert_matches!(result, CacheResult::Hit(..));

    sleep(Duration::from_secs(3)).await;

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    assert_matches!(result, CacheResult::HitAndRefresh(..));
}

#[tokio::test]
async fn test_ttl_expiration() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(
        &manager,
        None,
        Some(query_id),
        EvictionPolicy::Ttl {
            ttl: Duration::from_secs(5),
        },
    )
    .unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    insert_value(result, vec!["value1".to_string()]).await;

    check_hit(&manager, &query_id, "key1".to_string()).await;

    sleep(Duration::from_secs(6)).await;

    check_miss(&manager, &query_id, "key1".to_string()).await;
}

#[tokio::test]
async fn test_max_capacity_enforcement() {
    const COUNT: usize = 15;

    let manager = CacheManager::<String, String>::new(Some(1024));
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    for key in keys_iter(COUNT) {
        let value = "x".repeat(100);
        let result = manager.get_or_start_insert(&query_id, key).await;
        insert_value(result, vec![value]).await;
    }

    let count = manager.count(&query_id).await.unwrap();
    assert!(count < COUNT, "expected {count} < {COUNT}");

    let hits = count_hits(&manager, &query_id, keys_iter(COUNT)).await;
    assert!(hits < COUNT, "expected {hits} < {COUNT}");
}

#[tokio::test]
async fn test_multi_cache_capacity_sharing() {
    const COUNT: usize = 10;

    let manager = CacheManager::<String, String>::new(Some(2048));
    let query_id_1 = QueryId::from_unparsed_select("SELECT * FROM table1");
    let query_id_2 = QueryId::from_unparsed_select("SELECT * FROM table2");

    create_test_cache(&manager, None, Some(query_id_1), test_policy()).unwrap();
    create_test_cache(&manager, None, Some(query_id_2), test_policy()).unwrap();

    for key in keys_iter(COUNT) {
        let large_value = "x".repeat(100);

        let result = manager.get_or_start_insert(&query_id_1, key.clone()).await;
        insert_value(result, vec![large_value.clone()]).await;

        let result = manager.get_or_start_insert(&query_id_2, key).await;
        insert_value(result, vec![large_value]).await;
    }

    manager.run_pending_tasks(&query_id_1).await;
    manager.run_pending_tasks(&query_id_2).await;

    let hits_1 = count_hits(&manager, &query_id_1, keys_iter(COUNT)).await;
    let hits_2 = count_hits(&manager, &query_id_2, keys_iter(COUNT)).await;
    let total_hits = hits_1 + hits_2;

    assert!(
        total_hits < 20,
        "Expected some evictions, got {total_hits} hits",
    );
}

#[tokio::test]
async fn test_cache_result_debug() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    let not_cached = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let debug_str = format!("{:?}", not_cached);
    assert!(debug_str.contains("NotCached"));

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let miss = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let debug_str = format!("{:?}", miss);
    assert!(debug_str.contains("Miss"));

    insert_value(miss, vec!["value1".to_string()]).await;

    let hit = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let debug_str = format!("{:?}", hit);
    assert!(debug_str.contains("Hit"));
}

#[tokio::test]
async fn test_cache_insert_guard_debug() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;

    let CacheResult::Miss(mut guard) = result else {
        panic!("expected miss");
    };

    guard.push("value1".to_string());
    guard.push("value2".to_string());

    let debug_str = format!("{:?}", guard);
    assert!(debug_str.contains("CacheInsertGuard"));
    assert!(debug_str.contains("results"));
    assert!(debug_str.contains("filled"));
}

#[tokio::test]
async fn test_ttl_and_period_refresh() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(
        &manager,
        None,
        Some(query_id),
        EvictionPolicy::TtlAndPeriod {
            ttl: Duration::from_secs(10),
            refresh: Duration::from_secs(2),
            schedule: false,
        },
    )
    .unwrap();

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    insert_value(result, vec!["value1".to_string()]).await;

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    assert_matches!(result, CacheResult::Hit(..));

    sleep(Duration::from_millis(2100)).await;

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    assert_matches!(result, CacheResult::HitAndRefresh(..));
}

#[tokio::test]
async fn test_coalesce_concurrent_requests() {
    let manager = Arc::new(CacheManager::<String, String>::new(None));
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    manager
        .create_cache(
            None,
            Some(query_id),
            test_stmt(),
            vec![],
            test_policy(),
            test_ddl_req(),
            false,
            Some(Duration::from_millis(5000)),
        )
        .unwrap();

    let end_time_1 = Arc::new(AtomicU64::new(0));
    let end_time_2 = Arc::new(AtomicU64::new(0));

    let m = Arc::clone(&manager);
    let end = Arc::clone(&end_time_1);
    let handle_1 = tokio::spawn(async move {
        let start = Instant::now();

        let result = m.get_or_start_insert(&query_id, "key1".to_string()).await;
        let CacheResult::Miss(mut guard) = result else {
            panic!("should miss");
        };

        guard.push("value1".to_string());
        guard.set_metadata(QueryMetadata::Test);
        sleep(Duration::from_millis(2000)).await;
        guard.filled().await;

        end.store(start.elapsed().as_millis() as u64, Ordering::SeqCst);
    });

    let m = Arc::clone(&manager);
    let end = Arc::clone(&end_time_2);
    let handle_2 = tokio::spawn(async move {
        let start = Instant::now();

        sleep(Duration::from_millis(1000)).await;
        let result = m.get_or_start_insert(&query_id, "key1".to_string()).await;

        let CacheResult::Hit(..) = result else {
            panic!("should hit");
        };

        end.store(start.elapsed().as_millis() as u64, Ordering::SeqCst);
    });

    let _ = tokio::join!(handle_1, handle_2);

    let end_1_ms = end_time_1.load(Ordering::SeqCst);
    let end_2_ms = end_time_2.load(Ordering::SeqCst);
    let (end_1_ms, end_2_ms) = (end_1_ms.min(end_2_ms), end_1_ms.max(end_2_ms));
    let diff = end_2_ms - end_1_ms;

    assert!(
        diff < 500,
        "Expected completion within 500 ms, but got {diff} ms"
    );
}

#[tokio::test]
async fn test_periodic_refresh_callback() {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    manager
        .create_cache(
            None,
            Some(query_id),
            test_stmt(),
            vec![],
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(5),
                refresh: Duration::from_secs(1),
                schedule: true,
            },
            test_ddl_req(),
            false,
            None,
        )
        .unwrap();

    let refresh_count = Arc::new(AtomicU32::new(0));

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let CacheResult::Miss(mut guard) = result else {
        panic!("expected miss");
    };

    guard.push("value_0".to_string());
    guard.set_metadata(QueryMetadata::Test);

    let count = Arc::clone(&refresh_count);
    let refresh_callback = Arc::new(move |mut guard: crate::CacheInsertGuard<String, String>| {
        let current = count.fetch_add(1, Ordering::SeqCst) + 1;
        guard.push(format!("value_{}", current));
        guard.set_metadata(QueryMetadata::Test);
        tokio::spawn(async move {
            guard.filled().await;
        });
    });

    guard.set_refresh(refresh_callback);
    guard.filled().await;
    check_hit_value(
        &manager,
        &query_id,
        "key1".to_string(),
        vec!["value_0".to_string()],
    )
    .await;

    // get a little out of phase
    sleep(Duration::from_millis(200)).await;

    for i in 1..=3 {
        sleep(Duration::from_millis(1000)).await;

        check_hit_value(
            &manager,
            &query_id,
            "key1".to_string(),
            vec![format!("value_{}", i)],
        )
        .await;
    }

    assert_eq!(refresh_count.load(Ordering::SeqCst), 3);

    sleep(Duration::from_secs(10)).await;
    check_miss(&manager, &query_id, "key1".to_string()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slow_refresh_serves_stale_data() {
    let manager = CacheManager::<String, u32>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    manager
        .create_cache(
            None,
            Some(query_id),
            test_stmt(),
            vec![],
            EvictionPolicy::TtlAndPeriod {
                ttl: Duration::from_secs(60),
                refresh: Duration::from_secs(1),
                schedule: true,
            },
            test_ddl_req(),
            false,
            None,
        )
        .unwrap();

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

    let result = manager
        .get_or_start_insert(&query_id, "key1".to_string())
        .await;
    let CacheResult::Miss(mut guard) = result else {
        panic!("expected miss");
    };

    sleep(Duration::from_millis(200)).await;

    let value = current.load(Ordering::SeqCst);
    guard.push(value);
    guard.set_metadata(QueryMetadata::Test);

    let refresh = {
        let current = Arc::clone(&current);
        Arc::new(move |mut guard: crate::CacheInsertGuard<String, u32>| {
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

    guard.set_refresh(refresh);
    guard.filled().await;

    sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        sleep(Duration::from_secs(1)).await;

        let result = manager
            .get_or_start_insert(&query_id, "key1".to_string())
            .await;
        let CacheResult::Hit(result, _) = result else {
            panic!("expected hit");
        };

        let value = result.values[0];
        let current = current.load(Ordering::SeqCst);
        println!("test {value} {current}");

        assert!(
            value == current.saturating_sub(2),
            "cached value {} should be <= current value {}",
            value,
            current
        );
    }

    updater.abort();
}
