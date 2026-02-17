use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_shallow::{CacheManager, CacheResult, EvictionPolicy, QueryMetadata};
use readyset_sql::Dialect;
use readyset_sql::ast::ShallowCacheQuery;

const NUM_ENTRIES: usize = 1_000_000;
const NUM_LOOKUPS: usize = 10_000;
const NUM_INSERTS: usize = 10_000;

fn bench_cache_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");

    let manager: Arc<CacheManager<Vec<readyset_data::DfValue>, String>> =
        Arc::new(CacheManager::new(None));

    let query_id = QueryId::from_unparsed_select("SELECT bench");

    manager
        .create_cache(
            None,
            Some(query_id),
            ShallowCacheQuery::default(),
            vec![],
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(3600),
            },
            CacheDDLRequest {
                unparsed_stmt: "CREATE SHALLOW CACHE bench AS SELECT 1".to_string(),
                schema_search_path: vec![],
                dialect: Dialect::PostgreSQL.into(),
            },
            false,
            None,
        )
        .expect("failed to create cache");

    // Populate the cache with NUM_ENTRIES entries.
    rt.block_on(async {
        for i in 0..NUM_ENTRIES {
            let key = vec![readyset_data::DfValue::Int(i as i64)];
            let result = manager.get_or_start_insert(&query_id, key, |_| true).await;
            let CacheResult::Miss(mut guard) = result else {
                panic!("expected miss during setup");
            };
            guard.push(format!("value_{i}"));
            guard.set_metadata(QueryMetadata::Test);
            guard.filled().await;
        }
        manager.run_pending_tasks(&query_id).await;
    });

    // Pre-generate random lookup keys to avoid benchmarking RNG.
    let mut rng = rand::rng();
    let lookup_keys: Vec<Vec<readyset_data::DfValue>> = (0..NUM_LOOKUPS)
        .map(|_| {
            let i = rng.random_range(0..NUM_ENTRIES as i64);
            vec![readyset_data::DfValue::Int(i)]
        })
        .collect();

    let mut group = c.benchmark_group("shallow_cache");
    group.throughput(criterion::Throughput::Elements(NUM_LOOKUPS as u64));

    group.bench_function("get_or_start_insert_hit", |b| {
        b.to_async(&rt).iter(|| {
            let manager = Arc::clone(&manager);
            let keys = lookup_keys.clone();
            async move {
                for key in keys {
                    let result = manager.get_or_start_insert(&query_id, key, |_| true).await;
                    debug_assert!(result.is_hit());
                }
            }
        });
    });

    group.finish();
}

fn bench_cache_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");

    let manager: Arc<CacheManager<Vec<readyset_data::DfValue>, String>> =
        Arc::new(CacheManager::new(None));

    let query_id = QueryId::from_unparsed_select("SELECT bench_insert");

    manager
        .create_cache(
            None,
            Some(query_id),
            ShallowCacheQuery::default(),
            vec![],
            EvictionPolicy::Ttl {
                ttl: Duration::from_secs(3600),
            },
            CacheDDLRequest {
                unparsed_stmt: "CREATE SHALLOW CACHE bench_insert AS SELECT 1".to_string(),
                schema_search_path: vec![],
                dialect: Dialect::PostgreSQL.into(),
            },
            false,
            None,
        )
        .expect("failed to create cache");

    // Counter to generate unique keys across criterion iterations.
    let key_counter = AtomicUsize::new(0);

    let mut group = c.benchmark_group("shallow_cache");
    group.throughput(criterion::Throughput::Elements(NUM_INSERTS as u64));

    group.bench_function("insert", |b| {
        b.to_async(&rt).iter(|| {
            let manager = Arc::clone(&manager);
            let base = key_counter.fetch_add(NUM_INSERTS, Ordering::Relaxed);
            async move {
                for i in base..base + NUM_INSERTS {
                    let key = vec![readyset_data::DfValue::Int(i as i64)];
                    let result = manager.get_or_start_insert(&query_id, key, |_| true).await;
                    let CacheResult::Miss(mut guard) = result else {
                        panic!("expected miss during insertion benchmark");
                    };
                    guard.push(format!("value_{i}"));
                    guard.set_metadata(QueryMetadata::Test);
                    guard.filled().await;
                }
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_cache_hit, bench_cache_insert);
criterion_main!(benches);
