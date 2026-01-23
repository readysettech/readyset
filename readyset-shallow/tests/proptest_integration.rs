use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use readyset_client::consensus::CacheDDLRequest;
use readyset_client::query::QueryId;
use readyset_shallow::{CacheManager, CacheResult, EvictionPolicy, MySqlMetadata, QueryMetadata};
use readyset_sql::ast::ShallowCacheQuery;
use readyset_util::SizeOf;

fn test_metadata() -> QueryMetadata {
    QueryMetadata::MySql(MySqlMetadata {
        columns: Arc::new([]),
    })
}

async fn insert_value<K, V>(mut result: CacheResult<K, V>, values: Vec<V>, metadata: QueryMetadata)
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let guard = result.guard();
    values.into_iter().for_each(|v| guard.push(v));
    guard.set_metadata(metadata);
    guard.filled().await;
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
    name: Option<readyset_sql::ast::Relation>,
    query_id: Option<QueryId>,
    policy: EvictionPolicy,
) -> Result<(), readyset_errors::ReadySetError>
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

async fn run_insert_then_retrieve(keys: Vec<String>) -> Result<(), TestCaseError> {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    for key in &keys {
        let result = manager.get_or_start_insert(&query_id, key.clone()).await;
        insert_value(result, vec![format!("value_{key}")], test_metadata()).await;
    }

    for key in &keys {
        let result = manager.get_or_start_insert(&query_id, key.clone()).await;
        prop_assert!(result.is_hit());
        prop_assert_eq!(
            &result.result().values,
            &vec![format!("value_{key}")].into()
        );
    }

    Ok(())
}

async fn insert_keys(
    manager: &CacheManager<String, String>,
    query_id: &QueryId,
    keys: &[String],
    value_prefix: &str,
) {
    for key in keys {
        let result = manager.get_or_start_insert(query_id, key.clone()).await;
        insert_value(
            result,
            vec![format!("{value_prefix}_{key}")],
            test_metadata(),
        )
        .await;
    }
}

async fn verify_keys(
    manager: &CacheManager<String, String>,
    query_id: &QueryId,
    keys: &[String],
    value_prefix: &str,
) -> Result<(), TestCaseError> {
    for key in keys {
        let result = manager.get_or_start_insert(query_id, key.clone()).await;
        prop_assert!(result.is_hit());
        prop_assert_eq!(
            &result.result().values,
            &vec![format!("{value_prefix}_{key}")].into()
        );
    }
    Ok(())
}

async fn run_cache_isolation(keys1: Vec<String>, keys2: Vec<String>) -> Result<(), TestCaseError> {
    let manager = CacheManager::<String, String>::new(None);
    let query_id_1 = QueryId::from_unparsed_select("SELECT * FROM table1");
    let query_id_2 = QueryId::from_unparsed_select("SELECT * FROM table2");

    create_test_cache(&manager, None, Some(query_id_1), test_policy()).unwrap();
    create_test_cache(&manager, None, Some(query_id_2), test_policy()).unwrap();

    insert_keys(&manager, &query_id_1, &keys1, "cache1").await;
    insert_keys(&manager, &query_id_2, &keys2, "cache2").await;

    verify_keys(&manager, &query_id_1, &keys1, "cache1").await?;
    verify_keys(&manager, &query_id_2, &keys2, "cache2").await?;

    Ok(())
}

async fn run_no_data_loss(keys: HashSet<String>) -> Result<(), TestCaseError> {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    for key in &keys {
        let result = manager.get_or_start_insert(&query_id, key.clone()).await;
        insert_value(result, vec![format!("value_{key}")], test_metadata()).await;
    }

    let mut found = HashSet::new();
    for key in &keys {
        let result = manager.get_or_start_insert(&query_id, key.clone()).await;
        prop_assert!(result.is_hit());
        prop_assert_eq!(
            &result.result().values,
            &vec![format!("value_{key}")].into()
        );
        found.insert(key.clone());
    }

    prop_assert_eq!(found.len(), keys.len());
    Ok(())
}

async fn run_idempotent_reads(key: String, reads: usize) -> Result<(), TestCaseError> {
    let manager = CacheManager::<String, String>::new(None);
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    let result = manager.get_or_start_insert(&query_id, key.clone()).await;
    insert_value(result, vec![format!("value_{key}")], test_metadata()).await;

    let expected_values = vec![format!("value_{key}")];
    for _ in 0..reads {
        let result = manager.get_or_start_insert(&query_id, key.clone()).await;
        prop_assert!(result.is_hit());
        prop_assert_eq!(&result.result().values, &expected_values.clone().into());
    }

    Ok(())
}

async fn run_memory_accounting(value_sizes: Vec<usize>) -> Result<(), TestCaseError> {
    let manager = CacheManager::<String, String>::new(Some(10240));
    let query_id = QueryId::from_unparsed_select("SELECT * FROM test");

    create_test_cache(&manager, None, Some(query_id), test_policy()).unwrap();

    for (i, size) in value_sizes.iter().enumerate() {
        let key = format!("key_{i}");
        let value = "x".repeat(*size);
        let result = manager.get_or_start_insert(&query_id, key).await;
        insert_value(result, vec![value], test_metadata()).await;
    }

    manager.run_pending_tasks(&query_id).await;

    let mut found = 0;
    for i in 0..value_sizes.len() {
        let key = format!("key_{i}");
        let result = manager.get_or_start_insert(&query_id, key).await;
        if result.is_hit() {
            found += 1;
        }
    }

    prop_assert!(found > 0, "Expected at least one entry in cache");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_insert_then_retrieve(keys in prop::collection::vec("[a-z]+", 1..20)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_insert_then_retrieve(keys))?;
    }

    #[test]
    fn prop_cache_isolation(
        keys1 in prop::collection::vec("[a-z]+", 1..10),
        keys2 in prop::collection::vec("[A-Z]+", 1..10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_cache_isolation(keys1, keys2))?;
    }

    #[test]
    fn prop_no_data_loss(keys in prop::collection::hash_set("[a-z]{1,5}", 1..15)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_no_data_loss(keys))?;
    }

    #[test]
    fn prop_idempotent_reads(key in "[a-z]+", reads in 2usize..10) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_idempotent_reads(key, reads))?;
    }

    #[test]
    fn prop_memory_accounting(value_sizes in prop::collection::vec(10usize..100, 1..10)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_memory_accounting(value_sizes))?;
    }
}
