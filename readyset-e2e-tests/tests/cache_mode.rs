use assert_matches::assert_matches;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use tokio::test;

use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder, derive_test_name,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_server::CacheMode;
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    readyset
        .query_drop("CREATE CACHE FROM SELECT a FROM foo")
        .await
        .unwrap();

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_deep() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Deep);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    eventually! {
        readyset
            .query_drop("CREATE CACHE FROM SELECT a FROM foo")
            .await
            .is_ok()
    };

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_matches!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Readyset(..)
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_deep_then_shallow() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::DeepThenShallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    eventually! {
        readyset
            .query_drop("CREATE CACHE FROM SELECT a, RAND() FROM foo")
            .await
            .is_ok()
    };

    readyset
        .query_drop("SELECT a, RAND() FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a, RAND() FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

// In-request-path + cache_mode=shallow should auto-create a shallow cache on
// first SELECT, without an explicit CREATE CACHE statement or hint.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow_auto_create_in_request_path() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // No CREATE CACHE, no hint — the adapter should auto-create a shallow
    // cache because we are in MigrationMode::InRequestPath with
    // CacheMode::Shallow. First execution is a miss and populates the
    // cache via upstream.
    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    // Second execution should be served from the auto-created shallow cache.
    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_deep_type() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Deep);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    let row: Row = eventually! {
        run_test: {
            readyset
                .query_first("CREATE CACHE my_deep FROM SELECT a FROM foo WHERE a = ?")
                .await
                .unwrap()
        },
        then_assert: |result| {
            assert!(result.is_some(), "expected a result row");
            result.unwrap()
        }
    };

    let query_id: String = row.get("query_id").unwrap();
    let cache_name: String = row.get("name").unwrap();
    let query: String = row.get("query").unwrap();
    let cache_type: String = row.get("cache_type").unwrap();

    assert!(query_id.starts_with("q_"), "query id should start with q_");
    assert_eq!(cache_name, "my_deep");
    assert!(!query.is_empty());
    assert_eq!(cache_type, "deep");

    let row: Row = readyset
        .query_first("SELECT query_id, name, always FROM readyset.deep_caches WHERE name = 'my_deep'")
        .await
        .unwrap()
        .expect("expected one row in readyset.deep_caches");
    let vrel_query_id: String = row.get("query_id").unwrap();
    let vrel_name: String = row.get("name").unwrap();
    let vrel_always: bool = row.get("always").unwrap();
    assert_eq!(vrel_query_id, query_id);
    assert_eq!(vrel_name, "my_deep");
    assert!(!vrel_always);

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_shallow_type() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    let row: Row = readyset
        .query_first("CREATE CACHE my_shallow FROM SELECT a FROM foo")
        .await
        .unwrap()
        .expect("expected a result row");

    let query_id: String = row.get("query_id").unwrap();
    let cache_name: String = row.get("name").unwrap();
    let query: String = row.get("query").unwrap();
    let cache_type: String = row.get("cache_type").unwrap();

    assert!(query_id.starts_with("q_"), "query id should start with q_");
    assert_eq!(cache_name, "my_shallow");
    assert!(!query.is_empty());
    assert_eq!(cache_type, "shallow");

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_shallow_type_on_fallback() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::DeepThenShallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // RAND() is not supported by deep caching, so this will fall back to shallow.
    let row: Row = eventually! {
        run_test: {
            readyset
                .query_first("CREATE CACHE FROM SELECT a, RAND() FROM foo")
                .await
                .unwrap()
        },
        then_assert: |result| {
            assert!(result.is_some(), "expected a result row");
            result.unwrap()
        }
    };

    let cache_type: String = row.get("cache_type").unwrap();
    assert_eq!(cache_type, "shallow");

    shutdown_tx.shutdown().await;
}
