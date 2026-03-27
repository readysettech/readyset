use assert_matches::assert_matches;
use mysql_async::prelude::Queryable;
use tokio::test;

use readyset_adapter::backend::BackendBuilder;
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

    let test_name = derive_test_name!();
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

    let test_name = derive_test_name!();
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

    let test_name = derive_test_name!();
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
