use assert_matches::assert_matches;
use mysql_async::prelude::*;
use mysql_async::Conn;
use readyset_adapter::backend::{MigrationMode, QueryInfo, UnsupportedSetMode};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::{sleep, TestBuilder};
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use test_utils::tags;

async fn setup_fallback() -> (mysql_async::Opts, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    TestBuilder::new(BackendBuilder::new().require_authentication(false))
        .fallback(true)
        .build::<MySQLAdapter>()
        .await
}

async fn setup_with_cache(
    query_status_cache: &'static QueryStatusCache,
    fallback: bool,
    migration_mode: MigrationMode,
    set_mode: UnsupportedSetMode,
) -> (mysql_async::Opts, Handle, ShutdownSender) {
    TestBuilder::new(
        BackendBuilder::default()
            .require_authentication(false)
            .unsupported_set_mode(set_mode),
    )
    .replicate(fallback)
    .fallback(fallback)
    .query_status_cache(query_status_cache)
    .migration_mode(migration_mode)
    .build::<MySQLAdapter>()
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn set_autocommit() {
    let (opts, _handle, shutdown_tx) = setup_fallback().await;
    let mut conn = Conn::new(opts).await.unwrap();
    // SET autocommit=0 is supported under Error mode (the default): it transitions ProxyState
    // to AutocommitOff, routing all queries upstream. SET autocommit=1 returns to Fallback.
    // Full query-routing validation is covered by autocommit_lifecycle() tests below.
    conn.query_drop("SET @@SESSION.autocommit = 1;")
        .await
        .unwrap();
    conn.query_drop("SET @@SESSION.autocommit = 0;")
        .await
        .unwrap();
    conn.query_drop("SET @@LOCAL.autocommit = 1;")
        .await
        .unwrap();
    conn.query_drop("SET @@LOCAL.autocommit = 0;")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

// Shared helper covering the full autocommit lifecycle:
// 1. Create table, insert, create caches (regular + ALWAYS)
// 2. Verify initial query goes to ReadySet
// 3. SET autocommit=0
// 4. Verify regular query goes Upstream
// 5. Verify ALWAYS query still goes to ReadySet
// 6. SET autocommit=1
// 7. Verify regular query returns to ReadySet
// 8. Shutdown
async fn autocommit_lifecycle(set_mode: UnsupportedSetMode) {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup_with_cache(
        query_status_cache,
        true,
        MigrationMode::OutOfBand,
        set_mode,
    )
    .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM test")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE ALWAYS FROM SELECT y FROM test where x = ?")
        .await
        .unwrap();
    sleep().await;

    // Queries should initially go to ReadySet.
    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    // Disable autocommit.
    conn.query_drop("SET autocommit=0").await.unwrap();
    sleep().await;

    // Regular queries should now route upstream.
    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // ALWAYS queries should still go to ReadySet.
    conn.query_drop("SELECT y FROM test where x = 4")
        .await
        .unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    // Re-enable autocommit.
    conn.query_drop("SET autocommit=1").await.unwrap();
    sleep().await;

    // Queries should return to ReadySet.
    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn autocommit_state_query() {
    autocommit_lifecycle(UnsupportedSetMode::Proxy).await;
}

// Verify that SET autocommit=0/1 works under the default Error mode, not just Proxy mode.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn autocommit_error_mode_query_routing() {
    autocommit_lifecycle(UnsupportedSetMode::Error).await;
}

// Verify that SET autocommit=0/1 works under Allow mode.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn autocommit_allow_mode_query_routing() {
    autocommit_lifecycle(UnsupportedSetMode::Allow).await;
}

// Verify that SET autocommit=1 during an explicit transaction (BEGIN) returns to Fallback.
// MySQL treats SET autocommit=1 inside a transaction as an implicit COMMIT.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn autocommit_on_during_transaction() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup_with_cache(
        query_status_cache,
        true,
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t_ac (x int)").await.unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO t_ac (x) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM t_ac")
        .await
        .unwrap();
    sleep().await;

    // Verify initial query goes to ReadySet.
    conn.query_drop("SELECT * FROM t_ac").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    // Start an explicit transaction — queries should go upstream.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("SELECT * FROM t_ac").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // SET autocommit=1 during transaction should implicitly commit and return to Fallback.
    conn.query_drop("SET autocommit=1").await.unwrap();
    sleep().await;

    conn.query_drop("SELECT * FROM t_ac").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    shutdown_tx.shutdown().await;
}
