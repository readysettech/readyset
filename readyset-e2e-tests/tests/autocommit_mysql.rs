use assert_matches::assert_matches;
use mysql_async::prelude::*;
use mysql_async::{Conn, Row};
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

/// Regression test for the MonicaCRM-style hanging query bug (REA-4853).
///
/// `mysql-async` (used by ReadySet as a client to upstream MySQL) can incorrectly parse
/// COM_STMT_PREPARE response packets as OK_Packet, producing garbage status flags. When the
/// spurious `SERVER_MORE_RESULTS_EXISTS` (0x0008) flag leaks into the response sent to the
/// downstream client, the client blocks forever waiting for additional result sets that never
/// arrive.
///
/// The fix (REA-4888) has ReadySet construct its own status flags from ProxyState, so the
/// garbage flag is never forwarded. This test exercises the exact scenario: prepare a
/// statement on the proxied path, execute it, and verify the client gets results without
/// hanging. If the execute completes at all, the bug is fixed; the old behavior was an
/// indefinite hang.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn prepared_stmt_execute_no_hang() {
    let (opts, _handle, shutdown_tx) = setup_fallback().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE rea4853 (id int PRIMARY KEY, val text)")
        .await
        .unwrap();
    sleep().await;

    // INSERT is never cached by ReadySet, so it always takes the proxy/fallback path.
    // This exercises the COM_STMT_PREPARE -> COM_STMT_EXECUTE flow through the proxy,
    // which is where the garbage status flags from mysql-async caused the hang.
    let insert_stmt = conn
        .prep("INSERT INTO rea4853 (id, val) VALUES (?, ?)")
        .await
        .unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        conn.exec_drop(&insert_stmt, (1, "hello")),
    )
    .await;
    assert!(
        result.is_ok(),
        "prepared INSERT execution timed out — \
         likely a regression of the SERVER_MORE_RESULTS_EXISTS status flag bug (REA-4853)"
    );
    result.unwrap().unwrap();

    // Also exercise a SELECT via the proxy path. Use a range query with a parameterized
    // comparison, which ReadySet does not cache, so it falls back to upstream.
    let select_stmt = conn
        .prep("SELECT id, val FROM rea4853 WHERE id > ?")
        .await
        .unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        conn.exec::<(i32, String), _, _>(&select_stmt, (0,)),
    )
    .await;
    assert!(
        result.is_ok(),
        "prepared SELECT execution timed out — \
         likely a regression of the SERVER_MORE_RESULTS_EXISTS status flag bug (REA-4853)"
    );
    let rows = result.unwrap().unwrap();
    assert_eq!(rows, vec![(1, "hello".to_string())]);

    shutdown_tx.shutdown().await;
}

// Verify that the MySQL server status flags (SERVER_STATUS_AUTOCOMMIT and
// SERVER_STATUS_IN_TRANS) in response packets accurately reflect the connection's
// transaction and autocommit state across all relevant transitions.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn server_status_flags_reflect_transaction_state() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup_with_cache(
        query_status_cache,
        true,
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    // Set up a table and cache so we can exercise cached-result paths too.
    conn.query_drop("CREATE TABLE t_flags (x INT)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO t_flags (x) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM t_flags")
        .await
        .unwrap();
    sleep().await;

    use mysql_async::consts::StatusFlags;

    let ac = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
    let tx = StatusFlags::SERVER_STATUS_IN_TRANS;

    // 1. After initial connect: AUTOCOMMIT is set, IN_TRANS is not.
    //    Run a query to get a fresh response with status flags.
    let _: Vec<Row> = conn.query("SELECT * FROM t_flags").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After connect: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After connect: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    // 2. After BEGIN: both AUTOCOMMIT and IN_TRANS are set.
    conn.query_drop("BEGIN").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After BEGIN: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        flags.contains(tx),
        "After BEGIN: SERVER_STATUS_IN_TRANS should be set, got {flags:?}"
    );
    // Also verify on a follow-up query:
    let _: Vec<Row> = conn.query("SELECT 1").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After BEGIN follow-up: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        flags.contains(tx),
        "After BEGIN follow-up: SERVER_STATUS_IN_TRANS should be set, got {flags:?}"
    );

    // 3. After COMMIT: AUTOCOMMIT set, IN_TRANS cleared.
    conn.query_drop("COMMIT").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After COMMIT: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After COMMIT: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );
    // Also verify on a follow-up query:
    let _: Vec<Row> = conn.query("SELECT * FROM t_flags").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After COMMIT follow-up: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After COMMIT follow-up: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    // 4. After SET autocommit=0: IN_TRANS set, AUTOCOMMIT cleared.
    conn.query_drop("SET autocommit=0").await.unwrap();
    let flags = conn.status();
    assert!(
        !flags.contains(ac),
        "After SET autocommit=0: SERVER_STATUS_AUTOCOMMIT should NOT be set, got {flags:?}"
    );
    assert!(
        flags.contains(tx),
        "After SET autocommit=0: SERVER_STATUS_IN_TRANS should be set, got {flags:?}"
    );
    // Also verify on a follow-up query:
    let _: Vec<Row> = conn.query("SELECT * FROM t_flags").await.unwrap();
    let flags = conn.status();
    assert!(
        !flags.contains(ac),
        "After SET autocommit=0 follow-up: SERVER_STATUS_AUTOCOMMIT should NOT be set, got {flags:?}"
    );
    assert!(
        flags.contains(tx),
        "After SET autocommit=0 follow-up: SERVER_STATUS_IN_TRANS should be set, got {flags:?}"
    );

    // 5. After SET autocommit=1: AUTOCOMMIT set, IN_TRANS cleared.
    conn.query_drop("SET autocommit=1").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After SET autocommit=1: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After SET autocommit=1: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );
    // Also verify on a follow-up query:
    let _: Vec<Row> = conn.query("SELECT * FROM t_flags").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After SET autocommit=1 follow-up: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After SET autocommit=1 follow-up: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    // 6. BEGIN + ROLLBACK: IN_TRANS cleared after ROLLBACK.
    conn.query_drop("BEGIN").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After BEGIN (2): SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        flags.contains(tx),
        "After BEGIN (2): SERVER_STATUS_IN_TRANS should be set, got {flags:?}"
    );

    conn.query_drop("ROLLBACK").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After ROLLBACK: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After ROLLBACK: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    shutdown_tx.shutdown().await;
}

// Verify that COM_RESET_CONNECTION restores ProxyState to Fallback, so queries route back
// to ReadySet after being in InTransaction or AutocommitOff states.
// Regression test for the fix in commit a8fc1a346a (REA-6333).
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn reset_connection_restores_proxy_state() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup_with_cache(
        query_status_cache,
        true,
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t_reset (x INT)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO t_reset (x) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM t_reset")
        .await
        .unwrap();
    sleep().await;

    use mysql_async::consts::StatusFlags;

    let ac = StatusFlags::SERVER_STATUS_AUTOCOMMIT;
    let tx = StatusFlags::SERVER_STATUS_IN_TRANS;

    // Verify initial query goes to ReadySet.
    conn.query_drop("SELECT * FROM t_reset").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    // --- Test 1: Reset from InTransaction (BEGIN) ---

    conn.query_drop("BEGIN").await.unwrap();

    // Query should go upstream while in a transaction.
    conn.query_drop("SELECT * FROM t_reset").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // COM_RESET_CONNECTION should restore ProxyState to Fallback.
    let reset_supported = conn.reset().await.unwrap();
    assert!(
        reset_supported,
        "COM_RESET_CONNECTION should be supported by the server"
    );

    // Status flags should reflect autocommit on, not in transaction.
    let _: Vec<Row> = conn.query("SELECT * FROM t_reset").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After reset from InTransaction: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After reset from InTransaction: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    // Query should route back to ReadySet.
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    // --- Test 2: Reset from AutocommitOff ---

    conn.query_drop("SET autocommit=0").await.unwrap();

    // Query should go upstream with autocommit off.
    conn.query_drop("SELECT * FROM t_reset").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // COM_RESET_CONNECTION should restore ProxyState to Fallback.
    let reset_supported = conn.reset().await.unwrap();
    assert!(
        reset_supported,
        "COM_RESET_CONNECTION should be supported by the server"
    );

    // Status flags should reflect autocommit on, not in transaction.
    let _: Vec<Row> = conn.query("SELECT * FROM t_reset").await.unwrap();
    let flags = conn.status();
    assert!(
        flags.contains(ac),
        "After reset from AutocommitOff: SERVER_STATUS_AUTOCOMMIT should be set, got {flags:?}"
    );
    assert!(
        !flags.contains(tx),
        "After reset from AutocommitOff: SERVER_STATUS_IN_TRANS should NOT be set, got {flags:?}"
    );

    // Query should route back to ReadySet.
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    shutdown_tx.shutdown().await;
}
