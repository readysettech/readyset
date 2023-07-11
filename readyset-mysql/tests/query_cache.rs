use mysql_async::prelude::*;
use mysql_async::{Conn, Result, Row, Statement};
use readyset_adapter::backend::{MigrationMode, QueryInfo, UnsupportedSetMode};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{last_query_info, MySQLAdapter};
use readyset_client_test_helpers::{sleep, TestBuilder};
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use serial_test::serial;

pub async fn setup(
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
    .fallback(fallback)
    .query_status_cache(query_status_cache)
    .migration_mode(migration_mode)
    .build::<MySQLAdapter>()
    .await
}

// With in_request_path migration and fallback, an supported query should execute on ReadySet
// and be marked allowed on completion, an unsupported query should execute on ReadySet
// and then fallback, and be marked denied.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn in_request_path_query_with_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::InRequestPath,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t").await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset
    );

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    let info = last_query_info(&mut conn).await;
    assert_eq!(info.destination, QueryDestination::ReadysetThenUpstream);
    assert!(!info.noria_error.is_empty());

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    shutdown_tx.shutdown().await;
}

// With in_request_path query mode without fallback, a supported query should execute on ReadySet
// and be marked allowed on completion, an unsupported query should execute on ReadySet
// and then fallback, and be marked denied.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn in_request_path_query_without_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        false, // fallback disabled
        MigrationMode::InRequestPath,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t").await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()").await;
    res.unwrap_err(); // Unable to handle this unsupported query.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);

    shutdown_tx.shutdown().await;
}

// With the out_of_band query mode and fallback, both supported and unsupported
// queries should be executed against fallback, they should not be added to the
// allow list. Performing an explicit migration allows the query to be added to
// the allow list on next exeution.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn out_of_band_query_with_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = NOW()").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<Vec<Row>> = conn.query("CREATE CACHE test FROM SELECT * FROM t").await;
    res.unwrap();

    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t").await;
    res.unwrap(); // Executed successfully against noria.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn autocommit_state_query() {
    let _query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        _query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Proxy,
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

    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    // We should initially go to ReadySet because no unsupported SET has been sent, and we can
    // support this query.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    conn.query_drop("SELECT y FROM test where x = 4")
        .await
        .unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    conn.query_drop("SET autocommit=0").await.unwrap();
    sleep().await;

    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    // After turning off autocommit we should send to upstream because this is unsupported.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // However our ALWAYS query should still go to ReadySet.
    conn.query_drop("SELECT y FROM test where x = 4")
        .await
        .unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    conn.query_drop("SET autocommit=1").await.unwrap();
    sleep().await;

    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    // Turning autocommit back on should cause us to then send back to ReadySet.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    // Same for our ALWAYS query.
    conn.query_drop("SELECT y from test where x = 4")
        .await
        .unwrap();
    sleep().await;

    // Turning autocommit back on should cause us to then send back to ReadySet.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    shutdown_tx.shutdown().await;
}

// This is particularly challenging to test because the way that mysql_async works, is if the
// statement has ever been prepared on the connection, it will refuse to prepare it again and
// re-use the old prepare. This makes sense from an efficiency perspective but makes it challenging
// for us to test behavior prior to autocommit being turned off, and compare that to behavior
// after.
// For that reason this test uses standard query_drop in the before case.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn autocommit_prepare_execute() {
    let _query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        _query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Proxy,
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

    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    // We should initially go to ReadySet because no unsupported SET has been sent, and we can
    // support this query.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    conn.query_drop("SELECT y FROM test WHERE x = 4")
        .await
        .unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    conn.query_drop("SET autocommit=0").await.unwrap();
    sleep().await;

    let res: Result<_> = conn.prep("SELECT * FROM test").await;
    conn.exec_drop(res.unwrap(), ()).await.unwrap();
    sleep().await;

    // After turning off autocommit we should send to upstream because this is unsupported.
    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Upstream);

    // However our ALWAYS query should still go to ReadySet.
    let res: Result<_> = conn.prep("SELECT y FROM test WHERE x = ?").await;
    conn.exec_drop(res.unwrap(), (4,)).await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Readyset);

    shutdown_tx.shutdown().await;
}

// With in_request_path migration and fallback, a supported query should execute on ReadySet
// and be marked allowed on completion, an unsupported query should execute on ReadySet
// and then fallback, and be marked denied.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn in_request_path_prep_exec_with_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::InRequestPath,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<_> = conn.prep("SELECT * FROM t").await;
    assert!(res.is_ok());
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Both
    );

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset
    );

    let res: Result<_> = conn.prep("SELECT * FROM t WHERE a = NOW() AND b = 1").await;
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Both
    );

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<Statement> = conn
        .prep("SELECT * FROM t WHERE a = NOW() AND  b = 2")
        .await;
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    shutdown_tx.shutdown().await;
}

// With in_request_path query mode without fallback, a supported query should execute on ReadySet
// and be marked allowed on completion, an unsupported query should execute on ReadySet
// and then fallback, and be marked denied.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn in_request_path_prep_without_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        false, // fallback disabled
        MigrationMode::InRequestPath,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<_> = conn.prep("SELECT * FROM t").await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<_> = conn.prep("SELECT * FROM t WHERE a = NOW()").await;
    res.unwrap_err(); // Unable to handle this unsupported query.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 1);

    shutdown_tx.shutdown().await;
}

// With the out_of_band query mode and fallback, both supported and unsupported
// queries should be executed against fallback, they should not be added to the
// allow list. Performing an explicit migration allows the query to be added to
// the allow list on next exeution.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn out_of_band_prep_exec_with_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Statement> = conn.prep("SELECT * FROM t").await;
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Both
    );
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<Statement> = conn.prep("SELECT * FROM t WHERE a = NOW()").await;
    assert!(res.is_ok()); // Executed successfully against fallback.
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Both
    );
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    let res: Result<Vec<Row>> = conn.exec(res.unwrap(), ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let res: Result<_> = conn
        .query_drop("CREATE CACHE test FROM SELECT * FROM t")
        .await;
    res.unwrap();

    let stmt: Statement = conn
        .prep("SELECT * FROM t")
        .await
        .expect("Executed successfully against noria");
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset
    );

    let res: Result<Vec<Row>> = conn.exec(&stmt, ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset
    );

    let res: Result<_> = conn.query_drop("DROP CACHE test").await;
    res.unwrap();

    // Should go back to fallback after we dropped the query
    let res: Result<Vec<Row>> = conn.exec(&stmt, ()).await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 1);
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    shutdown_tx.shutdown().await;
}

// Allow migrations within the request path. Both migrations, the CREATE QUERY
// CACHE statement and the SELECT * FROM t should be rewritten to be the same
// entry in the query status cache. Otherwise we would have more than one entry
// in the allow list.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn in_request_path_rewritten_query_without_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        false, // fallback disabled
        MigrationMode::InRequestPath,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Vec<Row>> = conn
        .query("CREATE CACHE test FROM SELECT * FROM t WHERE a = ? AND b = ?")
        .await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = 4 AND b = 5").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    shutdown_tx.shutdown().await;
}

// With the out_of_band query mode without fallback, queries that are not
// explicitely migrated are denied. We verify that two queries end up being
// cached as the same query in the query status cache. Otherwise, the second
// query will fail as being disallowed.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn out_of_band_rewritten_query_without_fallback() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        false, // fallback disabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    sleep().await;
    let res: Result<Vec<Row>> = conn
        .query("CREATE CACHE test FROM SELECT * FROM t WHERE a = ? AND b = ?")
        .await;
    res.unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);
    let res: Result<Vec<Row>> = conn.query("SELECT * FROM t WHERE a = 4 AND b = 5").await;
    res.unwrap(); // Executed successfully against fallback.
    assert_eq!(query_status_cache.allow_list().len(), 1);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_all_caches() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        false, // fallback disabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;

    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();

    sleep().await;
    conn.query_drop("CREATE CACHE test FROM SELECT a, b FROM t WHERE a = ? AND b = ?")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE test_2 FROM SELECT a FROM t WHERE a = ?")
        .await
        .unwrap();
    assert_eq!(query_status_cache.allow_list().len(), 2);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    conn.query_drop("DROP ALL CACHES").await.unwrap();

    assert_eq!(query_status_cache.allow_list().len(), 0);
    assert_eq!(query_status_cache.deny_list().len(), 0);

    shutdown_tx.shutdown().await;
}
