use mysql_async::prelude::*;
use mysql_async::{Conn, Result, Row, Statement};
use readyset_adapter::backend::{MigrationMode, QueryInfo, UnsupportedSetMode};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{last_query_info, MySQLAdapter};
use readyset_client_test_helpers::{sleep, TestBuilder};
use readyset_server::Handle;
use readyset_sql::ast::Relation;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::tags;

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
    .replicate(fallback)
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
#[tags(serial, mysql_upstream)]
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
#[tags(serial, mysql_upstream)]
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
// the allow list on next execution.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
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
#[tags(serial, mysql_upstream)]
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
#[tags(serial, mysql_upstream)]
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
#[tags(serial, mysql_upstream)]
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
#[tags(serial, mysql_upstream)]
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
// the allow list on next execution.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
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
    assert_eq!(query_status_cache.deny_list().len(), 0);
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
#[tags(serial, mysql_upstream)]
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
// explicitly migrated are denied. We verify that two queries end up being
// cached as the same query in the query status cache. Otherwise, the second
// query will fail as being disallowed.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
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

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_binlog_transaction_compression() {
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
    conn.query_drop("CREATE TABLE t2 (a INT, b INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t2 VALUES  (1,1)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t2 VALUES  (2,1)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE test FROM SELECT a, b FROM t WHERE a = ?")
        .await
        .unwrap();

    conn.query_drop("CREATE CACHE test2 FROM SELECT a, b FROM t2 WHERE a = ?")
        .await
        .unwrap();

    // Ensure we are processing record in order.
    conn.query_drop("START TRANSACTION").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES  (1,1)")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM t WHERE a = 1").await.unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    sleep().await;
    let row: Vec<(u32, u32)> = conn.query("SELECT a, b FROM t WHERE a = 1").await.unwrap();
    assert_eq!(row.len(), 0);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    // Ensure we are processing records from multiple tables.
    conn.query_drop("START TRANSACTION").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES  (3,1)")
        .await
        .unwrap();
    conn.query_drop("UPDATE t2 SET b = 2 WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM t2 WHERE a = 2").await.unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    sleep().await;

    let row: Vec<(u32, u32)> = conn.query("SELECT a, b FROM t WHERE a = 3").await.unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    let row: Vec<(u32, u32)> = conn.query("SELECT a, b FROM t2 WHERE a = 1").await.unwrap();
    assert_eq!(row.len(), 1);
    assert!(row.iter().any(|(a, b)| *a == 1 && *b == 2));
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    let row: Vec<(u32, u32)> = conn.query("SELECT a, b FROM t2 WHERE a = 2").await.unwrap();
    assert_eq!(row.len(), 0);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_char_padding_lookup() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE `col_pad_lookup` (
        id int NOT NULL PRIMARY KEY,
        c CHAR(3)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO `col_pad_lookup` VALUES (1, 'ࠈࠈ');")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO `col_pad_lookup` VALUES (2, 'A');")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO `col_pad_lookup` VALUES (3, 'AAA');")
        .await
        .unwrap();
    sleep().await;
    conn.query_drop("CREATE CACHE test FROM SELECT id, c FROM col_pad_lookup WHERE c = ?")
        .await
        .unwrap();
    let row: Vec<(u32, String)> = conn
        .query("SELECT id, c FROM col_pad_lookup WHERE c = 'ࠈࠈ'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "ࠈࠈ");

    let row: Vec<(u32, String)> = conn
        .query("SELECT id, c FROM col_pad_lookup WHERE c = 'A'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "A");

    let row: Vec<(u32, String)> = conn
        .query("SELECT id, c FROM col_pad_lookup WHERE c = 'AAA'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "AAA");
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_binary_padding_lookup() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, _handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE `col_pad_bin_lookup` (
        id int NOT NULL PRIMARY KEY,
        b BINARY(3)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
    )
    .await
    .unwrap();
    conn.query_drop("INSERT INTO `col_pad_bin_lookup` VALUES (1, 'ࠈ');")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO `col_pad_bin_lookup` VALUES (2, '¥');")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO `col_pad_bin_lookup` VALUES (3, 'A');")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO `col_pad_bin_lookup` VALUES (4, 'A¥');")
        .await
        .unwrap();

    sleep().await;
    conn.query_drop("CREATE CACHE test FROM SELECT id, b FROM col_pad_bin_lookup WHERE b = ?")
        .await
        .unwrap();
    let row: Vec<(u32, String)> = conn
        .query("SELECT id, b FROM col_pad_bin_lookup WHERE b = 'ࠈ'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "ࠈ");

    let row: Vec<(u32, String)> = conn
        .query("SELECT id, b FROM col_pad_bin_lookup WHERE b = '¥\\0'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "¥\0");

    let row: Vec<(u32, String)> = conn
        .query("SELECT id, b FROM col_pad_bin_lookup WHERE b = 'A\\0\\0'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "A\0\0");

    let row: Vec<(u32, String)> = conn
        .query("SELECT id, b FROM col_pad_bin_lookup WHERE b = 'A¥'")
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(&mut conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, "A¥");
    shutdown_tx.shutdown().await;
}

async fn resnapshot_table(table: Relation, handle: &mut Handle, conn: &mut Conn) {
    let tables = handle.tables().await.unwrap();
    let current_table_id = tables.get(&table).unwrap();
    conn.query_drop(format!(
        "ALTER READYSET RESNAPSHOT TABLE {}",
        table.display_unquoted()
    ))
    .await
    .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    eventually!(
        let new_tables = handle.tables().await.unwrap();
        let new_table_id = new_tables.get(&table);
        new_table_id.is_some() && new_table_id.unwrap() != current_table_id
    )
}

async fn test_sensitiveness_lookup_inner(conn: &mut Conn, key_upper: &str, key_lower: &str) {
    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, varchar_ci FROM col_sensitiveness_lookup WHERE varchar_ci = '{key_lower}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);
    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, varchar_ci FROM col_sensitiveness_lookup WHERE varchar_ci = '{key_upper}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, varchar_cs FROM col_sensitiveness_lookup WHERE varchar_cs = '{key_lower}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 0);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, varchar_cs FROM col_sensitiveness_lookup WHERE varchar_cs = '{key_upper}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, char_ci FROM col_sensitiveness_lookup WHERE char_ci = '{key_lower}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, char_ci FROM col_sensitiveness_lookup WHERE char_ci = '{key_upper}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, char_cs FROM col_sensitiveness_lookup WHERE char_cs = '{key_lower}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 0);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);

    let row: Vec<(u32, String)> = conn
        .query(format!(
            "SELECT id, char_cs FROM col_sensitiveness_lookup WHERE char_cs = '{key_upper}'"
        ))
        .await
        .unwrap();
    assert_eq!(row.len(), 1);
    let last_status = last_query_info(conn).await;
    assert_eq!(last_status.destination, QueryDestination::Readyset);
    assert_eq!(row[0].1, key_upper);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_sensitiveness_lookup() {
    let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));
    let (opts, mut handle, shutdown_tx) = setup(
        query_status_cache,
        true, // fallback enabled
        MigrationMode::OutOfBand,
        UnsupportedSetMode::Error,
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE `col_sensitiveness_lookup` (
        id int NOT NULL PRIMARY KEY,
        varchar_ci VARCHAR(255) COLLATE utf8mb4_0900_ai_ci,
        varchar_cs VARCHAR(255) COLLATE utf8mb4_0900_as_cs,
        char_ci CHAR(4) COLLATE utf8mb4_0900_ai_ci,
        char_cs CHAR(4) COLLATE utf8mb4_0900_as_cs
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
    )
    .await
    .unwrap();

    conn.query_drop("INSERT INTO `col_sensitiveness_lookup` VALUES (1, 'A', 'A', 'A', 'A');")
        .await
        .unwrap();
    sleep().await;
    let table = Relation {
        schema: Some("noria".into()),
        name: "col_sensitiveness_lookup".into(),
    };

    // we want to test the code path during the snapshot. Force a snapshot here.
    resnapshot_table(table, &mut handle, &mut conn).await;
    conn.query_drop("CREATE CACHE c_varchar_ci FROM SELECT id, varchar_ci FROM col_sensitiveness_lookup WHERE varchar_ci = ?;")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE c_varchar_cs FROM SELECT id, varchar_cs FROM col_sensitiveness_lookup WHERE varchar_cs = ?;").await.unwrap();
    conn.query_drop("CREATE CACHE c_char_ci FROM SELECT id, char_ci FROM col_sensitiveness_lookup WHERE char_ci = ?;").await.unwrap();
    conn.query_drop("CREATE CACHE c_char_cs FROM SELECT id, char_cs FROM col_sensitiveness_lookup WHERE char_cs = ?;").await.unwrap();

    test_sensitiveness_lookup_inner(&mut conn, "A", "a").await;

    conn.query_drop("INSERT INTO `col_sensitiveness_lookup` VALUES (2, 'B', 'B', 'B', 'B');")
        .await
        .unwrap();
    sleep().await;
    test_sensitiveness_lookup_inner(&mut conn, "B", "b").await;

    shutdown_tx.shutdown().await;
}
