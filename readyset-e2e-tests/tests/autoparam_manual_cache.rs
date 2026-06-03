use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use test_utils::{tags, upstream};

/// Helper that fires an `EXPLAIN LAST STATEMENT` and asserts that the last
/// target/QueryDestination was `expected`.
async fn assert_last_target_was(rs_conn: &mut Conn, expected: QueryDestination) {
    let destination: QueryInfo = rs_conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    let msg = destination.noria_error;
    assert_eq!(destination.destination, expected, "{msg}");
}

/// End-to-end coverage for `CREATE CACHE WITH (AUTOPARAM OFF)`:
/// 1. A SELECT whose literal matches the cache's inline constant is served by the manual cache,
///    both ad-hoc and as a prepared statement.
/// 2. A SELECT with a different literal at the frozen position is a cache miss and is answered
///    by the upstream, with correct results.
/// 3. A second manual cache colliding on the same auto-parameterized shape is rejected.
/// 4. Dropping the manual cache stops it from capturing the shape.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn autoparam_off_manual_cache_end_to_end() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_off_manual_cache";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        // Prevent plain SELECTs from creating caches; the manual mapping must be what routes
        // them to readyset.
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t (id int, status varchar(16), v int); \
             INSERT INTO t (id, status, v) VALUES \
             (1, 'active', 10), (1, 'archived', 20), (2, 'active', 30);",
        )
        .await
        .unwrap();

    sleep().await;

    // The cache keeps `status = 'active'` inline and parameterizes only `id`.
    rs_conn
        .query_drop(
            "CREATE CACHE manual WITH (AUTOPARAM OFF) FROM \
             SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    sleep().await;

    // Ad-hoc SELECT with the matching frozen literal: served by the manual cache.
    let result: Vec<i32> = rs_conn
        .query("SELECT v FROM t WHERE id = 1 AND status = 'active'")
        .await
        .unwrap();
    assert_eq!(result, vec![10]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("manual".into())),
    )
    .await;

    // Prepared statement with the matching frozen literal: also served by the manual cache.
    let result: Vec<i32> = rs_conn
        .exec("SELECT v FROM t WHERE id = ? AND status = 'active'", (2,))
        .await
        .unwrap();
    assert_eq!(result, vec![30]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("manual".into())),
    )
    .await;

    // A different literal at the frozen position cannot be served by the cache: it must fall
    // back to the upstream and still return correct results.
    let result: Vec<i32> = rs_conn
        .query("SELECT v FROM t WHERE id = 1 AND status = 'archived'")
        .await
        .unwrap();
    assert_eq!(result, vec![20]);
    // `'archived'` doesn't match the cache's frozen `status = 'active'`, so this query isn't
    // served by the cache: a clean upstream miss.
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    // Re-creating the same manual cache is idempotent.
    rs_conn
        .query_drop(
            "CREATE CACHE manual WITH (AUTOPARAM OFF) FROM \
             SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    // A different manual cache colliding on the same auto-parameterized shape is rejected.
    let err = rs_conn
        .query_drop(
            "CREATE CACHE manual2 WITH (AUTOPARAM OFF) FROM \
             SELECT v FROM t WHERE id = ? AND status = 'archived'",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("manual"),
        "collision error should name the existing cache: {err}"
    );

    // Dropping the manual cache stops it from capturing the shape; the SELECT now proxies.
    rs_conn.query_drop("DROP CACHE manual").await.unwrap();
    sleep().await;

    let result: Vec<i32> = rs_conn
        .query("SELECT v FROM t WHERE id = 1 AND status = 'active'")
        .await
        .unwrap();
    assert_eq!(result, vec![10]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// A prepared statement prepared *before* its manual cache exists is routed to the cache once it
/// is created, on the next execute: the standard shape's own migration state never transitions,
/// so the re-prepare path must consult the manual-cache mapping directly.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn autoparam_manual_cache_prepare_before_create() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_prepare_before_create";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t (id int, status varchar(16), v int); \
             INSERT INTO t (id, status, v) VALUES (1, 'active', 10), (2, 'active', 30);",
        )
        .await
        .unwrap();

    sleep().await;

    let stmt = "SELECT v FROM t WHERE id = ? AND status = 'active'";

    // Prepare and execute before the cache exists: proxied upstream, correct result.
    let result: Vec<i32> = rs_conn.exec(stmt, (1,)).await.unwrap();
    assert_eq!(result, vec![10]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    rs_conn
        .query_drop(
            "CREATE CACHE manual WITH (AUTOPARAM OFF) FROM \
             SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    sleep().await;

    // Re-executing the already-prepared statement now routes to the manual cache.
    let result: Vec<i32> = rs_conn.exec(stmt, (2,)).await.unwrap();
    assert_eq!(result, vec![30]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("manual".into())),
    )
    .await;

    shutdown_tx.shutdown().await;
}
