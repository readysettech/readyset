use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use test_utils::tags;

/// Helper that fires an `EXPLAIN LAST STATEMENT` and asserts that the last
/// target/QueryDestination was `expected`.
async fn assert_last_target_was(rs_conn: &mut Conn, expected: QueryDestination) {
    let destination: QueryInfo = rs_conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, expected);
}

/// Tests TopK functionality with dual lookup patterns.
/// This test verifies that:
/// 1. A specific LIMIT value (1) can be cached and served from ReadySet
/// 2. A different LIMIT value (2) falls back to upstream when no cache exists
/// 3. A parameterized LIMIT cache can handle different values including the fallback case
#[tokio::test]
#[tags(serial, mysql_upstream)]
async fn test_topk_dual_lookup() {
    readyset_tracing::init_test_logging();
    let db_name = "topk_dual_lookup_test";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        // prevent plain SELECTs from creating caches.
        // Queries with no caches will fail if no fallback exists
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .set_topk(true)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE test (x int, y int); INSERT INTO test (x, y) VALUES (4, 2);")
        .await
        .unwrap();

    sleep().await;

    rs_conn
        .query_drop("CREATE CACHE top_1 FROM SELECT * FROM test ORDER BY x LIMIT 1")
        .await
        .unwrap();

    sleep().await;

    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test ORDER BY x LIMIT 1")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (4, 2));

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("top_1".into())),
    )
    .await;

    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test ORDER BY x LIMIT 2")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);

    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    rs_conn
        .query_drop("CREATE CACHE parameterized_cache FROM SELECT * FROM test ORDER BY x LIMIT ?")
        .await
        .unwrap();

    sleep().await;

    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test ORDER BY x LIMIT 2")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (4, 2));

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("parameterized_cache".into())),
    )
    .await;

    // Still hits the first cache even though the parameterized cache exists.
    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test ORDER BY x LIMIT 1")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (4, 2));

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("top_1".into())),
    )
    .await;

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests plain LIMIT functionality (without ORDER BY) with literal LIMIT values.
/// This test verifies that:
/// 1. A literal LIMIT cache strips the LIMIT and acts as a catch-all for any LIMIT value
/// 2. All queries with different LIMIT values hit the same cache
#[tokio::test]
#[tags(serial, mysql_upstream)]
async fn test_topk_limit_catch_all() {
    readyset_tracing::init_test_logging();
    let db_name = "plain_literal_limit_test";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .set_topk(true)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE test (id int, value int);
             INSERT INTO test (id, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);",
        )
        .await
        .unwrap();

    sleep().await;

    // LIMIT should be stripped and applied by the adapter in post-processing
    // doesn't matter if it's a literal or parameterized
    rs_conn
        .query_drop("CREATE CACHE strip FROM SELECT * FROM test LIMIT 1")
        .await
        .unwrap();

    sleep().await;

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM test LIMIT 1").await.unwrap();
    assert_eq!(result.len(), 1);

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("strip".into())),
    )
    .await;

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM test LIMIT 2").await.unwrap();
    assert_eq!(result.len(), 2);

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("strip".into())),
    )
    .await;

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM test").await.unwrap();
    assert_eq!(result.len(), 5);

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("strip".into())),
    )
    .await;

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}
