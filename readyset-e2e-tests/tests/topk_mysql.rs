use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use readyset_sql_parsing::ParsingPreset;
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

/// Tests TopK functionality with dual lookup patterns.
/// This test verifies that:
/// 1. A specific LIMIT value (1) can be cached and served from ReadySet
/// 2. A different LIMIT value (2) falls back to upstream when no cache exists
/// 3. A parameterized LIMIT cache can handle different values including the fallback case
#[tokio::test]
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
async fn test_topk_dual_lookup() {
    readyset_tracing::init_test_logging();
    let db_name = "topk_dual_lookup_test";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        // prevent plain SELECTs from creating caches.
        // Queries with no caches will fail if no fallback exists
        .migration_mode(MigrationMode::OutOfBand)
        // TODO: This should work in sqlparser after fixing REA-6179
        .parsing_preset(ParsingPreset::OnlyNom)
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

    rs_conn
        .query_drop(
            "CREATE CACHE with_predicate FROM SELECT * FROM test WHERE x > 3 ORDER BY x LIMIT 1",
        )
        .await
        .unwrap();

    sleep().await;

    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test WHERE x > 3 ORDER BY x LIMIT 1")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (4, 2));

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("with_predicate".into())),
    )
    .await;

    let result: Vec<(i32, i32)> = rs_conn
        .query("SELECT * FROM test WHERE x > 2 ORDER BY x LIMIT 1")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (4, 2));

    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("with_predicate".into())),
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
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
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

// TopK is one of the few nodes that require the on_eviction hook
// This is here to make sure that the hook gets called correctly
// and that the hook evicts as expected
#[tokio::test]
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
async fn test_topk_eviction_aux_cleanup() {
    readyset_tracing::init_test_logging();
    let db_name = "topk_eviction_aux_cleanup";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(false)
        .set_topk(true)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE posts (id int PRIMARY KEY, author_id int, score int);")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO posts (id, author_id, score) VALUES (1, 1, 200), (2, 1, 190), (3, 1, 180);")
        .await
        .unwrap();

    sleep().await;

    rs_conn
        .query_drop("CREATE CACHE topk_posts FROM SELECT id, score FROM posts WHERE author_id = ? ORDER BY score DESC LIMIT 3")
        .await
        .unwrap();

    sleep().await;

    let initial: Vec<(i32, i32)> = rs_conn
        .exec(
            "SELECT id, score FROM posts WHERE author_id = ? ORDER BY score DESC LIMIT 3",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(initial, vec![(1, 200), (2, 190), (3, 180)]);

    upstream_conn
        .query_drop("INSERT INTO posts (id, author_id, score) VALUES (4, 1, 200);")
        .await
        .unwrap();

    // Evict the only group (author_id = 1) to drop Readyset's local state for this key.
    // If the hook doesn't get called, the aux state will have stale data and will give the
    // main state duplicates.
    //
    // Use flush_partial (which uses Eviction::Bytes internally, same as production)
    handle.flush_partial().await.unwrap();
    sleep().await;

    let rows_after: Vec<(i32, i32)> = rs_conn
        .exec(
            "SELECT id, score FROM posts WHERE author_id = ? ORDER BY score DESC LIMIT 3",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(
        rows_after,
        vec![(1, 200), (4, 200), (2, 190)],
        "on_eviction hook didn't get called. Either the debug_assert in topk.rs failed or \
        state panicked because it received a stale negative"
    );

    shutdown_tx.shutdown().await;
    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}
