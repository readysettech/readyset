use mysql_async::prelude::Queryable;
use tokio::test;

use readyset_client_test_helpers::{
    TestBuilder, derive_test_name, sleep,
    mysql_helpers::{self, MySQLAdapter},
};
use readyset_tracing::init_test_logging;
use test_utils::{tags, upstream};

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cached_query_stats_vrel() {
    init_test_logging();

    let test_name = derive_test_name!();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("CREATE TABLE t (x INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    let (readyset_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(readyset_opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();

    // The vrel should exist and return no rows before any cached queries.
    let rows: Vec<(String,)> = rs
        .query("SELECT query_id FROM readyset.cached_query_stats")
        .await
        .unwrap();
    assert!(rows.is_empty());

    // Create a cache and run the query a few times.
    rs.query_drop("CREATE SHALLOW CACHE FROM SELECT x FROM t WHERE x = ?")
        .await
        .unwrap();

    let exec_count = 5;
    let expected_query_id = "q_a36dada0b788953c";

    for _ in 0..exec_count {
        rs.query_drop("SELECT x FROM t WHERE x = 1").await.unwrap();
    }

    // Wait a bit for the query logger to submit the metrics.
    sleep().await;

    // Check the upstream stats.
    let rows: Vec<(u64, f64)> = rs
        .query(format!(
            "SELECT total_count, avg_us FROM readyset.upstream_query_stats \
             WHERE query_id = '{expected_query_id}'"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let (total_count, avg_us) = rows[0];
    assert!(total_count >= 1);
    assert!(avg_us > 0.0);

    // Check the cached stats.
    let rows: Vec<(u64, f64)> = rs
        .query(format!(
            "SELECT total_count, avg_us FROM readyset.cached_query_stats \
             WHERE query_id = '{expected_query_id}'"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let (total_count, avg_us) = rows[0];
    assert!(total_count >= exec_count - 1);
    assert!(avg_us > 0.0);

    shutdown_tx.shutdown().await;
}
