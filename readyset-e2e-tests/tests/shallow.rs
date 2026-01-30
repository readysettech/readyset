use std::{collections::HashSet, time::Duration};

use mysql_async::prelude::Queryable;
use tokio::{test, time::sleep};

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder, derive_test_name,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_tracing::init_test_logging;
use test_utils::tags;

#[test]
#[tags(serial, slow, mysql_upstream)]
async fn scheduled_refresh_expiration() {
    init_test_logging();

    let test_name = derive_test_name!();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
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
        .query_drop(
            "CREATE SHALLOW CACHE
               POLICY TTL 10 SECONDS
               REFRESH EVERY 2 SECONDS
               FROM SELECT a, RAND(), SLEEP(3) FROM foo WHERE a = ?",
        )
        .await
        .unwrap();

    readyset
        .query_drop("SELECT a, RAND(), SLEEP(3) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a, RAND(), SLEEP(3) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    sleep(Duration::from_secs(11)).await;

    readyset
        .query_drop("SELECT a, RAND(), SLEEP(3) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial, slow, mysql_upstream)]
async fn execution_longer_than_ttl_is_cacheable() {
    init_test_logging();

    let test_name = derive_test_name!();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
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
        .query_drop(
            "CREATE SHALLOW CACHE
               POLICY TTL 3 SECONDS
               FROM SELECT a, SLEEP(6) FROM foo WHERE a = ?",
        )
        .await
        .unwrap();

    readyset
        .query_drop("SELECT a, SLEEP(6) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a, SLEEP(6) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    sleep(Duration::from_secs(1)).await;

    readyset
        .query_drop("SELECT a, SLEEP(6) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial, slow, mysql_upstream)]
async fn scheduled_refresh_starts_immediately() {
    init_test_logging();

    let test_name = derive_test_name!();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
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
        .query_drop(
            "CREATE SHALLOW CACHE
               POLICY TTL 10 SECONDS
               REFRESH EVERY 2 SECONDS
               FROM SELECT RAND(), SLEEP(5) FROM foo WHERE a = ?",
        )
        .await
        .unwrap();

    readyset
        .query_drop("SELECT RAND(), SLEEP(5) FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    async fn collect_rand_cache_result(readyset: &mut mysql_async::Conn, i: i32) -> String {
        let (rand, _): (f64, i32) = readyset
            .query_first("SELECT RAND(), SLEEP(5) FROM foo WHERE a = 1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            last_query_info(readyset).await.destination,
            QueryDestination::ReadysetShallow,
            "[{i}] Should have hit shallow cache"
        );
        rand.to_string()
    }

    // Get out of phase with the refresh.
    sleep(Duration::from_secs(1)).await;

    // Grab a unique cache result every 2 seconds.
    let mut seen = HashSet::new();
    for i in 1..=10 {
        assert!(
            seen.insert(collect_rand_cache_result(&mut readyset, i).await),
            "[{i}] Already encountered this cache entry"
        );
        if i < 10 {
            sleep(Duration::from_secs(2)).await;
        }
    }

    // Wait until the TTL is almost up (staying out of phase with the refresh), and then make sure
    // we're still refreshing.
    sleep(Duration::from_secs(8)).await;

    for i in 11..=14 {
        assert!(
            seen.insert(collect_rand_cache_result(&mut readyset, i).await),
            "[{i}] Already encountered this cache entry"
        );
        if i < 14 {
            sleep(Duration::from_secs(2)).await;
        }
    }

    shutdown_tx.shutdown().await;
}
