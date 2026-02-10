use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use database_utils::UpstreamConfig;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    Adapter, TestBuilder, derive_test_name,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_tracing::init_test_logging;
use test_utils::tags;
use tokio::sync::RwLock;
use tokio::{test, time::sleep};
use tokio_postgres::SimpleQueryMessage;

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

#[test]
#[tags(serial, slow, mysql_upstream)]
async fn show_shallow_cache_entries() {
    init_test_logging();

    let test_name = derive_test_name!();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE users (id INT, name TEXT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
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

    // Create a shallow cache
    readyset
        .query_drop(
            "CREATE SHALLOW CACHE
               POLICY TTL 60 SECONDS
               REFRESH EVERY 30 SECONDS
               FROM SELECT id, name FROM users WHERE id = ?",
        )
        .await
        .unwrap();

    // Execute query to populate cache entries
    readyset
        .query_drop("SELECT id, name FROM users WHERE id = 1")
        .await
        .unwrap();
    readyset
        .query_drop("SELECT id, name FROM users WHERE id = 2")
        .await
        .unwrap();

    // Test SHOW SHALLOW CACHE ENTRIES returns all entries
    let entries: Vec<(String, String, String, String, String)> =
        readyset.query("SHOW SHALLOW CACHE ENTRIES").await.unwrap();
    assert_eq!(entries.len(), 2, "Should have 2 cache entries");

    // Get the query_id from one of the entries
    let query_id = &entries[0].0;

    // Test SHOW SHALLOW CACHE ENTRIES WHERE query_id = '...'
    let filtered: Vec<(String, String, String, String, String)> = readyset
        .query(format!(
            "SHOW SHALLOW CACHE ENTRIES WHERE query_id = '{query_id}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        filtered.len(),
        2,
        "Filtered by query_id should return both entries for this cache"
    );

    // Test SHOW SHALLOW CACHE ENTRIES LIMIT 1
    let limited: Vec<(String, String, String, String, String)> = readyset
        .query("SHOW SHALLOW CACHE ENTRIES LIMIT 1")
        .await
        .unwrap();
    assert_eq!(limited.len(), 1, "LIMIT 1 should return only 1 entry");

    // Test SHOW SHALLOW CACHE ENTRIES WHERE query_id = '...' LIMIT 1
    let filtered_limited: Vec<(String, String, String, String, String)> = readyset
        .query(format!(
            "SHOW SHALLOW CACHE ENTRIES WHERE query_id = '{query_id}' LIMIT 1"
        ))
        .await
        .unwrap();
    assert_eq!(
        filtered_limited.len(),
        1,
        "Combined WHERE and LIMIT should return 1 entry"
    );

    // Test filtering with non-existent query_id returns empty
    let non_existent: Vec<(String, String, String, String, String)> = readyset
        .query("SHOW SHALLOW CACHE ENTRIES WHERE query_id = 'q_12345'")
        .await
        .unwrap();
    assert!(
        non_existent.is_empty(),
        "Non-existent query_id should return empty results"
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_change_upstream() {
    init_test_logging();

    let test_name = derive_test_name!();
    let db_a = format!("{test_name}_a");
    let db_b = format!("{test_name}_b");

    mysql_helpers::recreate_database(&db_a).await;
    mysql_helpers::recreate_database(&db_b).await;

    let upstream_opts_a = mysql_helpers::upstream_config().db_name(Some(&db_a));
    let mut upstream_a = mysql_async::Conn::new(upstream_opts_a).await.unwrap();
    upstream_a
        .query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .expect("create table in db_a");
    upstream_a
        .query_drop("INSERT INTO foo VALUES (1, 100)")
        .await
        .expect("insert into db_a");

    let upstream_opts_b = mysql_helpers::upstream_config().db_name(Some(&db_b));
    let mut upstream_b = mysql_async::Conn::new(upstream_opts_b).await.unwrap();
    upstream_b
        .query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .expect("create table in db_b");
    upstream_b
        .query_drop("INSERT INTO foo VALUES (1, 200)")
        .await
        .expect("insert into db_b");

    let url_a = MySQLAdapter::upstream_url(&db_a);
    let url_b = MySQLAdapter::upstream_url(&db_b);
    let upstream_config = Arc::new(RwLock::new(UpstreamConfig::from_url(&url_a)));

    let backend_builder = BackendBuilder::default()
        .require_authentication(false)
        .replication_enabled(false)
        .upstream_config(Some(upstream_config));

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend_builder)
        .recreate_database(false)
        .replicate_db(db_a.clone())
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    rs.query_drop(format!("USE {db_a}")).await.unwrap();

    rs.query_drop(
        "CREATE SHALLOW CACHE POLICY TTL 2 SECONDS FROM SELECT b FROM foo WHERE a = ?",
    )
    .await
    .expect("create shallow cache");

    // First query goes upstream to db_a.
    let row: (i32,) = rs
        .query_first("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query db_a (1st)")
        .expect("row should exist");
    assert_eq!(row.0, 100);
    assert_eq!(
        last_query_info(&mut rs).await.destination,
        QueryDestination::Upstream,
    );

    // Second query hits shallow cache.
    let row: (i32,) = rs
        .query_first("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query db_a (2nd)")
        .expect("row should exist");
    assert_eq!(row.0, 100);
    assert_eq!(
        last_query_info(&mut rs).await.destination,
        QueryDestination::ReadysetShallow,
    );

    // Switch upstream to db_b.
    rs.query_drop(format!("ALTER READYSET CHANGE UPSTREAM TO '{url_b}'"))
        .await
        .expect("change upstream");

    // Wait for routing check interval to fire.
    sleep(Duration::from_secs(2)).await;

    // The next query should fail because the connection is closed after a routing change.
    rs.query_drop("SELECT b FROM foo WHERE a = 1")
        .await
        .expect_err("query should fail after routing change");

    // Reconnect to ReadySet; the new connection picks up the new upstream.
    // USE db_b because the upstream URL now points to db_b, and MySQL's USE
    // statement is forwarded to the upstream connection.
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    rs.query_drop(format!("USE {db_b}")).await.unwrap();

    // Wait for TTL to expire so the shallow cache is stale.
    sleep(Duration::from_secs(3)).await;

    // Query should go upstream to db_b.
    let row: (i32,) = rs
        .query_first("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query after change upstream")
        .expect("row should exist");
    assert_eq!(row.0, 200);
    assert_eq!(
        last_query_info(&mut rs).await.destination,
        QueryDestination::Upstream,
    );

    shutdown_tx.shutdown().await;
}

fn first_row_col(rows: &[SimpleQueryMessage], col: usize) -> &str {
    match &rows[0] {
        SimpleQueryMessage::Row(row) => row.get(col).expect("column should exist"),
        _ => panic!("expected row"),
    }
}

#[test]
#[tags(serial, slow, postgres_upstream)]
async fn pg_change_upstream() {
    init_test_logging();

    let test_name = derive_test_name!();
    let db_a = format!("{test_name}_a");
    let db_b = format!("{test_name}_b");

    PostgreSQLAdapter::recreate_database(&db_a).await;
    PostgreSQLAdapter::recreate_database(&db_b).await;

    let mut cfg_a = psql_helpers::upstream_config();
    cfg_a.dbname(&db_a);
    let upstream_a = psql_helpers::connect(cfg_a).await;
    upstream_a
        .simple_query("CREATE TABLE foo (a INT, b INT)")
        .await
        .expect("create table in db_a");
    upstream_a
        .simple_query("INSERT INTO foo VALUES (1, 100)")
        .await
        .expect("insert into db_a");

    let mut cfg_b = psql_helpers::upstream_config();
    cfg_b.dbname(&db_b);
    let upstream_b = psql_helpers::connect(cfg_b).await;
    upstream_b
        .simple_query("CREATE TABLE foo (a INT, b INT)")
        .await
        .expect("create table in db_b");
    upstream_b
        .simple_query("INSERT INTO foo VALUES (1, 200)")
        .await
        .expect("insert into db_b");

    let url_a = PostgreSQLAdapter::upstream_url(&db_a);
    let url_b = PostgreSQLAdapter::upstream_url(&db_b);
    let upstream_config = Arc::new(RwLock::new(UpstreamConfig::from_url(&url_a)));

    let backend_builder = BackendBuilder::default()
        .require_authentication(false)
        .replication_enabled(false)
        .upstream_config(Some(upstream_config));

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend_builder)
        .recreate_database(false)
        .replicate_db(db_a.clone())
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut cfg_rs = rs_opts.clone();
    cfg_rs.dbname(&db_a);
    let rs = psql_helpers::connect(cfg_rs).await;

    rs.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 2 SECONDS FROM SELECT b FROM foo WHERE a = $1",
    )
    .await
    .expect("create shallow cache");

    // First query goes upstream to db_a.
    let rows = rs
        .simple_query("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query db_a (1st)");
    assert_eq!(first_row_col(&rows, 0), "100");
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
    );

    // Second query hits shallow cache.
    let rows = rs
        .simple_query("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query db_a (2nd)");
    assert_eq!(first_row_col(&rows, 0), "100");
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
    );

    // Switch upstream to db_b.
    rs.simple_query(&format!("ALTER READYSET CHANGE UPSTREAM TO '{url_b}'"))
        .await
        .expect("change upstream");

    // Wait for routing check interval to fire.
    sleep(Duration::from_secs(2)).await;

    // The next query should fail because the connection is closed after a routing change.
    rs.simple_query("SELECT b FROM foo WHERE a = 1")
        .await
        .expect_err("query should fail after routing change");
    assert!(rs.is_closed(), "connection should be closed");

    // Reconnect to ReadySet; the new connection picks up the new upstream.
    let mut cfg_rs = rs_opts.clone();
    cfg_rs.dbname(&db_a);
    let rs = psql_helpers::connect(cfg_rs).await;

    // Wait for TTL to expire so the shallow cache is stale.
    sleep(Duration::from_secs(3)).await;

    // Query should go upstream to db_b.
    let rows = rs
        .simple_query("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query after change upstream");
    assert_eq!(first_row_col(&rows, 0), "200");
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
    );

    // Second query should hit shallow cache with db_b data.
    let rows = rs
        .simple_query("SELECT b FROM foo WHERE a = 1")
        .await
        .expect("query db_b (cached)");
    assert_eq!(first_row_col(&rows, 0), "200");
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
    );

    shutdown_tx.shutdown().await;
}
