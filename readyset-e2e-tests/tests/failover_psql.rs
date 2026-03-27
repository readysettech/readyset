use std::panic::AssertUnwindSafe;
use std::time::Duration;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::SimpleQueryMessage;

fn first_row_col(rows: &[SimpleQueryMessage], col: usize) -> String {
    match rows.first().expect("expected at least one row") {
        SimpleQueryMessage::Row(row) => row.get(col).expect("column should exist").to_string(),
        other => panic!("expected Row, got {other:?}"),
    }
}

/// Helper: extract the "Status" value from SHOW READYSET STATUS rows.
fn readyset_status_from(rows: &[SimpleQueryMessage]) -> String {
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg
            && row.get(0) == Some("Status")
        {
            return row.get(1).expect("status value").to_string();
        }
    }
    panic!("Status row not found in SHOW READYSET STATUS");
}

/// Helper: extract the "Replication Status" value from SHOW READYSET STATUS rows.
fn replication_status_from(rows: &[SimpleQueryMessage]) -> String {
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg
            && row.get(0) == Some("Replication Status")
        {
            return row.get(1).expect("replication status value").to_string();
        }
    }
    panic!("Replication Status row not found in SHOW READYSET STATUS");
}

/// Helper: get the current WAL position from the upstream PostgreSQL server.
/// Returns the position as an LSN string (e.g., "0/16B3748").
async fn upstream_wal_position(conn: &tokio_postgres::Client) -> String {
    let rows = conn
        .simple_query("SELECT pg_current_wal_flush_lsn()")
        .await
        .expect("pg_current_wal_flush_lsn");
    first_row_col(&rows, 0)
}

/// STOP REPLICATION sets replication status to "Stopped" and START
/// REPLICATION restores it to "Running".
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn psql_stop_start_replication_updates_status() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_psql_stop_start_status";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db_name);
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    // Wait for replication to be online.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Stop replication.
    rs_conn
        .simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Replication Status should be "Stopped".
    let rows = rs_conn
        .simple_query("SHOW READYSET STATUS")
        .await
        .unwrap();
    assert_eq!(replication_status_from(&rows), "Stopped");

    // Stopping again should error.
    let err = rs_conn
        .simple_query("ALTER READYSET STOP REPLICATION")
        .await;
    assert!(
        err.is_err(),
        "Expected error stopping already-stopped replication"
    );

    // Start replication.
    rs_conn
        .simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Status should go back to online.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Starting again should error.
    let err = rs_conn
        .simple_query("ALTER READYSET START REPLICATION")
        .await;
    assert!(
        err.is_err(),
        "Expected error starting already-running replication"
    );

    shutdown_tx.shutdown().await;
}

/// After stopping replication, inserts to upstream should NOT appear in
/// cached queries. After starting replication again, they should catch up.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn psql_stop_replication_freezes_cache_data() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_psql_freeze_cache";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db_name);
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    // Wait for replication to be online.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Create a cache.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let result = rs_conn
                .simple_query("CREATE CACHE FROM SELECT val FROM t1 WHERE id = $1")
                .await;
            AssertUnwindSafe(move || result)
        },
        then_assert: |result| {
            result().expect("create cache");
        }
    );

    // Verify initial data is served from cache.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 1").await.unwrap();
        first_row_col(&rows, 0)
    }, then_assert: |val| {
        assert_eq!(val, "10");
    });

    // Stop replication.
    rs_conn
        .simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Insert new data upstream while replication is stopped.
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    // Give some time, then verify the new row does NOT appear in the cache.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rows = rs_conn
        .simple_query("SELECT val FROM t1 WHERE id = 2")
        .await
        .unwrap();
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        row_count, 0,
        "Row inserted while stopped should NOT appear in cache"
    );

    // Start replication — it should catch up and the row should appear.
    rs_conn
        .simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 2").await.unwrap();
            rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
        }, then_assert: |count| {
            assert_eq!(count, 1);
        }
    );

    shutdown_tx.shutdown().await;
}

/// SET REPLICATION POSITION and CHANGE CDC should error when replication
/// is not stopped.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn psql_failover_commands_require_stopped_replication() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_psql_require_stopped";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db_name);
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query("CREATE TABLE t1 (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    // Wait for replication to be online.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // SET REPLICATION POSITION should fail while replication is running.
    let err = rs_conn
        .simple_query("ALTER READYSET SET REPLICATION POSITION '0/1000000'")
        .await;
    assert!(
        err.is_err(),
        "SET REPLICATION POSITION should require stopped replication"
    );

    // CHANGE CDC should fail while replication is running.
    let err = rs_conn
        .simple_query("ALTER READYSET CHANGE CDC TO 'postgresql://other-host/db'")
        .await;
    assert!(
        err.is_err(),
        "CHANGE CDC should require stopped replication"
    );

    shutdown_tx.shutdown().await;
}

/// CHANGE CDC TO updates the CDC URL. Verify that pointing to a bad URL
/// prevents new data from replicating, then fixing the URL restores it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn psql_change_cdc_url() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_psql_change_cdc";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db_name);
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    // Wait for replication to be online and create a cache.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let result = rs_conn
                .simple_query("CREATE CACHE FROM SELECT val FROM t1 WHERE id = $1")
                .await;
            AssertUnwindSafe(move || result)
        },
        then_assert: |result| {
            result().expect("create cache");
        }
    );

    eventually!(run_test: {
        let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 1").await.unwrap();
        rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
    }, then_assert: |count| {
        assert_eq!(count, 1);
    });

    // Stop replication and point CDC to a URL with bad credentials.
    rs_conn
        .simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    let pg_host =
        std::env::var("PGHOST").unwrap_or_else(|_| "localhost".into());
    let pg_port =
        std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let bad_url = format!(
        "postgresql://bad_user:bad_pass@{pg_host}:{pg_port}/{db_name}"
    );

    let position = upstream_wal_position(&upstream_conn).await;
    rs_conn
        .simple_query(&format!(
            "ALTER READYSET SET REPLICATION POSITION '{position}'"
        ))
        .await
        .unwrap();

    rs_conn
        .simple_query(&format!("ALTER READYSET CHANGE CDC TO '{bad_url}'"))
        .await
        .unwrap();

    // Start replication with the bad URL.
    rs_conn
        .simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Insert data upstream while replication is broken.
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    // Give time for replication to attempt connection, then verify the
    // new row does NOT appear in the cache.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rows = rs_conn
        .simple_query("SELECT val FROM t1 WHERE id = 2")
        .await
        .unwrap();
    let count = rows
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 0,
        "Row should NOT replicate when CDC URL has bad credentials"
    );

    // Now fix the CDC URL: stop, change to the correct URL, start.
    rs_conn
        .simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    let good_url = PostgreSQLAdapter::upstream_url(db_name);
    rs_conn
        .simple_query(&format!("ALTER READYSET CHANGE CDC TO '{good_url}'"))
        .await
        .unwrap();

    rs_conn
        .simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Replication should come back online and catch up.
    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        readyset_status_from(&rows)
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // The row inserted while broken should now appear after catch-up.
    eventually!(attempts: 120, sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 2").await.unwrap();
            rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
        }, then_assert: |count| {
            assert_eq!(count, 1);
        }
    );

    // New writes should also replicate normally.
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (3, 30)")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 3").await.unwrap();
            rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
        }, then_assert: |count| {
            assert_eq!(count, 1);
        }
    );

    shutdown_tx.shutdown().await;
}
