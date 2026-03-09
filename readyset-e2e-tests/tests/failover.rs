use mysql_async::prelude::Queryable;
use mysql_async::Row;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use replication_offset::ReplicationOffset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Helper: extract the "Status" value from SHOW READYSET STATUS rows.
async fn readyset_status(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
    rows.iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Status")
        .expect("Status row not found")
        .get::<String, _>(1)
        .unwrap()
}

/// Helper: extract the "Replication Status" value from SHOW READYSET STATUS rows.
async fn replication_status(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
    rows.iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Replication Status")
        .expect("Replication Status row not found")
        .get::<String, _>(1)
        .unwrap()
}

/// Helper: get the current binlog position from the upstream MySQL server.
/// Returns the position as "file:offset" (e.g., "binlog.000003:58849").
async fn upstream_binlog_position(conn: &mut mysql_async::Conn) -> String {
    // MySQL 8.4+ uses SHOW BINARY LOG STATUS, older uses SHOW MASTER STATUS.
    let rows: Vec<Row> = match conn.query("SHOW BINARY LOG STATUS").await {
        Ok(rows) => rows,
        Err(_) => conn.query("SHOW MASTER STATUS").await.unwrap(),
    };
    let row = &rows[0];
    let file: String = row.get(0).expect("binlog file");
    let offset: u64 = row.get(1).expect("binlog offset");
    format!("{file}:{offset}")
}

/// Helper: get the current GTID executed set from the upstream MySQL server.
/// Returns the GTID set as a string (e.g., "uuid:1-10").
async fn upstream_gtid_executed(conn: &mut mysql_async::Conn) -> String {
    let gtid: String = conn
        .query_first("SELECT @@GLOBAL.GTID_EXECUTED")
        .await
        .unwrap()
        .expect("GTID_EXECUTED should be available");
    // MySQL may return newlines in the GTID set; normalize them.
    gtid.replace('\n', "")
}

/// Helper: extract the "Minimum Replication Offset" value from SHOW READYSET STATUS.
async fn min_replication_offset(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
    rows.iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Minimum Replication Offset")
        .expect("Minimum Replication Offset row not found")
        .get::<String, _>(1)
        .unwrap()
}

/// STOP REPLICATION sets replication status to "Stopped" and START
/// REPLICATION restores it to "Running".
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn stop_start_replication_updates_status() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_stop_start_status";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication to be online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Stop replication.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Replication Status should be "Stopped".
    let status = replication_status(&mut rs_conn).await;
    assert_eq!(status, "Stopped");

    // Stopping again should error.
    let err = rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await;
    assert!(err.is_err(), "Expected error stopping already-stopped replication");

    // Start replication.
    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Status should go back to online (eventually, after snapshot/replication starts).
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Starting again should error.
    let err = rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await;
    assert!(err.is_err(), "Expected error starting already-running replication");

    shutdown_tx.shutdown().await;
}

/// After stopping replication, inserts to upstream should NOT appear in
/// cached queries. After starting replication again, they should catch up.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn stop_replication_freezes_cache_data() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_freeze_cache";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();

    // Wait for replication to be online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Create a cache.
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    // Verify initial data is served from cache.
    eventually!(run_test: {
        rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (1,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((10,)));
    });

    // Stop replication.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Insert new data upstream while replication is stopped.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    // Give some time, then verify the new row does NOT appear in the cache.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let result: Option<(i32,)> = rs_conn
        .exec_first("SELECT val FROM t1 WHERE id = ?", (2,))
        .await
        .unwrap();
    assert_eq!(result, None, "Row inserted while stopped should NOT appear in cache");

    // Start replication — it should catch up and the row should appear.
    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (2,)).await.unwrap()
        }, then_assert: |row| {
            assert_eq!(row, Some((20,)));
        }
    );

    shutdown_tx.shutdown().await;
}

/// Simulate a failover scenario: stop replication, insert data upstream,
/// set the replication position to the CURRENT upstream position (after the
/// insert), then start replication. The data inserted while stopped should
/// be MISSING from the cache because we jumped past it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn set_replication_position_skips_missed_writes() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_set_position";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();

    // Wait for replication to be online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Create a cache and verify initial data.
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    eventually!(run_test: {
        rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (1,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((10,)));
    });

    // Stop replication.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Insert data upstream while replication is stopped.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    // Get the current upstream binlog position (AFTER the insert).
    let position = upstream_binlog_position(&mut upstream_conn).await;

    // Set readyset's replication position to the current upstream position,
    // effectively skipping the insert we just made.
    rs_conn
        .query_drop(format!(
            "ALTER READYSET SET REPLICATION POSITION '{position}'"
        ))
        .await
        .unwrap();

    // Start replication from the new position.
    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Wait for replication to come back online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // The skipped row (id=2) should NOT be in the cache.
    let result: Option<(i32,)> = rs_conn
        .exec_first("SELECT val FROM t1 WHERE id = ?", (2,))
        .await
        .unwrap();
    assert_eq!(
        result, None,
        "Row inserted before position jump should NOT be in cache"
    );

    // But new writes AFTER the position should replicate normally.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (3, 30)")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (3,)).await.unwrap()
        }, then_assert: |row| {
            assert_eq!(row, Some((30,)));
        }
    );

    // Original data (id=1) should still be intact.
    let result: Option<(i32,)> = rs_conn
        .exec_first("SELECT val FROM t1 WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(result, Some((10,)), "Pre-existing data should be unaffected");

    shutdown_tx.shutdown().await;
}

/// SET REPLICATION POSITION and CHANGE CDC should error when replication
/// is not stopped.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn failover_commands_require_stopped_replication() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_require_stopped";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication to be online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // SET REPLICATION POSITION should fail while replication is running.
    let err = rs_conn
        .query_drop("ALTER READYSET SET REPLICATION POSITION 'binlog.000001:100'")
        .await;
    assert!(
        err.is_err(),
        "SET REPLICATION POSITION should require stopped replication"
    );

    // CHANGE CDC should fail while replication is running.
    let err = rs_conn
        .query_drop("ALTER READYSET CHANGE CDC TO 'mysql://other-host/db'")
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
#[upstream(mysql80, mysql84)]
async fn change_cdc_url() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_change_cdc";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication to be online and create a cache.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    eventually!(run_test: {
        rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (1,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((10,)));
    });

    // Stop replication and point CDC to a URL with bad credentials.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    let mysql_host =
        std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let mysql_port =
        std::env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into());
    let bad_url = format!(
        "mysql://bad_user:bad_pass@{mysql_host}:{mysql_port}/{db_name}"
    );

    let position = upstream_binlog_position(&mut upstream_conn).await;
    rs_conn
        .query_drop(format!(
            "ALTER READYSET SET REPLICATION POSITION '{position}'"
        ))
        .await
        .unwrap();

    rs_conn
        .query_drop(format!("ALTER READYSET CHANGE CDC TO '{bad_url}'"))
        .await
        .unwrap();

    // Start replication with the bad URL.
    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Insert data upstream while replication is broken.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    // Give time for replication to attempt connection, then verify the
    // new row does NOT appear in the cache.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let result: Option<(i32,)> = rs_conn
        .exec_first("SELECT val FROM t1 WHERE id = ?", (2,))
        .await
        .unwrap();
    assert_eq!(
        result, None,
        "Row should NOT replicate when CDC URL has bad credentials"
    );

    // Now fix the CDC URL: stop, change to the correct URL, start.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    let good_url = MySQLAdapter::url_with_db(db_name);
    rs_conn
        .query_drop(format!("ALTER READYSET CHANGE CDC TO '{good_url}'"))
        .await
        .unwrap();

    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Replication should come back online and catch up.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // The row inserted while broken should now appear after catch-up.
    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (2,)).await.unwrap()
        }, then_assert: |row| {
            assert_eq!(row, Some((20,)));
        }
    );

    // New writes should also replicate normally.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (3, 30)")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (3,)).await.unwrap()
        }, then_assert: |row| {
            assert_eq!(row, Some((30,)));
        }
    );

    shutdown_tx.shutdown().await;
}

/// Transition from binlog file+pos to GTID replication. Start Readyset
/// without --require-gtid against a server with gtid_mode=ON, verify it
/// replicates using file+pos, then stop replication, set a GTID position,
/// start replication, and verify new writes replicate correctly via GTID.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80_nogtid, mysql84_nogtid)]
async fn set_replication_position_binlog_to_gtid() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_binlog_to_gtid";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication to be online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Verify we are replicating with binlog file+pos (not GTID).
    let offset = min_replication_offset(&mut rs_conn).await;
    let parsed: ReplicationOffset = offset.parse()
        .expect("should parse min replication offset");
    assert!(
        matches!(parsed, ReplicationOffset::MySql(_)),
        "Expected MySql (binlog file+pos) offset, got: {offset}"
    );

    // Create a cache and verify initial data replicates.
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    eventually!(run_test: {
        rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (1,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((10,)));
    });

    // Insert more data so the GTID set advances.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();

    eventually!(run_test: {
        rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (2,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((20,)));
    });

    // Stop replication.
    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Get the current GTID executed set from the upstream server.
    let gtid = upstream_gtid_executed(&mut upstream_conn).await;
    assert!(!gtid.is_empty(), "GTID_EXECUTED should not be empty");

    // Switch to GTID replication by setting the GTID position.
    rs_conn
        .query_drop(format!(
            "ALTER READYSET SET REPLICATION POSITION '{gtid}'"
        ))
        .await
        .expect("Switching from binlog file+pos to GTID should succeed");

    // Start replication with the GTID offset.
    rs_conn
        .query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    // Wait for replication to come back online.
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Verify we are now replicating with GTID.
    let offset = min_replication_offset(&mut rs_conn).await;
    let parsed: ReplicationOffset = offset.parse()
        .expect("should parse min replication offset");
    assert!(
        matches!(parsed, ReplicationOffset::Gtid(_)),
        "Expected Gtid offset, got: {offset}"
    );

    // Insert new data and verify it replicates via GTID.
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (3, 30)")
        .await
        .unwrap();

    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            rs_conn.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (3,)).await.unwrap()
        }, then_assert: |row| {
            assert_eq!(row, Some((30,)));
        }
    );

    shutdown_tx.shutdown().await;
}

/// SET REPLICATION POSITION should reject a PostgreSQL LSN when the current
/// offset is MySQL (cross-dialect transition is not allowed).
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn set_replication_position_rejects_mysql_to_postgres() {
    readyset_tracing::init_test_logging();
    let db_name = "failover_mysql_to_pg";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication to be online (MySQL).
    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    rs_conn
        .query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();

    // Setting a PostgreSQL LSN should fail — cross-dialect is blocked.
    let err = rs_conn
        .query_drop("ALTER READYSET SET REPLICATION POSITION '0/16B3748'")
        .await;
    assert!(
        err.is_err(),
        "Switching from MySQL to PostgreSQL offset should be rejected"
    );

    shutdown_tx.shutdown().await;
}
