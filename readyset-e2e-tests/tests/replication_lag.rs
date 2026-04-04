use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::Row;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder};
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio_postgres::SimpleQueryMessage;

/// Set up a MySQL-backed ReadySet instance with a single table, wait for
/// replication to come online, and return the ReadySet connection.
///
/// The `_handle` field keeps the ReadySet server alive for the test's duration.
async fn setup_mysql(
    db_name: &str,
    require_gtid: bool,
    heartbeat: bool,
) -> (mysql_async::Conn, Box<dyn std::any::Any>, ShutdownSender) {
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

    let mut builder = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .require_gtid(require_gtid)
        .replication_heartbeat(heartbeat);
    if heartbeat {
        builder = builder.replication_lag_interval(2);
    }
    let (rs_opts, handle, shutdown_tx) = builder.build::<MySQLAdapter>().await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually!(run_test: {
        let rows: Vec<Row> = rs_conn.query("SHOW READYSET STATUS").await.unwrap();
        rows.iter()
            .find(|r| r.get::<String, _>(0).unwrap() == "Status")
            .expect("Status row not found")
            .get::<String, _>(1)
            .unwrap()
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    (rs_conn, Box::new(handle), shutdown_tx)
}

/// Set up a Postgres-backed ReadySet instance with a single table, wait for
/// replication to come online, and return the ReadySet connection.
///
/// The `_handle` field keeps the ReadySet server alive for the test's duration.
async fn setup_psql(
    db_name: &str,
    heartbeat: bool,
) -> (tokio_postgres::Client, Box<dyn std::any::Any>, ShutdownSender) {
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

    let mut builder = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .replication_heartbeat(heartbeat);
    if heartbeat {
        builder = builder.replication_lag_interval(2);
    }
    let (rs_config, handle, shutdown_tx) = builder.build::<PostgreSQLAdapter>().await;

    let mut rs_cfg = rs_config.clone();
    rs_cfg.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_cfg).await;

    eventually!(run_test: {
        let rows = rs_conn.simple_query("SHOW READYSET STATUS").await.unwrap();
        rows.iter()
            .find_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg
                    && row.get(0) == Some("Status")
                {
                    Some(row.get(1).unwrap_or("").to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    (rs_conn, Box::new(handle), shutdown_tx)
}

/// Wait for the replication_status vrel to have exactly one row, then return it.
async fn mysql_replication_status_row(conn: &mut mysql_async::Conn) -> Row {
    eventually!(run_test: {
        let rows: Vec<Row> = conn
            .query("SELECT * FROM readyset.replication_status")
            .await
            .unwrap();
        rows.len()
    }, then_assert: |count| {
        assert_eq!(count, 1, "expected exactly one replication_status row");
    });

    conn.query_first("SELECT * FROM readyset.replication_status")
        .await
        .unwrap()
        .expect("expected one row")
}

/// Wait for the replication_status vrel to have exactly one row, then return it.
async fn psql_replication_status_row(
    conn: &tokio_postgres::Client,
) -> Vec<tokio_postgres::SimpleQueryMessage> {
    eventually!(run_test: {
        let rows = conn
            .simple_query("SELECT * FROM readyset.replication_status")
            .await
            .unwrap();
        rows.iter()
            .filter(|msg| matches!(msg, SimpleQueryMessage::Row(_)))
            .count()
    }, then_assert: |count| {
        assert_eq!(count, 1, "expected exactly one replication_status row");
    });

    conn.simple_query("SELECT * FROM readyset.replication_status")
        .await
        .unwrap()
}

/// Read heartbeat_staleness_seconds from the Postgres replication_status vrel.
async fn psql_staleness(conn: &tokio_postgres::Client) -> Option<f64> {
    let rows = conn
        .simple_query("SELECT * FROM readyset.replication_status")
        .await
        .unwrap();
    rows.iter().find_map(|msg| {
        if let SimpleQueryMessage::Row(row) = msg {
            // Column index 6 = heartbeat_staleness_seconds
            row.get(6).map(|s| s.parse::<f64>().expect("staleness should be a float"))
        } else {
            None
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn mysql_replication_status_vrel() {
    readyset_tracing::init_test_logging();
    let (mut rs_conn, _handle, shutdown_tx) =
        setup_mysql("replication_lag_mysql_vrel", false, false).await;

    let row = mysql_replication_status_row(&mut rs_conn).await;

    let mode: String = row.get("replication_mode").expect("replication_mode");
    assert!(
        mode == "mysql_file" || mode == "mysql_gtid",
        "unexpected replication mode: {mode}"
    );

    let upstream_offset: String = row.get("upstream_offset").expect("upstream_offset");
    assert!(!upstream_offset.is_empty(), "upstream_offset should not be empty");

    let replicator_offset: String = row.get("replicator_offset").expect("replicator_offset");
    assert!(!replicator_offset.is_empty(), "replicator_offset should not be empty");

    let persisted_offset: String = row.get("persisted_offset").expect("persisted_offset");
    assert!(!persisted_offset.is_empty(), "persisted_offset should not be empty");

    let consume_lag: u64 = row.get("consume_lag").expect("consume_lag");
    let persist_lag: u64 = row.get("persist_lag").expect("persist_lag");
    assert!(consume_lag < 10_000_000, "consume_lag unexpectedly large: {consume_lag}");
    assert!(persist_lag < 10_000_000, "persist_lag unexpectedly large: {persist_lag}");

    // Without --replication-heartbeat, staleness should be NULL.
    // Double-Option: outer = column existence, inner = SQL NULL.
    let staleness: Option<Option<f64>> = row.get("heartbeat_staleness_seconds");
    assert!(
        staleness == Some(None),
        "staleness should be NULL without heartbeat, got: {staleness:?}"
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn mysql_gtid_replication_status_vrel() {
    readyset_tracing::init_test_logging();
    let (mut rs_conn, _handle, shutdown_tx) =
        setup_mysql("replication_lag_mysql_gtid_vrel", true, false).await;

    let row = mysql_replication_status_row(&mut rs_conn).await;

    let mode: String = row.get("replication_mode").expect("replication_mode");
    assert_eq!(mode, "mysql_gtid", "expected GTID mode with --require-gtid");

    let upstream_offset: String = row.get("upstream_offset").expect("upstream_offset");
    assert!(!upstream_offset.is_empty(), "upstream_offset should not be empty");

    let consume_lag: u64 = row.get("consume_lag").expect("consume_lag");
    let persist_lag: u64 = row.get("persist_lag").expect("persist_lag");
    assert!(consume_lag < 100, "consume_lag unexpectedly large: {consume_lag}");
    assert!(persist_lag < 100, "persist_lag unexpectedly large: {persist_lag}");

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn psql_replication_status_vrel() {
    readyset_tracing::init_test_logging();
    let (rs_conn, _handle, shutdown_tx) = setup_psql("replication_lag_psql_vrel", false).await;

    let rows = psql_replication_status_row(&rs_conn).await;
    let row = rows
        .iter()
        .find_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                Some(row)
            } else {
                None
            }
        })
        .expect("expected at least one data row");

    assert_eq!(row.get(0).expect("replication_mode"), "postgres");

    let upstream_offset = row.get(1).expect("upstream_offset");
    assert!(!upstream_offset.is_empty(), "upstream_offset should not be empty");

    let replicator_offset = row.get(2).expect("replicator_offset");
    assert!(!replicator_offset.is_empty(), "replicator_offset should not be empty");

    let persisted_offset = row.get(3).expect("persisted_offset");
    assert!(!persisted_offset.is_empty(), "persisted_offset should not be empty");

    // Column index 6 = heartbeat_staleness_seconds
    let staleness = row.get(6);
    assert!(
        staleness.is_none(),
        "staleness should be NULL without heartbeat, got: {staleness:?}"
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn mysql_heartbeat_staleness() {
    readyset_tracing::init_test_logging();
    let (mut rs_conn, _handle, shutdown_tx) =
        setup_mysql("replication_lag_mysql_heartbeat", false, true).await;

    // With a 2s poll interval, staleness should populate after ~4s (2 cycles).
    eventually!(
        attempts: 20,
        sleep: Duration::from_secs(1),
        run_test: {
            let rows: Vec<Row> = rs_conn
                .query("SELECT * FROM readyset.replication_status")
                .await
                .unwrap();
            rows.first()
                .and_then(|r| r.get::<Option<f64>, _>("heartbeat_staleness_seconds"))
                .flatten()
        },
        then_assert: |staleness| {
            let staleness = staleness.expect("staleness should be Some after 2+ poll cycles");
            assert!(staleness >= 0.0, "staleness should be non-negative, got: {staleness}");
            assert!(staleness < 60.0, "staleness unexpectedly large: {staleness}");
        }
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql, modern, gtid)]
async fn mysql_gtid_heartbeat_staleness() {
    readyset_tracing::init_test_logging();
    let (mut rs_conn, _handle, shutdown_tx) =
        setup_mysql("replication_lag_mysql_gtid_heartbeat", true, true).await;

    eventually!(
        attempts: 20,
        sleep: Duration::from_secs(1),
        run_test: {
            let rows: Vec<Row> = rs_conn
                .query("SELECT * FROM readyset.replication_status")
                .await
                .unwrap();
            rows.first()
                .and_then(|r| r.get::<Option<f64>, _>("heartbeat_staleness_seconds"))
                .flatten()
        },
        then_assert: |staleness| {
            let staleness = staleness.expect("staleness should be Some after 2+ poll cycles");
            assert!(staleness >= 0.0, "staleness should be non-negative, got: {staleness}");
            assert!(staleness < 60.0, "staleness unexpectedly large: {staleness}");
        }
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn psql_heartbeat_staleness() {
    readyset_tracing::init_test_logging();
    let (rs_conn, _handle, shutdown_tx) =
        setup_psql("replication_lag_psql_heartbeat", true).await;

    // With a 2s poll interval, staleness should populate after ~4s (2 cycles).
    eventually!(
        attempts: 20,
        sleep: Duration::from_secs(1),
        run_test: {
            psql_staleness(&rs_conn).await
        },
        then_assert: |staleness| {
            let staleness = staleness.expect("staleness should be Some after 2+ poll cycles");
            assert!(staleness >= 0.0, "staleness should be non-negative, got: {staleness}");
            assert!(staleness < 60.0, "staleness unexpectedly large: {staleness}");
        }
    );

    shutdown_tx.shutdown().await;
}

/// Verify that heartbeat staleness stays bounded on a quiescent Postgres database.
///
/// Regression test: the WAL reader's table filter was cloned before the heartbeat table
/// was added, so heartbeat WAL events were silently dropped and staleness grew forever.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn psql_heartbeat_staleness_stays_bounded() {
    readyset_tracing::init_test_logging();
    let (rs_conn, _handle, shutdown_tx) =
        setup_psql("replication_lag_psql_hb_bounded", true).await;

    // Wait for staleness to appear (2 poll cycles at 2s each).
    eventually!(
        attempts: 20,
        sleep: Duration::from_secs(1),
        run_test: {
            psql_staleness(&rs_conn).await
        },
        then_assert: |staleness| {
            assert!(staleness.is_some(), "staleness should be non-NULL after 2+ polls");
        }
    );

    // Now let several more poll cycles elapse on a completely quiescent database.
    // With a 2s interval this is 3+ additional heartbeat writes.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Staleness should still be small — bounded by the poll interval plus some slack
    // for scheduling jitter, not growing unboundedly.
    let staleness = psql_staleness(&rs_conn)
        .await
        .expect("staleness should still be non-NULL");
    assert!(
        staleness < 10.0,
        "staleness should stay bounded on a quiescent database, got: {staleness}"
    );

    shutdown_tx.shutdown().await;
}
