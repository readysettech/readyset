//! End-to-end coverage that the Postgres replicator survives controller-level
//! `min_persisted_replication_offset` RPC failures. Pre-REA-6591, a single
//! failure of that RPC propagated through `?` and tore down the replicator;
//! the connector then restart-looped on each status-update interval. After
//! REA-6591 the failure is logged and the loop continues, falling back to a
//! cached LSN (or skipping the ack entirely) per `select_ack_lsn`.
//!
//! Unit tests in `replicators::postgres_connector::connector` cover the
//! decision logic of `select_ack_lsn` directly. This test exercises the
//! integration: with the controller RPC perpetually failing, WAL events must
//! still flow into readyset.

#![cfg(feature = "failure_injection")]

use std::time::Duration;

use fail::FailScenario;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder};
use readyset_util::eventually;
use readyset_util::failpoints;
use test_utils::{tags, upstream};
use tokio_postgres::SimpleQueryMessage;

fn first_row_col(rows: &[SimpleQueryMessage], col: usize) -> String {
    match rows.first().expect("expected at least one row") {
        SimpleQueryMessage::Row(row) => row.get(col).expect("column should exist").to_string(),
        other => panic!("expected Row, got {other:?}"),
    }
}

/// Drop guard so a panicking test can't leave the failpoint armed for the
/// next serial test in this binary.
struct TestCleanup;
impl Drop for TestCleanup {
    fn drop(&mut self) {
        let _ = fail::cfg(failpoints::POSTGRES_MIN_PERSISTED_RPC, "off");
    }
}

/// Regression test for REA-6591: with the controller's
/// `min_persisted_replication_offset` RPC perpetually failing, the Postgres
/// replicator must continue applying WAL events. Before the fix, a single
/// RPC failure propagated through `?` in `send_standby_status_update`,
/// tearing the connector down; the resulting restart loop would either
/// stall replication or take far longer than the test's 30 s budget to
/// catch up.
///
/// The failpoint is armed *before* readyset boots so the very first status
/// update sees the failure (exercises the `Err`/no-cache branch in
/// `select_ack_lsn`). Default `status_update_interval_secs` is 10, so the
/// 12 s pause between row batches guarantees at least one timer-driven
/// status update fires under the failpoint before the second batch of
/// inserts is verified.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn psql_min_persisted_rpc_failure_is_non_fatal() {
    readyset_tracing::init_test_logging();
    let db_name = "psql_min_persisted_rpc_nonfatal";
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

    let _cleanup = TestCleanup;

    let _fail_scenario = FailScenario::setup();
    fail::cfg(failpoints::POSTGRES_MIN_PERSISTED_RPC, "return")
        .expect("failed to configure POSTGRES_MIN_PERSISTED_RPC failpoint");

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    eventually!(attempts: 60, sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn.simple_query("SELECT val FROM t1 WHERE id = 1").await.unwrap();
            rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
        }, then_assert: |count| {
            assert_eq!(
                count, 1,
                "row 1 must replicate even though min-persisted RPC keeps failing"
            );
        }
    );

    // Wait past one status_update_interval (default 10 s) so at least one
    // timer-driven status update fires under the armed failpoint. If the fix
    // isn't in place the connector will have started its restart loop by now.
    tokio::time::sleep(Duration::from_secs(12)).await;

    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (2, 20)")
        .await
        .unwrap();
    upstream_conn
        .simple_query("INSERT INTO t1 VALUES (3, 30)")
        .await
        .unwrap();

    eventually!(attempts: 60, sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn
                .simple_query("SELECT val FROM t1 WHERE id IN (2, 3)")
                .await
                .unwrap();
            rows.iter().filter(|m| matches!(m, SimpleQueryMessage::Row(_))).count()
        }, then_assert: |count| {
            assert_eq!(
                count, 2,
                "rows inserted after the failpoint armed must still replicate"
            );
        }
    );

    let rows = rs_conn
        .simple_query("SELECT val FROM t1 WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(first_row_col(&rows, 0), "30");

    shutdown_tx.shutdown().await;
}
