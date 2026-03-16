//! E2E tests for Online Index Build (OIB).
//!
//! This file contains two categories of tests:
//!
//! 1. **Happy-path tests** (always compiled): verify that OIB completes successfully,
//!    the cache serves correct data, and concurrent writes during the build are handled.
//!
//! 2. **Failpoint tests** (compiled only with `failure_injection` feature): verify that
//!    OIB failures at different points are handled gracefully -- the build is cleaned up,
//!    the cache reports an error status, and queries continue to work against upstream.

use std::time::Duration;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::SimpleQueryMessage;

/// Count the number of Row messages in a simple_query result set.
fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

// ---------------------------------------------------------------------------
// Happy-path test
// ---------------------------------------------------------------------------

/// Verify that OIB completes successfully on the happy path.
///
/// 1. Creates a table with 10 rows via upstream.
/// 2. Issues CREATE CACHE to trigger OIB.
/// 3. Inserts additional rows while OIB may still be running (concurrent writes).
/// 4. Polls until the cache is serving data, then asserts both the original rows
///    and the concurrently-inserted rows are visible.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn oib_happy_path_cache_serves_correct_data() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    // Create the table and seed it with 10 rows before creating the cache.
    upstream_conn
        .simple_query(
            "CREATE TABLE oib_happy_path \
             (id INT PRIMARY KEY, category INT, label TEXT)",
        )
        .await
        .unwrap();

    upstream_conn
        .simple_query(
            "INSERT INTO oib_happy_path (id, category, label) VALUES \
             (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c'), (4, 20, 'd'), (5, 30, 'e'), \
             (6, 10, 'f'), (7, 20, 'g'), (8, 30, 'h'), (9, 10, 'i'), (10, 30, 'j')",
        )
        .await
        .unwrap();

    let rs_conn = psql_helpers::connect(rs_opts).await;

    // Wait for ReadySet to replicate the table so that CREATE CACHE can proceed.
    eventually!(
        attempts: 60,
        sleep: Duration::from_millis(500),
        message: "ReadySet did not replicate oib_happy_path",
        {
            rs_conn
                .simple_query("SELECT id FROM oib_happy_path WHERE id = 1")
                .await
                .is_ok()
        }
    );

    // Trigger OIB by creating the cache. Retry because CREATE CACHE can
    // transiently return SchemaGenerationMismatch during replication.
    eventually!(
        attempts: 10,
        sleep: Duration::from_millis(1000),
        run_test: {
            rs_conn
                .simple_query(
                    "CREATE CACHE FROM \
                     SELECT id, label FROM oib_happy_path WHERE category = $1",
                )
                .await
                .map_err(|e| e.to_string())
        },
        then_assert: |result| {
            result.expect("CREATE CACHE should succeed on the happy path");
        }
    );

    // Insert additional rows concurrently -- these should be visible once the cache
    // is fully warmed and WAL catch-up has applied the writes.
    upstream_conn
        .simple_query(
            "INSERT INTO oib_happy_path (id, category, label) VALUES \
             (11, 10, 'k'), (12, 20, 'l')",
        )
        .await
        .unwrap();

    // Poll until the cache serves the expected rows.  We expect category=10 to have
    // 5 rows: ids 1, 2, 6, 9 (original) and 11 (concurrent insert).
    eventually!(
        attempts: 60,
        sleep: Duration::from_millis(500),
        run_test: {
            rs_conn
                .simple_query(
                    "SELECT id FROM oib_happy_path WHERE category = 10 ORDER BY id",
                )
                .await
                .unwrap()
        },
        then_assert: |rows| {
            assert_eq!(
                row_count(&rows),
                5,
                "expected 5 rows with category=10 (4 original + 1 concurrent insert)"
            );
        }
    );

    // Also verify category=20: 3 original (ids 3,4,7) + 1 concurrent insert (id 12).
    eventually!(
        attempts: 60,
        sleep: Duration::from_millis(500),
        run_test: {
            rs_conn
                .simple_query(
                    "SELECT id FROM oib_happy_path WHERE category = 20 ORDER BY id",
                )
                .await
                .unwrap()
        },
        then_assert: |rows| {
            assert_eq!(
                row_count(&rows),
                4,
                "expected 4 rows with category=20 (3 original + 1 concurrent insert)"
            );
        }
    );

    shutdown_tx.shutdown().await;
}

// ---------------------------------------------------------------------------
// Failpoint tests (require the `failure_injection` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "failure_injection")]
mod failpoint_tests {
    use std::time::Duration;

    use fail::FailScenario;
    use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
    use readyset_client_test_helpers::TestBuilder;
    use readyset_util::failpoints;
    use readyset_util::{eventually, shutdown::ShutdownSender};
    use test_utils::{tags, upstream};
    use super::row_count;

    /// Shared test harness for OIB failpoint tests.
    ///
    /// Sets up a ReadySet instance with PostgreSQL upstream, creates a table with data,
    /// and provides connections for interacting with both ReadySet and upstream.
    struct OibFailpointHarness<'a> {
        rs_conn: tokio_postgres::Client,
        shutdown_tx: ShutdownSender,
        _handle: readyset_server::Handle,
        _failpoint_guard: FailScenario<'a>,
    }

    impl<'a> OibFailpointHarness<'a> {
        /// Create a new harness with a table named `oib_fp_<suffix>` containing test data.
        ///
        /// The table has columns `(id INT PRIMARY KEY, category INT, label TEXT)` with 10 rows.
        /// The harness waits for ReadySet to replicate the table before returning.
        async fn new(suffix: &str) -> Self {
            readyset_tracing::init_test_logging();

            let failpoint_guard = FailScenario::setup();

            let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
                .fallback(true)
                .build::<PostgreSQLAdapter>()
                .await;

            let mut upstream_config = psql_helpers::upstream_config();
            upstream_config.dbname("noria");
            let upstream_conn = psql_helpers::connect(upstream_config).await;

            let table = format!("oib_fp_{suffix}");
            upstream_conn
                .simple_query(&format!(
                    "CREATE TABLE {table} (id INT PRIMARY KEY, category INT, label TEXT)"
                ))
                .await
                .unwrap();

            upstream_conn
                .simple_query(&format!(
                    "INSERT INTO {table} (id, category, label) VALUES \
                     (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c'), (4, 20, 'd'), (5, 30, 'e'), \
                     (6, 10, 'f'), (7, 20, 'g'), (8, 30, 'h'), (9, 10, 'i'), (10, 30, 'j')"
                ))
                .await
                .unwrap();

            let rs_conn = psql_helpers::connect(rs_opts).await;

            // Wait for ReadySet to replicate the table so queries can be proxied.
            let select_query = format!("SELECT id FROM {table} WHERE id = 1");
            eventually!(
                attempts: 60,
                sleep: Duration::from_millis(500),
                message: format!("ReadySet did not replicate table {table}"),
                {
                    rs_conn.simple_query(&select_query).await.is_ok()
                }
            );

            Self {
                rs_conn,
                shutdown_tx,
                _handle: handle,
                _failpoint_guard: failpoint_guard,
            }
        }

        async fn teardown(self) {
            self.shutdown_tx.shutdown().await;
        }
    }

    /// When the OIB START failpoint fires, `build_indices` fails immediately before any work
    /// begins. CREATE CACHE should return an error containing the failure message, and the
    /// query should continue to work against upstream via fallback.
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres)]
    async fn oib_failpoint_start_fails_cache_creation() {
        let harness = OibFailpointHarness::new("start").await;

        // Arm the failpoint so OIB fails at the very start.
        fail::cfg(failpoints::ONLINE_INDEX_BUILD_START, "return").unwrap();

        // CREATE CACHE triggers a migration which runs OIB. The failpoint causes the build
        // to fail, which should propagate as an error from CREATE CACHE.
        let result = harness
            .rs_conn
            .simple_query(
                "CREATE CACHE FROM SELECT id, label FROM oib_fp_start WHERE category = $1",
            )
            .await;

        assert!(
            result.is_err(),
            "expected CREATE CACHE to fail when OIB START failpoint is active, but it succeeded"
        );

        let err_msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            err_msg.contains("background index build failed")
                || err_msg.contains("failpoint")
                || err_msg.contains("index build"),
            "expected index build failure message, got: {err_msg}"
        );

        // The query should still work against upstream via fallback.
        let rows = harness
            .rs_conn
            .simple_query("SELECT id, label FROM oib_fp_start WHERE category = 10 ORDER BY id")
            .await
            .unwrap();

        assert_eq!(
            row_count(&rows),
            4,
            "expected 4 rows with category=10 from upstream fallback"
        );

        harness.teardown().await;
    }

    /// When the OIB POST_SCAN failpoint fires, the snapshot scan has completed but the build
    /// fails before WAL catch-up. The partial build should be cleaned up and CREATE CACHE
    /// should report an error. Queries should fall back to upstream.
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres)]
    async fn oib_failpoint_post_scan_cleans_up_and_falls_back() {
        let harness = OibFailpointHarness::new("postscan").await;

        // Arm the failpoint so OIB fails after the snapshot scan.
        fail::cfg(failpoints::ONLINE_INDEX_BUILD_POST_SCAN, "return").unwrap();

        let result = harness
            .rs_conn
            .simple_query(
                "CREATE CACHE FROM SELECT id, label FROM oib_fp_postscan WHERE category = $1",
            )
            .await;

        assert!(
            result.is_err(),
            "expected CREATE CACHE to fail when OIB POST_SCAN failpoint is active, but it succeeded"
        );

        let err_msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            err_msg.contains("background index build failed")
                || err_msg.contains("failpoint")
                || err_msg.contains("index build"),
            "expected index build failure message, got: {err_msg}"
        );

        // The query should still work against upstream via fallback.
        let rows = harness
            .rs_conn
            .simple_query(
                "SELECT id, label FROM oib_fp_postscan WHERE category = 20 ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(
            row_count(&rows),
            3,
            "expected 3 rows with category=20 from upstream fallback"
        );

        harness.teardown().await;
    }

    /// When the OIB PRE_ACTIVATE failpoint fires, the sidekick-to-primary transfer has completed
    /// but the build fails before index activation. The pending indices should be cleaned up
    /// and CREATE CACHE should report an error. Queries should fall back to upstream.
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres)]
    async fn oib_failpoint_pre_activate_cleans_up_and_falls_back() {
        let harness = OibFailpointHarness::new("preact").await;

        // Arm the failpoint so OIB fails after transfer but before activation.
        fail::cfg(failpoints::ONLINE_INDEX_BUILD_PRE_ACTIVATE, "return").unwrap();

        let result = harness
            .rs_conn
            .simple_query(
                "CREATE CACHE FROM SELECT id, label FROM oib_fp_preact WHERE category = $1",
            )
            .await;

        assert!(
            result.is_err(),
            "expected CREATE CACHE to fail when OIB PRE_ACTIVATE failpoint is active, but it succeeded"
        );

        let err_msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            err_msg.contains("background index build failed")
                || err_msg.contains("failpoint")
                || err_msg.contains("index build"),
            "expected index build failure message, got: {err_msg}"
        );

        // The query should still work against upstream via fallback.
        let rows = harness
            .rs_conn
            .simple_query(
                "SELECT id, label FROM oib_fp_preact WHERE category = 30 ORDER BY id",
            )
            .await
            .unwrap();

        assert_eq!(
            row_count(&rows),
            3,
            "expected 3 rows with category=30 from upstream fallback"
        );

        harness.teardown().await;
    }

    /// After an OIB failure, disabling the failpoint and retrying CREATE CACHE should succeed.
    /// This verifies that OIB failure cleanup leaves the system in a recoverable state.
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres)]
    async fn oib_failpoint_recovery_after_start_failure() {
        let harness = OibFailpointHarness::new("recover").await;

        // First attempt: arm the failpoint so OIB fails.
        fail::cfg(failpoints::ONLINE_INDEX_BUILD_START, "1*return").unwrap();

        let result = harness
            .rs_conn
            .simple_query(
                "CREATE CACHE FROM SELECT id, label FROM oib_fp_recover WHERE category = $1",
            )
            .await;

        assert!(
            result.is_err(),
            "expected first CREATE CACHE to fail with failpoint active"
        );

        // The failpoint was configured with "1*return" so it fires only once.
        // Second attempt should succeed because OIB runs without the failpoint.
        eventually!(
            attempts: 10,
            sleep: Duration::from_millis(1000),
            run_test: {
                harness
                    .rs_conn
                    .simple_query(
                        "CREATE CACHE FROM SELECT id, label FROM oib_fp_recover WHERE category = $1",
                    )
                    .await
                    .map_err(|e| e.to_string())
            },
            then_assert: |result| {
                result.expect("CREATE CACHE should succeed after failpoint expires");
            }
        );

        // Verify the cache actually serves correct data.
        eventually!(
            attempts: 20,
            sleep: Duration::from_millis(500),
            run_test: {
                harness
                    .rs_conn
                    .simple_query(
                        "SELECT id FROM oib_fp_recover WHERE category = 10 ORDER BY id",
                    )
                    .await
                    .unwrap()
            },
            then_assert: |rows| {
                assert_eq!(
                    row_count(&rows),
                    4,
                    "expected 4 rows with category=10 after recovery"
                );
            }
        );

        harness.teardown().await;
    }

} // mod failpoint_tests
