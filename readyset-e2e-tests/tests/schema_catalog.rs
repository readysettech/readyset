#![cfg(feature = "failure_injection")]

use fail::FailScenario;
use mysql_async::Row;
use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::wait_for_schema_generation_change;
use readyset_server::Handle;
use readyset_util::failpoints;
use readyset_util::{eventually, shutdown::ShutdownSender};
use std::time::Duration;
use test_utils::tags;
use tokio::time::sleep;

// Delay the next schema catalog update long enough to test behavior when the adapter is stale
const CATALOG_UPDATE_DELAY_MS: u64 = 15_000;
const CATALOG_UPDATE_APPLY_WAIT: Duration = Duration::from_millis(CATALOG_UPDATE_DELAY_MS + 1_000);

struct SchemaGenerationRace<'a> {
    _handle: Handle,
    rs_conn: mysql_async::Conn,
    shutdown_tx: ShutdownSender,
    _failpoint_guard: FailScenario<'a>,
}

impl<'a> SchemaGenerationRace<'a> {
    /// Set up a Readyset instance with an upstream where the schema catalog stays stale until the
    /// failpoint-delayed update applies.
    async fn new() -> Self {
        readyset_tracing::init_test_logging();

        let failpoint_guard = FailScenario::setup();

        let (rs_opts, mut handle, shutdown_tx) =
            TestBuilder::default().build::<MySQLAdapter>().await;

        let db_name = rs_opts.db_name().unwrap().to_string();
        let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
        let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
        upstream_conn
            .query_drop("CREATE TABLE generation_mismatch (id INT PRIMARY KEY, balance INT)")
            .await
            .unwrap();

        // Wait for the adapter to pick up the initial table schema before delaying updates.
        //
        // We vary the query string slightly between attempts to avoid caching a rewritten query
        // using an out-of-date schema generation (which may not be invalidated yet).
        // TODO: remove the warmup variation once REA-6108 is fixed.
        let mut rs_conn = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
        let warmup_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut attempt = 0u64;
        loop {
            attempt += 1;
            let query = format!(
                "SELECT SUM(balance) FROM generation_mismatch WHERE id BETWEEN 1 AND 10 /* warmup:{attempt} */"
            );
            if rs_conn.query_drop(query).await.is_ok() {
                break;
            }
            if tokio::time::Instant::now() >= warmup_deadline {
                panic!(
                    "Timed out waiting for adapter to accept queries against generation_mismatch"
                );
            }
            sleep(Duration::from_millis(200)).await;
        }

        fail::cfg(
            failpoints::SCHEMA_CATALOG_SYNCHRONIZER_DELAY,
            &format!("1*sleep({CATALOG_UPDATE_DELAY_MS})"),
        )
        .expect("failed to set schema catalog synchronizer delay failpoint");

        let start_generation = handle.schema_catalog().await.unwrap().generation;

        upstream_conn
            .query_drop("ALTER TABLE generation_mismatch ADD COLUMN bonus BIGINT DEFAULT 0")
            .await
            .unwrap();

        // Wait for the server to apply the change and finish any resnapshotting, while the
        // failpoint-delayed schema catalog update keeps the adapter stale.
        wait_for_schema_generation_change(&mut handle, start_generation).await;

        // At this point, the server should have the ALTER TABLE applied, but the adapter's schema
        // catalog should be out of date.
        Self {
            _handle: handle,
            rs_conn,
            shutdown_tx,
            _failpoint_guard: failpoint_guard,
        }
    }

    async fn teardown(self) {
        self.shutdown_tx.shutdown().await;
    }
}

macro_rules! assert_schema_generation_error {
    ($err:expr) => {{
        let err_string = $err.to_string();
        assert!(
            err_string
                .to_ascii_lowercase()
                .contains("schema generation"),
            "expected schema generation mismatch error, got: {err_string}"
        );
    }};
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn create_cache_errors_when_catalog_is_stale() {
    let mut harness = SchemaGenerationRace::new().await;

    let err = harness
        .rs_conn
        .query::<Row, _>("CREATE CACHE FROM SELECT * FROM generation_mismatch")
        .await
        .expect_err("expected schema generation mismatch when using stale schema catalog");

    assert_schema_generation_error!(err);

    // Let adapter catch up after the delayed update applies
    sleep(CATALOG_UPDATE_APPLY_WAIT).await;

    let _ = harness
        .rs_conn
        .query::<Row, _>("CREATE CACHE FROM SELECT * FROM generation_mismatch")
        .await
        .expect("expected to succeed after schema catalog synchronized");

    harness.teardown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn explain_create_cache_errors_when_catalog_is_stale() {
    let mut harness = SchemaGenerationRace::new().await;

    let supported: String = harness
        .rs_conn
        .query::<Row, _>("EXPLAIN CREATE CACHE FROM SELECT * FROM generation_mismatch")
        .await
        .unwrap()
        .first()
        .unwrap()
        .get(2)
        .unwrap();

    assert!(
        supported.starts_with("no"),
        "expected supported to start with \"no\", got: {supported}"
    );
    assert_schema_generation_error!(supported);

    // Let adapter catch up after the delayed update applies
    sleep(CATALOG_UPDATE_APPLY_WAIT).await;

    // TODO(mvzink): Remove once REA-6108 is fixed; first explain gets cached and never invalidated
    harness
        .rs_conn
        .query_drop("DROP ALL PROXIED QUERIES")
        .await
        .unwrap();

    let supported: String = harness
        .rs_conn
        .query::<Row, _>("EXPLAIN CREATE CACHE FROM SELECT * FROM generation_mismatch")
        .await
        .unwrap()
        .first()
        .unwrap()
        .get(2)
        .unwrap();
    assert_eq!(supported, "yes");

    harness.teardown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn create_cache_concurrently_errors_when_catalog_is_stale() {
    let mut harness = SchemaGenerationRace::new().await;

    let id: String = harness
        .rs_conn
        .query::<Row, _>("CREATE CACHE CONCURRENTLY FROM SELECT * FROM generation_mismatch")
        .await
        .unwrap()
        .first()
        .unwrap()
        .get(0)
        .unwrap();

    eventually!(
        run_test: {
            let status: String = harness
                .rs_conn
                .query::<Row, _>(format!("SHOW READYSET MIGRATION STATUS {id}"))
                .await
                .unwrap()
                .first()
                .unwrap()
                .get(0)
                .unwrap();
            status
        },
        then_assert: |status| {
            assert_schema_generation_error!(status);
        }
    );

    // Let adapter catch up after the delayed update applies
    sleep(CATALOG_UPDATE_APPLY_WAIT).await;

    let id: String = harness
        .rs_conn
        .query::<Row, _>("CREATE CACHE CONCURRENTLY FROM SELECT * FROM generation_mismatch")
        .await
        .unwrap()
        .first()
        .unwrap()
        .get(0)
        .unwrap();

    eventually!(
        run_test: {
            let status: String = harness
                .rs_conn
                .query::<Row, _>(format!("SHOW READYSET MIGRATION STATUS {id}"))
                .await
                .unwrap()
                .first()
                .unwrap()
                .get(0)
                .unwrap();
            status
        },
        then_assert: |status| {
            assert_eq!(status, "Completed")
        }
    );

    harness.teardown().await;
}

/// Verifies that a delayed SSE schema catalog update causes a schema generation mismatch error
/// and that the adapter recovers once the update is applied. Uses a failpoint to delay the
/// adapter's `apply_update()` call, creating a window where the adapter's generation is behind
/// the server's. During this window `CREATE CACHE` is rejected. After the delay expires, the
/// adapter catches up and the same command succeeds.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn schema_generation_mismatch_recovered_after_delayed_sse_update() {
    readyset_tracing::init_test_logging();
    let failpoint_guard = FailScenario::setup();

    let (rs_opts, mut handle, shutdown_tx) =
        TestBuilder::default().build::<MySQLAdapter>().await;

    let db_name = rs_opts.db_name().unwrap().to_string();
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create a table and wait for the adapter to serve queries against it, confirming the
    // initial SSE snapshot was received and the schema catalog is initialized.
    upstream_conn
        .query_drop("CREATE TABLE sse_race (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();

    let mut rs_conn = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    let warmup_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        let query = format!(
            "SELECT * FROM sse_race WHERE id = 1 /* warmup:{attempt} */"
        );
        if rs_conn.query_drop(query).await.is_ok() {
            break;
        }
        if tokio::time::Instant::now() >= warmup_deadline {
            panic!("Timed out waiting for adapter to accept queries against sse_race");
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Delay the next schema catalog update application. The SSE event will be received by the
    // synchronizer but apply_update() will be blocked by the failpoint.
    const DELAY_MS: u64 = 15_000;
    fail::cfg(
        failpoints::SCHEMA_CATALOG_SYNCHRONIZER_DELAY,
        &format!("1*sleep({DELAY_MS})"),
    )
    .expect("failed to set schema catalog synchronizer delay failpoint");

    let start_generation = handle.schema_catalog().await.unwrap().generation;

    // DDL on upstream advances the server's generation. The SSE event is delivered but the
    // synchronizer's apply_update is stalled by the failpoint.
    upstream_conn
        .query_drop("ALTER TABLE sse_race ADD COLUMN extra INT")
        .await
        .unwrap();

    // Server has processed the DDL; its generation has advanced.
    wait_for_schema_generation_change(&mut handle, start_generation).await;

    // The adapter's schema catalog is still at the old generation because the synchronizer
    // is delayed. CREATE CACHE is rejected with a schema generation mismatch.
    let err = rs_conn
        .query::<Row, _>("CREATE CACHE FROM SELECT * FROM sse_race WHERE id = 1")
        .await
        .expect_err("expected schema generation mismatch when adapter is stale");
    assert_schema_generation_error!(err);

    // Wait for the failpoint delay to expire so the adapter catches up.
    sleep(Duration::from_millis(DELAY_MS + 2_000)).await;

    rs_conn
        .query_drop("DROP ALL PROXIED QUERIES")
        .await
        .unwrap();
    rs_conn
        .query::<Row, _>("CREATE CACHE FROM SELECT * FROM sse_race WHERE id = 1")
        .await
        .expect("expected success after adapter caught up");

    shutdown_tx.shutdown().await;
    drop(failpoint_guard);
}
