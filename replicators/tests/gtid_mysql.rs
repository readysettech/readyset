//! Integration tests for MySQL GTID-based replication.
//!
//! These tests verify that Readyset correctly handles GTID (Global Transaction Identifier)
//! replication, including:
//! - Basic snapshot and binlog replication with GTID tracking
//! - Crash recovery with pending GTID handling
//! - Catch-up detection after offline periods
//! - Multiple sequential transactions
//! - UPDATE and DELETE operations
//! - Transaction rollback handling
//!
//! Tests use the `mysql80_gtid` and `mysql84_gtid` upstream variants, which provision
//! MySQL containers with `--gtid-mode=ON --enforce-gtid-consistency=ON`.
//!
//! ## GTID Tags
//!
//! GTID tags (introduced in MySQL 8.4) are supported end-to-end: data structures,
//! replication connector, and SID block building all handle tagged GTIDs.
//! Integration tests for tagged GTIDs use the `mysql84_gtid` upstream variant.
//!
//! Unit tests for GTID tag handling are in `replication-offset/src/mysql_gtid.rs`.
//!
//! Run with:
//! ```notrust
//! cargo test -p replicators --test gtid_mysql
//! ```

use std::env;
use std::sync::Arc;
#[cfg(feature = "failure_injection")]
use std::sync::atomic::{AtomicUsize, Ordering};

use database_utils::{DatabaseURL, UpstreamConfig as Config};
#[cfg(feature = "failure_injection")]
use fail::FailScenario;
use mysql_async::prelude::Queryable;
use readyset_client::ReadySetHandle;
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_data::{DfValue, Dialect};
use readyset_errors::ReadySetResult;
use readyset_server::Builder;
use readyset_sql::ast::Relation;
use readyset_sql_parsing::ParsingPreset;
use readyset_telemetry_reporter::TelemetrySender;
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::retry_with_exponential_backoff;
use readyset_util::shutdown::ShutdownSender;
use replicators::table_filter::TableFilter;
use replicators::{ControllerMessage, NoriaAdapter, ReplicatorMessage};
use test_utils::{tags, upstream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::error;

/// The database name used for GTID tests
const GTID_TEST_DB: &str = "gtid_test_db";

/// Assert the MySQL server has GTID mode enabled.
async fn assert_gtid_enabled(conn: &mut mysql_async::Conn) {
    let gtid_mode: String = conn
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .expect("failed to query gtid_mode")
        .expect("gtid_mode variable missing");
    assert_eq!(
        gtid_mode, "ON",
        "GTID tests require a MySQL server with gtid_mode=ON"
    );
}

/// Get the MySQL URL for testing from environment variables
fn mysql_url() -> String {
    format!(
        "mysql://{}:{}@{}:{}/{}",
        env::var("MYSQL_USER").unwrap_or_else(|_| "root".into()),
        env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "noria".into()),
        env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
        GTID_TEST_DB,
    )
}

/// A channel wrapper for receiving controller messages
struct TestChannel {
    rx: UnboundedReceiver<ControllerMessage>,
}

impl TestChannel {
    async fn snapshot_completed(&mut self) -> ReadySetResult<()> {
        use ControllerMessage::*;
        loop {
            match self.rx.recv().await {
                Some(SnapshotDone) => return Ok(()),
                Some(UnrecoverableError(e)) => return Err(e),
                Some(RecoverableError(e)) => return Err(e),
                Some(_) => continue,
                None => return Err(readyset_errors::internal_err!("Channel closed")),
            }
        }
    }
}

/// MySQL database connection wrapper
struct DbConnection {
    conn: mysql_async::Conn,
}

impl DbConnection {
    async fn connect(url: &str) -> ReadySetResult<Self> {
        let opts: mysql_async::Opts = url.parse().unwrap();
        let test_db_name = opts.db_name().unwrap();
        let no_db_opts = mysql_async::OptsBuilder::from_opts(opts.clone())
            .db_name::<String>(None)
            .prefer_socket(false);

        // First, connect without a db
        let mut client = retry_with_exponential_backoff!(
            { mysql_async::Conn::new(no_db_opts.clone()).await },
            retries: 10,
            delay: 100,
            backoff: 2,
        )
        .unwrap();

        assert_gtid_enabled(&mut client).await;

        // Then drop and recreate the test db
        client
            .query_drop(format!("DROP SCHEMA IF EXISTS {test_db_name};"))
            .await?;
        client
            .query_drop(format!("CREATE SCHEMA {test_db_name};"))
            .await?;

        // Then switch to the test db
        client.query_drop(format!("USE {test_db_name};")).await?;

        Ok(DbConnection { conn: client })
    }

    async fn query(&mut self, query: &str) -> ReadySetResult<()> {
        self.conn.query_drop(query).await?;
        Ok(())
    }

    /// Execute a query using a tagged GTID (`SET @@SESSION.GTID_NEXT = 'AUTOMATIC:tag'`),
    /// then reset to `AUTOMATIC`. Requires MySQL 8.4+ with GTID_MODE=ON.
    async fn query_with_tag(&mut self, query: &str, tag: &str) -> ReadySetResult<()> {
        self.conn
            .query_drop(format!("SET @@SESSION.GTID_NEXT = 'AUTOMATIC:{tag}'"))
            .await?;
        self.conn.query_drop(query).await?;
        self.conn
            .query_drop("SET @@SESSION.GTID_NEXT = 'AUTOMATIC'")
            .await?;
        Ok(())
    }

    async fn stop(self) {
        drop(self.conn);
    }
}

/// Test handle for managing Readyset instance
struct TestHandle {
    url: String,
    dialect: Dialect,
    #[allow(dead_code)] // Kept for server lifetime management
    noria: readyset_server::Handle,
    authority: Arc<Authority>,
    replication_rt: Option<tokio::runtime::Runtime>,
    controller_rx: Option<TestChannel>,
    #[allow(dead_code)]
    replicator_tx: Option<UnboundedSender<ReplicatorMessage>>,
}

impl Drop for TestHandle {
    fn drop(&mut self) {
        if let Some(rt) = self.replication_rt.take() {
            rt.shutdown_background();
        }
    }
}

const MAX_ATTEMPTS: usize = 40;

impl TestHandle {
    async fn start_noria(
        url: String,
        config: Option<Config>,
    ) -> ReadySetResult<(TestHandle, ShutdownSender)> {
        let mut builder = Builder::for_tests();
        builder.set_dialect(readyset_sql::Dialect::MySQL);
        Self::start_noria_with_builder(url, config, builder).await
    }

    async fn start_noria_with_builder(
        url: String,
        config: Option<Config>,
        mut builder: Builder,
    ) -> ReadySetResult<(TestHandle, ShutdownSender)> {
        readyset_tracing::init_test_logging();

        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
            authority_store,
        )));

        let persistence = readyset_server::PersistenceParameters {
            mode: readyset_server::DurabilityMode::DeleteOnExit,
            ..Default::default()
        };
        builder.set_persistence(persistence);

        let telemetry_sender = builder.telemetry.clone();
        let parsing_preset = builder.parsing_preset;
        let (noria, shutdown_tx) = builder.start(Arc::clone(&authority)).await.unwrap();

        let mut handle = TestHandle {
            url: url.clone(),
            dialect: Dialect::DEFAULT_MYSQL,
            noria,
            authority: Arc::clone(&authority),
            replication_rt: None,
            controller_rx: None,
            replicator_tx: None,
        };

        handle.replicator_tx = Some(
            handle
                .start_repl(config, telemetry_sender, parsing_preset)
                .await?,
        );

        Ok((handle, shutdown_tx))
    }

    async fn start_repl(
        &mut self,
        config: Option<Config>,
        telemetry_sender: TelemetrySender,
        parsing_preset: ParsingPreset,
    ) -> ReadySetResult<UnboundedSender<ReplicatorMessage>> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let controller = ReadySetHandle::new(Arc::clone(&self.authority)).await;

        let (controller_tx, controller_rx) = tokio::sync::mpsc::unbounded_channel();
        let (replicator_tx, mut replicator_rx): (
            UnboundedSender<ReplicatorMessage>,
            UnboundedReceiver<ReplicatorMessage>,
        ) = tokio::sync::mpsc::unbounded_channel();

        self.controller_rx = Some(TestChannel { rx: controller_rx });

        let url: DatabaseURL = self.url.parse().unwrap();
        let mut table_filter = TableFilter::try_new(
            url.dialect(),
            None, // replication_tables
            None, // replication_tables_ignore
            url.db_name(),
        )?;

        let upstream_db_url = Some(self.url.clone().into());
        let cdc_db_url = Some(self.url.clone().into());
        let config = Config {
            upstream_db_url,
            cdc_db_url,
            ..config.unwrap_or_default()
        };

        runtime.spawn(async move {
            let Err(error) = NoriaAdapter::start(
                controller,
                &config,
                &url,
                &mut table_filter,
                &controller_tx,
                &mut replicator_rx,
                telemetry_sender,
                true,  // server_startup
                false, // enable_statement_logging
                parsing_preset,
                tokio::sync::mpsc::unbounded_channel().0, // table_status_tx sink
            )
            .await;
            error!(%error, "Error in replicator");
            let _ = controller_tx.send(ControllerMessage::UnrecoverableError(error));
        });

        if let Some(rt) = self.replication_rt.replace(runtime) {
            rt.shutdown_background();
        }

        Ok(replicator_tx)
    }

    async fn controller(&self) -> ReadySetHandle {
        ReadySetHandle::new(Arc::clone(&self.authority)).await
    }

    async fn get_results(
        &mut self,
        view_name: &str,
        expected_results: &[&[DfValue]],
    ) -> ReadySetResult<Vec<Vec<DfValue>>> {
        let mut attempt: usize = 0;
        let result = loop {
            match self.get_results_inner(view_name).await {
                Err(_) if attempt < MAX_ATTEMPTS => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Ok(res) if res != expected_results && attempt < MAX_ATTEMPTS => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Ok(res) => {
                    break res;
                }
                Err(err) => return Err(err),
            }
        };
        Ok(result)
    }

    async fn get_results_inner(&mut self, view_name: &str) -> ReadySetResult<Vec<Vec<DfValue>>> {
        use readyset_client::recipe::changelist::{Change, ChangeList, CreateCache};
        use readyset_sql_parsing::parse_select;

        let schema = GTID_TEST_DB;
        let query_name = format!("q_{view_name}");
        let select_stmt = format!("SELECT * FROM {schema}.{view_name}");
        self.controller()
            .await
            .extend_recipe(ChangeList::from_changes(
                vec![
                    Change::Drop {
                        name: Relation {
                            schema: Some(schema.into()),
                            name: query_name.clone().into(),
                        },
                        if_exists: true,
                    },
                    Change::CreateCache(CreateCache {
                        name: Some(Relation {
                            schema: Some(schema.into()),
                            name: query_name.clone().into(),
                        }),
                        statement: Box::new(
                            parse_select(readyset_sql::Dialect::MySQL, select_stmt.clone())
                                .unwrap(),
                        ),
                        always: false,
                        schema_generation_used: None,
                    }),
                ],
                self.dialect,
            ))
            .await?;
        let mut getter = self
            .controller()
            .await
            .view(Relation {
                schema: Some(schema.into()),
                name: query_name.into(),
            })
            .await?
            .into_reader_handle()
            .unwrap();
        let results = getter.lookup(&[0.into()], true).await?;
        let mut results = results.into_vec();
        results.sort();
        Ok(results)
    }

    async fn stop(&mut self) {
        if let Some(rt) = self.replication_rt.take() {
            rt.shutdown_background();
        }
    }
}

/// Macro to check query results with retries
macro_rules! check_results {
    ($ctx:ident, $view:expr, $test_name:expr, $expected_results:expr $(,)?) => {
        let expected_results: &[&[DfValue]] = $expected_results;
        let result = $ctx.get_results($view, expected_results).await.unwrap();
        let result_slices = result
            .iter()
            .map(|row| row.as_slice())
            .collect::<Vec<&[DfValue]>>();
        pretty_assertions::assert_eq!(
            expected_results,
            result_slices.as_slice(),
            "test {} failed",
            $test_name
        );
    };
}

// =============================================================================
// Basic GTID Replication Test
// =============================================================================

/// Test that GTID-based replication works correctly for basic operations.
/// This test:
/// 1. Checks if the MySQL server has GTID_MODE=ON, skips if not
/// 2. Creates a test table with initial data
/// 3. Starts Readyset and waits for snapshot completion
/// 4. Verifies snapshot data is replicated
/// 5. Inserts a row and verifies binlog replication works
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_basic_replication() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table with initial data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_test CASCADE;
             CREATE TABLE gtid_test (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_test VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_test",
        "gtid_snapshot",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Insert a row via replication
    client
        .query("INSERT INTO gtid_test VALUES (2, 200);")
        .await
        .unwrap();

    // Verify the replicated row appears
    check_results!(
        ctx,
        "gtid_test",
        "gtid_replication",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(200)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// Multiple Transactions Test
// =============================================================================

/// Test that multiple sequential GTID transactions are replicated correctly.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_multiple_transactions() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table
    client
        .query(
            "DROP TABLE IF EXISTS gtid_multi CASCADE;
             CREATE TABLE gtid_multi (id INT PRIMARY KEY, val INT);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Insert multiple rows in separate transactions
    for i in 1..=5 {
        client
            .query(&format!(
                "INSERT INTO gtid_multi VALUES ({}, {});",
                i,
                i * 10
            ))
            .await
            .unwrap();
    }

    // Verify all rows are replicated
    check_results!(
        ctx,
        "gtid_multi",
        "gtid_multi_txns",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(20)],
            &[DfValue::Int(3), DfValue::Int(30)],
            &[DfValue::Int(4), DfValue::Int(40)],
            &[DfValue::Int(5), DfValue::Int(50)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// UPDATE and DELETE Operations Test
// =============================================================================

/// Test that UPDATE and DELETE operations replicate correctly in GTID mode.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_update_delete() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table with initial data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_upd_del CASCADE;
             CREATE TABLE gtid_upd_del (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_upd_del VALUES (1, 10), (2, 20), (3, 30);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_upd_del",
        "gtid_snapshot_upd_del",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(20)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    // UPDATE a row
    client
        .query("UPDATE gtid_upd_del SET val = 21 WHERE id = 2;")
        .await
        .unwrap();

    // Verify UPDATE is replicated
    check_results!(
        ctx,
        "gtid_upd_del",
        "gtid_after_update",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(21)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    // DELETE a row
    client
        .query("DELETE FROM gtid_upd_del WHERE id = 1;")
        .await
        .unwrap();

    // Verify DELETE is replicated
    check_results!(
        ctx,
        "gtid_upd_del",
        "gtid_after_delete",
        &[
            &[DfValue::Int(2), DfValue::Int(21)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// Catch-up Detection Test
// =============================================================================

/// Test that Readyset correctly catches up on GTID replication after being offline.
/// This test:
/// 1. Starts Readyset with initial data
/// 2. Stops the replicator
/// 3. Inserts data to MySQL while replicator is offline
/// 4. Restarts the replicator
/// 5. Verifies that the offline writes are caught up
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_catch_up_after_offline() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table with initial data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_catchup CASCADE;
             CREATE TABLE gtid_catchup (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_catchup VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify initial data
    check_results!(
        ctx,
        "gtid_catchup",
        "gtid_catchup_initial",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Stop the replicator
    ctx.stop().await;

    // Insert rows while replicator is offline
    client
        .query("INSERT INTO gtid_catchup VALUES (2, 201);")
        .await
        .unwrap();
    client
        .query("INSERT INTO gtid_catchup VALUES (3, 202);")
        .await
        .unwrap();

    // Verify that the offline writes are NOT visible in Readyset yet
    // (replicator is stopped, so no new data should appear)
    check_results!(
        ctx,
        "gtid_catchup",
        "gtid_catchup_while_offline",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Restart the replicator
    let telemetry_sender = TelemetrySender::new_no_op();
    ctx.replicator_tx = Some(
        ctx.start_repl(
            Some(Config {
                require_gtid: true,
                ..Default::default()
            }),
            telemetry_sender,
            ParsingPreset::for_tests(),
        )
        .await
        .unwrap(),
    );
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify all data is caught up (initial + offline writes)
    check_results!(
        ctx,
        "gtid_catchup",
        "gtid_catchup_after_restart",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(201)],
            &[DfValue::Int(3), DfValue::Int(202)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// Transaction Rollback Test
// =============================================================================

/// Test that rolled back transactions are not replicated in GTID mode.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_transaction_rollback() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table with initial data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_rollback CASCADE;
             CREATE TABLE gtid_rollback (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_rollback VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_rollback",
        "gtid_rollback_snapshot",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Execute a transaction that rolls back
    client.query("START TRANSACTION;").await.unwrap();
    client
        .query("INSERT INTO gtid_rollback VALUES (2, 999);")
        .await
        .unwrap();
    client.query("ROLLBACK;").await.unwrap();

    // Insert a committed row
    client
        .query("INSERT INTO gtid_rollback VALUES (3, 300);")
        .await
        .unwrap();

    // Verify only committed rows are replicated (rollback row should not appear)
    check_results!(
        ctx,
        "gtid_rollback",
        "gtid_rollback_after_rollback",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(3), DfValue::Int(300)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// Crash Recovery Test (requires failure_injection feature)
// =============================================================================

/// Test that GTID crash recovery correctly skips already-applied events within
/// a single GTID transaction.
///
/// This test:
/// 1. Creates a table and starts Readyset
/// 2. Configures a failpoint to panic after processing 2 row events
/// 3. Inserts 5 rows in a SINGLE transaction (all share the same GTID)
/// 4. The replicator crashes after the 2nd row event within the transaction
/// 5. Verifies only 2 rows are visible (partial transaction state)
/// 6. Restarts the replicator
/// 7. Verifies all 5 rows are present (crash recovery applied remaining rows,
///    skipping the already-applied ones using event_index tracking)
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_crash_recovery() {
    readyset_tracing::init_test_logging();
    let _fail_scenario = FailScenario::setup();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create the test table with initial data (this will be snapshotted)
    client
        .query(
            "DROP TABLE IF EXISTS gtid_crash CASCADE;
             CREATE TABLE gtid_crash (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_crash VALUES (0, 100);",
        )
        .await
        .unwrap();

    // Use batch size of 1 so each row event is flushed individually,
    // ensuring the failpoint crash leaves exactly 2 rows applied.
    let config = Config {
        require_gtid: true,
        replication_batch_size: 1,
        ..Default::default()
    };

    // Start Readyset and wait for snapshot
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_crash",
        "gtid_crash_snapshot",
        &[&[DfValue::Int(0), DfValue::Int(100)]],
    );

    // Use a callback to track failpoint invocations and panic after 2
    static ROW_EVENT_COUNT: AtomicUsize = AtomicUsize::new(0);
    ROW_EVENT_COUNT.store(0, Ordering::SeqCst);

    fail::cfg_callback(failpoints::MYSQL_GTID_ROW_EVENT, move || {
        let count = ROW_EVENT_COUNT.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "FAILPOINT: mysql-gtid-row-event triggered, count={}",
            count + 1
        );
        if count >= 2 {
            panic!("Injected crash after {} row events", count + 1);
        }
    })
    .unwrap();

    // Insert 5 rows in a SINGLE transaction - all will share the same GTID
    // The failpoint will panic after processing rows 1 and 2, during row 3
    client.query("START TRANSACTION;").await.unwrap();
    for i in 1..=5 {
        client
            .query(&format!("INSERT INTO gtid_crash VALUES ({}, {});", i, i))
            .await
            .unwrap();
    }
    client.query("COMMIT;").await.unwrap();

    // Give the replicator time to process some events and hit the failpoint
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Stop the crashed replicator
    ctx.stop().await;

    // Verify partial state: only snapshot + 2 rows applied before crash
    // (replicator crashed after processing rows 1 and 2, before row 3)
    check_results!(
        ctx,
        "gtid_crash",
        "gtid_crash_partial_state",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
        ],
    );

    // Clear the failpoint before restarting
    fail::cfg(failpoints::MYSQL_GTID_ROW_EVENT, "off").unwrap();

    // Restart the replicator - it should recover and apply remaining events
    let telemetry_sender = TelemetrySender::new_no_op();
    ctx.replicator_tx = Some(
        ctx.start_repl(
            Some(config),
            telemetry_sender,
            ParsingPreset::for_tests(),
        )
        .await
        .unwrap(),
    );
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify all data is present after crash recovery
    // The recovery should have:
    // - Recognized the pending GTID with event_index=2
    // - Skipped already-applied events (rows 1-2) using event_index
    // - Applied remaining events (rows 3-5)
    check_results!(
        ctx,
        "gtid_crash",
        "gtid_crash_after_recovery",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
            &[DfValue::Int(3), DfValue::Int(3)],
            &[DfValue::Int(4), DfValue::Int(4)],
            &[DfValue::Int(5), DfValue::Int(5)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test crash recovery when new transactions are inserted after the crash.
///
/// Validates that crash recovery works correctly when there are additional
/// transactions committed to MySQL between the crash and the restart. On
/// reconnect, the replicator must:
/// - Correctly apply the stale-skip counter to the original pending GTID (A)
/// - Process subsequent transactions (B) in full, without applying the stale
///   skip counter to them
///
/// 1. Creates table, inserts snapshot row
/// 2. Starts replicator, waits for snapshot
/// 3. Inserts 5 rows in a single transaction (GTID A)
/// 4. Failpoint: crashes after 2 row events
/// 5. Verifies partial state: snapshot + 2 rows
/// 6. Before restarting, inserts a new transaction (GTID B) in a second table
/// 7. Restarts replicator (failpoint off)
/// 8. Verifies: all 5 rows from GTID A are present (crash recovery completed)
/// 9. Verifies: all rows from GTID B are present (not skipped by stale counter)
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_crash_recovery_with_post_crash_transactions() {
    readyset_tracing::init_test_logging();
    let _fail_scenario = FailScenario::setup();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create TWO test tables: one for the crashing transaction, one for the post-crash transaction
    client
        .query(
            "DROP TABLE IF EXISTS gtid_skip_a CASCADE;
             DROP TABLE IF EXISTS gtid_skip_b CASCADE;
             CREATE TABLE gtid_skip_a (id INT PRIMARY KEY, val INT);
             CREATE TABLE gtid_skip_b (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_skip_a VALUES (0, 100);",
        )
        .await
        .unwrap();

    // Start Readyset and wait for snapshot
    // Use batch size of 1 so each row event is flushed individually,
    // ensuring the failpoint crash leaves exactly 2 rows applied.
    let config = Config {
        require_gtid: true,
        replication_batch_size: 1,
        ..Default::default()
    };

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_skip_a",
        "gtid_skip_a_snapshot",
        &[&[DfValue::Int(0), DfValue::Int(100)]],
    );

    // Configure failpoint: crash after processing 2 row events
    static SKIP_ROW_EVENT_COUNT: AtomicUsize = AtomicUsize::new(0);
    SKIP_ROW_EVENT_COUNT.store(0, Ordering::SeqCst);

    fail::cfg_callback(failpoints::MYSQL_GTID_ROW_EVENT, move || {
        let count = SKIP_ROW_EVENT_COUNT.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "FAILPOINT: mysql-gtid-row-event triggered, count={}",
            count + 1
        );
        if count >= 2 {
            panic!("Injected crash after {} row events", count + 1);
        }
    })
    .unwrap();

    // Insert 5 rows in a SINGLE transaction (GTID A)
    // The failpoint will crash after processing rows 1 and 2
    client.query("START TRANSACTION;").await.unwrap();
    for i in 1..=5 {
        client
            .query(&format!("INSERT INTO gtid_skip_a VALUES ({i}, {i});"))
            .await
            .unwrap();
    }
    client.query("COMMIT;").await.unwrap();

    // Give the replicator time to process some events and hit the failpoint
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Stop the crashed replicator
    ctx.stop().await;

    // Verify partial state: only snapshot + 2 rows applied before crash
    check_results!(
        ctx,
        "gtid_skip_a",
        "gtid_skip_a_partial_state",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
        ],
    );

    // Clear the failpoint before restarting
    fail::cfg(failpoints::MYSQL_GTID_ROW_EVENT, "off").unwrap();

    // Insert a NEW transaction (GTID B) BEFORE restarting the replicator.
    // This transaction is in a different table so we can verify it independently.
    // On restart, the replicator will receive GTID A first (crash recovery) then GTID B.
    client
        .query(
            "INSERT INTO gtid_skip_b VALUES (100, 1001);
             INSERT INTO gtid_skip_b VALUES (101, 1002);
             INSERT INTO gtid_skip_b VALUES (102, 1003);",
        )
        .await
        .unwrap();

    // Restart the replicator - it should:
    // 1. Recover GTID A: skip 2 already-applied events, apply remaining 3
    // 2. Process GTID B: apply all 3 rows without any skipping
    let telemetry_sender = TelemetrySender::new_no_op();
    ctx.replicator_tx = Some(
        ctx.start_repl(
            Some(config),
            telemetry_sender,
            ParsingPreset::for_tests(),
        )
        .await
        .unwrap(),
    );
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify all rows from GTID A are present (crash recovery completed)
    check_results!(
        ctx,
        "gtid_skip_a",
        "gtid_skip_a_after_recovery",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
            &[DfValue::Int(3), DfValue::Int(3)],
            &[DfValue::Int(4), DfValue::Int(4)],
            &[DfValue::Int(5), DfValue::Int(5)],
        ],
    );

    // Verify all rows from GTID B are present (not skipped by stale counter)
    check_results!(
        ctx,
        "gtid_skip_b",
        "gtid_skip_b_all_present",
        &[
            &[DfValue::Int(100), DfValue::Int(1001)],
            &[DfValue::Int(101), DfValue::Int(1002)],
            &[DfValue::Int(102), DfValue::Int(1003)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// GTID Tags Tests (require MySQL 8.4+ with GTID_MODE=ON)
// =============================================================================

/// Test that a single tagged GTID transaction is replicated correctly.
/// 1. Creates a table with initial data (snapshotted, untagged)
/// 2. Starts Readyset and verifies the snapshot
/// 3. Inserts a row using a tagged GTID (`AUTOMATIC:blue`)
/// 4. Verifies the tagged transaction is replicated
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_basic_replication() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create table with initial (untagged) data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_basic CASCADE;
             CREATE TABLE gtid_tag_basic (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_tag_basic VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_tag_basic",
        "gtid_tag_basic_snapshot",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Insert a row using a tagged GTID
    client
        .query_with_tag("INSERT INTO gtid_tag_basic VALUES (2, 200);", "blue")
        .await
        .unwrap();

    // Verify the tagged row is replicated
    check_results!(
        ctx,
        "gtid_tag_basic",
        "gtid_tag_basic_after_tagged_insert",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(200)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test that alternating tagged and untagged GTID transactions all replicate.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_mixed_tagged_and_untagged() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_mixed CASCADE;
             CREATE TABLE gtid_tag_mixed (id INT PRIMARY KEY, val INT);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Alternate between tagged and untagged inserts
    client
        .query_with_tag("INSERT INTO gtid_tag_mixed VALUES (1, 10);", "blue")
        .await
        .unwrap();
    client
        .query("INSERT INTO gtid_tag_mixed VALUES (2, 20);")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_mixed VALUES (3, 30);", "blue")
        .await
        .unwrap();
    client
        .query("INSERT INTO gtid_tag_mixed VALUES (4, 40);")
        .await
        .unwrap();

    // Verify all rows replicated regardless of tag presence
    check_results!(
        ctx,
        "gtid_tag_mixed",
        "gtid_tag_mixed_all_rows",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(20)],
            &[DfValue::Int(3), DfValue::Int(30)],
            &[DfValue::Int(4), DfValue::Int(40)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test that rows from multiple different GTID tags all replicate correctly.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_multiple_tags() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_multi CASCADE;
             CREATE TABLE gtid_tag_multi (id INT PRIMARY KEY, val INT);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Insert rows with different tags
    client
        .query_with_tag("INSERT INTO gtid_tag_multi VALUES (1, 10);", "blue")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_multi VALUES (2, 20);", "red")
        .await
        .unwrap();
    client
        .query("INSERT INTO gtid_tag_multi VALUES (3, 30);")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_multi VALUES (4, 40);", "blue")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_multi VALUES (5, 50);", "red")
        .await
        .unwrap();

    // Verify all rows from all tag sources are replicated
    check_results!(
        ctx,
        "gtid_tag_multi",
        "gtid_tag_multi_all_rows",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(20)],
            &[DfValue::Int(3), DfValue::Int(30)],
            &[DfValue::Int(4), DfValue::Int(40)],
            &[DfValue::Int(5), DfValue::Int(50)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test that UPDATE and DELETE operations using tagged GTIDs replicate correctly.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_update_delete() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_upd_del CASCADE;
             CREATE TABLE gtid_tag_upd_del (id INT PRIMARY KEY, val INT);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Insert rows using tagged GTIDs
    client
        .query_with_tag("INSERT INTO gtid_tag_upd_del VALUES (1, 10);", "blue")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_upd_del VALUES (2, 20);", "blue")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_upd_del VALUES (3, 30);", "blue")
        .await
        .unwrap();

    // Verify initial data
    check_results!(
        ctx,
        "gtid_tag_upd_del",
        "gtid_tag_upd_del_initial",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(20)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    // UPDATE a row with a tagged GTID
    client
        .query_with_tag("UPDATE gtid_tag_upd_del SET val = 21 WHERE id = 2;", "blue")
        .await
        .unwrap();

    // Verify UPDATE is replicated
    check_results!(
        ctx,
        "gtid_tag_upd_del",
        "gtid_tag_upd_del_after_update",
        &[
            &[DfValue::Int(1), DfValue::Int(10)],
            &[DfValue::Int(2), DfValue::Int(21)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    // DELETE a row with a tagged GTID
    client
        .query_with_tag("DELETE FROM gtid_tag_upd_del WHERE id = 1;", "blue")
        .await
        .unwrap();

    // Verify DELETE is replicated
    check_results!(
        ctx,
        "gtid_tag_upd_del",
        "gtid_tag_upd_del_after_delete",
        &[
            &[DfValue::Int(2), DfValue::Int(21)],
            &[DfValue::Int(3), DfValue::Int(30)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test that Readyset catches up on tagged GTID replication after being offline.
/// 1. Starts Readyset with initial data
/// 2. Stops the replicator
/// 3. Inserts data with tagged GTIDs while replicator is offline
/// 4. Restarts the replicator
/// 5. Verifies that the offline writes are caught up
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_catch_up_after_offline() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create table with initial untagged data
    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_catchup CASCADE;
             CREATE TABLE gtid_tag_catchup (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_tag_catchup VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify initial data
    check_results!(
        ctx,
        "gtid_tag_catchup",
        "gtid_tag_catchup_initial",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Stop the replicator
    ctx.stop().await;

    // Insert rows with tagged GTIDs while replicator is offline
    client
        .query_with_tag("INSERT INTO gtid_tag_catchup VALUES (2, 201);", "blue")
        .await
        .unwrap();
    client
        .query_with_tag("INSERT INTO gtid_tag_catchup VALUES (3, 202);", "red")
        .await
        .unwrap();
    client
        .query("INSERT INTO gtid_tag_catchup VALUES (4, 203);")
        .await
        .unwrap();

    // Verify offline writes are NOT visible yet
    check_results!(
        ctx,
        "gtid_tag_catchup",
        "gtid_tag_catchup_while_offline",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Restart the replicator
    let telemetry_sender = TelemetrySender::new_no_op();
    ctx.replicator_tx = Some(
        ctx.start_repl(
            Some(Config {
                require_gtid: true,
                ..Default::default()
            }),
            telemetry_sender,
            ParsingPreset::for_tests(),
        )
        .await
        .unwrap(),
    );
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify all data is caught up (initial + offline tagged & untagged writes)
    check_results!(
        ctx,
        "gtid_tag_catchup",
        "gtid_tag_catchup_after_restart",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(201)],
            &[DfValue::Int(3), DfValue::Int(202)],
            &[DfValue::Int(4), DfValue::Int(203)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

/// Test crash recovery with tagged GTIDs.
///
/// This test:
/// 1. Creates a table and starts Readyset, verifies snapshot
/// 2. Sets a failpoint to panic after processing 2 row events
/// 3. Inserts 5 rows in a single transaction using a tagged GTID (`AUTOMATIC:blue`)
/// 4. Replicator crashes after applying rows 1-2
/// 5. Verifies partial state (snapshot + 2 rows)
/// 6. Clears failpoint, restarts replicator
/// 7. Verifies all 5 rows present — crash recovery must recognize the pending tagged GTID,
///    match it by UUID+tag+sequence_number, and skip already-applied events using event_index
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, 84, gtid)]
async fn gtid_tag_crash_recovery() {
    readyset_tracing::init_test_logging();
    let _fail_scenario = FailScenario::setup();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Create table with initial data (snapshotted)
    client
        .query(
            "DROP TABLE IF EXISTS gtid_tag_crash CASCADE;
             CREATE TABLE gtid_tag_crash (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_tag_crash VALUES (0, 100);",
        )
        .await
        .unwrap();

    // Use batch size of 1 so each row event is flushed individually,
    // ensuring the failpoint crash leaves exactly 2 rows applied.
    let config = Config {
        require_gtid: true,
        replication_batch_size: 1,
        ..Default::default()
    };

    // Start Readyset and wait for snapshot
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot data
    check_results!(
        ctx,
        "gtid_tag_crash",
        "gtid_tag_crash_snapshot",
        &[&[DfValue::Int(0), DfValue::Int(100)]],
    );

    // Set failpoint to panic after 2 row events
    static TAG_ROW_EVENT_COUNT: AtomicUsize = AtomicUsize::new(0);
    TAG_ROW_EVENT_COUNT.store(0, Ordering::SeqCst);

    fail::cfg_callback(failpoints::MYSQL_GTID_ROW_EVENT, move || {
        let count = TAG_ROW_EVENT_COUNT.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "FAILPOINT (tag test): mysql-gtid-row-event triggered, count={}",
            count + 1
        );
        if count >= 2 {
            panic!("Injected crash after {} row events (tag test)", count + 1);
        }
    })
    .unwrap();

    // Insert 5 rows in a SINGLE transaction using a tagged GTID.
    // The failpoint will panic after processing rows 1 and 2, during row 3.
    // We set GTID_NEXT directly here (instead of query_with_tag) because we need
    // the tag to apply to the entire multi-statement transaction.
    client
        .conn
        .query_drop("SET @@SESSION.GTID_NEXT = 'AUTOMATIC:blue'")
        .await
        .unwrap();
    client.query("START TRANSACTION;").await.unwrap();
    for i in 1..=5 {
        client
            .query(&format!("INSERT INTO gtid_tag_crash VALUES ({i}, {i});"))
            .await
            .unwrap();
    }
    client.query("COMMIT;").await.unwrap();
    client
        .conn
        .query_drop("SET @@SESSION.GTID_NEXT = 'AUTOMATIC'")
        .await
        .unwrap();

    // Give the replicator time to process some events and hit the failpoint
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Stop the crashed replicator
    ctx.stop().await;

    // Verify partial state: snapshot + 2 rows applied before crash
    check_results!(
        ctx,
        "gtid_tag_crash",
        "gtid_tag_crash_partial_state",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
        ],
    );

    // Clear the failpoint before restarting
    fail::cfg(failpoints::MYSQL_GTID_ROW_EVENT, "off").unwrap();

    // Restart the replicator — it should recover and apply remaining events
    let telemetry_sender = TelemetrySender::new_no_op();
    ctx.replicator_tx = Some(
        ctx.start_repl(
            Some(config),
            telemetry_sender,
            ParsingPreset::for_tests(),
        )
        .await
        .unwrap(),
    );
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify all data is present after crash recovery.
    // The recovery should have:
    // - Recognized the pending tagged GTID (uuid:blue:seq) with event_index=2
    // - Skipped already-applied events (rows 1-2) using event_index
    // - Applied remaining events (rows 3-5)
    check_results!(
        ctx,
        "gtid_tag_crash",
        "gtid_tag_crash_after_recovery",
        &[
            &[DfValue::Int(0), DfValue::Int(100)],
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(2)],
            &[DfValue::Int(3), DfValue::Int(3)],
            &[DfValue::Int(4), DfValue::Int(4)],
            &[DfValue::Int(5), DfValue::Int(5)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// Binlog Rotation in GTID Mode
// =============================================================================

/// Test that FLUSH BINARY LOGS (which triggers a ROTATE_EVENT) does not break
/// replication in GTID mode. In GTID mode, position tracking uses GTIDs and
/// rotate events must be ignored.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_binlog_rotation() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS gtid_rotate CASCADE;
             CREATE TABLE gtid_rotate (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_rotate VALUES (1, 100);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "gtid_rotate",
        "gtid_rotate_snapshot",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // Force a binlog rotation — sends a ROTATE_EVENT in the stream.
    client.query("FLUSH BINARY LOGS;").await.unwrap();

    // Insert after rotation to verify replication survived.
    client
        .query("INSERT INTO gtid_rotate VALUES (2, 200);")
        .await
        .unwrap();

    check_results!(
        ctx,
        "gtid_rotate",
        "gtid_rotate_after_flush",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(200)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

// =============================================================================
// DDL Replication in GTID Mode
// =============================================================================

/// Test that DDL statements (CREATE TABLE) replicate correctly in GTID mode.
///
/// This exercises two fixes:
/// - Rotate events are skipped in GTID mode (instead of erroring on
///   mysql_position())
/// - DDL GTID is finalized immediately (no XID_EVENT follows DDL), so the
///   schema offset advances and the new table is recognized
///
/// Steps:
/// 1. Start Readyset with GTID mode (empty snapshot)
/// 2. CREATE TABLE during live replication
/// 3. Insert data into the new table
/// 4. Verify the new table appears in SHOW READYSET TABLES and data replicates
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn gtid_ddl_replication() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();

    let mut client = DbConnection::connect(&url).await.unwrap();

    // Clean up any leftover tables
    client
        .query(
            "DROP TABLE IF EXISTS gtid_ddl_t1 CASCADE;
             DROP TABLE IF EXISTS gtid_ddl_t2 CASCADE;
             CREATE TABLE gtid_ddl_t1 (id INT PRIMARY KEY, val INT);
             INSERT INTO gtid_ddl_t1 VALUES (1, 100);",
        )
        .await
        .unwrap();

    // Start Readyset
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            require_gtid: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Verify snapshot
    check_results!(
        ctx,
        "gtid_ddl_t1",
        "gtid_ddl_snapshot",
        &[&[DfValue::Int(1), DfValue::Int(100)]],
    );

    // DML before DDL
    client
        .query("INSERT INTO gtid_ddl_t1 VALUES (2, 200);")
        .await
        .unwrap();

    check_results!(
        ctx,
        "gtid_ddl_t1",
        "gtid_ddl_before_create",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(200)],
        ],
    );

    // Two consecutive CREATE TABLEs during live replication. DDL is
    // implicitly committed by MySQL — if the first DDL's GTID is not
    // finalized, the second DDL will be skipped because the schema
    // offset did not advance past the first.
    client
        .query("CREATE TABLE gtid_ddl_t2 (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();
    client
        .query("CREATE TABLE gtid_ddl_t3 (id INT PRIMARY KEY, val INT);")
        .await
        .unwrap();

    // DML after both DDLs
    client
        .query("INSERT INTO gtid_ddl_t1 VALUES (3, 300);")
        .await
        .unwrap();

    check_results!(
        ctx,
        "gtid_ddl_t1",
        "gtid_ddl_after_create",
        &[
            &[DfValue::Int(1), DfValue::Int(100)],
            &[DfValue::Int(2), DfValue::Int(200)],
            &[DfValue::Int(3), DfValue::Int(300)],
        ],
    );

    // Verify both tables created via DDL are recognized by Readyset
    for table_name in ["gtid_ddl_t2", "gtid_ddl_t3"] {
        let mut found = false;
        for _ in 0..MAX_ATTEMPTS {
            let tables = ctx.controller().await.tables().await.unwrap();
            if tables.keys().any(|r| r.name.as_str() == table_name) {
                found = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        assert!(
            found,
            "{table_name} should appear in SHOW READYSET TABLES after DDL replication"
        );
    }

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}
