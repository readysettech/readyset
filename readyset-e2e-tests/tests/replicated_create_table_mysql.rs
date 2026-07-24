//! MySQL binlogs CREATE TABLE IF NOT EXISTS even when the table already exists and the statement
//! did nothing. Replaying it must leave the table Readyset already replicates untouched: the rows
//! it serves stay equal to upstream's, and it stays replicated.

use std::panic::AssertUnwindSafe;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};

const ROWS: &str = "SELECT g, x FROM t ORDER BY g, x";

async fn rows(conn: &mut Conn) -> mysql_async::Result<Vec<(i32, i32)>> {
    conn.query(ROWS).await
}

/// Replicates `db`, whose table `t` has rows, and waits until Readyset serves them from a cache.
/// Fallback is off, so every row Readyset returns afterwards comes from its own dataflow.
async fn setup(db: &str) -> (Conn, Conn, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    mysql_helpers::recreate_database(db).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(db)))
        .await
        .unwrap();
    upstream
        .query_drop("CREATE TABLE t (g INT, x INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (1, 30), (2, 40)")
        .await
        .unwrap();

    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(false)
        .replicate_db(db)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();
    wait_for_rows_to_match_upstream(&mut rs, &mut upstream, 2).await;
    (upstream, rs, handle, shutdown_tx)
}

/// Waits until Readyset serves the row with `g = sentinel` from a cache, then requires its rows to
/// equal upstream's. Binlog events replay in order, so once the sentinel row is visible every DDL
/// statement issued before it has been replayed too.
///
/// Every attempt plans the query afresh, so it reads `t` as the replay left it rather than
/// through a cache planned over the table as it was before.
async fn wait_for_rows_to_match_upstream(rs: &mut Conn, upstream: &mut Conn, sentinel: i32) {
    let expected = rows(upstream).await.unwrap();
    eventually!(run_test: {
        let dropped = rs.query_drop("DROP ALL CACHES").await;
        let rows = rows(rs).await;
        AssertUnwindSafe(move || (dropped, rows))
    }, then_assert: |results| {
        let (dropped, rows) = results();
        dropped.expect("DROP ALL CACHES failed");
        let rows = rows.expect("Readyset failed to serve the query");
        assert!(rows.iter().any(|(g, _)| *g == sentinel), "replication has not caught up yet");
        assert_eq!(rows, expected, "Readyset's rows diverged from upstream's");
    });
}

async fn teardown(mut upstream: Conn, shutdown_tx: ShutdownSender, db: &str) {
    shutdown_tx.shutdown().await;
    upstream
        .query_drop(format!("DROP DATABASE {db}"))
        .await
        .unwrap();
}

/// Upstream ignores the statement, so `x` stays INT there. Applying it as a real CREATE would
/// recreate `t` as an empty table with a BIGINT column, discarding every row already replicated.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn create_table_if_not_exists_preserves_rows() {
    let db = "create_table_if_not_exists_preserves_rows";
    let (mut upstream, mut rs, _handle, shutdown_tx) = setup(db).await;

    upstream
        .query_drop("CREATE TABLE IF NOT EXISTS t (g INT, x BIGINT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (3, 50)")
        .await
        .unwrap();
    wait_for_rows_to_match_upstream(&mut rs, &mut upstream, 3).await;

    teardown(upstream, shutdown_tx, db).await;
}

/// Upstream ignores the statement, so `t` stays InnoDB there. Readyset rejects a real CREATE with
/// this engine, and a rejected CREATE marks its table not replicated. The existing table must be
/// recognized before the statement is validated so it stays replicated.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn create_table_if_not_exists_unsupported_engine_stays_replicated() {
    let db = "create_table_if_not_exists_unsupported_engine";
    let (mut upstream, mut rs, _handle, shutdown_tx) = setup(db).await;

    upstream
        .query_drop("CREATE TABLE IF NOT EXISTS t (g INT, x INT) ENGINE=MEMORY")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (3, 50)")
        .await
        .unwrap();
    wait_for_rows_to_match_upstream(&mut rs, &mut upstream, 3).await;
    let statuses: Vec<(String, String, Option<String>)> =
        rs.query("SHOW READYSET ALL TABLES").await.unwrap();
    let (_, status, _) = statuses
        .iter()
        .find(|(table, _, _)| *table == format!("`{db}`.`t`"))
        .unwrap_or_else(|| panic!("t is missing from {statuses:?}"));
    assert_ne!(status, "Not replicated", "a rejected replay marked t not replicated");

    teardown(upstream, shutdown_tx, db).await;
}
