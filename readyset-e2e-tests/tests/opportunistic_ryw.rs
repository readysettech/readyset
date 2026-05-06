//! E2E tests for the session-level opportunistic read-your-writes window.
//!
//! Applies only outside transactions: after any write on a session, reads on that same
//! session bypass the cache for the configured window. In-transaction routing is
//! governed by the per-cache `TrxCachePolicy` (`NEVER` / `ALWAYS` / `UNTIL WRITE`), not
//! this window. The window gives upstream replication a chance to catch up before
//! serving cached reads of just-written rows; it is opportunistic, not a guarantee.
use std::time::Duration;

use mysql_async::prelude::Queryable;
use readyset_adapter::backend::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder, derive_test_name,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_server::{CacheMode, Handle};
use readyset_tracing::init_test_logging;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio::test;
use tokio::time::sleep;

/// Set up an adapter with a configurable RYW window. Returns a connected client, the
/// readyset server handle, and the shutdown handle. The caller must hold the `Handle`
/// for the test's duration: dropping it shuts the controller down mid-test. Uses
/// shallow caching to make the cache vs. upstream choice observable via
/// `QueryDestination`.
async fn setup_with_opportunistic_ryw(
    test_name: &str,
    opportunistic_ryw_ms: Option<u64>,
) -> (mysql_async::Conn, Handle, ShutdownSender) {
    mysql_helpers::recreate_database(test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (1), (2), (3)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow)
        .opportunistic_ryw_ms(opportunistic_ryw_ms);
    let (readyset_opts, handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();
    (readyset, handle, shutdown_tx)
}

/// Canonical RYW scenario: INSERT outside any txn; subsequent reads on the same session
/// bypass the cache for the configured window even though the cache is `UntilWrite`
/// (auto-created in InRequestPath + Shallow). After the window elapses, reads serve from
/// cache again.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn opportunistic_ryw_window_skips_cache_after_write() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_with_opportunistic_ryw(&test_name, Some(500)).await;

    // Warm the auto-cache.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "cache must be hot before the write"
    );

    // INSERT outside any transaction starts the RYW window.
    conn.query_drop("INSERT INTO foo VALUES (4)").await.unwrap();

    // Subsequent reads bypass the cache even though we are not in a txn.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "RYW window must skip the cache even outside any transaction"
    );

    // After the window elapses, the cache serves again.
    sleep(Duration::from_millis(700)).await;
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "cache must serve again once the RYW window elapses"
    );

    shutdown_tx.shutdown().await;
}

/// Disabled RYW (the default): writes do not affect cache routing outside transactions.
/// This guards against accidentally shipping a default that breaks unbounded caching.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn opportunistic_ryw_disabled_does_not_skip_cache() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_with_opportunistic_ryw(&test_name, None).await;

    // Warm the auto-cache.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
    );

    conn.query_drop("INSERT INTO foo VALUES (5)").await.unwrap();

    // Without RYW, the cache continues to serve after the write.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "RYW disabled: post-write reads must still serve from cache"
    );

    shutdown_tx.shutdown().await;
}

/// RYW fires after a transaction that wrote: the post-COMMIT read is routed upstream
/// (until the window elapses), so the canonical `BEGIN; INSERT; COMMIT; SELECT` pattern
/// reads its own writes from the upstream rather than from the cache.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn opportunistic_ryw_fires_after_commit() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_with_opportunistic_ryw(&test_name, Some(500)).await;

    // Warm the auto-cache.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
    );

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO foo VALUES (6)").await.unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "RYW must fire post-COMMIT so the SELECT sees the just-committed write"
    );

    shutdown_tx.shutdown().await;
}

/// ROLLBACK clears the RYW window: writes that were rolled back never landed, so reads
/// after ROLLBACK serve from cache as if the writes never happened.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn opportunistic_ryw_cleared_on_rollback() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_with_opportunistic_ryw(&test_name, Some(60_000)).await;

    // Warm the auto-cache.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
    );

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO foo VALUES (7)").await.unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();

    // Writes rolled back: cache continues to serve.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "ROLLBACK must drop RYW since the writes never landed"
    );

    shutdown_tx.shutdown().await;
}
