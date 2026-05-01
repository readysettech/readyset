//! E2E tests for [`TrxCachePolicy`] (REA-6596).
//!
//! Verifies the user-visible "serve from cache until first write" behavior the
//! Supabase-style auto-cache configuration relies on. The matrix below pairs the
//! `(MigrationMode, CacheMode, policy)` tuples that produce distinct routing decisions.
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

/// Set up an adapter with `cache_mode = Shallow` (in-request-path is the default migration
/// mode in `TestBuilder`). Returns a connected client, the readyset server handle, and
/// the shutdown handle. The caller must hold the `Handle` for the test's duration:
/// dropping it shuts the controller down mid-test.
async fn setup_shallow_adapter(
    test_name: &str,
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
        .cache_mode(CacheMode::Shallow);
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

/// Auto-created shallow caches in InRequestPath mode default to `UntilWrite`. A read-only
/// transaction must serve from cache; a transaction that has observed a write must proxy
/// upstream until COMMIT/ROLLBACK; subsequent transactions reset and serve from cache again.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn until_write_in_request_path_shallow() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_shallow_adapter(&test_name).await;

    // Warm the auto-created cache (first read goes upstream, second hits the cache).
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "cache must be hot before transaction starts"
    );

    // Read-only-so-far transaction: UntilWrite serves from cache.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "UntilWrite must serve from cache before any write in the transaction"
    );

    // Write flips the per-transaction had_write flag.
    conn.query_drop("INSERT INTO foo VALUES (4)").await.unwrap();

    // Subsequent read in the same transaction goes upstream.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "UntilWrite must skip the cache after a write in the same transaction"
    );

    conn.query_drop("COMMIT").await.unwrap();

    // After the transaction closes, the cache is hot again.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "cache must serve again once the writing transaction commits"
    );

    shutdown_tx.shutdown().await;
}

/// Manual `CREATE CACHE FROM ...` (no policy keyword) produces a `Never` cache. Even a
/// read-only-so-far transaction must proxy upstream — this guards against the auto-cache
/// rollout accidentally re-classifying explicit-DDL caches.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn manual_no_keyword_stays_never() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_shallow_adapter(&test_name).await;

    // Replace the auto-cache with a manually-created one (still no keyword -> Never).
    conn.query_drop("DROP ALL CACHES").await.unwrap();
    conn.query_drop("CREATE SHALLOW CACHE FROM SELECT a FROM foo WHERE a = ?")
        .await
        .unwrap();

    // Warm the cache outside any transaction.
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

    // Inside an explicit transaction, Never always proxies — even on the very first read.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "manual no-keyword CREATE CACHE must proxy in transactions"
    );
    conn.query_drop("COMMIT").await.unwrap();

    shutdown_tx.shutdown().await;
}

/// `CREATE SHALLOW CACHE UNTIL WRITE FROM ...` — the manual opt-in form — behaves the same
/// way as the auto-created cache.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn manual_until_write() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_shallow_adapter(&test_name).await;

    conn.query_drop("DROP ALL CACHES").await.unwrap();
    conn.query_drop("CREATE SHALLOW CACHE UNTIL WRITE FROM SELECT a FROM foo WHERE a = ?")
        .await
        .unwrap();

    // Warm the cache.
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

    // Same dance: serve from cache before the write, upstream after.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
    );
    conn.query_drop("INSERT INTO foo VALUES (5)").await.unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
    );
    conn.query_drop("COMMIT").await.unwrap();

    shutdown_tx.shutdown().await;
}

/// `CREATE CACHE ALWAYS FROM ...` ignores transaction state and the write flag. The cache
/// must serve every read regardless of context — this is the existing behavior the new
/// enum preserves verbatim.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn manual_always_unaffected_by_writes() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_shallow_adapter(&test_name).await;

    conn.query_drop("DROP ALL CACHES").await.unwrap();
    conn.query_drop("CREATE SHALLOW CACHE ALWAYS FROM SELECT a FROM foo WHERE a = ?")
        .await
        .unwrap();

    // Warm the cache.
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
    // Even after a write, Always serves from cache.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "ALWAYS must serve from cache regardless of in-transaction writes"
    );
    conn.query_drop("COMMIT").await.unwrap();

    shutdown_tx.shutdown().await;
}

/// `SET autocommit=0` carries the `had_write` flag from a preceding explicit transaction.
/// The plan calls this out as a critical edge case — verify it directly.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn autocommit_off_carries_had_write() {
    init_test_logging();
    let test_name = derive_test_name!();
    let (mut conn, _handle, shutdown_tx) = setup_shallow_adapter(&test_name).await;

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

    // BEGIN; INSERT (had_write = true); SET autocommit=0 -> AutocommitOff{had_write=true}.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO foo VALUES (7)").await.unwrap();
    conn.query_drop("SET autocommit = 0").await.unwrap();

    // Read after the carry-over: still proxied upstream.
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "had_write must carry across InTransaction -> AutocommitOff"
    );

    // COMMIT under autocommit=0 begins a fresh implicit transaction; the flag resets.
    conn.query_drop("COMMIT").await.unwrap();
    conn.query_drop("SELECT a FROM foo WHERE a = 1")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
        "implicit COMMIT under autocommit=0 must reset had_write"
    );

    shutdown_tx.shutdown().await;
}
