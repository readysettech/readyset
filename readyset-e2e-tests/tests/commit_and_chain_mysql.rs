//! Regression tests for REA-6786: COMMIT AND CHAIN and ROLLBACK AND CHAIN start a new
//! transaction, upstream and in Readyset.

use mysql_async::Conn;
use mysql_async::prelude::*;
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender, derive_test_name};
use readyset_server::{CacheMode, Handle};
use readyset_sql_parsing::ParsingPreset;
use test_utils::{tags, upstream};

/// Start a shallow cache adapter with table `t` in database `db` on the upstream. Return a
/// connection to the adapter, a direct connection to the upstream, and the handles, which the
/// caller must keep for the test's duration.
async fn setup(db: &str) -> (Conn, Conn, Handle, TestShutdownSender<MySQLAdapter>) {
    mysql_helpers::recreate_database(db).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(db)))
        .await
        .unwrap();
    upstream
        .query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t VALUES (1, 0)")
        .await
        .unwrap();

    let backend = BackendBuilder::new()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        // nom-sql rejects AND CHAIN, so the test presets proxy the statement as unparsed text.
        .parsing_preset(ParsingPreset::for_prod())
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(rs_opts).await.unwrap();
    conn.query_drop(format!("USE {db}")).await.unwrap();
    (conn, upstream, handle, shutdown_tx)
}

/// The ids in `t` greater than 1, read directly from the upstream.
async fn inserted_ids(upstream: &mut Conn) -> Vec<i32> {
    upstream
        .query("SELECT id FROM t WHERE id > 1 ORDER BY id")
        .await
        .unwrap()
}

/// A write after COMMIT AND CHAIN or ROLLBACK AND CHAIN is part of the new transaction, so a
/// ROLLBACK undoes it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn chain_starts_upstream_transaction() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    let (mut conn, mut upstream, _handle, shutdown_tx) = setup(&db).await;

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (2, 0)").await.unwrap();
    conn.query_drop("COMMIT AND CHAIN").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (3, 0)").await.unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();
    assert_eq!(inserted_ids(&mut upstream).await, [2]);

    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (4, 0)").await.unwrap();
    conn.query_drop("ROLLBACK AND CHAIN").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (5, 0)").await.unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();
    assert_eq!(inserted_ids(&mut upstream).await, [2]);

    shutdown_tx.shutdown().await;
}
