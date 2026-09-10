use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender};
use readyset_server::Handle;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Build an adapter with fallback, a three row table `t`, and a shallow cache on `query`.
async fn setup(
    query: &str,
    options: &str,
) -> (
    mysql_async::Conn,
    mysql_async::Conn,
    mysql_async::Opts,
    Handle,
    TestShutdownSender<MySQLAdapter>,
) {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(8))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    conn.query_drop(format!("CREATE SHALLOW CACHE {options} FROM {query}"))
        .await
        .unwrap();
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    (conn, upstream_conn, opts, handle, shutdown_tx)
}

async fn assert_filled(conn: &mut mysql_async::Conn) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
}

async fn assert_hit(conn: &mut mysql_async::Conn) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn sql_select_limit_partitions_entries() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_filled(&mut a).await;
    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET sql_select_limit = 1").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_filled(&mut b).await;
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_hit(&mut b).await;

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut a).await;

    // Binary protocol entries are keyed the same way.
    let ids: Vec<i64> = b.exec(QUERY, ()).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_filled(&mut b).await;
    let ids: Vec<i64> = a.exec(QUERY, ()).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_filled(&mut a).await;

    b.query_drop("SET sql_select_limit = DEFAULT").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut b).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn sql_select_limit_survives_refresh() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id DESC";
    let (mut a, mut upstream, opts, _handle, shutdown_tx) =
        setup(QUERY, "POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS").await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET sql_select_limit = 1").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![3]);
    assert_filled(&mut b).await;

    upstream
        .query_drop("INSERT INTO t (id, s) VALUES (4, 'd')")
        .await
        .unwrap();

    eventually!(run_test: {
        let ids: Vec<i64> = b.query(QUERY).await.unwrap();
        let info = last_query_info(&mut b).await;
        AssertUnwindSafe(move || (info, ids))
    }, then_assert: |result| {
        let (info, ids) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(ids, vec![4]);
    });

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![4, 3, 2, 1]);

    shutdown_tx.shutdown().await;
}
