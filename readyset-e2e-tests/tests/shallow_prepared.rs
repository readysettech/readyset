use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn binds_on_both_sides_of_in_list_mysql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, 3)")
        .await
        .unwrap();
    let query = "SELECT a FROM t WHERE COALESCE(b, ?) IN (?, ?) ORDER BY a";
    conn.query_drop(format!("CREATE SHALLOW CACHE FROM {query}"))
        .await
        .unwrap();

    let rows: Vec<i32> = conn.exec(query, (1, 2, 3)).await.unwrap();
    assert_eq!(rows, [3, 4]);
    let rows: Vec<i32> = conn.exec(query, (1, 2, 3)).await.unwrap();
    assert_eq!(rows, [3, 4]);
    assert_matches!(
        mysql_helpers::last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    let rows: Vec<i32> = conn.exec(query, (2, 1, 3)).await.unwrap();
    assert_eq!(rows, [2, 4]);
    let rows: Vec<i32> = conn.exec(query, (2, 1, 3)).await.unwrap();
    assert_eq!(rows, [2, 4]);
    assert_matches!(
        mysql_helpers::last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn binds_on_both_sides_of_in_list_psql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    conn.simple_query("CREATE TABLE t (a INT, b INT)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, 3)")
        .await
        .unwrap();
    let query = "SELECT a FROM t WHERE COALESCE(b, $1) IN ($2, $3) ORDER BY a";
    conn.simple_query(&format!("CREATE SHALLOW CACHE FROM {query}"))
        .await
        .unwrap();

    let rows = conn.query(query, &[&1, &2, &3]).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(),
        [3, 4]
    );
    let rows = conn.query(query, &[&1, &2, &3]).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(),
        [3, 4]
    );
    assert_matches!(
        psql_helpers::last_query_info(&conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    let rows = conn.query(query, &[&2, &1, &3]).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(),
        [2, 4]
    );
    let rows = conn.query(query, &[&2, &1, &3]).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(),
        [2, 4]
    );
    assert_matches!(
        psql_helpers::last_query_info(&conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn refresh_with_bind_left_of_in_list_mysql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT)").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    let query = "SELECT a FROM t WHERE IFNULL(?, 'x') IN ('a', 'b') ORDER BY a";
    conn.query_drop(format!(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH 1 SECONDS FROM {query}"
    ))
    .await
    .unwrap();

    let rows: Vec<i32> = conn.exec(query, ("a",)).await.unwrap();
    assert_eq!(rows, [1]);
    assert_matches!(
        mysql_helpers::last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    let rows: Vec<i32> = conn.exec(query, ("a",)).await.unwrap();
    assert_eq!(rows, [1]);
    assert_matches!(
        mysql_helpers::last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    conn.query_drop("INSERT INTO t VALUES (2)").await.unwrap();

    eventually!(run_test: {
        let rows: Vec<i32> = conn.exec(query, ("a",)).await.unwrap();
        let info = mysql_helpers::last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (rows, info))
    }, then_assert: |result| {
        let (rows, info) = result();
        assert_eq!(rows, [1, 2]);
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn refresh_with_bind_left_of_in_list_psql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    conn.simple_query("CREATE TABLE t (a INT)").await.unwrap();
    conn.simple_query("INSERT INTO t VALUES (1)").await.unwrap();
    let query = "SELECT a FROM t WHERE COALESCE($1, 'x') IN ('a', 'b') ORDER BY a";
    conn.simple_query(&format!(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH 1 SECONDS FROM {query}"
    ))
    .await
    .unwrap();

    let rows = conn.query(query, &[&"a"]).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(), [1]);
    assert_matches!(
        psql_helpers::last_query_info(&conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    let rows = conn.query(query, &[&"a"]).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.get(0)).collect::<Vec<i32>>(), [1]);
    assert_matches!(
        psql_helpers::last_query_info(&conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );

    conn.simple_query("INSERT INTO t VALUES (2)").await.unwrap();

    eventually!(run_test: {
        let rows = conn.query(query, &[&"a"]).await.unwrap();
        let rows: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
        let info = psql_helpers::last_query_info(&conn).await;
        AssertUnwindSafe(move || (rows, info))
    }, then_assert: |result| {
        let (rows, info) = result();
        assert_eq!(rows, [1, 2]);
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn scheduled_refresh_with_bind_left_of_in_list_mysql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (a INT)").await.unwrap();
    conn.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    let query = "SELECT a FROM t WHERE IFNULL(?, 'x') IN ('a', 'b') ORDER BY a";
    conn.query_drop(format!(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS FROM {query}"
    ))
    .await
    .unwrap();

    let rows: Vec<i32> = conn.exec(query, ("a",)).await.unwrap();
    assert_eq!(rows, [1]);
    assert_matches!(
        mysql_helpers::last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );

    conn.query_drop("INSERT INTO t VALUES (2)").await.unwrap();

    eventually!(run_test: {
        let rows: Vec<i32> = conn.exec(query, ("a",)).await.unwrap();
        let info = mysql_helpers::last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (rows, info))
    }, then_assert: |result| {
        let (rows, info) = result();
        assert_eq!(rows, [1, 2]);
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
    });

    shutdown_tx.shutdown().await;
}
