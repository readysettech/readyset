use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, SimpleQueryMessage};

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{
    self, PostgreSQLAdapter, assert_shallow_hit, last_query_info,
};
use readyset_client_test_helpers::{TestBuilder, derive_test_name};
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Run `query` with `param` as a named prepared statement, and return the number of rows in the
/// result set.
async fn named_prepare<P>(conn: &Client, query: &str, param: P) -> usize
where
    P: ToSql + Sync,
{
    let statement = conn.prepare(query).await.unwrap();
    // tokio_postgres's query method implicitly uses named prepared statements.
    conn.query(&statement, &[&param]).await.unwrap().len()
}

/// Run `query` with the integer `param` as an unnamed prepared statement, and return the number of
/// rows in the result set.
async fn unnamed_prepare(conn: &Client, query: &str, param: i32) -> usize {
    let params = [(&param as &(dyn ToSql + Sync), Type::INT4)];
    // tokio_postgres's query_typed method implicitly uses unnamed prepared statements.
    conn.query_typed(query, &params).await.unwrap().len()
}

/// Run `query` over the simple protocol and return the column metadata from the result set.
async fn simple_query_columns(conn: &Client, query: &str) -> Vec<(String, Type)> {
    let messages = conn.simple_query(query).await.unwrap();
    let fields = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::CommandComplete(complete) => complete.fields.as_deref(),
            _ => None,
        })
        .unwrap();
    fields
        .iter()
        .map(|field| {
            (
                field.name().to_owned(),
                Type::from_oid(field.type_oid()).unwrap(),
            )
        })
        .collect()
}

/// Run `query` with `param` until the shallow cache serves it with no rows.
async fn wait_for_empty_hit<P>(conn: &Client, query: &str, param: P)
where
    P: ToSql + Sync,
{
    eventually!(run_test: {
        let rows = conn.query(query, &[&param]).await.unwrap();
        let info = last_query_info(conn).await;
        AssertUnwindSafe(move || (rows.is_empty(), info))
    }, then_assert: |result| {
        let (empty, info) = result();
        assert!(empty);
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
    });
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn empty_results_psql() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate_db(&derive_test_name())
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    conn.simple_query("CREATE TABLE t (k INT, v TEXT); INSERT INTO t VALUES (1, 'a')")
        .await
        .unwrap();
    conn.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH 1 SECONDS \
         FROM SELECT v FROM t WHERE k = $1",
    )
    .await
    .unwrap();
    conn.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS \
         FROM SELECT k FROM t WHERE v = $1",
    )
    .await
    .unwrap();

    assert_eq!(
        named_prepare(&conn, "SELECT v FROM t WHERE k = $1", 2).await,
        0
    );
    assert_eq!(
        named_prepare(&conn, "SELECT v FROM t WHERE k = $1", 2).await,
        0
    );
    assert_shallow_hit(&conn).await;
    assert_eq!(
        simple_query_columns(&conn, "SELECT v FROM t WHERE k = 2").await,
        [("v".into(), Type::TEXT)]
    );
    assert_shallow_hit(&conn).await;

    assert_eq!(
        unnamed_prepare(&conn, "SELECT v FROM t WHERE k = $1", 3).await,
        0
    );
    assert_eq!(
        unnamed_prepare(&conn, "SELECT v FROM t WHERE k = $1", 3).await,
        0
    );
    assert_shallow_hit(&conn).await;
    assert_eq!(
        simple_query_columns(&conn, "SELECT v FROM t WHERE k = 3").await,
        [("v".into(), Type::TEXT)]
    );
    assert_shallow_hit(&conn).await;

    assert_eq!(
        named_prepare(&conn, "SELECT v FROM t WHERE k = $1", 1).await,
        1
    );
    assert_eq!(
        named_prepare(&conn, "SELECT k FROM t WHERE v = $1", "a").await,
        1
    );
    conn.simple_query("DELETE FROM t").await.unwrap();
    wait_for_empty_hit(&conn, "SELECT v FROM t WHERE k = $1", 1).await;
    wait_for_empty_hit(&conn, "SELECT k FROM t WHERE v = $1", "a").await;

    shutdown_tx.shutdown().await;
}

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
