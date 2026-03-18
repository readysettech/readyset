use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Regression test: creating a cache for a query that projects the same column
/// more than once used to trip a debug_assert in make_reader_processing because
/// returned_cols would not form a contiguous 0..N slice.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
async fn duplicate_projected_columns_mysql() {
    init_test_logging();

    let db_name = "dup_proj_cols_mysql";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop("CREATE TABLE dogs (id INT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    upstream_conn
        .query_drop("INSERT INTO dogs VALUES (1, 'Jack'), (2, 'Rex')")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // This should not panic — the duplicate column in the projection is valid SQL.
    eventually!(run_test: {
        let res = rs_conn
            .query_drop("CREATE CACHE FROM SELECT name, name FROM dogs WHERE id = ?")
            .await;
        AssertUnwindSafe(|| res)
    }, then_assert: |result| {
        result().unwrap();
    });

    // Verify that results are correct.
    let upstream_rows: Vec<(String, String)> = upstream_conn
        .exec("SELECT name, name FROM dogs WHERE id = ?", (1,))
        .await
        .unwrap();

    eventually!(run_test: {
        let rows: Vec<(String, String)> = rs_conn
            .exec("SELECT name, name FROM dogs WHERE id = ?", (1,))
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result, upstream_rows);
    });

    shutdown_tx.shutdown().await;
}

/// Same test against PostgreSQL.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres13, postgres15)]
async fn duplicate_projected_columns_postgres() {
    init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .build::<PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            "CREATE TABLE dogs (id INT PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();

    upstream_conn
        .execute("INSERT INTO dogs VALUES (1, 'Jack'), (2, 'Rex')", &[])
        .await
        .unwrap();

    // This should not panic — the duplicate column in the projection is valid SQL.
    // Use simple_query because CREATE CACHE is a custom Readyset command that
    // doesn't work through the extended query protocol (prepare/bind/execute).
    eventually!(run_test: {
        let res = rs_conn
            .simple_query(
                "CREATE CACHE FROM SELECT name, name FROM dogs WHERE id = $1",
            )
            .await;
        AssertUnwindSafe(move || res)
    }, then_assert: |result| {
        result().unwrap();
    });

    // Verify that results are correct.
    let upstream_rows: Vec<(String, String)> = upstream_conn
        .query("SELECT name, name FROM dogs WHERE id = $1", &[&1i32])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    eventually!(run_test: {
        let rows: Vec<(String, String)> = rs_conn
            .query("SELECT name, name FROM dogs WHERE id = $1", &[&1i32])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        rows
    }, then_assert: |result| {
        assert_eq!(result, upstream_rows);
    });

    shutdown_tx.shutdown().await;
}
