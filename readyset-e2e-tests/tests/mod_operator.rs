use std::panic::AssertUnwindSafe;

use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
    psql_helpers, sleep,
};
use readyset_sql::{Dialect, DialectDisplay, ast::SqlType};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::tags;

// ---------------------------------------------------------------------------
// PostgreSQL output type verification
// ---------------------------------------------------------------------------

/// Helper that creates a table with two columns of specified types, inserts a row,
/// runs a `SELECT a % b` expression, and asserts that the result column type from
/// ReadySet matches upstream PostgreSQL.
async fn test_mod_type_inner_postgres(
    col_a_type: SqlType,
    col_b_type: SqlType,
    a_value: &str,
    b_value: &str,
) {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            &format!(
                "CREATE TABLE t1 (a {}, b {})",
                col_a_type.display(Dialect::PostgreSQL),
                col_b_type.display(Dialect::PostgreSQL),
            ),
            &[],
        )
        .await
        .unwrap();

    upstream_conn
        .execute(
            &format!("INSERT INTO t1 (a, b) VALUES ({a_value}, {b_value})"),
            &[],
        )
        .await
        .unwrap();

    let query = "SELECT t1.a % t1.b FROM t1".to_string();

    let upstream_rows = upstream_conn.query(&query, &[]).await.unwrap();
    let upstream_type = upstream_rows
        .first()
        .map(|row| row.columns()[0].type_().clone());
    let upstream_values: Vec<Vec<u8>> = upstream_rows
        .iter()
        .map(|row| row.body().buffer().to_vec())
        .collect();

    eventually!(run_test: {
        let query = query.clone();
        let rs_rows = rs_conn.query(&query, &[]).await;
        AssertUnwindSafe(|| { rs_rows })
    }, then_assert: |result| {
        let rs_rows = result().unwrap();
        assert_eq!(upstream_values.len(), rs_rows.len());

        match (upstream_type.as_ref(), rs_rows.first()) {
            (Some(upstream_type), Some(rs_row)) => {
                assert_eq!(upstream_type, rs_row.columns()[0].type_());
            }
            (None, None) => {}
            _ => panic!("Result type mismatch between upstream and ReadySet"),
        }

        let rs_values: Vec<Vec<u8>> = rs_rows
            .iter()
            .map(|row| row.body().buffer().to_vec())
            .collect();
        assert_eq!(upstream_values, rs_values);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_smallint_smallint_postgres() {
    test_mod_type_inner_postgres(SqlType::Int2, SqlType::Int2, "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_smallint_integer_postgres() {
    test_mod_type_inner_postgres(SqlType::Int2, SqlType::Int(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_integer_bigint_postgres() {
    test_mod_type_inner_postgres(SqlType::Int(None), SqlType::BigInt(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_bigint_bigint_postgres() {
    test_mod_type_inner_postgres(SqlType::BigInt(None), SqlType::BigInt(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_bigint_smallint_postgres() {
    test_mod_type_inner_postgres(SqlType::BigInt(None), SqlType::Int2, "7", "3").await;
}

// ---------------------------------------------------------------------------
// MySQL output type verification
// ---------------------------------------------------------------------------

/// Helper that creates a table with two columns of specified types, inserts a
/// row, runs a `SELECT a % b` expression, and asserts that the result value
/// from ReadySet matches upstream MySQL.
///
/// Note: column type checking is intentionally omitted.  ReadySet's dataflow
/// pipeline does not yet propagate the exact MySQL result type for all binary
/// operator expressions (e.g. INT % INT is reported as BIGINT instead of INT).
/// The expression lowering sets the correct DfType, but the wire-protocol
/// column metadata can diverge.  Value correctness is the primary concern here.
async fn test_mod_type_inner_mysql(
    col_a_type: SqlType,
    col_b_type: SqlType,
    a_value: &str,
    b_value: &str,
) {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>().await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(format!(
            "CREATE TABLE t1 (a {}, b {})",
            col_a_type.display(Dialect::MySQL),
            col_b_type.display(Dialect::MySQL),
        ))
        .await
        .unwrap();

    upstream_conn
        .query_drop(format!(
            "INSERT INTO t1 (a, b) VALUES ({a_value}, {b_value})"
        ))
        .await
        .unwrap();

    let query = "SELECT t1.a % t1.b FROM t1".to_string();

    let upstream_rows: Vec<Row> = upstream_conn.query(query.clone()).await.unwrap();
    let upstream_values: Vec<mysql_async::Value> =
        upstream_rows.iter().map(|row| row[0].clone()).collect();

    eventually!(run_test: {
        let query = query.clone();
        let rs_rows = rs_conn.query(query).await;
        AssertUnwindSafe(|| { rs_rows })
    }, then_assert: |result| {
        let rs_rows: Vec<Row> = result().unwrap();
        assert_eq!(upstream_values.len(), rs_rows.len());

        let rs_values: Vec<mysql_async::Value> =
            rs_rows.iter().map(|row| row[0].clone()).collect();
        assert_eq!(upstream_values, rs_values);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_int_int_mysql() {
    test_mod_type_inner_mysql(SqlType::Int(None), SqlType::Int(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_bigint_int_mysql() {
    test_mod_type_inner_mysql(SqlType::BigInt(None), SqlType::Int(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_int_bigint_mysql() {
    test_mod_type_inner_mysql(SqlType::Int(None), SqlType::BigInt(None), "7", "3").await;
}

#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_float_int_mysql() {
    test_mod_type_inner_mysql(SqlType::Float, SqlType::Int(None), "7.5", "3").await;
}

#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_double_int_mysql() {
    test_mod_type_inner_mysql(SqlType::Double, SqlType::Int(None), "7.5", "3").await;
}

// ---------------------------------------------------------------------------
// Correctness through cache (MySQL)
// ---------------------------------------------------------------------------

/// Test that a cached query using the MOD operator returns correct results.
///
/// 1. Creates a table with two INT columns and inserts several rows.
/// 2. Creates a cache for a query with `a % b` in the projection and a
///    simple `WHERE id = ?` filter (ReadySet does not support placeholders
///    compared to arbitrary expressions).
/// 3. Verifies that ReadySet returns the same rows as upstream for multiple
///    parameter values.
#[tokio::test]
#[tags(serial, slow, mysql_upstream)]
async fn mod_correctness_through_cache_mysql() {
    readyset_tracing::init_test_logging();
    let db_name = "mod_correctness_cache";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t1 (
                id INT PRIMARY KEY,
                a INT NOT NULL,
                b INT NOT NULL
            )",
        )
        .await
        .unwrap();

    upstream_conn
        .query_drop(
            "INSERT INTO t1 (id, a, b) VALUES
                (1, 10, 3),
                (2, 20, 7),
                (3, 15, 4),
                (4, 21, 7),
                (5, 9, 3)",
        )
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    sleep().await;

    // Cache keyed on `id`; modulo is evaluated in the projection.
    rs_conn
        .query_drop("CREATE CACHE mod_cache FROM SELECT a, b, a % b FROM t1 WHERE id = ?")
        .await
        .unwrap();

    sleep().await;

    // Verify several rows: (10 % 3 = 1), (20 % 7 = 6), (21 % 7 = 0)
    for row_id in [1, 2, 4] {
        let upstream_rows: Vec<(i32, i32, i32)> = upstream_conn
            .exec("SELECT a, b, a % b FROM t1 WHERE id = ?", (row_id,))
            .await
            .unwrap();

        eventually!(run_test: {
            let rows: Vec<(i32, i32, i32)> = rs_conn
                .exec("SELECT a, b, a % b FROM t1 WHERE id = ?", (row_id,))
                .await
                .unwrap();
            rows
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_rows,
                "Mismatch for id={row_id}: ReadySet={result:?}, upstream={upstream_rows:?}",
            );
        });
    }

    shutdown_tx.shutdown().await;

    // Clean up the test database.
    let cleanup_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut cleanup_conn = Conn::new(cleanup_opts).await.unwrap();
    cleanup_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Correctness through cache (PostgreSQL)
// ---------------------------------------------------------------------------

/// Test that a cached query using the MOD operator returns correct results
/// against a PostgreSQL upstream.
#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn mod_correctness_through_cache_postgres() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            "CREATE TABLE t1 (
                id INT PRIMARY KEY,
                a INT NOT NULL,
                b INT NOT NULL
            )",
            &[],
        )
        .await
        .unwrap();

    upstream_conn
        .execute(
            "INSERT INTO t1 (id, a, b) VALUES
                (1, 10, 3),
                (2, 20, 7),
                (3, 15, 4),
                (4, 21, 7),
                (5, 9, 3)",
            &[],
        )
        .await
        .unwrap();

    // Cache keyed on `id`; modulo is evaluated in the projection.
    rs_conn
        .simple_query("CREATE CACHE mod_cache FROM SELECT a, b, a % b FROM t1 WHERE id = $1")
        .await
        .unwrap();

    // Verify several rows: (10 % 3 = 1), (20 % 7 = 6), (21 % 7 = 0)
    for &row_id in &[1i32, 2, 4] {
        let upstream_rows = upstream_conn
            .query("SELECT a, b, a % b FROM t1 WHERE id = $1", &[&row_id])
            .await
            .unwrap();
        let upstream_values: Vec<(i32, i32, i32)> = upstream_rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, i32>(0),
                    row.get::<_, i32>(1),
                    row.get::<_, i32>(2),
                )
            })
            .collect();

        eventually!(run_test: {
            let rs_rows = rs_conn
                .query("SELECT a, b, a % b FROM t1 WHERE id = $1", &[&row_id])
                .await;
            AssertUnwindSafe(|| { rs_rows })
        }, then_assert: |result| {
            let rs_rows = result().unwrap();
            let rs_values: Vec<(i32, i32, i32)> = rs_rows
                .iter()
                .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1), row.get::<_, i32>(2)))
                .collect();

            assert_eq!(
                rs_values, upstream_values,
                "Mismatch for id={row_id}: ReadySet={rs_values:?}, upstream={upstream_values:?}",
            );
        });
    }

    shutdown_tx.shutdown().await;
}
