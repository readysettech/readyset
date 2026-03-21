use std::panic::AssertUnwindSafe;

use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// MySQL implicitly casts non-string types (INT, DOUBLE, etc.) to their string
/// representation when used as the first argument to SUBSTRING.  Before the fix,
/// ReadySet returned NULL for `SUBSTRING(int_col, 1, 3)` because the value
/// could not be converted to `&str`.
///
/// This test runs the query through the full ReadySet pipeline (snapshot,
/// cache creation, cached read) and compares against upstream MySQL.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn substring_implicit_cast_non_string_types() {
    readyset_tracing::init_test_logging();
    let db_name = "substring_implicit_cast";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t1 (
                id INT PRIMARY KEY,
                int_col INT NOT NULL,
                double_col DOUBLE NOT NULL,
                date_col DATE NOT NULL
            )",
        )
        .await
        .unwrap();

    upstream_conn
        .query_drop(
            "INSERT INTO t1 (id, int_col, double_col, date_col) VALUES
                (1, 12345, 3.14159, '2025-03-15'),
                (2, 99999, 0.5, '1999-12-31'),
                (3, 7, 100.125, '2000-01-01')",
        )
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .recreate_database(false)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Cache: SUBSTRING on an INT column.
    eventually!(run_test: {
        let res = rs_conn
            .query_drop(
                "CREATE CACHE substr_int FROM \
                 SELECT id, SUBSTRING(int_col, 1, 3) AS s FROM t1 WHERE id = ?"
            )
            .await;
        AssertUnwindSafe(|| { res })
    }, then_assert: |result| {
        result().unwrap();
    });

    // Cache: SUBSTRING on a DOUBLE column.
    eventually!(run_test: {
        let res = rs_conn
            .query_drop(
                "CREATE CACHE substr_dbl FROM \
                 SELECT id, SUBSTRING(double_col, 1, 4) AS s FROM t1 WHERE id = ?"
            )
            .await;
        AssertUnwindSafe(|| { res })
    }, then_assert: |result| {
        result().unwrap();
    });

    // Cache: SUBSTRING on a DATE column.
    eventually!(run_test: {
        let res = rs_conn
            .query_drop(
                "CREATE CACHE substr_date FROM \
                 SELECT id, SUBSTRING(date_col, 1, 4) AS s FROM t1 WHERE id = ?"
            )
            .await;
        AssertUnwindSafe(|| { res })
    }, then_assert: |result| {
        result().unwrap();
    });

    // Verify SUBSTRING(int_col, 1, 3) for each row.
    for row_id in [1, 2, 3] {
        let upstream_rows: Vec<Row> = upstream_conn
            .exec(
                "SELECT id, SUBSTRING(int_col, 1, 3) AS s FROM t1 WHERE id = ?",
                (row_id,),
            )
            .await
            .unwrap();
        let upstream_values: Vec<(i32, Option<String>)> = upstream_rows
            .iter()
            .map(|row| (row.get(0).unwrap(), row.get(1)))
            .collect();

        eventually!(run_test: {
            let rows: Vec<Row> = rs_conn
                .exec(
                    "SELECT id, SUBSTRING(int_col, 1, 3) AS s FROM t1 WHERE id = ?",
                    (row_id,),
                )
                .await
                .unwrap();
            let values: Vec<(i32, Option<String>)> = rows
                .iter()
                .map(|row| (row.get(0).unwrap(), row.get(1)))
                .collect();
            values
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_values,
                "SUBSTRING(int_col) mismatch for id={row_id}: rs={result:?}, upstream={upstream_values:?}",
            );
        });
    }

    // Verify SUBSTRING(double_col, 1, 4) for each row.
    for row_id in [1, 2, 3] {
        let upstream_rows: Vec<Row> = upstream_conn
            .exec(
                "SELECT id, SUBSTRING(double_col, 1, 4) AS s FROM t1 WHERE id = ?",
                (row_id,),
            )
            .await
            .unwrap();
        let upstream_values: Vec<(i32, Option<String>)> = upstream_rows
            .iter()
            .map(|row| (row.get(0).unwrap(), row.get(1)))
            .collect();

        eventually!(run_test: {
            let rows: Vec<Row> = rs_conn
                .exec(
                    "SELECT id, SUBSTRING(double_col, 1, 4) AS s FROM t1 WHERE id = ?",
                    (row_id,),
                )
                .await
                .unwrap();
            let values: Vec<(i32, Option<String>)> = rows
                .iter()
                .map(|row| (row.get(0).unwrap(), row.get(1)))
                .collect();
            values
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_values,
                "SUBSTRING(double_col) mismatch for id={row_id}: rs={result:?}, upstream={upstream_values:?}",
            );
        });
    }

    // Verify SUBSTRING(date_col, 1, 4) for each row.
    for row_id in [1, 2, 3] {
        let upstream_rows: Vec<Row> = upstream_conn
            .exec(
                "SELECT id, SUBSTRING(date_col, 1, 4) AS s FROM t1 WHERE id = ?",
                (row_id,),
            )
            .await
            .unwrap();
        let upstream_values: Vec<(i32, Option<String>)> = upstream_rows
            .iter()
            .map(|row| (row.get(0).unwrap(), row.get(1)))
            .collect();

        eventually!(run_test: {
            let rows: Vec<Row> = rs_conn
                .exec(
                    "SELECT id, SUBSTRING(date_col, 1, 4) AS s FROM t1 WHERE id = ?",
                    (row_id,),
                )
                .await
                .unwrap();
            let values: Vec<(i32, Option<String>)> = rows
                .iter()
                .map(|row| (row.get(0).unwrap(), row.get(1)))
                .collect();
            values
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_values,
                "SUBSTRING(date_col) mismatch for id={row_id}: rs={result:?}, upstream={upstream_values:?}",
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
