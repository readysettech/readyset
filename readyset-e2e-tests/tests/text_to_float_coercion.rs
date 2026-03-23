use std::panic::AssertUnwindSafe;

use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// MySQL implicitly coerces non-numeric strings to 0.0 and extracts numeric
/// prefixes from mixed strings (e.g., "123abc" → 123.0) when casting TEXT to
/// FLOAT or DOUBLE.  Before the fix, ReadySet used Rust's `str::parse` which
/// errored on non-numeric input, causing failures in the dataflow when a CAST
/// expression encountered non-numeric text.
///
/// This test caches CAST(text_col AS DOUBLE) and CAST(text_col AS FLOAT)
/// queries with non-numeric text values and verifies that ReadySet matches
/// upstream MySQL through the full pipeline (snapshot → cache → read).
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn text_to_float_coercion() {
    readyset_tracing::init_test_logging();
    let db_name = "text_to_float_coercion";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t1 (
                id INT PRIMARY KEY,
                val VARCHAR(50)
            )",
        )
        .await
        .unwrap();

    // Include: valid numeric, non-numeric, numeric prefix, empty string.
    upstream_conn
        .query_drop(
            "INSERT INTO t1 (id, val) VALUES
                (1, '42.5'),
                (2, 'hello'),
                (3, '123abc'),
                (4, '')",
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

    // Cache: CAST(val AS DOUBLE).
    eventually!(run_test: {
        let res = rs_conn
            .query_drop(
                "CREATE CACHE cast_double FROM \
                 SELECT id, CAST(val AS DOUBLE) AS d FROM t1 WHERE id = ?"
            )
            .await;
        AssertUnwindSafe(|| { res })
    }, then_assert: |result| {
        result().unwrap();
    });

    // Cache: CAST(val AS FLOAT) — MySQL uses FLOAT(p) syntax internally but
    // CAST(... AS FLOAT) works in MySQL 8+.
    eventually!(run_test: {
        let res = rs_conn
            .query_drop(
                "CREATE CACHE cast_float FROM \
                 SELECT id, CAST(val AS FLOAT) AS f FROM t1 WHERE id = ?"
            )
            .await;
        AssertUnwindSafe(|| { res })
    }, then_assert: |result| {
        result().unwrap();
    });

    // Verify CAST(val AS DOUBLE) for each row.
    //   id=1 '42.5'   → 42.5
    //   id=2 'hello'  → 0.0
    //   id=3 '123abc' → 123.0
    //   id=4 ''       → 0.0
    for row_id in [1, 2, 3, 4] {
        let upstream_rows: Vec<Row> = upstream_conn
            .exec(
                "SELECT id, CAST(val AS DOUBLE) AS d FROM t1 WHERE id = ?",
                (row_id,),
            )
            .await
            .unwrap();
        let upstream_values: Vec<(i32, Option<f64>)> = upstream_rows
            .iter()
            .map(|row| (row.get(0).unwrap(), row.get(1)))
            .collect();

        eventually!(run_test: {
            let rows: Vec<Row> = rs_conn
                .exec(
                    "SELECT id, CAST(val AS DOUBLE) AS d FROM t1 WHERE id = ?",
                    (row_id,),
                )
                .await
                .unwrap();
            let values: Vec<(i32, Option<f64>)> = rows
                .iter()
                .map(|row| (row.get(0).unwrap(), row.get(1)))
                .collect();
            values
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_values,
                "CAST(val AS DOUBLE) mismatch for id={row_id}: rs={result:?}, upstream={upstream_values:?}",
            );
        });
    }

    // Verify CAST(val AS FLOAT) for each row.
    for row_id in [1, 2, 3, 4] {
        let upstream_rows: Vec<Row> = upstream_conn
            .exec(
                "SELECT id, CAST(val AS FLOAT) AS f FROM t1 WHERE id = ?",
                (row_id,),
            )
            .await
            .unwrap();
        let upstream_values: Vec<(i32, Option<f32>)> = upstream_rows
            .iter()
            .map(|row| (row.get(0).unwrap(), row.get(1)))
            .collect();

        eventually!(run_test: {
            let rows: Vec<Row> = rs_conn
                .exec(
                    "SELECT id, CAST(val AS FLOAT) AS f FROM t1 WHERE id = ?",
                    (row_id,),
                )
                .await
                .unwrap();
            let values: Vec<(i32, Option<f32>)> = rows
                .iter()
                .map(|row| (row.get(0).unwrap(), row.get(1)))
                .collect();
            values
        }, then_assert: |result| {
            assert_eq!(
                result, upstream_values,
                "CAST(val AS FLOAT) mismatch for id={row_id}: rs={result:?}, upstream={upstream_values:?}",
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
