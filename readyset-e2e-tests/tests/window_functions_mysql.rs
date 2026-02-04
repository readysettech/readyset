//! E2E tests for window function queries that filter on generated output columns.
//!
//! Window functions generate output columns (e.g., ROW_NUMBER, RANK) that queries
//! may filter on (e.g., `WHERE rn <= ?`). This creates complex indexing requirements.
//!
//! **Current Implementation:** Window uses `RequiresFullReplay` when the output
//! column is in the query filter. This loads the entire parent table into Window's
//! state, which is memory-intensive but simple and correct.
//!
//! **Previous Bug (REA-6092, REA-5942):** Earlier attempts at partial materialization
//! caused "partially overlapping partial indices" errors because:
//! 1. Range filters on output columns create BTreeMap indices
//! 2. Window's suggest_indexes returned HashMap indices
//! 3. The index type mismatch caused migration planning to fail
//!
//! **Future Optimization (REA-6327):** Partial materialization could reduce memory
//! usage, but requires prefix-matching partial index support in dataflow-state.

use std::panic::AssertUnwindSafe;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::tags;

/// Window function queries that filter on the generated output column.
#[tokio::test]
#[tags(serial, mysql_upstream)]
async fn test_window_output_column_filter_patterns() {
    readyset_tracing::init_test_logging();
    let db_name = "window_output_column_filter_patterns";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .set_straddled_joins(true)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE items (
                id INT PRIMARY KEY,
                category VARCHAR(255),
                region VARCHAR(255),
                val INT,
                qty INT
            )",
        )
        .await
        .unwrap();

    upstream_conn
        .query_drop(
            "INSERT INTO items (id, category, region, val, qty) VALUES
             (1, 'electronics', 'north', 10, 10),
             (2, 'electronics', 'north', 20, 20),
             (3, 'electronics', 'north', 30, 5),
             (4, 'clothing', 'north', 40, 15),
             (5, 'clothing', 'south', 50, 25),
             (6, 'electronics', 'south', 60, 8),
             (7, 'clothing', 'south', 70, 30)",
        )
        .await
        .unwrap();

    sleep().await;

    // Simple ROW_NUMBER with range filter (REA-6092)
    rs_conn
        .query_drop(
            "CREATE CACHE q1 FROM
             SELECT COUNT(*) FROM (
                 SELECT ROW_NUMBER() OVER () AS rn
                 FROM items
             ) AS inner_q
             WHERE inner_q.rn <= ?",
        )
        .await
        .unwrap();

    // Partition + output column filter (REA-6092)
    rs_conn
        .query_drop(
            "CREATE CACHE q2 FROM
             SELECT * FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                 FROM items
             ) ranked
             WHERE category = ? AND rn <= ?",
        )
        .await
        .unwrap();

    // DISTINCT + DENSE_RANK + range filter (REA-5942)
    rs_conn
        .query_drop(
            "CREATE CACHE q3 FROM
             SELECT DISTINCT val FROM (
                 SELECT val,
                        DENSE_RANK() OVER (ORDER BY val DESC) AS rnk
                 FROM items
             ) olap
             WHERE rnk <= ?",
        )
        .await
        .unwrap();

    sleep().await;

    // rn <= 3 → 3 rows in subquery → COUNT(*) = 3
    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT COUNT(*) FROM (
                    SELECT ROW_NUMBER() OVER () AS rn
                    FROM items
                ) AS inner_q
                WHERE inner_q.rn <= ?",
                (3i32,),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 1, "q1 should return exactly one row");
        let count: i64 = rows[0].get::<i64, usize>(0).unwrap();
        assert_eq!(count, 3, "COUNT(*) for rn <= 3 should be 3");
    });

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                    FROM items
                ) ranked
                WHERE category = ? AND rn <= ?",
                ("electronics", 2i32),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q2 should return 2 rows (rn=1 and rn=2 for electronics)");
        // Columns: id(0), category(1), region(2), val(3), qty(4), rn(5)
        let row0 = (
            rows[0].get::<i32, usize>(0).unwrap(),
            rows[0].get::<String, usize>(1).unwrap(),
            rows[0].get::<i32, usize>(4).unwrap(),
            rows[0].get::<i64, usize>(5).unwrap(),
        );
        assert_eq!(row0, (3, "electronics".to_string(), 5, 1), "rn=1 should be id=3 with qty=5");
        let row1 = (
            rows[1].get::<i32, usize>(0).unwrap(),
            rows[1].get::<String, usize>(1).unwrap(),
            rows[1].get::<i32, usize>(4).unwrap(),
            rows[1].get::<i64, usize>(5).unwrap(),
        );
        assert_eq!(row1, (6, "electronics".to_string(), 8, 2), "rn=2 should be id=6 with qty=8");
    });

    // rnk <= 2 → ORDER BY val DESC gives rnk 1=70, 2=60 → DISTINCT val = 2 rows (60, 70)
    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT DISTINCT val FROM (
                    SELECT val,
                           DENSE_RANK() OVER (ORDER BY val DESC) AS rnk
                    FROM items
                ) olap
                WHERE rnk <= ?",
                (2i32,),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q3 should return 2 rows (val 70 and 60 for rnk <= 2)");
        let mut vals: Vec<i32> = rows.iter().map(|r| r.get::<i32, usize>(0).unwrap()).collect();
        vals.sort();
        assert_eq!(vals, vec![60, 70]);
    });

    // Equality on generated column (rn = ?). Tests scenario where generated
    // column might be first in index.
    rs_conn
        .query_drop(
            "CREATE CACHE q4 FROM
             SELECT * FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                 FROM items
             ) ranked
             WHERE rn = ? AND category = ?",
        )
        .await
        .unwrap();

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                    FROM items
                ) ranked
                WHERE rn = ? AND category = ?",
                (1i64, "electronics"),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 1, "q4 should return 1 row (rn=1 for electronics)");
        let row0 = (
            rows[0].get::<i32, usize>(0).unwrap(),
            rows[0].get::<String, usize>(1).unwrap(),
            rows[0].get::<i64, usize>(5).unwrap(),
        );
        assert_eq!(row0, (3, "electronics".to_string(), 1));
    });

    // Test 5: Lower bound range filter (rn >= ?)
    rs_conn
        .query_drop(
            "CREATE CACHE q5 FROM
             SELECT * FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                 FROM items
             ) ranked
             WHERE category = ? AND rn >= ?",
        )
        .await
        .unwrap();

    sleep().await;

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                    FROM items
                ) ranked
                WHERE category = ? AND rn >= ?",
                ("electronics", 3i64),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q5 should return 2 rows (rn=3 and rn=4 for electronics)");
        let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<i32, usize>(0).unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2], "Should be id=1 (rn=3) and id=2 (rn=4)");
    });

    // Between filter (rn >= ? AND rn <= ?)
    rs_conn
        .query_drop(
            "CREATE CACHE q6 FROM
             SELECT * FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                 FROM items
             ) ranked
             WHERE category = ? AND rn >= ? AND rn <= ?",
        )
        .await
        .unwrap();

    sleep().await;

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY category ORDER BY qty) AS rn
                    FROM items
                ) ranked
                WHERE category = ? AND rn >= ? AND rn <= ?",
                ("electronics", 2i64, 3i64),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q6 should return 2 rows (rn=2 and rn=3 for electronics)");
        let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<i32, usize>(0).unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 6], "Should be id=6 (rn=2) and id=1 (rn=3)");
    });

    // Multiple partition columns (PARTITION BY category, region)
    // Tests prefix matching with longer prefixes like [1, 2] → [1, 2, 5]
    rs_conn
        .query_drop(
            "CREATE CACHE q7 FROM
             SELECT * FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY category, region ORDER BY qty) AS rn
                 FROM items
             ) ranked
             WHERE category = ? AND region = ? AND rn <= ?",
        )
        .await
        .unwrap();

    sleep().await;

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY category, region ORDER BY qty) AS rn
                    FROM items
                ) ranked
                WHERE category = ? AND region = ? AND rn <= ?",
                ("electronics", "north", 2i64),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q7 should return 2 rows for electronics+north with rn <= 2");
        let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<i32, usize>(0).unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3], "Should be id=3 (rn=1) and id=1 (rn=2)");
    });

    rs_conn
        .query_drop(
            "CREATE CACHE q8 FROM
             SELECT * FROM (
                 SELECT *,
                        SUM(val) OVER (PARTITION BY category ORDER BY id) AS running_sum
                 FROM items
             ) ranked
             WHERE category = ? AND running_sum <= ?",
        )
        .await
        .unwrap();

    sleep().await;

    eventually!(run_test: {
        let result = rs_conn
            .exec(
                "SELECT * FROM (
                    SELECT *,
                           SUM(val) OVER (PARTITION BY category ORDER BY id) AS running_sum
                    FROM items
                ) ranked
                WHERE category = ? AND running_sum <= ?",
                ("electronics", 40i64),
            )
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let rows: Vec<Row> = result().unwrap();
        assert_eq!(rows.len(), 2, "q8 should return 2 rows with running_sum <= 40");
        let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<i32, usize>(0).unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2], "Should be id=1 (sum=10) and id=2 (sum=30)");
    });

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}
