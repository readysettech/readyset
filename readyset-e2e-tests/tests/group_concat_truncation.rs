use std::panic::AssertUnwindSafe;

use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Verifies that GROUP_CONCAT results served from Readyset's cache are
/// truncated to `group_concat_max_len` (default 1024), matching MySQL's
/// behavior. Without fallback, the query MUST be served from Readyset's
/// dataflow, not proxied to upstream.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn group_concat_truncation_matches_upstream() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    // Create a table and insert enough rows that GROUP_CONCAT output exceeds
    // the default group_concat_max_len of 1024 bytes.
    upstream_conn
        .query_drop("CREATE TABLE gc_trunc (grp INT, val VARCHAR(20))")
        .await
        .unwrap();

    // 200 rows x 9-char values + 199 commas = 1999 bytes, well over 1024.
    let values: Vec<String> = (0..200)
        .map(|i| format!("(1, 'val_{i:05}')"))
        .collect();
    upstream_conn
        .query_drop(format!(
            "INSERT INTO gc_trunc (grp, val) VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();

    // ORDER BY makes the concatenation order deterministic so we can compare
    // the exact truncated prefix between upstream and Readyset.
    let query =
        "SELECT GROUP_CONCAT(val ORDER BY val SEPARATOR ',') FROM gc_trunc WHERE grp = 1";

    // Upstream MySQL truncates at group_concat_max_len (default 1024).
    let upstream_rows: Vec<Row> = upstream_conn.query(query).await.unwrap();
    let upstream_value: String = upstream_rows[0].get(0).unwrap();
    assert!(
        upstream_value.len() <= 1024,
        "upstream GROUP_CONCAT should be truncated to 1024, got {} bytes",
        upstream_value.len()
    );

    // Readyset should produce an identical truncated result from its cache.
    eventually!(run_test: {
        let rs_rows: Result<Vec<Row>, _> = rs_conn.query(query).await;
        AssertUnwindSafe(move || rs_rows)
    }, then_assert: |result| {
        let rs_rows: Vec<Row> = result().unwrap();
        let rs_value: String = rs_rows[0].get(0).unwrap();
        assert_eq!(
            upstream_value, rs_value,
            "Readyset GROUP_CONCAT should match upstream truncation \
             (expected {} bytes, got {} bytes)",
            upstream_value.len(),
            rs_value.len()
        );
    });

    shutdown_tx.shutdown().await;
}
