use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Value};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Regression test: a cached query keyed on a BOOL-typed column used to panic
/// the adapter's connection task when executed with a binary-protocol
/// DATETIME parameter (TimestampTz::coerce_to(DfType::Bool) unwrapped a zero
/// date while remapping the reader key). The table is created on the
/// upstream after the builder connects, so the raw `BOOL` DDL replicates via
/// binlog and keeps DfType::Bool (snapshotting via SHOW CREATE TABLE would
/// normalize it to tinyint(1) and miss the bug).
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn timestamp_bool_key_mysql() {
    readyset_tracing::init_test_logging();
    let db = "timestamp_bool_key_mysql";
    mysql_helpers::recreate_database(db).await;
    let mut up = Conn::new(mysql_helpers::upstream_config().db_name(Some(db)))
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();

    up.query_drop("CREATE TABLE t (id INT PRIMARY KEY, b BOOL)")
        .await
        .unwrap();
    up.query_drop("INSERT INTO t (id, b) VALUES (1, false), (2, NULL)")
        .await
        .unwrap();

    let q = "SELECT id FROM t WHERE b = ?";

    // Nonzero DATETIME parameter: MySQL compares tinyint(1) against it
    // numerically (20220209131415 vs 0/1), matching no rows. Fixed readyset
    // coerces the timestamp key to true, also finding no b=1 rows.
    let ts = Params::Positional(vec![Value::Date(2022, 2, 9, 13, 14, 15, 0)]);
    let expected: Vec<i32> = up.exec(q, ts.clone()).await.unwrap();
    eventually!(run_test: {
        let res: Result<Vec<i32>, _> = rs.exec(q, ts.clone()).await;
        AssertUnwindSafe(move || res)
    }, then_assert: |res| assert_eq!(res().unwrap(), expected));

    let info = mysql_helpers::last_query_info(&mut rs).await;
    assert!(matches!(info.destination, QueryDestination::Readyset(_)));

    // Zero DATETIME parameter: coerces to TimestampTz::zero(), which is
    // falsy, matching the b=0 row. Retried with eventually! rather than a
    // bare assert_eq!, since the query above only proves the cache answers
    // correctly for an empty result -- it does not prove the INSERT has
    // replicated yet.
    let zero = Params::Positional(vec![Value::Date(0, 0, 0, 0, 0, 0, 0)]);
    let expected_zero: Vec<i32> = up.exec(q, zero.clone()).await.unwrap();
    eventually!(run_test: {
        let res: Result<Vec<i32>, _> = rs.exec(q, zero.clone()).await;
        AssertUnwindSafe(move || res)
    }, then_assert: |res| assert_eq!(res().unwrap(), expected_zero));

    shutdown_tx.shutdown().await;
}
