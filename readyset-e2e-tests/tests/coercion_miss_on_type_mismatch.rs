use mysql_async::prelude::Queryable;
use readyset_adapter::backend::MigrationMode;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Regression test for REA-5809: caching a group_concat join subquery with a
/// type-mismatched join condition (INT = TEXT) and a range filter causes a
/// domain crash with "no tag found for value".
///
/// A prior cached query with compatible types sets up overlapping dataflow
/// nodes. The second query joins t1.id (INT) against group_concat output
/// (TEXT) with a range filter; the coercion failure was returning Miss, which
/// triggered a replay for a key that can never exist, panicking the domain.
/// The fix returns empty() instead.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
async fn test_group_concat_join_type_mismatch() {
    readyset_tracing::init_test_logging();
    let db_name = "gc_type_mismatch";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, _shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::InRequestPath)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t1 (
                id INT PRIMARY KEY,
                a VARCHAR(50),
                b VARCHAR(50)
            )",
        )
        .await
        .unwrap();

    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 'x', 'p'), (2, 'y', 'q'), (3, 'z', 'r')")
        .await
        .unwrap();

    // Cache group_concat join subquery with compatible types (VARCHAR = TEXT)
    // to set up dataflow nodes that overlap with the next query.
    // Use eventually! to wait for snapshotting to complete.
    eventually! {
        rs_conn
            .query::<mysql_async::Row, _>(
                "SELECT group_concat(t1.a separator ', ') \
                 FROM t1 \
                 INNER JOIN (SELECT group_concat(t1.b separator ', ') AS gc FROM t1) AS sub \
                 ON (t1.a = sub.gc)",
            )
            .await
            .is_ok()
    }

    // Type mismatch: t1.id (INT) = group_concat result (TEXT) + range filter.
    // The coercion failure must return empty, not Miss.
    eventually! {
        rs_conn
            .query::<mysql_async::Row, _>(
                "SELECT t1.id \
                 FROM t1 \
                 INNER JOIN (SELECT group_concat(t1.a separator ', ') AS gc FROM t1) AS sub \
                 ON (t1.id = sub.gc) \
                 WHERE (t1.id > 10)",
            )
            .await
            .is_ok()
    }
}
