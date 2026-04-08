use mysql_async::prelude::Queryable;
use readyset_adapter::backend::MigrationMode;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// REA-6500: CREATE CACHE should succeed for CONCAT(int_col, utf8mb4_literal, latin1_col).
///
/// MySQL resolves collation via coercibility rules (column beats literal beats numeric).
/// Readyset previously picked the first text argument's collation (the literal's utf8mb4),
/// then failed when trying to cast the latin1 column.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn test_concat_mixed_collation_latin1_and_utf8mb4() {
    readyset_tracing::init_test_logging();
    let db_name = "concat_collation";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t_latin1 (
                id   INT NOT NULL PRIMARY KEY,
                name VARCHAR(50) CHARACTER SET latin1
            )",
        )
        .await
        .unwrap();

    upstream_conn
        .query_drop("INSERT INTO t_latin1 VALUES (1, 'hello')")
        .await
        .unwrap();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::InRequestPath)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for snapshot, then create cache for the exact pattern from the bug report:
    // CONCAT(int_col, utf8mb4_literal, latin1_col)
    eventually! {
        rs_conn
            .query_drop(
                "CREATE CACHE FROM SELECT CONCAT(t1.id, 'sep', t1.name) AS val FROM t_latin1 t1",
            )
            .await
            .is_ok()
    }

    let rows: Vec<(String,)> = rs_conn
        .query("SELECT CONCAT(t1.id, 'sep', t1.name) AS val FROM t_latin1 t1")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "1sephello");

    shutdown_tx.shutdown().await;
}
