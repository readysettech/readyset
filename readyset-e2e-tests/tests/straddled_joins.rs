use mysql_async::prelude::Queryable;
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{
    self, MySQLAdapter, assert_materializations_have_key, last_query_info,
};
use readyset_client_test_helpers::{TestBuilder, sleep};
use test_utils::{tags, upstream};

/// REA-6145: no tag found for value
#[tokio::test]
#[tags(serial)]
#[upstream(mysql57, mysql80, mysql84)]
async fn test_sj_eviction_no_remapping() {
    readyset_tracing::init_test_logging();
    let db_name = "sj_eviction_no_remapping";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .set_straddled_joins(true)
        .replicate_db(db_name.to_string())
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t1 (
  id int NOT NULL AUTO_INCREMENT,
  t2_id int DEFAULT NULL,
  key_v varchar(255) DEFAULT NULL,
  deleted_at datetime DEFAULT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE t2 (
  id int NOT NULL AUTO_INCREMENT,
  key_i int DEFAULT NULL,
  deleted_at datetime DEFAULT NULL,
  PRIMARY KEY (id)
);",
        )
        .await
        .unwrap();

    sleep().await;

    rs_conn
        .query_drop("CREATE CACHE sj_cache FROM SELECT * FROM t1 INNER JOIN t2 ON (t1.t2_id = t2.id) WHERE t1.deleted_at IS NULL AND t2.deleted_at IS NULL AND t2.key_i = ? AND t1.key_v = ?;")
        .await
        .unwrap();

    sleep().await;

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM t1 INNER JOIN t2 ON (t1.t2_id = t2.id) WHERE t1.deleted_at IS NULL AND t2.deleted_at IS NULL AND t2.key_i = 1 AND t1.key_v = '3';").await.unwrap();
    assert_eq!(result.len(), 0);

    let last_statement = last_query_info(&mut rs_conn).await;
    assert_eq!(
        last_statement.destination,
        QueryDestination::Readyset(Some("sj_cache".into()))
    );

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM t1 INNER JOIN t2 ON (t1.t2_id = t2.id) WHERE t1.deleted_at IS NULL AND t2.deleted_at IS NULL AND t2.key_i = 1 AND t1.key_v = '2';").await.unwrap();
    assert_eq!(result.len(), 0);

    let last_statement = last_query_info(&mut rs_conn).await;
    assert_eq!(
        last_statement.destination,
        QueryDestination::Readyset(Some("sj_cache".into()))
    );

    let result: Vec<(i32, i32)> = rs_conn.query("SELECT * FROM t1 INNER JOIN t2 ON (t1.t2_id = t2.id) WHERE t1.deleted_at IS NULL AND t2.deleted_at IS NULL AND t2.key_i = 1 AND t1.key_v = '1';").await.unwrap();
    assert_eq!(result.len(), 0);

    let last_statement = last_query_info(&mut rs_conn).await;
    assert_eq!(
        last_statement.destination,
        QueryDestination::Readyset(Some("sj_cache".into()))
    );

    assert_materializations_have_key(&mut rs_conn, true, "3").await;

    handle.flush_partial().await.unwrap();
    handle.flush_partial().await.unwrap();
    handle.flush_partial().await.unwrap();
    sleep().await;

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}
