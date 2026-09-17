//! Verifies that schema and caches survive a controller restart against the same consensus
//! store and base-table storage. Exercises the schema_catalog write-on-commit and the recovery
//! replay in `update_leader_state` end-to-end.

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use readyset_client::consensus::AuthorityControl;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_util::eventually;
use test_utils::{tags, upstream};

async fn readyset_status(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
    rows.iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Status")
        .expect("Status row not found")
        .get::<String, _>(1)
        .unwrap()
}

fn cache_id_and_name(rows: &[Row]) -> (String, String) {
    let row = rows.first().expect("no caches");
    (
        row.get::<String, _>(0).unwrap(),
        row.get::<String, _>(1).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern)]
async fn cache_and_schema_survive_controller_restart() {
    readyset_tracing::init_test_logging();
    let db_name = "controller_restart_recovery";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    // First boot: declare a cache, confirm it lands.
    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t WHERE id = ?")
        .await
        .unwrap();

    let caches: Vec<Row> = rs_conn.query("SHOW CACHES").await.unwrap();
    let created_cache = cache_id_and_name(&caches);
    // The cache was created without a name, so its implicit name is its query id.
    assert_eq!(created_cache.1, created_cache.0);

    drop(rs_conn);

    // Second boot: same store and storage dir. Schema and cache come back from the persisted
    // authority keys without re-issuing any DDL through the adapter.
    let (rs_opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually! {
        let caches: Vec<Row> = rs_conn.query("SHOW CACHES").await.unwrap();
        !caches.is_empty()
    }

    // The replayed cache must keep the identity it had on the first boot, including the
    // implicit name derived from the query id at create time.
    let caches: Vec<Row> = rs_conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(cache_id_and_name(&caches), created_cache);

    shutdown_tx.shutdown().await;
}

/// A view's DDL must stay in the persisted schema catalog after the view is compiled to
/// dataflow. The unrelated CREATE TABLE after the cache is created makes the controller
/// rewrite the persisted catalog at a point where the view is compiled; on restart, the view
/// must come back from the catalog so the cache over it can be recreated.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern)]
async fn view_cache_survives_controller_restart_after_unrelated_ddl() {
    readyset_tracing::init_test_logging();
    let db_name = "controller_restart_view_recovery";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("CREATE VIEW v AS SELECT id, val FROM t")
        .await
        .unwrap();

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually!(run_test: {
        readyset_status(&mut rs_conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });

    // Compile the view to dataflow by caching a query over it.
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT id, val FROM v WHERE id = ?")
        .await
        .unwrap();

    // Replicate an unrelated DDL change so the controller rewrites the persisted schema
    // catalog. The second cache doubles as the signal that the table has replicated.
    upstream_conn
        .query_drop("CREATE TABLE unrelated (id INT PRIMARY KEY)")
        .await
        .unwrap();
    eventually! {
        rs_conn
            .query_drop("CREATE CACHE FROM SELECT id FROM unrelated WHERE id = ?")
            .await
            .is_ok()
    }

    // The catalog rewrite triggered by the unrelated CREATE TABLE must still have the
    // view's DDL even though the view is compiled by this point.
    let entries = handle.authority().schema_catalog_entries().await.unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.unparsed_stmt.contains(&format!("CREATE VIEW `{db_name}`.`v`"))),
        "view DDL missing from persisted schema catalog: {entries:?}"
    );

    drop(rs_conn);

    let (rs_opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    eventually! {
        let caches: Vec<Row> = rs_conn.query("SHOW CACHES").await.unwrap();
        caches.len() == 2
    }

    let val: Option<i32> = rs_conn
        .exec_first("SELECT val FROM v WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(val, Some(10));

    shutdown_tx.shutdown().await;
}
