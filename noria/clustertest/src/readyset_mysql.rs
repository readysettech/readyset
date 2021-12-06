use crate::utils::query_until_expected;
use crate::*;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use noria::get_metric;
use noria::metrics::{recorded, DumpedMetricValue};
use serial_test::serial;
use std::time::Duration;

const PROPAGATION_DELAY_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table_insert_test() {
    let cluster_name = "ct_create_table_insert";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let _ = conn
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    conn.query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut conn,
            r"SELECT * FROM t1;",
            (),
            &[(1, 4)],
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
#[serial]
// TODO(ENG-641): Test is failing.
async fn show_tables_test() {
    let cluster_name = "ct_show_tables";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t2a (uid INT NOT NULL, value INT NOT NULL,);")
        .await
        .unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t2b (uid INT NOT NULL, value INT NOT NULL,);")
        .await
        .unwrap();

    let tables: Vec<String> = conn.query("SHOW TABLES;").await.unwrap();
    deployment.teardown().await.unwrap();
    assert_eq!(tables, vec!["t2a", "t2b"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
#[serial]
// TODO(ENG-641): Test is failing.
async fn describe_table_test() {
    let cluster_name = "ct_describe_table";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t3 (uid INT NOT NULL, value INT NOT NULL,);")
        .await
        .unwrap();

    let table: Vec<Row> = conn.query("DESCRIBE t3;").await.unwrap();
    let descriptor = table.get(0).unwrap();
    let cols = descriptor.columns_ref();
    let cols = cols
        .iter()
        .map(|c| c.name_ref())
        .into_iter()
        .collect::<Vec<_>>();
    let vals: Vec<Value> = descriptor.clone().unwrap();

    let cols_truth = vec![
        "Field".as_bytes(),
        "Type".as_bytes(),
        "Null".as_bytes(),
        "Key".as_bytes(),
        "Default".as_bytes(),
        "Extra".as_bytes(),
    ];
    let vals_truth = vec![
        Value::Bytes("uid".as_bytes().to_vec()),
        Value::Bytes("int".as_bytes().to_vec()),
        Value::Bytes("NO".as_bytes().to_vec()),
        Value::Bytes("".as_bytes().to_vec()),
        Value::NULL,
        Value::Bytes("".as_bytes().to_vec()),
    ];

    deployment.teardown().await.unwrap();

    assert_eq!(vals, vals_truth);
    assert_eq!(cols, cols_truth);
}

/// This test verifies that a prepared statement can be executed
/// on both noria and mysql.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mirror_prepare_exec_test() {
    let cluster_name = "ct_mirror_prepare_exec";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();

    // Create a table and write to it through the adapter.
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut adapter_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    adapter_conn
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();

    adapter_conn
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();
    adapter_conn
        .query_drop(r"INSERT INTO t1 VALUES (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter_conn,
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &[(2, 5)],
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
    // Kill the one and only server, everything should go to fallback.
    deployment
        .kill_server(&deployment.server_addrs()[0])
        .await
        .unwrap();
    let result: Vec<(i32, i32)> = adapter_conn
        .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,))
        .await
        .unwrap();
    assert_eq!(result, vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn async_migrations_sanity_check() {
    let cluster_name = "ct_async_migrations_sanity_check";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();
    // Enable async migrations with an interval of 500ms.
    deployment.enable_async_migrations(500);

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut adapter_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    adapter_conn
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter_conn
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();
    adapter_conn
        .query_drop(r"INSERT INTO t1 VALUES (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter_conn,
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &[(2, 5)],
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // Sleep so we have time to perform the migration async.
    sleep(Duration::from_secs(2)).await;

    assert!(
        query_until_expected(
            &mut adapter_conn,
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &[(2, 5)],
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // TODO(justin): Add utilities to abstract out this ridiculous way of getting
    // metrics.
    let metrics_dump = &deployment.metrics.get_metrics().await.unwrap()[0].metrics;
    assert_eq!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_RESULT),
        Some(DumpedMetricValue::Counter(1.0))
    );

    deployment.teardown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn query_view_after_failure() {
    let cluster_name = "ct_query_view_after_failure";
    let mut deployment = DeploymentParams::new(cluster_name);
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let _ = conn
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    conn.query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 where uid = ?";
    loop {
        let _: std::result::Result<Vec<Row>, _> = conn.exec(query.clone(), (1,)).await;
        let metrics_dump = &deployment.metrics.get_metrics().await.unwrap()[0].metrics;
        if Some(DumpedMetricValue::Counter(1.0))
            == get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_RESULT)
        {
            break;
        }
    }

    // TODO(ENG-862): This is required as propagation of the INSERT must occur
    // before kill_server or we may panic on recovery.
    sleep(std::time::Duration::from_secs(10)).await;

    deployment
        .kill_server(&deployment.server_addrs()[0])
        .await
        .unwrap();
    deployment
        .start_server(ServerParams::default())
        .await
        .unwrap();

    for _ in 0..10 {
        let _: std::result::Result<Vec<Row>, _> = conn.exec(query.clone(), (1,)).await;
    }

    // After a restart, we hit noria on the same view because we re-retrieve the view.
    let metrics_dump = &deployment.metrics.get_metrics().await.unwrap()[0].metrics;
    assert!(matches!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_RESULT),
        Some(_)
    ));

    deployment.teardown().await.unwrap();
}
