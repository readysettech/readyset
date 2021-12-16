use crate::utils::{query_until_expected, UntilResults};
use crate::*;
use mysql_async::prelude::Queryable;
use mysql_async::Row;
use noria::get_metric;
use noria::metrics::{recorded, DumpedMetricValue};
use serial_test::serial;
use std::time::Duration;
use test_utils::skip_slow_tests;

const PROPAGATION_DELAY_TIMEOUT: Duration = Duration::from_secs(60);

fn readyset_mysql(name: &str) -> DeploymentBuilder {
    DeploymentBuilder::new(name)
        .deploy_mysql()
        .deploy_mysql_adapter()
}

#[clustertest]
async fn create_table_insert_test() {
    let mut deployment = readyset_mysql("ct_create_table_insert")
        .with_servers(2, ServerParams::default())
        .start()
        .await
        .unwrap();

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
            UntilResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

/// This test verifies that a prepared statement can be executed
/// on both noria and mysql.
#[clustertest]
async fn mirror_prepare_exec_test() {
    let mut deployment = readyset_mysql("ct_mirror_prepare_exec")
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

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
            UntilResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
    // Kill the one and only server, everything should go to fallback.
    deployment
        .kill_server(&deployment.server_addrs()[0], false)
        .await
        .unwrap();
    let result: Vec<(i32, i32)> = adapter_conn
        .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,))
        .await
        .unwrap();
    assert_eq!(result, vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn async_migrations_confidence_check() {
    let mut deployment = readyset_mysql("ct_async_migrations_confidence_check")
        .add_server(ServerParams::default())
        .async_migrations(500)
        .start()
        .await
        .unwrap();

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
            UntilResults::empty_or(&[(2, 5)]),
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
            UntilResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // TODO(justin): Add utilities to abstract out this ridiculous way of getting
    // metrics.
    let metrics_dump = &deployment.metrics().get_metrics().await.unwrap()[0].metrics;
    let counter_value = get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_RESULT);
    match counter_value {
        Some(DumpedMetricValue::Counter(n)) => assert!(n >= 1.0),
        _ => panic!("Incorrect metric type or missing metric"),
    }

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn failure_during_query() {
    let mut deployment = readyset_mysql("ct_failure_during_query")
        .with_servers(2, ServerParams::default())
        .quorum(2)
        .start()
        .await
        .unwrap();

    let opts = mysql_async::Opts::from_url(&deployment.mysql_db_str().unwrap()).unwrap();
    let mut upstream_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    upstream_conn
        .query_drop("CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);")
        .await
        .unwrap();
    upstream_conn
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut adapter_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    adapter_conn
        .query_drop("CREATE QUERY CACHE AS SELECT * FROM t1 where uid = ?")
        .await
        .unwrap();

    // Whoever services the read query is going to panic.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("read-query", "panic").await;
    }

    // Should return the correct results from fallback.
    let result: Vec<(i32, i32)> = adapter_conn
        .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,))
        .await
        .unwrap();
    assert_eq!(result, vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn query_cached_view_after_failure() {
    let mut deployment = readyset_mysql("ct_query_view_after_failure")
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

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
    conn.query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    conn.query_drop("CREATE QUERY CACHE AS SELECT * FROM t1 where uid = ?")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 where uid = ?";
    loop {
        let _: std::result::Result<Vec<Row>, _> = conn.exec(query.clone(), (1,)).await;
        let metrics_dump = &deployment.metrics().get_metrics().await.unwrap()[0].metrics;
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
        .kill_server(&deployment.server_addrs()[0], false)
        .await
        .unwrap();

    deployment
        .start_server(ServerParams::default(), false)
        .await
        .unwrap();

    deployment
        .backend_ready(Duration::from_secs(60))
        .await
        .unwrap();

    for _ in 0..10 {
        let _: std::result::Result<Vec<Row>, _> = conn.exec(query.clone(), (1,)).await;
    }

    // After a restart, we hit noria on the same view because we re-retrieve the view.
    let metrics_dump = &deployment.metrics().get_metrics().await.unwrap()[0].metrics;
    assert!(matches!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_RESULT),
        Some(_)
    ));

    deployment.teardown().await.unwrap();
}

/// Fail the controller 10 times and check if we can execute the query. This
/// test will pass if we correctly execute queries against fallback.
#[clustertest]
async fn end_to_end_with_restarts() {
    if skip_slow_tests() {
        return;
    }

    let mut deployment = readyset_mysql("ct_repeated_failure")
        .quorum(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

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
            UntilResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    for _ in 0..10 {
        let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
        let volume_id = deployment
            .server_handle(&controller_uri)
            .unwrap()
            .params
            .volume_id
            .clone()
            .unwrap();
        println!("Killing server: {}", controller_uri);
        deployment
            .kill_server(&controller_uri, false)
            .await
            .unwrap();
        println!("Starting new server");
        deployment
            .start_server(ServerParams::default().with_volume(&volume_id), false)
            .await
            .unwrap();

        assert!(
            query_until_expected(
                &mut conn,
                r"SELECT * FROM t1;",
                (),
                UntilResults::empty_or(&[(1, 4)]),
                PROPAGATION_DELAY_TIMEOUT,
            )
            .await
        );
    }

    deployment.teardown().await.unwrap();
}

/// Fail the controller 10 times and check if we can query a view following
/// a restart.
#[clustertest]
async fn view_survives_restart() {
    if skip_slow_tests() {
        return;
    }

    let mut deployment = readyset_mysql("ct_view_survives_restarts")
        .quorum(2)
        .with_servers(2, ServerParams::default())
        .start()
        .await
        .unwrap();

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
    conn.query_drop(r"CREATE QUERY CACHE test AS SELECT * FROM t1 where uid = ?")
        .await
        .unwrap();

    loop {
        let view = deployment.leader_handle().view("test").await;
        if view.is_ok() {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    for _ in 0..10 {
        let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
        println!("Killing server: {}", controller_uri);
        deployment
            .kill_server(&controller_uri, false)
            .await
            .unwrap();
        println!("Starting new server");
        deployment
            .start_server(ServerParams::default(), false)
            .await
            .unwrap();

        // Request the view until it exists.
        loop {
            let view = deployment.leader_handle().view("test").await;
            if view.is_ok() {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    deployment.teardown().await.unwrap();
}
