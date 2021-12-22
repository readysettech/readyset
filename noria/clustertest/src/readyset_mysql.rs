use crate::utils::*;
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
        query_until_expected_from_noria(
            &mut adapter_conn,
            deployment.metrics(),
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            UntilResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

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
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
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

    conn.query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 WHERE uid = ?";
    assert!(
        query_until_expected_from_noria(
            &mut conn,
            deployment.metrics(),
            query.clone(),
            (1,),
            UntilResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

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

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 WHERE uid = ?";
    assert!(
        query_until_expected_from_noria(
            &mut conn,
            deployment.metrics(),
            query.clone(),
            (1,),
            UntilResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn correct_data_after_restart() {
    let mut deployment = readyset_mysql("ct_correct_data_after_restart")
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

    let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
    let volume_id = deployment
        .server_handle(&controller_uri)
        .unwrap()
        .params
        .volume_id
        .clone()
        .unwrap();
    deployment
        .kill_server(&controller_uri, false)
        .await
        .unwrap();
    deployment
        .start_server(ServerParams::default().with_volume(&volume_id), false)
        .await
        .unwrap();
    deployment
        .backend_ready(Duration::from_secs(60))
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

/// This test verifies that following a worker failure we can create new views.
#[clustertest]
async fn create_view_after_worker_failure() {
    let mut deployment = readyset_mysql("ct_create_view_after_worker_failure")
        .quorum(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let opts = mysql_async::Opts::from_url(&deployment.mysql_db_str().unwrap()).unwrap();
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let _ = conn
        .query_drop(
            r"CREATE TABLE t1 (
                uid INT PRIMARY KEY,
                value INT
              );
              CREATE TABLE t2 (
                uid INT PRIMARY KEY,
                value INT
              );
              INSERT INTO t1 VALUES (1,2);
              INSERT INTO t2 VALUES (3,4);
            ",
        )
        .await
        .unwrap();

    let opts = mysql_async::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut adapter_conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    adapter_conn
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1;")
        .await
        .unwrap();
    adapter_conn
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t2;")
        .await
        .unwrap();

    sleep(Duration::from_secs(5)).await;

    let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
    let (volume_id, addr) = {
        let server_handle = deployment
            .server_handles()
            .values_mut()
            .find(|v| v.addr != controller_uri)
            .unwrap();
        let volume_id = server_handle.params.volume_id.clone().unwrap();
        let addr = server_handle.addr.clone();
        (volume_id, addr)
    };
    deployment.kill_server(&addr, false).await.unwrap();

    // Create a new server and wait until the deployment has enough healthy workers.
    deployment
        .start_server(ServerParams::default().with_volume(&volume_id), true)
        .await
        .unwrap();

    // Wait so that we trigger failure recovery behavior before CREATE CACHED QUERY.
    sleep(Duration::from_secs(5)).await;

    // This currently fails as the leader crashes, so we cannot reach quorum.
    adapter_conn
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?;")
        .await
        .unwrap();

    deployment.teardown().await.unwrap();
}

/// Fail the controller 10 times and check if we can execute the query. This
/// test will pass if we correctly execute queries against fallback.
#[clustertest]
async fn end_to_end_with_restarts() {
    if skip_slow_tests() {
        return;
    }

    let mut deployment = readyset_mysql("ct_end_to_end_with_restarts")
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
    conn.query_drop(r"CREATE CACHED QUERY test AS SELECT * FROM t1 WHERE uid = ?")
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
