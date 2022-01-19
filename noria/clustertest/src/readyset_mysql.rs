use crate::utils::*;
use crate::*;
use mysql_async::prelude::Queryable;
use noria::get_metric;
use noria::metrics::{recorded, DumpedMetricValue};
use serial_test::serial;
use std::time::Duration;
use test_utils::skip_slow_tests;
use tokio::time::{sleep, timeout};

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

    let mut adapter = deployment.adapter().await;
    let _ = adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            r"SELECT * FROM t1;",
            (),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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
    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();

    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
    // Kill the one and only server, everything should go to fallback.
    deployment
        .kill_server(&deployment.server_addrs()[0], false)
        .await
        .unwrap();
    let result: Vec<(i32, i32)> = adapter
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

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // Sleep so we have time to perform the migration async.
    sleep(Duration::from_secs(2)).await;

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t1 WHERE uid = ?;",
            (2,),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
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

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);")
        .await
        .unwrap();
    upstream
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Whoever services the read query is going to panic.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("read-query", "panic").await;
    }

    // Should return the correct results from fallback.
    let result: Vec<(i32, i32)> = adapter
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

    let mut adapter = deployment.adapter().await;
    let _ = adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 WHERE uid = ?";
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            query.clone(),
            (1,),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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
            &mut adapter,
            deployment.metrics(),
            query.clone(),
            (1,),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

/// Creates a two servers deployment and performs a failure and recovery on one
/// of the servers. After the failure, we verify that we can still perform the
/// query on Noria and we return the correct results.
#[clustertest]
async fn correct_data_after_restart() {
    let mut deployment = readyset_mysql("ct_correct_data_after_restart")
        .quorum(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.adapter().await;
    let _ = adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            r"SELECT * FROM t1;",
            (),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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

    // Query until we are able to get the results from Noria.
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t1;",
            (),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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

    let mut upstream = deployment.upstream().await;
    let _ = upstream
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

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1;")
        .await
        .unwrap();
    adapter
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
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?;")
        .await
        .unwrap();

    deployment.teardown().await.unwrap();
}

/// Fail the non-leader worker 10 times while issuing writes.
// This test currently fails because we drop writes to failed workers.
#[clustertest]
#[should_panic]
// TODO(ENG-933): Flaky test.
#[ignore]
async fn update_during_failure() {
    let mut deployment = readyset_mysql("ct_update_during_failure")
        .quorum(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    let _ = upstream
        .query_drop(
            r"CREATE TABLE t1 (
                uid INT NOT NULL,
                value INT NOT NULL
            );
            CREATE TABLE t2 (
                uid INT NOT NULL,
                value INT NOT NULL
            );
            INSERT INTO t1 VALUES (1,2);
            INSERT INTO t2 VALUES (1,2);
        ",
        )
        .await
        .unwrap();
    sleep(Duration::from_secs(5)).await;

    let mut adapter = deployment.adapter().await;

    // Perform the same writes on t1 and t1 so we use the same `results` struct
    // for both.
    let mut results = EventuallyConsistentResults::new();
    results.write(&[(1, 2)]);

    let (volume_id, addr) = {
        let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
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

    // Update the value to (1,3).
    upstream
        .query_drop("UPDATE t1 SET value = 3 WHERE uid = 1;")
        .await
        .unwrap();
    upstream
        .query_drop("UPDATE t2 SET value = 3 WHERE uid = 1;")
        .await
        .unwrap();
    results.write(&[(1, 3)]);
    // Let the write propagate while the worker is dead.
    sleep(Duration::from_secs(10)).await;

    // Wait for the deployment to completely recover from the failure.
    deployment
        .start_server(ServerParams::default().with_volume(&volume_id), true)
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t1 WHERE uid = 1;",
            (),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t2 WHERE uid = 1;",
            (),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn upquery_to_failed_reader_domain() {
    let mut deployment = readyset_mysql("ct_upquery_failed_domain_immediately")
        .with_servers(1, ServerParams::default())
        .quorum(1)
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);")
        .await
        .unwrap();
    upstream
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // The upquery will fail on the reader that handles it.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("handle-packet", "panic").await;
    }

    // Should return the correct results from fallback.
    let result: Result<mysql_async::Result<Vec<(i32, i32)>>, _> = timeout(
        Duration::from_secs(5),
        adapter.exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,)),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap(), vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

/// This fails the *second* domain that an upquery is issued to. In this case the
/// upquery hangs until the view query timeout.
// This test introduces a 5 second query timeout that fails due to the upquery hanging
// until RPC timeout.
#[clustertest]
#[should_panic]
async fn upquery_through_failed_domain() {
    let mut deployment = readyset_mysql("ct_failure_during_query")
        .with_servers(1, ServerParams::default())
        .quorum(1)
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);")
        .await
        .unwrap();
    upstream
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // The upquery will fail on the second domain that handles it.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("handle-packet", "1*off->1*panic->off")
            .await;
    }

    // Should return the correct results from fallback.
    let result: Result<mysql_async::Result<Vec<(i32, i32)>>, _> = timeout(
        Duration::from_secs(5),
        adapter.exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,)),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap(), vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[should_panic]
async fn update_propagation_through_failed_domain() {
    let mut deployment = readyset_mysql("ct_update_propagation_through_failed_domain")
        .with_servers(2, ServerParams::default())
        .quorum(2)
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);")
        .await
        .unwrap();
    upstream
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop("CREATE CACHED QUERY AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    let mut results = EventuallyConsistentResults::new();
    results.write(&[(2, 5)]);

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t1 where uid = ?;",
            (2,),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // Fail the reader node when it receives the update.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("reader-handle-packet", "panic").await;
    }

    upstream
        .query_drop(r"UPDATE t1 SET value = 4 WHERE uid = 2")
        .await
        .unwrap();
    results.write(&[(2, 4)]);

    // Sleep so that if the UPDATE would have cascade failed, we would never
    // reach quorum.
    sleep(Duration::from_secs(5)).await;

    // Turn off the panic so we don't fail on subsequent updates.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("reader-handle-packet", "off").await;
    }

    upstream
        .query_drop(r"UPDATE t1 SET value = 12 WHERE uid = 2")
        .await
        .unwrap();
    results.write(&[(2, 12)]);

    deployment
        .start_server(ServerParams::default(), false)
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            r"SELECT * FROM t1 where uid = ?;",
            (2,),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

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

    let mut adapter = deployment.adapter().await;
    let _ = adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            r"SELECT * FROM t1;",
            (),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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
            query_until_expected_from_noria(
                &mut adapter,
                deployment.metrics(),
                r"SELECT * FROM t1;",
                (),
                &EventuallyConsistentResults::empty_or(&[(1, 4)]),
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

    let mut adapter = deployment.adapter().await;
    let _ = adapter
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (1, 4);")
        .await
        .unwrap();
    adapter
        .query_drop(r"CREATE CACHED QUERY test AS SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    loop {
        let view = deployment.leader_handle().view("test").await;
        if view.is_ok() {
            break;
        }

        sleep(std::time::Duration::from_millis(100)).await;
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

            sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    deployment.teardown().await.unwrap();
}

/// Fail the non-leader worker 10 times while issuing writes.
// This test currently fails because we drop writes to failed workers.
#[clustertest]
#[ignore]
async fn writes_survive_restarts() {
    if skip_slow_tests() {
        return;
    }

    let mut deployment = readyset_mysql("ct_writes_survive_restarts")
        .quorum(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    let _ = upstream
        .query_drop(
            r"CREATE TABLE t1 (
                uid INT NOT NULL,
                value INT NOT NULL
            );
            CREATE TABLE t2 (
                uid INT NOT NULL,
                value INT NOT NULL
            );
            INSERT INTO t1 VALUES (1,2);
            INSERT INTO t2 VALUES (1,2);
        ",
        )
        .await
        .unwrap();
    sleep(Duration::from_secs(5)).await;

    let mut adapter = deployment.adapter().await;
    adapter
        .query_drop(r"CREATE CACHED QUERY AS SELECT * FROM t1 where uid = 1;")
        .await
        .unwrap();

    // Perform the same writes on t1 and t1 so we use the same `results` struct
    // for both.
    let mut results = EventuallyConsistentResults::new();
    results.write(&[(1, 2)]);

    let mut counter = 3u32;
    for _ in 0..10 {
        let (volume_id, addr) = {
            let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
            let server_handle = deployment
                .server_handles()
                .values_mut()
                .find(|v| v.addr != controller_uri)
                .unwrap();
            let volume_id = server_handle.params.volume_id.clone().unwrap();
            let addr = server_handle.addr.clone();
            (volume_id, addr)
        };
        println!("Killing server: {}", addr);
        deployment.kill_server(&addr, false).await.unwrap();
        let t1_write = format!("UPDATE t1 SET value = {} WHERE uid = 1;", counter);
        let t2_write = format!("UPDATE t2 SET value = {} WHERE uid = 1;", counter);
        upstream.query_drop(t1_write).await.unwrap();
        upstream.query_drop(t2_write).await.unwrap();
        results.write(&[(1, counter)]);

        println!("Starting new server");
        deployment
            .start_server(ServerParams::default().with_volume(&volume_id), false)
            .await
            .unwrap();

        assert!(
            query_until_expected_from_noria(
                &mut adapter,
                deployment.metrics(),
                r"SELECT * FROM t1 WHERE uid = 1;",
                (),
                &results,
                PROPAGATION_DELAY_TIMEOUT,
            )
            .await
        );
        assert!(
            query_until_expected_from_noria(
                &mut adapter,
                deployment.metrics(),
                r"SELECT * FROM t2 WHERE uid = 1;",
                (),
                &results,
                PROPAGATION_DELAY_TIMEOUT,
            )
            .await
        );

        counter = counter + 1;
    }

    deployment.teardown().await.unwrap();
}

// Perform an operation on the upstream MySQL DB that Noria can't yet handle
// replication for, and validate that the relevant metric records the failure
#[clustertest]
async fn replication_failure_metrics() {
    // TODO(ENG-92): Replace DROP table with a different schema failure when
    // supported.
    replication_test_inner(
        "ct_replication_failure_metrics",
        "CREATE TABLE t1 (id int); DROP TABLE t1",
        recorded::REPLICATOR_FAILURE,
    )
    .await;
}

// Perform an operation on the upstream MySQL DB that Noria should be able to
// successfully handle the replication for, and validate that the relevant
// metric records the success
#[clustertest]
async fn replication_success_metrics() {
    replication_test_inner(
        "ct_replication_success_metrics",
        "CREATE TABLE t1 (id int);",
        recorded::REPLICATOR_SUCCESS,
    )
    .await;
}

async fn replication_test_inner(test: &str, query: &str, metric_label: &str) {
    let mut deployment = readyset_mysql(test)
        .with_servers(1, ServerParams::default())
        .start()
        .await
        .unwrap();
    let addr = deployment.server_addrs()[0].clone();

    // Validate that the given `metric_label` should be unset
    let metrics = deployment
        .metrics()
        .get_metrics_for_server(addr.clone())
        .await
        .unwrap()
        .metrics;
    assert_eq!(get_metric!(metrics, metric_label), None);

    deployment.upstream().await.query_drop(query).await.unwrap();

    // Allow time for the replication binlog to propagate
    let start = Instant::now();
    let timeout = PROPAGATION_DELAY_TIMEOUT;
    let mut found_metric = None;
    loop {
        if start.elapsed() > timeout {
            break;
        }

        let metrics = deployment
            .metrics()
            .get_metrics_for_server(addr.clone())
            .await
            .unwrap()
            .metrics;

        found_metric = get_metric!(metrics, metric_label);
        if found_metric.is_some() {
            // We were able to retrieve metrics, don't need to wait for binlog
            // propagation anymore
            break;
        }

        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(found_metric, Some(DumpedMetricValue::Counter(1f64)));

    deployment.teardown().await.unwrap();
}
