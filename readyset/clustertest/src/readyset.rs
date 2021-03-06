use ::readyset::get_metric;
use ::readyset::metrics::{recorded, DumpedMetricValue};
use readyset_data::DataType;
use serial_test::serial;

use crate::*;

// This test verifies that requests routed to specific regions,
// actually does just that.
//
// Steps:
//   1. Create a two server deployment with regions `r1` and `r2`,
//      respectivly.
//   2. A recipe is installed for a query `q` which has view replicas
//      replicated into both regions.
//   3. Use the `r1` view for `q`.
//   4. Verify that the readyset-server in `r1` issued an upquery.
//   5. Verify that this reached the base table.
#[clustertest]
async fn query_regional_routing_test() {
    let mut deployment = DeploymentBuilder::new("ct_server_regional")
        .primary_region("r1")
        .add_server(ServerParams::default().with_region("r1"))
        .start()
        .await
        .unwrap();

    deployment
        .leader_handle()
        .extend_recipe(
            "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      CREATE CACHE q FROM
        SELECT *
        FROM t1;"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();

    let r1_addr = deployment.server_addrs()[0].clone();
    let r2_addr = deployment
        .start_server(ServerParams::default().with_region("r2"), true)
        .await
        .unwrap();

    // Replicate the reader for `q`.
    deployment
        .leader_handle()
        .replicate_readers(vec!["q".to_owned()], Some(r2_addr.clone()))
        .await
        .unwrap();

    // Insert row (1, 2, 2) into t1.
    let mut t1 = deployment.leader_handle().table("t1").await.unwrap();
    t1.insert(vec![
        DataType::from(1i32),
        DataType::from(2i32),
        DataType::from(2i32),
    ])
    .await
    .unwrap();

    // Query via r2.
    let mut view_r2 = deployment
        .leader_handle()
        .view_from_region("q", "r2")
        .await
        .unwrap();
    assert_eq!(
        view_r2.lookup(&[0i32.into()], true).await.unwrap(),
        vec![vec![
            DataType::from(1i32),
            DataType::from(2i32),
            DataType::from(2i32)
        ]]
    );

    let r1_metrics = deployment
        .metrics()
        .get_metrics_for_server(r1_addr)
        .await
        .unwrap()
        .metrics;
    let r2_metrics = deployment
        .metrics()
        .get_metrics_for_server(r2_addr)
        .await
        .unwrap()
        .metrics;

    assert_eq!(
        get_metric!(r1_metrics, recorded::SERVER_VIEW_QUERY_MISS),
        None
    );
    assert_eq!(
        get_metric!(r1_metrics, recorded::SERVER_VIEW_QUERY_HIT),
        None
    );
    assert_eq!(
        get_metric!(r2_metrics, recorded::SERVER_VIEW_QUERY_MISS),
        Some(DumpedMetricValue::Counter(0.0))
    );
    assert_eq!(
        get_metric!(r2_metrics, recorded::SERVER_VIEW_QUERY_HIT),
        Some(DumpedMetricValue::Counter(1.0))
    );

    deployment.teardown().await.unwrap();
}

// This test verifies that the controller is elected from the
// primary region.
//
// Steps:
//   1. Create a four server deployment with regions `r1`, `r1`, `r2`, and
//      `r3`, with primary region `r1`.
//   2. Retrieve the ServerHandle associated with the controller
//      server and verify that it is in `r1`.
//   3. Kill the server associated with the controller.
//   4. Verify the new controller elected is also in `r1`.
#[clustertest]
async fn controller_in_primary_test() {
    let mut deployment = DeploymentBuilder::new("ct_controller_in_primary")
        .primary_region("r1")
        .add_server(ServerParams::default().with_region("r1"))
        .add_server(ServerParams::default().with_region("r1"))
        .add_server(ServerParams::default().with_region("r2"))
        .add_server(ServerParams::default().with_region("r3"))
        .start()
        .await
        .unwrap();

    let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
    let controller_handle = deployment.server_handles().get(&controller_uri).unwrap();
    assert_eq!(controller_handle.params.region, Some("r1".to_string()));

    deployment.kill_server(&controller_uri, true).await.unwrap();
    let new_controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
    let new_controller_handle = deployment
        .server_handles()
        .get(&new_controller_uri)
        .unwrap();
    assert_ne!(new_controller_uri, controller_uri);
    assert_eq!(new_controller_handle.params.region, Some("r1".to_string()));

    deployment.teardown().await.unwrap();
}

// Ignored as this test cannot issue RPCs after killing the worker as it
// will get into a failing state and will not accept RPCs.
#[clustertest]
#[ignore]
async fn query_failure_recovery_with_volume_id() {
    let mut deployment = DeploymentBuilder::new("ct_failure_recovery_with_volume_id")
        .add_server(ServerParams::default().with_volume("v1"))
        .start()
        .await
        .unwrap();

    deployment
        .leader_handle()
        .extend_recipe(
            "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      CREATE CACHE q FROM
        SELECT *
        FROM t1;"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();

    // Insert row (1, 2, 2) into t1.
    let mut t1 = deployment.leader_handle().table("t1").await.unwrap();
    t1.insert(vec![
        DataType::from(1i32),
        DataType::from(2i32),
        DataType::from(2i32),
    ])
    .await
    .unwrap();

    // Create a second server now that the entire dataflow graph is
    // on the first server.
    let r1_addr = deployment.server_addrs()[0].clone();
    deployment
        .start_server(ServerParams::default().with_volume("v2"), true)
        .await
        .unwrap();

    deployment.kill_server(&r1_addr, true).await.unwrap();

    let res = deployment.leader_handle().view("q").await;
    assert!(res.is_err());

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn new_leader_worker_set() {
    let mut deployment = DeploymentBuilder::new("ct_new_leader_worker_set")
        .with_servers(3, ServerParams::default())
        .start()
        .await
        .unwrap();

    let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();

    // Kill the first server to trigger failure recovery.
    deployment.kill_server(&controller_uri, true).await.unwrap();

    // Check the number of healthy workers in the system.
    assert_eq!(
        deployment
            .leader_handle()
            .healthy_workers()
            .await
            .unwrap()
            .len(),
        2
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn balance_base_table_domains() {
    let mut deployment = DeploymentBuilder::new("ct_balance_base_table_domains")
        .with_servers(2, ServerParams::default())
        .start()
        .await
        .unwrap();

    deployment
        .leader_handle()
        .extend_recipe(
            "
        CREATE TABLE t1 (id INT PRIMARY KEY);
        CREATE TABLE t2 (id INT PRIMARY KEY);"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();

    let info = deployment.leader_handle().get_info().await.unwrap();

    dbg!(&info);

    // 2 workers
    assert_eq!(info.len(), 2);
    // each with 1 domain shard
    for (_, domains) in &*info {
        assert_eq!(domains.len(), 1);
    }

    deployment.teardown().await.unwrap();
}

async fn get_metric(
    deployment: &mut DeploymentHandle,
    address: Url,
    name: &str,
) -> Option<DumpedMetricValue> {
    let metrics = deployment
        .metrics()
        .get_metrics_for_server(address)
        .await
        .unwrap()
        .metrics;
    get_metric!(metrics, name)
}

// Validate that, on promotion of a follower to leader, its
// `CONTROLLER_IS_LEADER` metric changes from 0 (not leader) to 1 (leader)
#[clustertest]
async fn new_leader_metrics() {
    let mut deployment = DeploymentBuilder::new("ct_new_leader_metrics")
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

    let original_leader = deployment.server_addrs()[0].clone();
    let new_server = deployment
        .start_server(ServerParams::default(), true)
        .await
        .unwrap();

    // `new_server` should be reporting that it's a follower (0)
    assert_eq!(
        get_metric(
            &mut deployment,
            new_server.clone(),
            recorded::CONTROLLER_IS_LEADER
        )
        .await,
        Some(DumpedMetricValue::Gauge(0f64)),
    );

    // Killing the original leader will result in `new_server` becoming leader
    deployment
        .kill_server(&original_leader, true)
        .await
        .unwrap();

    // `new_server` should have received its promotion to leader now, and be
    // reporting that it's the leader (1)
    assert_eq!(
        get_metric(
            &mut deployment,
            new_server.clone(),
            recorded::CONTROLLER_IS_LEADER
        )
        .await,
        Some(DumpedMetricValue::Gauge(1f64)),
    );

    deployment.teardown().await.unwrap();
}

// Validate that `NORIA_STARTUP_TIMESTAMP` is being populated with a reasonably
// plausible timestamp
#[clustertest]
async fn ensure_startup_timestamp_metric() {
    // TODO: Move over to an integration test when metrics support is added to
    // integration tests
    // All received times must be at least this value
    let test_start_timetsamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;

    let mut deployment = DeploymentBuilder::new("ct_ensure_startup_timestamp_metric")
        .with_servers(2, ServerParams::default())
        .deploy_mysql()
        .deploy_mysql_adapter()
        .start()
        .await
        .unwrap();

    for address in deployment.server_addrs() {
        let found_timestamp = match get_metric(&mut deployment, address.clone(), recorded::NORIA_STARTUP_TIMESTAMP).await {
            Some(DumpedMetricValue::Counter(v)) => v,
            err => panic!(
                "For readyset-server {}, expected a Some(DumpedMetricValue::Counter), but instead received {:?}",
                address, err
            ),
        };

        assert!(
            test_start_timetsamp <= found_timestamp,
            "readyset-server {} has too early of a timestamp ({} but should be after {})",
            address,
            found_timestamp,
            test_start_timetsamp
        );
    }

    deployment.teardown().await.unwrap();
}
