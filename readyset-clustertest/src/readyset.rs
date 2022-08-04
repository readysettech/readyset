use launchpad::eventually;
use noria::get_metric;
use noria::metrics::{recorded, DumpedMetricValue};
use readyset_data::DataType;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serial_test::serial;

use crate::*;

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
                "For noria-server {}, expected a Some(DumpedMetricValue::Counter), but instead received {:?}",
                address, err
            ),
        };

        assert!(
            test_start_timetsamp <= found_timestamp,
            "noria-server {} has too early of a timestamp ({} but should be after {})",
            address,
            found_timestamp,
            test_start_timetsamp
        );
    }

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn replicated_readers() {
    let mut deployment = DeploymentBuilder::new("ct_replicated_readers")
        .with_servers(2, ServerParams::default())
        .reader_replicas(2)
        .start()
        .await
        .unwrap();
    let lh = deployment.leader_handle();

    lh.extend_recipe(
        "CREATE TABLE t (id int, val int);
         CREATE CACHE q FROM SELECT id, sum(val) FROM t WHERE id = ? GROUP BY id;"
            .parse()
            .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", lh.graphviz().await.unwrap());

    let mut t = lh.table("t").await.unwrap();
    t.insert_many(vec![
        vec![DataType::from(1), DataType::from(1)],
        vec![DataType::from(1), DataType::from(2)],
        vec![DataType::from(2), DataType::from(3)],
        vec![DataType::from(2), DataType::from(4)],
    ])
    .await
    .unwrap();

    let mut view_0 = lh.view_with_replica("q", 0).await.unwrap();
    let mut view_1 = lh.view_with_replica("q", 1).await.unwrap();

    // We should schedule the readers onto different workers (with different addresses)
    assert_eq!(view_0.num_shards(), 1);
    assert_eq!(view_1.num_shards(), 1);
    assert_ne!(view_0.shard_addrs(), view_1.shard_addrs());

    let view_0_key_1 = view_0.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(
        view_0_key_1.into_vec()[0],
        vec![DataType::from(1), DataType::from(Decimal::from_i32(3))]
    );

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_2.into_vec()[0],
        vec![DataType::from(2), DataType::from(Decimal::from_i32(7))]
    );

    t.insert_many(vec![
        vec![DataType::from(1), DataType::from(3)],
        vec![DataType::from(2), DataType::from(2)],
    ])
    .await
    .unwrap();

    eventually! {
        let view_0_key_1 = view_0.lookup(&[1.into()], true).await.unwrap();
        view_0_key_1.into_vec()[0] == vec![DataType::from(1), DataType::from(Decimal::from_i32(6))]
    }

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_2.into_vec()[0],
        vec![DataType::from(2), DataType::from(Decimal::from_i32(9))]
    );

    let view_0_key_2 = view_0.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_0_key_2.into_vec()[0],
        vec![DataType::from(2), DataType::from(Decimal::from_i32(9))]
    );

    let view_1_key_1 = view_1.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_1.into_vec()[0],
        vec![DataType::from(1), DataType::from(Decimal::from_i32(6))]
    );
}

#[clustertest]
async fn replicated_readers_with_unions() {
    let mut deployment = DeploymentBuilder::new("ct_replicated_readers_with_unions")
        .with_servers(2, ServerParams::default())
        .reader_replicas(2)
        .start()
        .await
        .unwrap();
    let lh = deployment.leader_handle();

    lh.extend_recipe(
        "CREATE TABLE t (id int, val1 int, val2 int);
         CREATE CACHE q FROM
         SELECT count(*) FROM t
         WHERE id = ?
           AND (val1 = 1 OR val1 = 2)
           AND (val2 = 1 OR val2 = 2);"
            .parse()
            .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", lh.graphviz().await.unwrap());

    let mut t = lh.table("t").await.unwrap();
    t.insert_many(vec![
        vec![DataType::from(1), DataType::from(1), DataType::from(1)],
        vec![DataType::from(1), DataType::from(1), DataType::from(2)],
        vec![DataType::from(1), DataType::from(2), DataType::from(1)],
        vec![DataType::from(1), DataType::from(2), DataType::from(2)],
        vec![DataType::from(1), DataType::from(2), DataType::from(3)],
        vec![DataType::from(2), DataType::from(1), DataType::from(1)],
        vec![DataType::from(2), DataType::from(1), DataType::from(2)],
        vec![DataType::from(2), DataType::from(3), DataType::from(2)],
    ])
    .await
    .unwrap();

    let mut view_0 = lh.view_with_replica("q", 0).await.unwrap();
    let mut view_1 = lh.view_with_replica("q", 1).await.unwrap();

    assert_eq!(view_0.num_shards(), 1);
    assert_eq!(view_1.num_shards(), 1);
    assert_ne!(view_0.shard_addrs(), view_1.shard_addrs());

    let view_0_key_1 = view_0.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(view_0_key_1.into_vec()[0], vec![DataType::from(4)]);

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(view_1_key_2.into_vec()[0], vec![DataType::from(2)]);

    let view_1_key_1 = view_1.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(view_1_key_1.into_vec()[0], vec![DataType::from(4)]);

    let view_0_key_2 = view_0.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(view_0_key_2.into_vec()[0], vec![DataType::from(2)]);
}

#[clustertest]
async fn no_readers_worker_doesnt_get_readers() {
    let mut deployment = DeploymentBuilder::new("ct_no_readers_worker_doesnt_get_readers")
        .add_server(ServerParams::default().no_readers())
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();
    let lh = deployment.leader_handle();

    lh.extend_recipe(
        "CREATE TABLE t (id int, val1 int, val2 int);
         CREATE CACHE q0 FROM SELECT id FROM t WHERE id = ?;
         CREATE CACHE q1 FROM SELECT val1 FROM t WHERE id = ?;
         CREATE CACHE q2 FROM SELECT val2 FROM t WHERE id = ?;
         CREATE CACHE q3 FROM SELECT id, val1, val2 FROM t WHERE id = ?;"
            .parse()
            .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", lh.graphviz().await.unwrap());

    let view_0 = lh.view("q0").await.unwrap();
    let view_1 = lh.view("q1").await.unwrap();
    let view_2 = lh.view("q2").await.unwrap();
    let view_3 = lh.view("q3").await.unwrap();

    // All views should be scheduled onto the same worker, regardless of balance
    //
    // Sadly we can't check *which* worker here, since we can't (currently) tell the workers apart.
    for view in [&view_0, &view_1, &view_2, &view_3] {
        assert_eq!(view.num_shards(), 1);
        assert_eq!(view.shard_addrs(), view_0.shard_addrs());
    }
}
