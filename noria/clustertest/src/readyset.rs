use crate::*;
use noria::metrics::{recorded, DumpedMetricValue};
use noria::{get_metric, DataType};
use serial_test::serial;

// This test verifies that requests routed to specific regions,
// actually does just that.
//
// Steps:
//   1. Create a two server deployment with regions `r1` and `r2`,
//      respectivly.
//   2. A recipe is installed for a query `q` which has view replicas
//      replicated into both regions.
//   3. Use the `r1` view for `q`.
//   4. Verify that the noria-server in `r1` issued an upquery.
//   5. Verify that this reached the base table.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn query_regional_routing_test() {
    let cluster_name = "ct_server_regional";
    let mut deployment = DeploymentParams::new(
        cluster_name,
        NoriaBinarySource::Build(BuildParams {
            root_project_path: get_project_root(),
            target_dir: get_project_root().join("test_target"),
            release: true,
            rebuild: false,
        }),
    );
    deployment.set_sharding(1);
    deployment.set_primary_region("r1");
    deployment.add_server(ServerParams::default().with_region("r1"));

    let mut deployment = start_multi_process(deployment).await.unwrap();
    deployment
        .handle
        .install_recipe(
            "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      QUERY q:
        SELECT *
        FROM t1;",
        )
        .await
        .unwrap();

    let r1_addr = deployment.server_addrs()[0].clone();
    let r2_addr = deployment
        .start_server(ServerParams::default().with_region("r2"))
        .await
        .unwrap();

    // Replicate the reader for `q`.
    deployment
        .handle
        .replicate_readers(vec!["q".to_owned()], Some(r2_addr.clone()))
        .await
        .unwrap();

    // Insert row (1, 2, 2) into t1.
    let mut t1 = deployment.handle.table("t1").await.unwrap();
    t1.insert(vec![
        DataType::from(1i32),
        DataType::from(2i32),
        DataType::from(2i32),
    ])
    .await
    .unwrap();

    // Query via r2.
    let mut view_r2 = deployment
        .handle
        .view_from_region("q", "r2".into())
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
        .metrics
        .get_metrics_for_server(r1_addr)
        .await
        .unwrap()
        .metrics;
    let r2_metrics = deployment
        .metrics
        .get_metrics_for_server(r2_addr)
        .await
        .unwrap()
        .metrics;
    assert_eq!(
        get_metric!(r1_metrics, recorded::SERVER_VIEW_QUERY_RESULT),
        None
    );
    assert_eq!(
        get_metric!(r2_metrics, recorded::SERVER_VIEW_QUERY_RESULT),
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
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn controller_in_primary_test() {
    let cluster_name = "ct_controller_in_primary";
    let mut deployment = DeploymentParams::new(
        cluster_name,
        NoriaBinarySource::Build(BuildParams {
            root_project_path: get_project_root(),
            target_dir: get_project_root().join("test_target"),
            release: true,
            rebuild: false,
        }),
    );
    deployment.set_primary_region("r1");
    deployment.add_server(ServerParams::default().with_region("r1"));
    deployment.add_server(ServerParams::default().with_region("r1"));
    deployment.add_server(ServerParams::default().with_region("r2"));
    deployment.add_server(ServerParams::default().with_region("r3"));

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let controller_uri = deployment.handle.controller_uri().await.unwrap();
    let controller_handle = deployment.server_handles().get(&controller_uri).unwrap();
    assert_eq!(controller_handle.params.region, Some("r1".to_string()));

    deployment.kill_server(&controller_uri).await.unwrap();
    let new_controller_uri = deployment.handle.controller_uri().await.unwrap();
    let new_controller_handle = deployment
        .server_handles()
        .get(&new_controller_uri)
        .unwrap();
    assert_ne!(new_controller_uri, controller_uri);
    assert_eq!(new_controller_handle.params.region, Some("r1".to_string()));

    deployment.teardown().await.unwrap();
}
