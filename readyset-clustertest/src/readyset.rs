use ::readyset_client::metrics::{recorded, DumpedMetricValue};
use ::readyset_client::recipe::changelist::ChangeList;
use ::readyset_client::{failpoints, get_metric};
use launchpad::eventually;
use readyset_data::{DfValue, Dialect};
use readyset_tracing::info;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serial_test::serial;

use crate::readyset_mysql::PROPAGATION_DELAY_TIMEOUT;
use crate::utils::{query_until_expected, EventuallyConsistentResults, QueryExecution};
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
            ChangeList::from_str(
                "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      CREATE CACHE q FROM
        SELECT *
        FROM t1;",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Insert row (1, 2, 2) into t1.
    let mut t1 = deployment.leader_handle().table("t1").await.unwrap();
    t1.insert(vec![
        DfValue::from(1i32),
        DfValue::from(2i32),
        DfValue::from(2i32),
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
    res.unwrap_err();

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
            ChangeList::from_str(
                "
        CREATE TABLE t1 (id INT PRIMARY KEY);
        CREATE TABLE t2 (id INT PRIMARY KEY);",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let info = deployment.leader_handle().get_info().await.unwrap();

    // 2 workers
    assert_eq!(info.len(), 2);
    // each with 1 domain shard
    for domains in info.values() {
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
        ChangeList::from_str(
            "CREATE TABLE t (id int, val int);
         CREATE CACHE q FROM SELECT id, sum(val) FROM t WHERE id = ? GROUP BY id;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", lh.graphviz().await.unwrap());

    let mut t = lh.table("t").await.unwrap();
    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(2), DfValue::from(3)],
        vec![DfValue::from(2), DfValue::from(4)],
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
        vec![DfValue::from(1), DfValue::from(Decimal::from_i32(3))]
    );

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_2.into_vec()[0],
        vec![DfValue::from(2), DfValue::from(Decimal::from_i32(7))]
    );

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(3)],
        vec![DfValue::from(2), DfValue::from(2)],
    ])
    .await
    .unwrap();

    eventually! {
        let view_0_key_1 = view_0.lookup(&[1.into()], true).await.unwrap();
        view_0_key_1.into_vec()[0] == vec![DfValue::from(1), DfValue::from(Decimal::from_i32(6))]
    }

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_2.into_vec()[0],
        vec![DfValue::from(2), DfValue::from(Decimal::from_i32(9))]
    );

    let view_0_key_2 = view_0.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(
        view_0_key_2.into_vec()[0],
        vec![DfValue::from(2), DfValue::from(Decimal::from_i32(9))]
    );

    let view_1_key_1 = view_1.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(
        view_1_key_1.into_vec()[0],
        vec![DfValue::from(1), DfValue::from(Decimal::from_i32(6))]
    );
    deployment.teardown().await.unwrap();
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
        ChangeList::from_str(
            "CREATE TABLE t (id int, val1 int, val2 int);
         CREATE CACHE q FROM
         SELECT count(*) FROM t
         WHERE id = ?
           AND (val1 = 1 OR val1 = 2)
           AND (val2 = 1 OR val2 = 2);",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", lh.graphviz().await.unwrap());

    let mut t = lh.table("t").await.unwrap();
    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(1), DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(1), DfValue::from(2), DfValue::from(1)],
        vec![DfValue::from(1), DfValue::from(2), DfValue::from(2)],
        vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)],
        vec![DfValue::from(2), DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(2), DfValue::from(3), DfValue::from(2)],
    ])
    .await
    .unwrap();

    let mut view_0 = lh.view_with_replica("q", 0).await.unwrap();
    let mut view_1 = lh.view_with_replica("q", 1).await.unwrap();

    assert_eq!(view_0.num_shards(), 1);
    assert_eq!(view_1.num_shards(), 1);
    assert_ne!(view_0.shard_addrs(), view_1.shard_addrs());

    let view_0_key_1 = view_0.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(view_0_key_1.into_vec()[0], vec![DfValue::from(4)]);

    let view_1_key_2 = view_1.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(view_1_key_2.into_vec()[0], vec![DfValue::from(2)]);

    let view_1_key_1 = view_1.lookup(&[1.into()], true).await.unwrap();
    assert_eq!(view_1_key_1.into_vec()[0], vec![DfValue::from(4)]);

    let view_0_key_2 = view_0.lookup(&[2.into()], true).await.unwrap();
    assert_eq!(view_0_key_2.into_vec()[0], vec![DfValue::from(2)]);
    deployment.teardown().await.unwrap();
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
        ChangeList::from_str(
            "CREATE TABLE t (id int, val1 int, val2 int);
         CREATE CACHE q0 FROM SELECT id FROM t WHERE id = ?;
         CREATE CACHE q1 FROM SELECT val1 FROM t WHERE id = ?;
         CREATE CACHE q2 FROM SELECT val2 FROM t WHERE id = ?;
         CREATE CACHE q3 FROM SELECT id, val1, val2 FROM t WHERE id = ?;",
            Dialect::DEFAULT_MYSQL,
        )
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
    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn server_and_adapter_auto_restart() {
    let mut deployment = DeploymentBuilder::new("ct_adapter_restart")
        .add_server(ServerParams::default())
        .deploy_mysql_adapter()
        .auto_restart(true)
        .start()
        .await
        .unwrap();

    let adapter_handle = deployment
        .first_adapter_handle()
        .expect("adapter handle expected to exist");
    adapter_handle
        .process
        .kill()
        .await
        .expect("failed to kill adapter");
    sleep(Duration::from_secs(ProcessHandle::RESTART_INTERVAL_S * 2)).await;
    assert!(adapter_handle.process.check_alive().await);
    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn server_auto_restarts() {
    let mut deployment = DeploymentBuilder::new("ct_server_restart")
        .add_server(ServerParams::default())
        .auto_restart(true)
        .start()
        .await
        .unwrap();
    let r1_addr = deployment.server_addrs()[0].clone();
    let server_handle = deployment
        .server_handles()
        .get_mut(&r1_addr)
        .expect("server handle expected to exist");
    server_handle
        .process
        .kill()
        .await
        .expect("failed to kill server");
    sleep(Duration::from_secs(ProcessHandle::RESTART_INTERVAL_S * 2)).await;
    assert!(server_handle.process.check_alive().await);
    deployment.teardown().await.unwrap();
}

/// Performs a simple create table, insert, and query to verify that the deployment is healthy.
async fn assert_deployment_health(mut dh: DeploymentHandle) {
    let mut adapter = dh.first_adapter().await;
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

    tokio::spawn(async move {
        // There is a race condition with auto_restart and wait_for_failpoint:
        // Let's say we have the startup order: [Server, Upstream, Adapter, Authority]
        // The First server comes up and waits for a failpoint. One is set since the adapter
        // and authority are not up yet.
        // The test proceeds to startup the upstream, adapter, and authority--in the
        // meantime, the server's health reporter is running in the background and
        // discovering that it is unhealthy due to not being able to connect to the
        // authority. If the server is removed before the authority comes up, the next
        // server will start waiting for a failpoint. If that happens before the Authority
        // is up, a failpoint will be set when we remove it starting upt the authority. If
        // the authority comes up first, though, the server will wait forever for a
        // failpoint that never comes. We do an additional unset of the failpoints at this point to
        // make sure we don't wait in that case.

        // Since we spawned processes that will always wait for failpoints when they restart,
        // periodically send no-op failpoints
        loop {
            ClusterComponent::send_noop_failpoints(&mut dh, failpoints::AUTHORITY).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
}

#[clustertest]
async fn server_ready_before_adapter() {
    let mut deployment = DeploymentBuilder::new("ct_server_before_adapter")
        .auto_restart(true)
        .start()
        .await
        .unwrap();
    deployment
        .start_server(ServerParams::default().with_volume("v1"), true)
        .await
        .expect("server failed to become healthy");

    deployment
        .start_mysql_adapter(true)
        .await
        .expect("adapter failed to become healthy");

    assert_deployment_health(deployment).await;
}

#[clustertest]
async fn adapter_ready_before_server() {
    let mut deployment = DeploymentBuilder::new("ct_adapter_before_server")
        .auto_restart(true)
        .start()
        .await
        .unwrap();

    deployment
        .start_mysql_adapter(true)
        .await
        .expect("adapter failed to become healthy");

    deployment
        .start_server(ServerParams::default().with_volume("v1"), true)
        .await
        .expect("server failed to become healthy");

    assert_deployment_health(deployment).await;
}

#[clustertest]
async fn adapter_reports_unhealthy_consul_down() {
    let mut deployment = DeploymentBuilder::new("ct_adapter_reports_unhealthy_consul_down")
        .wait_for_failpoint(FailpointDestination::Adapter)
        .auto_restart(true)
        .start()
        .await
        .unwrap();

    deployment
        .start_mysql_adapter(false)
        .await
        .expect("adapter failed to become healthy");

    let adapter_handle = deployment.first_adapter_handle().unwrap();

    let timeout = Duration::new(2, 0);
    let poll_interval = timeout.checked_div(100).unwrap();
    wait_for_adapter_router(adapter_handle.metrics_port, timeout, poll_interval)
        .await
        .unwrap();

    adapter_handle
        .set_failpoint(failpoints::AUTHORITY, "pause")
        .await;

    let _ = wait_for_adapter_startup(adapter_handle.metrics_port, timeout).await;

    assert!(!endpoint_reports_healthy(adapter_handle.metrics_port).await);

    adapter_handle
        .set_failpoint(failpoints::AUTHORITY, "off")
        .await;

    wait_for_adapter_startup(adapter_handle.metrics_port, timeout)
        .await
        .unwrap();

    assert!(endpoint_reports_healthy(adapter_handle.metrics_port).await);
}

// Given a failpoint name, will assert that pausing that failpoint and then resuming it for the
// server will result in first unhealthy status, and then healthy status.
async fn assert_server_failpoint_pause_resume(deployment_name: &str, failpoint: &str) {
    let mut deployment = DeploymentBuilder::new(deployment_name)
        .wait_for_failpoint(FailpointDestination::Server)
        .auto_restart(false)
        .start()
        .await
        .unwrap();

    deployment
        .start_server(ServerParams::default().with_volume("v1"), false)
        .await
        .expect("server failed to become healthy");

    let r1_addr = deployment.server_addrs()[0].clone();
    let server_handle = deployment
        .server_handles()
        .get_mut(&r1_addr)
        .expect("server handle expected to exist");

    let port = server_handle.addr.port().unwrap();

    let timeout = Duration::new(2, 0);
    let poll_interval = timeout.checked_div(100).unwrap();
    wait_for_server_router(port, timeout, poll_interval)
        .await
        .unwrap();

    server_handle.set_failpoint(failpoint, "pause").await;

    let _ = wait_for_server_startup(port, timeout).await;

    assert!(!endpoint_reports_healthy(port).await);

    server_handle.set_failpoint(failpoint, "off").await;

    // Server must become healthy for this to not panic on unwrap within the supplied timeout.
    wait_for_server_startup(port, timeout).await.unwrap();
}

#[clustertest]
async fn server_reports_unhealthy_worker_down() {
    assert_server_failpoint_pause_resume("ct_server_reports_unhealthy_worker_down", "start-worker")
        .await
}

#[clustertest]
async fn server_reports_unhealthy_controller_down() {
    assert_server_failpoint_pause_resume(
        "ct_server_reports_unhealthy_controller_down",
        "start-controller",
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterComponent {
    Authority,
    Upstream,
    Adapter,
    Server,
}

impl ClusterComponent {
    const STARTUP_TIMEOUT: Duration = Duration::new(10, 0);
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    pub async fn startup(
        &self,
        deployment: &mut DeploymentHandle,
        authority_up: &mut bool,
        upstream_up: &mut bool,
    ) {
        debug!("Starting up {:?}", self);
        let server_url = deployment.server_addrs().first().cloned();
        let server_handle = server_url
            .and_then(|server_url| deployment.server_handles().get_mut(&server_url).cloned());
        let adapter_handle = deployment.first_adapter_handle();

        match self {
            ClusterComponent::Authority => {
                if let Some(adapter_handle) = adapter_handle {
                    adapter_handle
                        .set_failpoint(failpoints::AUTHORITY, "off")
                        .await;
                }
                if let Some(mut server_handle) = server_handle {
                    server_handle
                        .set_failpoint(failpoints::AUTHORITY, "off")
                        .await;
                }
                *authority_up = true;
                debug!("Authority up");
            }
            ClusterComponent::Upstream => {
                if let Some(adapter_handle) = adapter_handle {
                    adapter_handle
                        .set_failpoint(failpoints::UPSTREAM, "off")
                        .await;
                }
                if let Some(mut server_handle) = server_handle {
                    server_handle
                        .set_failpoint(failpoints::UPSTREAM, "off")
                        .await;
                }
                *upstream_up = true;
                debug!("Upstream up");
            }
            ClusterComponent::Adapter => {
                deployment
                    .start_mysql_adapter(false)
                    .await
                    .expect("readyset failed to startup");

                let adapter_handle = deployment.first_adapter_handle().unwrap();
                wait_for_adapter_router(
                    adapter_handle.metrics_port,
                    Self::STARTUP_TIMEOUT,
                    Self::POLL_INTERVAL,
                )
                .await
                .unwrap();

                debug!("Adapter up");
            }
            ClusterComponent::Server => {
                let server_url = deployment
                    .start_server(ServerParams::default(), false)
                    .await
                    .expect("readyset-server failed to startup");

                let server_handle = deployment.server_handles().get_mut(&server_url).unwrap();
                wait_for_server_router(
                    server_handle.addr.port().unwrap(),
                    Self::STARTUP_TIMEOUT,
                    Self::POLL_INTERVAL,
                )
                .await
                .unwrap();

                debug!("Server up");
            }
        }

        if *authority_up && *upstream_up {
            deployment.adapter_start_params().wait_for_failpoint = false;
            deployment.server_start_params().wait_for_failpoint = false;
        }
    }

    /// Sets no-op sleep 'failpoint', in case they are waiting on one.
    async fn send_noop_failpoints(deployment: &mut DeploymentHandle, failpoint: &str) {
        assert_eq!(1, deployment.adapter_handles().len());
        let adapter_handle = deployment
            .first_adapter_handle()
            .expect("adapter handle expected to exist");
        debug!("sending sleep failpoint to adapter");
        adapter_handle.set_failpoint(failpoint, "sleep(1)").await;

        assert_eq!(1, deployment.server_addrs().len());
        let server_url = deployment.server_addrs().first().unwrap().clone();
        let server_handle = deployment.server_handles().get_mut(&server_url).unwrap();
        debug!("sending sleep failpoint to server");
        server_handle.set_failpoint(failpoint, "sleep(1)").await;
    }
}

async fn test_deployment_startup_order(startup_order: Vec<ClusterComponent>) {
    info!(?startup_order, "Testing deployment startup");

    let mut deployment = DeploymentBuilder::new(
        format!(
            "ct_{:?}_{:?}_{:?}_{:?}",
            startup_order[0], startup_order[1], startup_order[2], startup_order[3]
        )
        .as_str(),
    )
    .wait_for_failpoint(FailpointDestination::Both)
    .auto_restart(true)
    .start()
    .await
    .unwrap();

    // If the authority or adapter are not up yet, we set failpoints to mimic this behavior
    let mut authority_up = false;
    let mut upstream_up = false;

    for component in startup_order {
        component
            .startup(&mut deployment, &mut authority_up, &mut upstream_up)
            .await;
    }

    assert_deployment_health(deployment).await;
}

#[clustertest]
#[ignore = "ENG-1668: fix flakiness as well as known failures"]
async fn startup_permutations() {
    use itertools::Itertools;
    use ClusterComponent::*;

    let startup_orders: Vec<Vec<ClusterComponent>> = [Authority, Upstream, Server, Adapter]
        .into_iter()
        .permutations(4)
        .collect();

    // FIXME[ENG-1668]: Either the system cannot startup healthily for the following situations, or
    // there are bugs in the failure injection we are doing in tests.
    let known_failures = vec![
        vec![Authority, Adapter, Server, Upstream],
        vec![Upstream, Adapter, Server, Authority],
        vec![Adapter, Authority, Server, Upstream],
        vec![Adapter, Upstream, Server, Authority],
        vec![Adapter, Server, Authority, Upstream],
    ];

    for startup_order in startup_orders {
        if known_failures.contains(&startup_order) {
            continue;
        }
        test_deployment_startup_order(startup_order).await;
    }
}

#[clustertest]
#[ignore = "FIXME ENG-1668: convenience test for debugging"]
async fn startup_permutations_failures() {
    // Delete this test once these are passing and just use startup_permutations above
    use ClusterComponent::*;
    let known_failures = vec![
        vec![Authority, Adapter, Server, Upstream],
        vec![Upstream, Adapter, Server, Authority],
        vec![Adapter, Authority, Server, Upstream],
        vec![Adapter, Upstream, Server, Authority],
        vec![Adapter, Server, Authority, Upstream],
    ];

    for startup_order in known_failures {
        test_deployment_startup_order(startup_order).await;
    }
}
