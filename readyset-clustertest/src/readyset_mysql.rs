use ::readyset_client::metrics::{recorded, DumpedMetricValue};
use ::readyset_client::query::QueryId;
use ::readyset_client::{get_metric, ViewCreateRequest};
use database_utils::QueryableConnection;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::QueryInfo;
use readyset_client_metrics::QueryDestination;
use readyset_util::eventually;
use readyset_util::hash::hash;
use serial_test::serial;
use test_utils::slow;
use tokio::time::{sleep, timeout};

use crate::utils::*;
use crate::*;

pub const PROPAGATION_DELAY_TIMEOUT: Duration = Duration::from_secs(90);

fn readyset_mysql(name: &str) -> DeploymentBuilder {
    DeploymentBuilder::new(DatabaseType::MySQL, name)
        .deploy_upstream()
        .deploy_adapter()
        .allow_full_materialization()
}

async fn last_statement_destination(conn: &mut mysql_async::Conn) -> QueryDestination {
    conn.query_first::<QueryInfo, _>("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap()
        .destination
}

#[clustertest]
async fn create_table_insert_test() {
    let mut deployment = readyset_mysql("ct_create_table_insert")
        .with_servers(2, ServerParams::default())
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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

    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
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
#[ignore = "ENG-2045: flaky test"]
async fn mirror_prepare_exec_test() {
    let mut deployment = readyset_mysql("ct_mirror_prepare_exec")
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

    // Create a table and write to it through the adapter.
    let mut adapter = deployment.first_adapter().await;
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
            QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = ?", (2,)),
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
        .as_mysql_conn()
        .unwrap()
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

    let mut adapter = deployment.first_adapter().await;
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
            QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = ?", (2,)),
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
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 where uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[ignore = "Flaky test (ENG-1929)"]
async fn failure_during_query() {
    let mut deployment = readyset_mysql("ct_failure_during_query")
        .with_servers(2, ServerParams::default())
        .min_workers(2)
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Whoever services the read query is going to panic.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("read-query", "panic").await;
    }

    // Should return the correct results from fallback.
    let result: Vec<(i32, i32)> = adapter
        .as_mysql_conn()
        .unwrap()
        .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,))
        .await
        .unwrap();
    assert_eq!(result, vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[ignore = "Flaky test (ENG-1930)"]
async fn query_cached_view_after_failure() {
    let mut deployment = readyset_mysql("ct_query_view_after_failure")
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 WHERE uid = ?";
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(query, (1,)),
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
            QueryExecution::PrepareExecute(query, (1,)),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn test_fallback_recovery_period() {
    let mut deployment = readyset_mysql("test_fallback_recovery_period")
        .with_servers(1, ServerParams::default())
        .min_workers(1)
        .query_max_failure_seconds(5)
        .fallback_recovery_seconds(1200)
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL
    );",
        )
        .await
        .unwrap();

    upstream
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2,5);")
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // Query until we hit noria once.
    let query = "SELECT * FROM t1 WHERE uid = ?";
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(query, (1,)),
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

    // Prep/exec path
    let res: (i32, i32) = adapter
        .as_mysql_conn()
        .unwrap()
        .exec_first(query, (1,))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res, (1, 4));

    let res: QueryInfo = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(r"EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res.destination, QueryDestination::ReadysetThenUpstream);

    // Standard query path (not prep/exec)
    let flattened_query = "SELECT * FROM t1 WHERE uid = 1";
    let res: (i32, i32) = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(flattened_query)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res, (1, 4));

    let res: QueryInfo = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(r"EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res.destination, QueryDestination::ReadysetThenUpstream);

    // We wait out the query_max_failure_seconds period and then try again, at which point we
    // should be in the recovery period.
    sleep(std::time::Duration::from_secs(6)).await;

    // Should hit fallback exclusively now because we should now be in the recovery period.
    // Regular query path.
    let res: (i32, i32) = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(flattened_query)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res, (1, 4));

    let res: QueryInfo = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(r"EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res.destination, QueryDestination::Upstream);

    // prep/exec path.
    let res: (i32, i32) = adapter
        .as_mysql_conn()
        .unwrap()
        .exec_first(query, (1,))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res, (1, 4));

    let res: QueryInfo = adapter
        .as_mysql_conn()
        .unwrap()
        .query_first(r"EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res.destination, QueryDestination::Upstream);

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn dry_run_evaluates_support() {
    let mut deployment = readyset_mysql("dry_run_evaluates_support")
        .add_server(ServerParams::default())
        .explicit_migrations(500)
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 where uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // After executing the query, the query will be dry run against ReadySet
    // in the background. Until the dry run is complete, the query has a
    // "pending" status in the proxied query table. Once the dry run
    // is complete, it will have a "yes" status.
    let select_query = match nom_sql::parse_query(
        nom_sql::Dialect::MySQL,
        "SELECT * FROM `t1` WHERE (`uid` = $1)",
    )
    .unwrap()
    {
        nom_sql::SqlQuery::Select(s) => s,
        _ => unreachable!(),
    };
    let query_id = QueryId::new(hash(&ViewCreateRequest::new(
        select_query,
        vec![deployment.name().into()],
    )))
    .to_string();
    let mut results = EventuallyConsistentResults::new();
    results.write(&[(
        query_id.clone(),
        "SELECT * FROM `t1` WHERE (`uid` = $1)".to_string(),
        "pending".to_string(),
    )]);
    results.write(&[(
        query_id,
        "SELECT * FROM `t1` WHERE (`uid` = $1)".to_string(),
        "yes".to_string(),
    )]);

    // Verify that the query eventually reaches the "yes" state in the
    // proxied query table.
    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::<String, ()>::Query("SHOW PROXIED QUERIES"),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn proxied_queries_filtering() {
    let mut deployment = readyset_mysql("proxied_queries_filtering")
        .add_server(ServerParams::default())
        .explicit_migrations(500)
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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
        .query_drop(r"INSERT INTO t1 VALUES (1, 4), (2, 5);")
        .await
        .unwrap();

    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 where uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // After executing the query, the query will be dry run against ReadySet
    // in the background. Until the dry run is complete, the query has a
    // "pending" status in the proxied query table. Once the dry run
    // is complete, it will have a "yes" status.
    let select_query = match nom_sql::parse_query(
        nom_sql::Dialect::MySQL,
        "SELECT * FROM `t1` WHERE (`uid` = $1)",
    )
    .unwrap()
    {
        nom_sql::SqlQuery::Select(s) => s,
        _ => unreachable!(),
    };
    let query_id = QueryId::new(hash(&ViewCreateRequest::new(
        select_query,
        vec![deployment.name().into()],
    )))
    .to_string();
    let mut results = EventuallyConsistentResults::new();
    results.write(&[(
        query_id.clone(),
        "SELECT * FROM `t1` WHERE (`uid` = $1)".to_string(),
        "pending".to_string(),
    )]);
    results.write(&[(
        query_id.clone(),
        "SELECT * FROM `t1` WHERE (`uid` = $1)".to_string(),
        "yes".to_string(),
    )]);

    // Verify that the query eventually reaches the "yes" state in the
    // proxied query table.
    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::<String, ()>::Query("SHOW PROXIED QUERIES"),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    let query = format!("SHOW PROXIED QUERIES WHERE query_id = '{}';", &query_id);
    let proxied_queries = adapter
        .as_mysql_conn()
        .unwrap()
        .query::<(String, String, String), _>(query)
        .await
        .unwrap();

    assert_eq!(proxied_queries.len(), 1);

    assert_eq!(proxied_queries[0].0, query_id);

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn cached_queries_filtering() {
    let mut deployment = readyset_mysql("cached_queries_filtering")
        .add_server(ServerParams::default())
        .explicit_migrations(500)
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE TABLE t1 (uid INT)")
        .await
        .unwrap();

    // Cache two queries
    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();
    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1")
        .await
        .unwrap();

    // Get all cached query IDs
    let query_ids = adapter
        .as_mysql_conn()
        .unwrap()
        .query::<(String, String, String, String), _>("SHOW CACHES")
        .await
        .unwrap();
    assert_eq!(query_ids.len(), 2);

    // Filter on one of the IDs
    let (query_id, _, _, _) = query_ids.first().unwrap();
    let cached_queries = adapter
        .as_mysql_conn()
        .unwrap()
        .query::<(String, String, String, String), _>(&format!(
            "SHOW CACHES WHERE query_id = '{}'",
            query_id
        ))
        .await
        .unwrap();

    assert_eq!(cached_queries.len(), 1);

    assert_eq!(cached_queries[0].1, *query_id);

    deployment.teardown().await.unwrap();
}

/// Creates a two servers deployment and performs a failure and recovery on one
/// of the servers. After the failure, we verify that we can still perform the
/// query on ReadySet and we return the correct results.
#[clustertest]
#[ignore = "Flaky test (REA-3107)"]
async fn correct_data_after_restart() {
    let mut deployment = readyset_mysql("ct_correct_data_after_restart")
        .min_workers(2)
        .allow_full_materialization()
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

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
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1")
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
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

    // Ensure that workers are ready before issuing queries, since the graph may need to be rebuilt.
    deployment.leader_handle().ready().await.unwrap();
    deployment
        .wait_for_workers(PROPAGATION_DELAY_TIMEOUT * 2)
        .await
        .unwrap();

    // Query until we are able to get the results from ReadySet.
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

/// This test verifies that following a worker failure we can create new views.
#[clustertest]
#[ignore = "Flaky test (ENG-1694)"]
async fn create_view_after_worker_failure() {
    let mut deployment = readyset_mysql("ct_create_view_after_worker_failure")
        .min_workers(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1;")
        .await
        .unwrap();
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t2;")
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

    // Wait so that we trigger failure recovery behavior before CREATE CACHE.
    sleep(Duration::from_secs(5)).await;

    // This currently fails as the leader crashes, so we cannot reach quorum.
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?;")
        .await
        .unwrap();

    deployment.teardown().await.unwrap();
}

/// Fail the non-leader worker 10 times while issuing writes.
// This test currently fails because we drop writes to failed workers.
#[clustertest]
async fn update_during_failure() {
    let mut deployment = readyset_mysql("ct_update_during_failure")
        .min_workers(2)
        .add_server(ServerParams::default())
        .add_server(ServerParams::default())
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
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

    let mut adapter = deployment.first_adapter().await;

    // Perform the same writes on t1 and t1 so we use the same `results` struct
    // for both.
    let mut results = EventuallyConsistentResults::new();
    results.write(&[(1, 2)]);

    let addr = {
        let controller_uri = deployment.leader_handle().controller_uri().await.unwrap();
        let server_handle = deployment
            .server_handles()
            .values_mut()
            .find(|v| v.addr != controller_uri)
            .unwrap();
        server_handle.addr.clone()
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

    // Wait until we detect that the worker has failed before trying to perform
    // a migration. Migrations involving failed workers currently fail the leader.
    deployment
        .wait_for_workers(PROPAGATION_DELAY_TIMEOUT * 2)
        .await
        .unwrap();

    // Wait for the deployment to completely recover from the failure.
    deployment
        .start_server(ServerParams::default(), true)
        .await
        .unwrap();

    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1 WHERE uid = 1")
        .await
        .unwrap();

    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t2 WHERE uid = 1")
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = 1", ()),
            &results,
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t2 where uid = 1", ()),
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
        .min_workers(1)
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    // The upquery will fail on the reader that handles it.
    for s in deployment.server_handles().values_mut() {
        s.set_failpoint("handle-packet", "panic").await;
    }

    // Should return the correct results from fallback.
    let result: Result<mysql_async::Result<Vec<(i32, i32)>>, _> = timeout(
        Duration::from_secs(5),
        adapter
            .as_mysql_conn()
            .unwrap()
            .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,)),
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
async fn upquery_through_failed_domain() {
    let mut deployment = readyset_mysql("ct_failure_during_query")
        .with_servers(1, ServerParams::default())
        .min_workers(1)
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
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
        adapter
            .as_mysql_conn()
            .unwrap()
            .exec(r"SELECT * FROM t1 WHERE uid = ?;", (2,)),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap(), vec![(2, 5)]);

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[ignore] // TODO(ENG-1029): Investigate whether this requires revert
async fn update_propagation_through_failed_domain() {
    let mut deployment = readyset_mysql("ct_update_propagation_through_failed_domain")
        .with_servers(2, ServerParams::default())
        .min_workers(2)
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop("CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();

    let mut results = EventuallyConsistentResults::new();
    results.write(&[(2, 5)]);

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = ?", (2,)),
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
        .start_server(ServerParams::default(), true)
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = ?", (2,)),
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
#[slow]
async fn end_to_end_with_restarts() {
    let mut deployment = readyset_mysql("ct_end_to_end_with_restarts")
        .min_workers(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1")
        .await
        .unwrap();

    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
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
                QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
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
#[slow]
async fn view_survives_restart() {
    let mut deployment = readyset_mysql("ct_view_survives_restarts")
        .min_workers(2)
        .with_servers(2, ServerParams::default())
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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
        .query_drop(r"CREATE CACHE test FROM SELECT * FROM t1 WHERE uid = ?")
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
#[slow]
async fn writes_survive_restarts() {
    let mut deployment = readyset_mysql("ct_writes_survive_restarts")
        .min_workers(2)
        .add_server(ServerParams::default().with_volume("v1"))
        .add_server(ServerParams::default().with_volume("v2"))
        .start()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
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

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1 where uid = 1;")
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
        upstream.query_drop(&t1_write).await.unwrap();
        upstream.query_drop(&t2_write).await.unwrap();
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
                QueryExecution::PrepareExecute("SELECT * FROM t1 where uid = 1", ()),
                &results,
                PROPAGATION_DELAY_TIMEOUT,
            )
            .await
        );
        assert!(
            query_until_expected_from_noria(
                &mut adapter,
                deployment.metrics(),
                QueryExecution::PrepareExecute("SELECT * FROM t2 where uid = 1", ()),
                &results,
                PROPAGATION_DELAY_TIMEOUT,
            )
            .await
        );

        counter += 1;
    }

    deployment.teardown().await.unwrap();
}

// Perform an operation on the upstream MySQL DB that ReadySet should be able to
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

#[clustertest]
async fn correct_deployment_permissions() {
    let mut deployment = readyset_mysql("ct_correct_deployment_permissions")
        .with_servers(1, ServerParams::default())
        .with_user("client", "password")
        .min_workers(1)
        .start_with_seed(&[
            "CREATE USER IF NOT EXISTS 'client'@'%' IDENTIFIED BY 'password';",
            "REVOKE ALL PRIVILEGES ON *.* FROM 'client'@'%';",
            "GRANT SELECT, RELOAD, LOCK TABLES, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'client'@'%';",
            "FLUSH PRIVILEGES",
            "CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);",
            "INSERT INTO t1 VALUES (1, 4), (2, 5);",
        ], Duration::from_secs(5), true)
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 where uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[slow]
async fn post_deployment_permissions_lock_table() {
    let mut deployment = readyset_mysql("ct_post_deployment_permissions_lock_table")
        .with_servers(1, ServerParams::default())
        .with_user("client", "password")
        .replicator_restart_timeout(5)
        .start_with_seed(&[
            "CREATE USER IF NOT EXISTS 'client'@'%' IDENTIFIED BY 'password';",
            "REVOKE ALL PRIVILEGES ON *.* FROM 'client'@'%';",
            "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'client'@'%';",
            "FLUSH PRIVILEGES",
            "CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);",
            "INSERT INTO t1 VALUES (1, 4), (2, 5);",
        ], Duration::from_secs(5), true)
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("GRANT LOCK TABLES ON *.* TO 'client'@'%';")
        .await
        .unwrap();

    // Time for replication to work, 4x the replicator restart timeout.
    sleep(Duration::from_secs(20)).await;

    let mut adapter = deployment.first_adapter().await;
    adapter
        .query_drop(r"CREATE CACHE FROM SELECT * FROM t1 WHERE uid = ?")
        .await
        .unwrap();
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 WHERE uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            Duration::from_secs(20),
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
#[ignore = "ENG-2067: flaky test"]
#[slow]
async fn post_deployment_permissions_replication() {
    let mut deployment = readyset_mysql("ct_post_deployment_permissions_replication")
        .with_servers(1, ServerParams::default())
        .with_user("client", "password")
        .replicator_restart_timeout(5)
        .start_with_seed(
            &[
                "CREATE USER IF NOT EXISTS 'client'@'%' IDENTIFIED BY 'password';",
                "REVOKE ALL PRIVILEGES ON *.* FROM 'client'@'%';",
                "GRANT SELECT, RELOAD, LOCK TABLES, SHOW DATABASES ON *.* TO 'client'@'%';",
                "FLUSH PRIVILEGES",
                "CREATE TABLE t1 (uid INT NOT NULL, value INT NOT NULL);",
                "INSERT INTO t1 VALUES (1, 4), (2, 5);",
            ],
            Duration::from_secs(5),
            true,
        )
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;
    upstream
        .query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'client'@'%';")
        .await
        .unwrap();

    // Time for replication to work, 4x the replicator restart timeout.
    sleep(Duration::from_secs(20)).await;

    let mut adapter = deployment.first_adapter().await;
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(r"SELECT * FROM t1 where uid = ?", (2,)),
            &EventuallyConsistentResults::empty_or(&[(2, 5)]),
            Duration::from_secs(20),
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn views_synchronize_between_deployments() {
    let mut deployment = readyset_mysql("views_synchronize_between_deployments")
        .with_servers(1, ServerParams::default())
        .with_adapters(2)
        .views_polling_interval(Duration::from_secs(1))
        .explicit_migrations(500)
        .deploy_upstream()
        .start()
        .await
        .unwrap();

    let mut adapter_0 = deployment.adapter(0).await;
    let mut adapter_1 = deployment.adapter(1).await;

    adapter_0
        .query_drop("CREATE TABLE t1 (x INT);")
        .await
        .unwrap();

    // Get a query in the status cache for adapter 1
    adapter_1.query_drop("SELECT * FROM t1;").await.unwrap();
    assert_eq!(
        last_statement_destination(adapter_1.as_mysql_conn().unwrap()).await,
        QueryDestination::Upstream
    );

    // Then create that query via adapter 0
    adapter_0
        .query_drop("CREATE CACHE FROM SELECT * FROM t1;")
        .await
        .unwrap();

    // Ensure it's been successfully created in adapter 0
    adapter_0.query_drop("SELECT * FROM t1;").await.unwrap();
    assert_eq!(
        last_statement_destination(adapter_0.as_mysql_conn().unwrap()).await,
        QueryDestination::Readyset
    );

    // Eventually it should show up in adapter 1 too
    eventually! {
        adapter_1.as_mysql_conn().unwrap().query_drop("SELECT * FROM t1;");
        last_statement_destination(adapter_1.as_mysql_conn().unwrap()).await == QueryDestination::Readyset
    }

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn standalone_create_table_insert_test() {
    readyset_tracing::init_test_logging();
    let mut deployment = readyset_mysql("ct_standalone_create_table_insert")
        .standalone()
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;
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

    assert!(
        query_until_expected(
            &mut adapter,
            QueryExecution::PrepareExecute("SELECT * FROM t1", ()),
            &EventuallyConsistentResults::empty_or(&[(1, 4)]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn enable_experimental_placeholder_inlining() {
    let mut deployment = readyset_mysql("ct_enable_experimental_placeholder_inlining")
        .with_servers(1, ServerParams::default())
        .explicit_migrations(500)
        .enable_experimental_placeholder_inlining()
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

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
        .query_drop(r"INSERT INTO t1 VALUES (2, 4);")
        .await
        .unwrap();
    adapter
        .query_drop(r"INSERT INTO t1 VALUES (2, 4);")
        .await
        .unwrap();

    // CREATE CACHE should fail
    let _ = adapter.query_drop(r"CREATE CACHE FROM SELECT sum(value) FROM t1 WHERE value = ? GROUP BY uid HAVING count(uid) = ?").await;

    // Run the query for the first time
    let result: Vec<i32> = adapter
        .as_mysql_conn()
        .unwrap()
        .exec(
            r"SELECT sum(value) FROM t1 WHERE value = ? GROUP BY uid HAVING count(uid) = ?",
            (4, 1),
        )
        .await
        .unwrap();
    assert_eq!(&result, &[4]);

    // Query should go straight to upstream since we don't have any caches for the query.
    assert_eq!(
        last_statement_destination(adapter.as_mysql_conn().unwrap()).await,
        QueryDestination::Upstream
    );

    // We should asynchronously inline this query and eventually see the results against noria.
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(
                "SELECT sum(value) FROM t1 WHERE value = ? GROUP BY uid HAVING count(uid) = ?",
                (4, 1,)
            ),
            &EventuallyConsistentResults::empty_or(&[4]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    // Run the query again with a new set of parameters
    let result: Vec<i32> = adapter
        .as_mysql_conn()
        .unwrap()
        .exec(
            r"SELECT sum(value) FROM t1 WHERE value = ? GROUP BY uid HAVING count(uid) = ?",
            (4, 2),
        )
        .await
        .unwrap();
    assert_eq!(&result, &[8]);

    // These parameters are not migrated - expect to fall back
    assert_eq!(
        last_statement_destination(adapter.as_mysql_conn().unwrap()).await,
        QueryDestination::ReadysetThenUpstream
    );

    // Again, we asynchronously migrate these params and eventually see the result against noria.
    assert!(
        query_until_expected_from_noria(
            &mut adapter,
            deployment.metrics(),
            QueryExecution::PrepareExecute(
                "SELECT sum(value) FROM t1 WHERE value = ? GROUP BY uid HAVING count(uid) = ?",
                (4, 2,)
            ),
            &EventuallyConsistentResults::empty_or(&[8]),
            PROPAGATION_DELAY_TIMEOUT,
        )
        .await
    );

    deployment.teardown().await.unwrap();
}

#[clustertest]
async fn show_query_metrics() {
    readyset_tracing::init_test_logging();
    let mut deployment = readyset_mysql("show_query_metrics")
        .standalone()
        .prometheus_metrics(true)
        .start()
        .await
        .unwrap();
    let mut adapter = deployment.first_adapter().await;

    adapter.query_drop("CREATE TABLE t (c INT)").await.unwrap();

    // Proxy a query
    adapter.query_drop("SELECT c FROM t").await.unwrap();
    // Run a query against ReadySet
    deployment.leader_handle().ready().await.unwrap();
    eventually! {
        adapter
            .query_drop("CREATE CACHE FROM SELECT c FROM t WHERE c = ?")
            .await
            .is_ok()
    }

    eventually! {
        adapter.query_drop("SELECT c FROM t where c = 1").await.unwrap();
        last_statement_destination(adapter.as_mysql_conn().unwrap()).await == QueryDestination::Readyset
    }

    // Check `SHOW PROXIED QUERIES`
    let proxied_result: Vec<(String, String, String, String, String, String, String)> = adapter
        .as_mysql_conn()
        .unwrap()
        .query(r"SHOW PROXIED QUERIES")
        .await
        .unwrap();

    // Assert that we get a non-zero value for the metrics
    assert!(&proxied_result[0].3 != "0");
    assert!(&proxied_result[0].4 != "0.0");
    assert!(&proxied_result[0].5 != "0.0");
    assert!(&proxied_result[0].6 != "0.0");

    // Check `SHOW CACHES`
    #[allow(clippy::type_complexity)]
    let caches_result: Vec<(
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    )> = adapter
        .as_mysql_conn()
        .unwrap()
        .query(r"SHOW CACHES")
        .await
        .unwrap();

    // Assert that we get a non-zero value for the metrics
    assert!(&caches_result[0].3 != "0");
    assert!(&caches_result[0].4 != "0.0");
    assert!(&caches_result[0].5 != "0.0");
    assert!(&caches_result[0].6 != "0.0");
}
