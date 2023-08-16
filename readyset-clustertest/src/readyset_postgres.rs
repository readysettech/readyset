use database_utils::{QueryableConnection, SimpleQueryResults};
use readyset_client_metrics::QueryDestination;
use readyset_data::DfValue;
use readyset_util::eventually;
use serial_test::serial;

use crate::*;

fn readyset_postgres(name: &str) -> DeploymentBuilder {
    DeploymentBuilder::new(DatabaseType::PostgreSQL, name)
        .standalone()
        .deploy_upstream()
        .deploy_adapter()
}

fn readyset_postgres_cleanup(name: &str) -> DeploymentBuilder {
    DeploymentBuilder::new(DatabaseType::PostgreSQL, name)
        .standalone()
        .cleanup()
        .deploy_upstream()
        .deploy_adapter()
}

async fn last_statement_destination(conn: &mut DatabaseConnection) -> QueryDestination {
    match conn.simple_query("EXPLAIN LAST STATEMENT").await.unwrap() {
        SimpleQueryResults::MySql(_) => panic!("MySQL connection in pg test"),
        SimpleQueryResults::Postgres(res) => {
            let row = res.into_iter().next().unwrap();
            let dest = row.get("Query_destination").unwrap();
            dest.try_into().unwrap()
        }
    }
}

async fn replication_slot_exists(conn: &mut DatabaseConnection) -> bool {
    const QUERY: &str = "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'readyset'";
    if let Ok(row) = match conn {
        DatabaseConnection::MySQL(_) => return false,
        DatabaseConnection::PostgreSQL(client, _) => client.query_one(QUERY, &[]).await,
        DatabaseConnection::PostgreSQLPool(client) => client.query_one(QUERY, &[]).await,
    } {
        let value: &str = row.get(0);
        value == "readyset"
    } else {
        false
    }
}

async fn publication_exists(conn: &mut DatabaseConnection) -> bool {
    const QUERY: &str = "SELECT pubname FROM pg_publication WHERE pubname = 'readyset'";
    if let Ok(row) = match conn {
        DatabaseConnection::MySQL(_) => return false,
        DatabaseConnection::PostgreSQL(client, _) => client.query_one(QUERY, &[]).await,
        DatabaseConnection::PostgreSQLPool(client) => client.query_one(QUERY, &[]).await,
    } {
        let value: &str = row.get(0);
        value == "readyset"
    } else {
        false
    }
}

#[clustertest]
async fn cleanup_works() {
    let deployment_name = "ct_cleanup_works";
    let mut deployment = readyset_postgres(deployment_name).start().await.unwrap();

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

    // TODO: Refactor query_until_expected to support postgres. For now this is a naive way to wait
    // for replication.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut upstream = deployment.upstream().await;

    deployment.teardown().await.unwrap();

    // At this point deployment related assets should still exist. Let's check for them.
    if let DatabaseConnection::PostgreSQL(_, _) = upstream {
        assert!(replication_slot_exists(&mut upstream).await);
        assert!(publication_exists(&mut upstream).await);
    }

    // Start up in cleanup mode.
    let mut deployment = readyset_postgres_cleanup(deployment_name)
        .start_without_waiting()
        .await
        .unwrap();

    let mut upstream = deployment.upstream().await;

    // Wait for adapters to die naturally, which should happen when cleanup finishes.
    deployment.wait_for_adapter_death().await;

    assert!(!replication_slot_exists(&mut upstream).await);
    assert!(!publication_exists(&mut upstream).await);
}

/// Test that a deployment with embedded readers, configured to replicate readers n times, continues
/// to work with a number of adapters < n.
#[clustertest]
async fn embedded_readers_adapters_lt_replicas() {
    let deployment_name = "embedded_readers_adapters_lt_replicas";
    let mut deployment = DeploymentBuilder::new(DatabaseType::PostgreSQL, deployment_name)
        .deploy_upstream()
        .reader_replicas(2)
        .with_adapters(1)
        .with_servers(1, ServerParams::default().no_readers())
        .embedded_readers(true)
        .allow_full_materialization()
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

    adapter.query_drop("CREATE TABLE t (x int);").await.unwrap();
    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    eventually! {
        adapter
            .query_drop("CREATE CACHE FROM SELECT x FROM t;")
            .await
            .is_ok()
    }

    eventually! {
        adapter
            .query_drop("CREATE CACHE FROM SELECT count(*) FROM t WHERE x = $1;")
            .await
            .is_ok()
    }

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t;")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()]]);
        }
    }

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .execute(&"SELECT count(*) FROM t WHERE x = $1;", [DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()]]);
        }
    }

    adapter
        .query_drop("INSERT INTO t (x) VALUES (2);")
        .await
        .unwrap();

    eventually! {
        run_test: {
            let mut res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t;")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            res.sort();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()], vec![2.into()]]);
        }
    }

    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .execute(&"SELECT count(*) FROM t WHERE x = $1;", [DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![2.into()]]);
        }
    }

    // Spin up a second adapter - it should join the cluster and start serving reads

    deployment.start_adapter(true).await.unwrap();
    let mut second_adapter = deployment.adapter(1).await;

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = second_adapter
                .execute(&"SELECT count(*) FROM t WHERE x = $1;", [DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut second_adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![2.into()]]);
        }
    }

    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    // *both* adapters should now receive writes

    eventually! {
        run_test: {
            let res1: Vec<Vec<DfValue>> = adapter
                .execute(&"SELECT count(*) FROM t WHERE x = $1;", [DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let res2: Vec<Vec<DfValue>> = second_adapter
                .execute(&"SELECT count(*) FROM t WHERE x = $1;", [DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            (res1, res2)
        },
        then_assert: |(res1, res2)| {
            assert_eq!(res1, vec![vec![3.into()]]);
            assert_eq!(res2, vec![vec![3.into()]]);
        }
    }
    deployment.teardown().await.unwrap();
}

/// Test that a (partial or full) reader domain panicking in a deployment eventually recovers
#[clustertest]
async fn reader_domain_panic_handling() {
    let deployment_name = "reader_domain_panic_handling";
    let mut deployment = DeploymentBuilder::new(DatabaseType::PostgreSQL, deployment_name)
        .deploy_upstream()
        .reader_replicas(1)
        .with_adapters(1)
        .with_servers(1, ServerParams::default().no_readers())
        .embedded_readers(true)
        .allow_full_materialization()
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

    adapter.query_drop("CREATE TABLE t (x int);").await.unwrap();
    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    // 1. full readers

    eventually! {
        adapter
            .query_drop("CREATE CACHE FROM SELECT x FROM t;")
            .await
            .is_ok()
    }

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()]]);
        }
    }

    // Force the reader domain to panic *once* by setting a failpoint then sending it a write
    deployment
        .adapter_handle(0)
        .unwrap()
        .set_failpoint("handle-packet", "1*panic->off")
        .await;

    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()], vec![1.into()]]);
        }
    }

    // 2. partial readers

    adapter
        .query_drop("CREATE CACHE FROM SELECT count(*) FROM t where x = ?;")
        .await
        .unwrap();

    // Force the reader domain to panic *once* by setting a failpoint then sending a replay request
    deployment
        .adapter_handle(0)
        .unwrap()
        .set_failpoint("handle-packet", "1*panic->off")
        .await;

    adapter
        .query_drop("select count(*) from t where x = 1")
        .await
        .unwrap();

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .execute(&"select count(*) from t where x = $1", &[DfValue::from(1i32)])
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![2.into()]]);
        }
    }

    deployment.teardown().await.unwrap();
}

/// Test that a base table domain panicking in a deployment eventually recovers
#[clustertest]
async fn base_domain_panic_handling() {
    let deployment_name = "base_domain_panic_handling";
    let mut deployment = DeploymentBuilder::new(DatabaseType::PostgreSQL, deployment_name)
        .deploy_upstream()
        .reader_replicas(1)
        .with_adapters(1)
        .with_servers(1, ServerParams::default().no_readers())
        .embedded_readers(true)
        .allow_full_materialization()
        .start()
        .await
        .unwrap();

    let mut adapter = deployment.first_adapter().await;

    adapter.query_drop("CREATE TABLE t (x int);").await.unwrap();
    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    eventually! {
        adapter
            .query_drop("CREATE CACHE FROM SELECT x FROM t;")
            .await
            .is_ok()
    }

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()]]);
        }
    }

    // Force the base table domain to panic *once* by setting a failpoint then sending it a write
    let controller_uri = deployment.handle.controller_uri().await.unwrap();
    let server_handle = deployment.server_handle(&controller_uri).unwrap();
    server_handle
        .set_failpoint("handle-packet", "1*panic->off")
        .await;

    adapter
        .query_drop("INSERT INTO t (x) VALUES (1);")
        .await
        .unwrap();

    eventually! {
        run_test: {
            let res: Vec<Vec<DfValue>> = adapter
                .query("SELECT x FROM t")
                .await
                .unwrap()
                .try_into()
                .unwrap();
            let dest = last_statement_destination(&mut adapter).await;
            (res, dest)
        },
        then_assert: |(res, dest)| {
            assert_eq!(dest, QueryDestination::Readyset);
            assert_eq!(res, vec![vec![1.into()], vec![1.into()]]);
        }
    }

    deployment.teardown().await.unwrap();
}
