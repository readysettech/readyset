use std::collections::HashMap;

use tokio::test;

use readyset_adapter::BackendBuilder;
use readyset_client::CacheMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_tracing::init_test_logging;
use test_utils::{tags, upstream};

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn user_default_schema_is_used() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(&test_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(
            "DROP ROLE IF EXISTS alice;
             DROP ROLE IF EXISTS bob;
             CREATE ROLE alice LOGIN PASSWORD 'pass';
             CREATE ROLE bob LOGIN PASSWORD 'pass';
             CREATE SCHEMA alice_schema;
             CREATE SCHEMA bob_schema;
             GRANT USAGE ON SCHEMA alice_schema TO alice;
             GRANT USAGE ON SCHEMA bob_schema TO bob;
             ALTER ROLE alice SET search_path = alice_schema;
             ALTER ROLE bob SET search_path = bob_schema;
             CREATE TABLE alice_schema.foo (name text);
             CREATE TABLE bob_schema.foo (name text);
             INSERT INTO alice_schema.foo VALUES ('alice');
             INSERT INTO bob_schema.foo VALUES ('bob');
             GRANT SELECT ON alice_schema.foo TO alice;
             GRANT SELECT ON bob_schema.foo TO bob;",
        )
        .await
        .unwrap();

    let mut users = HashMap::new();
    users.insert("alice".to_string(), "pass".to_string());
    users.insert("bob".to_string(), "pass".to_string());
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .cache_mode(CacheMode::Shallow)
            .users(users),
    )
    .fallback(true)
    .replicate_db(&test_name)
    .recreate_database(false)
    .build::<PostgreSQLAdapter>()
    .await;

    let mut alice_cfg = rs_opts.clone();
    alice_cfg.dbname(&test_name);
    alice_cfg.user("alice").password(b"pass");
    let alice = psql_helpers::connect(alice_cfg).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT name FROM foo")
        .await
        .unwrap();

    let name: String = alice
        .query_one("SELECT name FROM foo", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "alice");
    assert_eq!(
        last_query_info(&alice).await.destination,
        QueryDestination::Upstream,
    );

    let name: String = alice
        .query_one("SELECT name FROM foo", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "alice");
    assert_eq!(
        last_query_info(&alice).await.destination,
        QueryDestination::ReadysetShallow,
    );

    let mut bob_cfg = rs_opts.clone();
    bob_cfg.dbname(&test_name);
    bob_cfg.user("bob").password(b"pass");
    let bob = psql_helpers::connect(bob_cfg).await;
    bob.simple_query("CREATE SHALLOW CACHE FROM SELECT name FROM foo")
        .await
        .unwrap();

    let name: String = bob
        .query_one("SELECT name FROM foo", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "bob");
    assert_eq!(
        last_query_info(&bob).await.destination,
        QueryDestination::Upstream,
    );

    let name: String = bob
        .query_one("SELECT name FROM foo", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "bob");
    assert_eq!(
        last_query_info(&bob).await.destination,
        QueryDestination::ReadysetShallow,
    );

    shutdown_tx.shutdown().await;
}
