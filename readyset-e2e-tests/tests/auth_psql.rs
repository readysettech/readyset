use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::test;
use tokio_postgres::Client;

use readyset_adapter::backend::AllowedUsers;
use readyset_adapter::BackendBuilder;
use readyset_client::CacheMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Read `query` until Readyset serves it from the shallow cache, returning the
/// value that serve produced. With authentication on, the cache ACL resolves
/// each (identity, cache) pair on the freshness worker, so a user's own reads
/// proxy until their probe lands.
async fn eventually_shallow(conn: &Client, query: &str) -> String {
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "query was never served from the shallow cache",
        {
            conn.query_one(query, &[]).await.unwrap();
            matches!(
                last_query_info(conn).await.destination,
                QueryDestination::ReadysetShallow(_)
            )
        }
    );
    conn.query_one(query, &[]).await.unwrap().get(0)
}

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
            .users(Arc::new(AllowedUsers::new(users, None))),
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

    // Only the converged serve asserts a destination: a first read proxies either
    // because the cache ACL has yet to resolve this user or because the cache is
    // still empty, and which one it is depends on when the probe lands.
    let name: String = alice
        .query_one("SELECT name FROM foo", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, "alice");

    assert_eq!(
        eventually_shallow(&alice, "SELECT name FROM foo").await,
        "alice"
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
        eventually_shallow(&bob, "SELECT name FROM foo").await,
        "bob"
    );

    shutdown_tx.shutdown().await;
}
