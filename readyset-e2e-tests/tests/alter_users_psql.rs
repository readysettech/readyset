//! End-to-end coverage for runtime allowed-user management on the PostgreSQL front door:
//! `ALTER READYSET ADD/MODIFY/DROP USER` and the `readyset.users` vrel (REA-6702).

use std::collections::HashMap;
use std::sync::Arc;

use readyset_adapter::backend::AllowedUsers;
use readyset_adapter::BackendBuilder;
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_server::Handle;
use readyset_tracing::init_test_logging;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio::test;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

/// The runtime-managed user under test. Postgres roles are cluster-wide, so the name is unique to
/// this file to avoid colliding with roles (and their dependent objects) left by other tests.
const TEST_USER: &str = "alter_users_alice";

fn empty_authority() -> Arc<Authority> {
    Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))))
}

/// The admin/bootstrap user, resolved from the same environment the upstream harness uses so it is
/// a valid upstream identity. It runs the `ALTER READYSET` statements under test.
fn admin_creds() -> (String, String) {
    (
        std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
        std::env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()),
    )
}

/// Create a LOGIN role upstream so the adapter's per-user fallback connection for that client
/// succeeds. The proxy's own allowed-users check is separate (driven by `ALTER READYSET`).
async fn create_upstream_role(test_name: &str, role: &str, password: &str) {
    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(&format!(
            "DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD '{password}';"
        ))
        .await
        .unwrap();
}

/// Build an authenticated Postgres proxy backed by `authority`, with the given allowed-users set.
/// The returned [`Handle`] must be kept alive for the duration of the test: dropping it shuts
/// down the controller.
async fn proxy_with_users(
    authority: Arc<Authority>,
    test_name: &str,
    users: HashMap<String, String>,
) -> (Config, Handle, ShutdownSender) {
    let (rs_config, handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .authority(authority)
    .fallback(true)
    .replicate_db(test_name)
    .recreate_database(false)
    .build::<PostgreSQLAdapter>()
    .await;

    (rs_config, handle, shutdown_tx)
}

fn client_config(rs_config: &Config, test_name: &str, user: &str, password: &str) -> Config {
    let mut cfg = rs_config.clone();
    cfg.dbname(test_name).user(user).password(password.as_bytes());
    cfg
}

/// A user added at runtime can immediately authenticate against the Postgres front door.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn e2e_add_user_then_connect() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    create_upstream_role(&test_name, TEST_USER, "secret").await;

    let (admin_user, admin_password) = admin_creds();
    let users = HashMap::from([(admin_user.clone(), admin_password.clone())]);
    let (rs_config, _handle, shutdown_tx) =
        proxy_with_users(empty_authority(), &test_name, users).await;

    let admin =
        psql_helpers::connect(client_config(&rs_config, &test_name, &admin_user, &admin_password))
            .await;
    admin
        .simple_query(&format!(
            "ALTER READYSET ADD USER '{TEST_USER}' PASSWORD 'secret'"
        ))
        .await
        .unwrap();

    let alice = client_config(&rs_config, &test_name, TEST_USER, "secret")
        .connect(NoTls)
        .await
        .map(|(client, connection)| {
            tokio::spawn(connection);
            client
        })
        .expect("the added user should be able to connect after ADD USER");
    let one: i32 = alice.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);

    shutdown_tx.shutdown().await;
}

/// DROP removes the user: subsequent connection attempts are rejected.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn e2e_drop_user_blocks_connect() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    create_upstream_role(&test_name, TEST_USER, "secret").await;

    let (admin_user, admin_password) = admin_creds();
    let users = HashMap::from([(admin_user.clone(), admin_password.clone())]);
    let (rs_config, _handle, shutdown_tx) =
        proxy_with_users(empty_authority(), &test_name, users).await;

    let admin =
        psql_helpers::connect(client_config(&rs_config, &test_name, &admin_user, &admin_password))
            .await;
    admin
        .simple_query(&format!(
            "ALTER READYSET ADD USER '{TEST_USER}' PASSWORD 'secret'"
        ))
        .await
        .unwrap();

    // Confirm the added user works before the drop.
    let _conn =
        psql_helpers::connect(client_config(&rs_config, &test_name, TEST_USER, "secret")).await;

    admin
        .simple_query(&format!("ALTER READYSET DROP USER '{TEST_USER}'"))
        .await
        .unwrap();
    assert!(
        client_config(&rs_config, &test_name, TEST_USER, "secret")
            .connect(NoTls)
            .await
            .is_err(),
        "the dropped user must not be able to connect after DROP USER"
    );

    shutdown_tx.shutdown().await;
}

/// `SELECT "user" FROM readyset.users` lists the allowed usernames (and never a password column).
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn e2e_readyset_users_vrel() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let (admin_user, admin_password) = admin_creds();
    let users = HashMap::from([(admin_user.clone(), admin_password.clone())]);
    let (rs_config, _handle, shutdown_tx) =
        proxy_with_users(empty_authority(), &test_name, users).await;

    let admin =
        psql_helpers::connect(client_config(&rs_config, &test_name, &admin_user, &admin_password))
            .await;
    admin
        .simple_query(&format!(
            "ALTER READYSET ADD USER '{TEST_USER}' PASSWORD 'secret'"
        ))
        .await
        .unwrap();

    // `user` is a reserved word in Postgres (it evaluates to CURRENT_USER), so the vrel column
    // must be quoted. The simple query protocol returns every column as text.
    let rows = admin
        .simple_query("SELECT \"user\" FROM readyset.users")
        .await
        .unwrap();
    let mut names: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    names.sort();
    assert_eq!(names, vec![TEST_USER.to_string(), admin_user]);

    shutdown_tx.shutdown().await;
}
