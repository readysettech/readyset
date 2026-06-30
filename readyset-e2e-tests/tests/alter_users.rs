//! End-to-end coverage for runtime allowed-user management:
//! `ALTER READYSET ADD/MODIFY/DROP USER` and the `readyset.users` vrel (REA-6702).

use std::collections::HashMap;
use std::sync::Arc;

use database_utils::UpstreamConfig;
use mysql_async::prelude::Queryable;
use mysql_srv::{AuthPlugin, CachingSha2Password, MysqlNativePassword};
use readyset_adapter::backend::AllowedUsers;
use readyset_adapter::BackendBuilder;
use readyset_client::consensus::{
    Authority, AuthorityControl, LocalAuthority, LocalAuthorityStore, UserStore,
};
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio::sync::RwLock;

/// The bootstrap user, which is also the `--upstream-db-url` user in the MySQL test harness.
const ROOT_USER: &str = "root";
const ROOT_PASSWORD: &str = "noria";

fn empty_authority() -> Arc<Authority> {
    Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))))
}

/// Build an authenticated Readyset proxy backed by `authority`, with the given allowed-users set.
/// The returned [`Handle`] must be kept alive for the duration of the test: dropping it shuts
/// down the controller.
async fn proxy_with_users(
    auth_plugin: AuthPlugin,
    authority: Arc<Authority>,
    users: HashMap<String, String>,
) -> (mysql_async::Opts, Handle, ShutdownSender) {
    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .authority(authority)
    .auth_plugin(auth_plugin)
    .fallback(true)
    .build::<MySQLAdapter>()
    .await;

    (rs_opts, handle, shutdown_tx)
}

/// Build an authenticated Readyset proxy backed by `authority`, seeded with just the root user.
async fn alter_users_proxy(
    auth_plugin: AuthPlugin,
    authority: Arc<Authority>,
) -> (mysql_async::Opts, Handle, ShutdownSender) {
    let users = HashMap::from([(ROOT_USER.to_string(), ROOT_PASSWORD.to_string())]);
    proxy_with_users(auth_plugin, authority, users).await
}

fn opts_for(rs_opts: &mysql_async::Opts, user: &str, password: &str) -> mysql_async::Opts {
    mysql_async::OptsBuilder::from_opts(rs_opts.clone())
        .user(Some(user))
        .pass(Some(password))
        .into()
}

async fn connect_root(rs_opts: &mysql_async::Opts) -> mysql_async::Conn {
    mysql_async::Conn::new(opts_for(rs_opts, ROOT_USER, ROOT_PASSWORD))
        .await
        .unwrap()
}

/// Create (or rotate the password of) `user` in the upstream MySQL so the adapter's per-user
/// fallback connection for that client can authenticate. The proxy authenticates the client
/// against its own allowed-users set (driven by `ALTER READYSET`), but then passes the client
/// credentials through to open the upstream connection, so the user must also exist upstream.
async fn set_upstream_user(user: &str, password: &str) {
    let mut upstream = mysql_async::Conn::new(mysql_helpers::upstream_config())
        .await
        .unwrap();
    upstream
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await
        .unwrap();
    upstream
        .query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'"))
        .await
        .unwrap();
    upstream
        .query_drop(format!("GRANT ALL PRIVILEGES ON *.* TO '{user}'@'%'"))
        .await
        .unwrap();
}

/// A user added at runtime can immediately authenticate, under both MySQL auth plugins.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_add_user_then_connect() {
    readyset_tracing::init_test_logging();
    set_upstream_user("alice", "secret").await;
    for plugin in [
        AuthPlugin::Native(MysqlNativePassword),
        AuthPlugin::Sha2(CachingSha2Password),
    ] {
        let (rs_opts, _handle, shutdown_tx) = alter_users_proxy(plugin, empty_authority()).await;

        let mut root = connect_root(&rs_opts).await;
        root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
            .await
            .unwrap();

        let mut alice = mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
            .await
            .expect("alice should be able to connect after ADD USER");
        let row: Option<(u32,)> = alice.query_first("SELECT 1").await.unwrap();
        assert_eq!(row, Some((1,)));

        shutdown_tx.shutdown().await;
    }
}

/// MODIFY rotates the password: the old one is rejected and the new one accepted.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_modify_user_password() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) =
        alter_users_proxy(AuthPlugin::Sha2(CachingSha2Password), empty_authority()).await;
    set_upstream_user("alice", "secret").await;

    let mut root = connect_root(&rs_opts).await;
    root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
        .await
        .unwrap();
    // Authenticate once with the initial password so the caching_sha2_password fast-auth digest
    // is cached. MODIFY must replace that digest, not leave the old password able to fast-auth.
    mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
        .await
        .expect("alice connects with the initial password");
    root.query_drop("ALTER READYSET MODIFY USER 'alice' PASSWORD 'newsecret'")
        .await
        .unwrap();
    // The proxy passes client credentials through to the upstream, so rotate alice's upstream
    // password to match the new one before reconnecting with it.
    set_upstream_user("alice", "newsecret").await;

    assert!(
        mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
            .await
            .is_err(),
        "the old password must be rejected after MODIFY"
    );
    let mut alice = mysql_async::Conn::new(opts_for(&rs_opts, "alice", "newsecret"))
        .await
        .expect("the new password must be accepted after MODIFY");
    let row: Option<(u32,)> = alice.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));

    shutdown_tx.shutdown().await;
}

/// DROP removes the user: subsequent connection attempts are rejected.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_drop_user_blocks_connect() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) =
        alter_users_proxy(AuthPlugin::Sha2(CachingSha2Password), empty_authority()).await;
    set_upstream_user("alice", "secret").await;

    let mut root = connect_root(&rs_opts).await;
    root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
        .await
        .unwrap();
    // Confirm alice works before the drop. This also caches her fast-auth digest, so the
    // post-drop attempt below exercises DROP clearing the digest, not just the slow-auth path.
    mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
        .await
        .unwrap();

    root.query_drop("ALTER READYSET DROP USER 'alice'")
        .await
        .unwrap();
    assert!(
        mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
            .await
            .is_err(),
        "alice must not be able to connect after DROP USER"
    );

    shutdown_tx.shutdown().await;
}

/// `SELECT * FROM readyset.users` lists the allowed usernames (and never a password column).
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_readyset_users_vrel() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) =
        alter_users_proxy(AuthPlugin::Sha2(CachingSha2Password), empty_authority()).await;

    let mut root = connect_root(&rs_opts).await;
    root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
        .await
        .unwrap();

    let mut names: Vec<String> = root
        .query("SELECT user FROM readyset.users")
        .await
        .unwrap();
    names.sort();
    assert_eq!(names, vec!["alice".to_string(), ROOT_USER.to_string()]);

    shutdown_tx.shutdown().await;
}

/// A runtime mutation is persisted to the Authority under the `allowed_users` key.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_add_user_persists_to_authority() {
    readyset_tracing::init_test_logging();
    let authority = empty_authority();
    let (rs_opts, _handle, shutdown_tx) =
        alter_users_proxy(AuthPlugin::Sha2(CachingSha2Password), authority.clone()).await;

    // No key until the first mutation.
    assert!(authority.load_allowed_users().await.unwrap().is_none());

    let mut root = connect_root(&rs_opts).await;
    root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
        .await
        .unwrap();

    // The persisted set seeds the bootstrap users (root) and includes the new user.
    let persisted = authority
        .load_allowed_users()
        .await
        .unwrap()
        .expect("the allowed_users key should exist after the first mutation");
    assert_eq!(persisted.get("alice").map(String::as_str), Some("secret"));
    assert_eq!(persisted.get(ROOT_USER).map(String::as_str), Some(ROOT_PASSWORD));

    shutdown_tx.shutdown().await;
}

/// DROP and MODIFY of the `--upstream-db-url` user are rejected, since that identity backs
/// upstream connectivity and cannot be re-derived.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_cannot_mutate_upstream_user() {
    readyset_tracing::init_test_logging();

    let mut users = HashMap::new();
    users.insert(ROOT_USER.to_string(), ROOT_PASSWORD.to_string());
    let upstream_config = UpstreamConfig::from_url(MySQLAdapter::url_with_db("noria"));
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .upstream_config(Some(Arc::new(RwLock::new(upstream_config))))
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .authority(empty_authority())
    .auth_plugin(AuthPlugin::Sha2(CachingSha2Password))
    .fallback(true)
    .build::<MySQLAdapter>()
    .await;

    let mut root = connect_root(&rs_opts).await;
    assert!(
        root.query_drop(format!("ALTER READYSET DROP USER '{ROOT_USER}'"))
            .await
            .is_err(),
        "dropping the upstream-URL user must be rejected"
    );
    assert!(
        root.query_drop(format!(
            "ALTER READYSET MODIFY USER '{ROOT_USER}' PASSWORD 'rotated'"
        ))
        .await
        .is_err(),
        "modifying the upstream-URL user must be rejected"
    );

    shutdown_tx.shutdown().await;
}

/// A user added at runtime survives an adapter restart: the Authority is the source of truth, so
/// a fresh proxy that resolves its allowed-users set from the same Authority still authenticates
/// the added user without it being re-added.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_added_user_persists_across_restart() {
    readyset_tracing::init_test_logging();
    set_upstream_user("alice", "secret").await;
    let bootstrap = HashMap::from([(ROOT_USER.to_string(), ROOT_PASSWORD.to_string())]);

    // The `LocalAuthorityStore` is the durable state that survives the restart. Each boot gets its
    // own `Authority` over that shared store, mirroring a real restart that re-reads persisted
    // state: reusing one `Authority` instance would keep the first server's leader lease and stall
    // the second server's controller startup.
    let store = Arc::new(LocalAuthorityStore::new());
    let authority = || Arc::new(Authority::from(LocalAuthority::new_with_store(store.clone())));

    // First boot: resolve the allowed-users set from the Authority, mirroring how the adapter
    // binary seeds the `allowed_users` key on startup.
    let first = authority();
    let resolved = first
        .load_or_init_allowed_users(bootstrap.clone())
        .await
        .unwrap();
    let (rs_opts, handle, shutdown_tx) =
        proxy_with_users(AuthPlugin::Sha2(CachingSha2Password), first, resolved).await;

    let mut root = connect_root(&rs_opts).await;
    root.query_drop("ALTER READYSET ADD USER 'alice' PASSWORD 'secret'")
        .await
        .unwrap();
    // Tear the first boot down before the second one starts, releasing its references to the
    // shared authority.
    drop(root);
    shutdown_tx.shutdown().await;
    drop(handle);

    // Second boot: a fresh `Authority` over the same durable store re-resolves the persisted set,
    // which is now authoritative and already contains alice, so the bootstrap is ignored.
    let second = authority();
    // The first server registered its worker under an ephemeral key that clears when its last
    // `Arc<Authority>` drops, which completes asynchronously after shutdown. Wait for it to clear
    // so the second server can register its own worker at the same address.
    while !second.get_workers().await.unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    let resolved = second.load_or_init_allowed_users(bootstrap).await.unwrap();
    assert_eq!(resolved.get("alice").map(String::as_str), Some("secret"));
    let (rs_opts, _handle, shutdown_tx) =
        proxy_with_users(AuthPlugin::Sha2(CachingSha2Password), second, resolved).await;

    let mut alice = mysql_async::Conn::new(opts_for(&rs_opts, "alice", "secret"))
        .await
        .expect("alice should authenticate after a restart");
    let row: Option<(u32,)> = alice.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));

    shutdown_tx.shutdown().await;
}
