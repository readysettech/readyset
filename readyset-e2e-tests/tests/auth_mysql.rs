use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_srv::{AuthPlugin, CachingSha2Password, MysqlNativePassword};
use readyset_adapter::backend::AllowedUsers;
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::shutdown::ShutdownSender;
use rsa::RsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use test_utils::{tags, upstream};

const TEST_USER: &str = "root";
const TEST_PASSWORD: &str = "noria";

/// Build a [`TestBuilder`] with authentication enabled.
///
/// The returned Readyset proxy requires [`TEST_USER`]/[`TEST_PASSWORD`]
/// credentials and uses the given [`AuthPlugin`] for the server-side handshake.
fn auth_test_builder(auth_plugin: AuthPlugin) -> TestBuilder {
    let mut users = HashMap::new();
    users.insert(TEST_USER.to_string(), TEST_PASSWORD.to_string());

    TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .auth_plugin(auth_plugin)
    .fallback(true)
}

/// Attach [`TEST_USER`]/[`TEST_PASSWORD`] credentials to a set of client
/// connection options returned by [`TestBuilder::build`].
fn with_auth(rs_opts: mysql_async::Opts) -> mysql_async::Opts {
    mysql_async::OptsBuilder::from_opts(rs_opts)
        .user(Some(TEST_USER))
        .pass(Some(TEST_PASSWORD))
        .into()
}

/// Connect to Readyset with `caching_sha2_password` authentication and verify
/// that a simple query succeeds.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_sha2_connect_via_readyset() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Sha2(CachingSha2Password))
            .build::<MySQLAdapter>()
            .await;

    let mut conn = mysql_async::Conn::new(with_auth(rs_opts)).await.unwrap();
    let row: Option<(u32,)> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));

    shutdown_tx.shutdown().await;
}

/// Connect to Readyset with `mysql_native_password` authentication and verify
/// that a simple query succeeds.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_native_connect_via_readyset() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Native(MysqlNativePassword))
            .build::<MySQLAdapter>()
            .await;

    let mut conn = mysql_async::Conn::new(with_auth(rs_opts)).await.unwrap();
    let row: Option<(u32,)> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));

    shutdown_tx.shutdown().await;
}

/// Attempting to connect through Readyset with an incorrect password must fail
/// with an access-denied error.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_sha2_wrong_password() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Sha2(CachingSha2Password))
            .build::<MySQLAdapter>()
            .await;

    let opts = mysql_async::OptsBuilder::from_opts(rs_opts)
        .user(Some(TEST_USER))
        .pass(Some("wrong_password"));
    let result = mysql_async::Conn::new(opts).await;
    assert!(
        result.is_err(),
        "expected connection with wrong password to be rejected"
    );

    shutdown_tx.shutdown().await;
}

/// Verify that `SHOW READYSET RSA PUBLIC KEY` returns a PEM-encoded
/// RSA public key.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_show_rsa_public_key() {
    readyset_tracing::init_test_logging();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Sha2(CachingSha2Password))
            .build::<MySQLAdapter>()
            .await;

    let mut conn = mysql_async::Conn::new(with_auth(rs_opts)).await.unwrap();
    let row: Option<(String,)> = conn
        .query_first("SHOW READYSET RSA PUBLIC KEY")
        .await
        .unwrap();
    let (pem,) = row.expect("expected one row with the RSA public key");
    RsaPublicKey::from_public_key_pem(&pem)
        .expect("SHOW READYSET RSA PUBLIC KEY should return a valid PEM-encoded RSA public key");

    shutdown_tx.shutdown().await;
}

/// Verify that startup-time auth-cache pre-population makes every
/// `caching_sha2_password` connection take the fast-auth path, so the
/// RSA-based full-auth branch is never reached.
#[cfg(feature = "failure_injection")]
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_sha2_prepopulated_cache_skips_full_auth() {
    use fail::FailScenario;
    use readyset_util::failpoints;

    readyset_tracing::init_test_logging();
    let _scenario = FailScenario::setup();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Sha2(CachingSha2Password))
            .build::<MySQLAdapter>()
            .await;
    let opts = with_auth(rs_opts);

    // Panic if any connection ever reaches the full-auth branch.
    fail::cfg(failpoints::CACHING_SHA2_FULL_AUTH_BEGIN, "panic")
        .expect("failed to configure full-auth failpoint");

    for _ in 0..2 {
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        let row: Option<(u32,)> = conn.query_first("SELECT 1").await.unwrap();
        assert_eq!(row, Some((1,)));
        conn.disconnect().await.unwrap();
    }

    fail::remove(failpoints::CACHING_SHA2_FULL_AUTH_BEGIN);
    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn authenticated_upstream_uses_client_user() {
    readyset_tracing::init_test_logging();
    let mut upstream = mysql_async::Conn::new(mysql_helpers::upstream_config())
        .await
        .unwrap();
    upstream
        .query_drop(
            "DROP USER IF EXISTS 'alice'@'%';
             CREATE USER 'alice'@'%' IDENTIFIED BY 'secret';
             GRANT ALL PRIVILEGES ON *.* TO 'alice'@'%';",
        )
        .await
        .unwrap();

    let users = HashMap::from([("alice".to_string(), "secret".to_string())]);
    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .auth_plugin(AuthPlugin::Sha2(CachingSha2Password))
    .fallback(true)
    .build::<MySQLAdapter>()
    .await;

    let mut conn = mysql_async::Conn::new(
        mysql_async::OptsBuilder::from_opts(rs_opts)
            .user(Some("alice"))
            .pass(Some("secret")),
    )
    .await
    .unwrap();
    let user: Option<(String,)> = conn.query_first("SELECT CURRENT_USER()").await.unwrap();
    assert!(user.unwrap().0.starts_with("alice@"));

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn unauthenticated_upstream_uses_configured_user() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        TestBuilder::new(BackendBuilder::new().require_authentication(false))
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;

    let mut conn =
        mysql_async::Conn::new(mysql_async::OptsBuilder::from_opts(rs_opts).user(Some("nobody")))
            .await
            .unwrap();
    let user: Option<(String,)> = conn.query_first("SELECT CURRENT_USER()").await.unwrap();
    assert!(user.unwrap().0.starts_with("root@"));

    shutdown_tx.shutdown().await;
}
