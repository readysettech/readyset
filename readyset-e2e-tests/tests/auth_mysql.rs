use std::collections::HashMap;

use mysql_async::prelude::Queryable;
use mysql_srv::{AuthPlugin, CachingSha2Password};
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
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
            .users(users),
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
        auth_test_builder(AuthPlugin::default())
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

/// Connect twice through the same Readyset instance using
/// `caching_sha2_password`. The first connection primes the auth cache via
/// full-auth; the second exercises the fast-auth path on cache hit.
///
/// Uses failpoints to fail the test if the wrong auth path is taken for a
/// given connection.
#[cfg(feature = "failure_injection")]
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn e2e_sha2_fast_auth_after_full_auth() {
    use fail::FailScenario;
    use readyset_util::failpoints;

    readyset_tracing::init_test_logging();
    let _scenario = FailScenario::setup();

    let (rs_opts, _handle, shutdown_tx): (_, _, ShutdownSender) =
        auth_test_builder(AuthPlugin::Sha2(CachingSha2Password))
            .build::<MySQLAdapter>()
            .await;
    let opts = with_auth(rs_opts);

    // First connection: the cache is empty, so we must take the full-auth
    // path. Panic if the fast-auth branch is ever reached.
    fail::cfg(failpoints::CACHING_SHA2_FAST_AUTH_SUCCESS, "panic")
        .expect("failed to configure fast-auth failpoint");
    let mut first = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let row: Option<(u32,)> = first.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));
    first.disconnect().await.unwrap();
    fail::remove(failpoints::CACHING_SHA2_FAST_AUTH_SUCCESS);

    // Second connection: the cache is primed, so we must take the fast-auth
    // path. Panic if the full-auth branch is ever reached.
    fail::cfg(failpoints::CACHING_SHA2_FULL_AUTH_BEGIN, "panic")
        .expect("failed to configure full-auth failpoint");
    let mut second = mysql_async::Conn::new(opts).await.unwrap();
    let row: Option<(u32,)> = second.query_first("SELECT 1").await.unwrap();
    assert_eq!(row, Some((1,)));
    second.disconnect().await.unwrap();
    fail::remove(failpoints::CACHING_SHA2_FULL_AUTH_BEGIN);

    shutdown_tx.shutdown().await;
}
