use assert_matches::assert_matches;
use mysql_async::Conn;
use mysql_async::prelude::*;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::UnsupportedSetMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{MySQLAdapter, last_query_info};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};

async fn setup_with(
    backend_builder: BackendBuilder,
) -> (mysql_async::Opts, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    TestBuilder::new(backend_builder)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await
}

/// Regression: `SET time_zone = 'us/eastern'` must be treated as supported; otherwise default
/// `UnsupportedSetMode::Proxy` flips the session to `ProxyAlways`.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn lowercase_iana_timezone_stays_on_readyset() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Proxy),
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t (x int)").await.unwrap();
    conn.query_drop("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT x FROM t")
            .await
            .is_ok()
    };

    conn.query_drop("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset(_),
        "cached query should be served by Readyset before the SET"
    );

    conn.query_drop("SET time_zone = 'us/eastern'")
        .await
        .unwrap();

    conn.query_drop("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset(_),
        "lowercase IANA timezone should be accepted, leaving the session on Readyset"
    );

    shutdown_tx.shutdown().await;
}

/// Companion to `lowercase_iana_timezone_stays_on_readyset`: a genuinely unknown zone must still
/// flip the session to `ProxyAlways` under `UnsupportedSetMode::Proxy`.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn unknown_timezone_flips_session_to_proxy_always() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Proxy),
    )
    .await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t (x int)").await.unwrap();
    conn.query_drop("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT x FROM t")
            .await
            .is_ok()
    };

    conn.query_drop("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset(_),
        "cached query should be served by Readyset before the SET"
    );

    // MySQL rejects unknown zones; we only care that Readyset marks the session unsupported.
    assert!(
        conn.query_drop("SET time_zone = 'Narnia/Cair_Paravel'")
            .await
            .is_err(),
        "upstream should reject the unknown zone",
    );

    conn.query_drop("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
        "unsupported SET must flip the session to ProxyAlways"
    );

    shutdown_tx.shutdown().await;
}
