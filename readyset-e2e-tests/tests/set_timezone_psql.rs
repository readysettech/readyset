use assert_matches::assert_matches;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::UnsupportedSetMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio_postgres::{Config, SimpleQueryMessage};

async fn setup_with(backend_builder: BackendBuilder) -> (Config, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    TestBuilder::new(backend_builder)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

/// Regression: `SET TimeZone = 'etc/utc'` is a UTC-equivalent value and must be treated as
/// supported; otherwise default `UnsupportedSetMode::Proxy` flips the session to `ProxyAlways`.
/// The case-insensitive `is_utc()` allowlist is exactly what this exercises.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn lowercase_iana_utc_alias_stays_on_readyset() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Proxy),
    )
    .await;
    let conn = psql_helpers::connect(opts).await;

    conn.simple_query("CREATE TABLE t (x int)").await.unwrap();
    conn.simple_query("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    eventually! {
        conn.simple_query("CREATE CACHE FROM SELECT x FROM t")
            .await
            .is_ok()
    };

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "cached query should be served by Readyset before the SET"
    );

    conn.simple_query("SET TimeZone = 'etc/utc'").await.unwrap();

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "lowercase IANA UTC alias should be accepted, leaving the session on Readyset"
    );

    shutdown_tx.shutdown().await;
}

/// Companion to `lowercase_iana_utc_alias_stays_on_readyset`: a valid non-UTC named zone that
/// PostgreSQL accepts upstream must still flip the session to `ProxyAlways` under
/// `UnsupportedSetMode::Proxy`. This is the central correctness claim of the `is_utc()` gate —
/// without it, cached UTC-stamped results would be served to a session that thinks it's
/// `America/New_York`.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn non_utc_timezone_flips_session_to_proxy_always() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Proxy),
    )
    .await;
    let conn = psql_helpers::connect(opts).await;

    conn.simple_query("CREATE TABLE t (x int)").await.unwrap();
    conn.simple_query("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    eventually! {
        conn.simple_query("CREATE CACHE FROM SELECT x FROM t")
            .await
            .is_ok()
    };

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "cached query should be served by Readyset before the SET"
    );

    // PostgreSQL accepts `America/New_York` upstream; Readyset must still flag it unsupported
    // and proxy subsequent queries.
    conn.simple_query("SET TimeZone = 'America/New_York'")
        .await
        .unwrap();

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Upstream,
        "non-UTC SET must flip the session to ProxyAlways"
    );

    shutdown_tx.shutdown().await;
}

/// Under `UnsupportedSetMode::Allow` a non-UTC `SET TimeZone` must keep the
/// session on Readyset *and* must not localize cached TIMESTAMPTZ values:
/// the follow-up read returns the same UTC wallclock as the baseline.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn non_utc_timezone_under_allow_mode_does_not_localize_cache() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Allow),
    )
    .await;
    let conn = psql_helpers::connect(opts).await;

    conn.simple_query("CREATE TABLE t (ts timestamptz)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO t (ts) VALUES ('2024-06-15 12:00:00+00')")
        .await
        .unwrap();

    eventually! {
        conn.simple_query("CREATE CACHE FROM SELECT ts FROM t")
            .await
            .is_ok()
    };

    // Baseline: UTC session, value comes back as UTC wallclock.
    let rows = conn.simple_query("SELECT ts FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "baseline read should hit Readyset"
    );
    let baseline = first_row_col(&rows, 0);

    // Non-UTC SET. Under Allow mode the backend does *not* flip the
    // session, so the next read must still hit Readyset.
    conn.simple_query("SET TimeZone = 'America/New_York'")
        .await
        .unwrap();

    let rows = conn.simple_query("SELECT ts FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "Allow mode must not flip the session even on non-UTC SET"
    );
    let after_set = first_row_col(&rows, 0);

    assert_eq!(
        baseline, after_set,
        "non-UTC SET under Allow mode must not localize cached TIMESTAMPTZ values \
         (baseline {baseline:?}, after-set {after_set:?})"
    );

    shutdown_tx.shutdown().await;
}

/// `RESET TimeZone` must not flip the session to ProxyAlways: subsequent
/// cached SELECTs must still hit Readyset.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn reset_time_zone_does_not_poison_session() {
    let (opts, _handle, shutdown_tx) = setup_with(
        BackendBuilder::new()
            .require_authentication(false)
            .unsupported_set_mode(UnsupportedSetMode::Proxy),
    )
    .await;
    let conn = psql_helpers::connect(opts).await;

    conn.simple_query("CREATE TABLE t (x int)").await.unwrap();
    conn.simple_query("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    eventually! {
        conn.simple_query("CREATE CACHE FROM SELECT x FROM t")
            .await
            .is_ok()
    };

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "cached query should be served by Readyset before the RESET"
    );

    // Upstream PG accepts `RESET TimeZone` and resets the parameter to
    // its config default. Readyset's parser may reject it (which sends
    // the statement upstream as a fallthrough). Either way the operation
    // must not corrupt session state.
    let _ = conn.simple_query("RESET TimeZone").await;

    conn.simple_query("SELECT x FROM t").await.unwrap();
    assert_matches!(
        last_query_info(&conn).await.destination,
        QueryDestination::Readyset(_),
        "RESET TimeZone must not flip the session to ProxyAlways"
    );

    shutdown_tx.shutdown().await;
}

fn first_row_col(rows: &[SimpleQueryMessage], col: usize) -> String {
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg {
            return row.get(col).expect("column should exist").to_string();
        }
    }
    panic!("no row in result: {rows:?}");
}
