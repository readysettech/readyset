use std::assert_matches;
use std::panic::AssertUnwindSafe;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender};
use readyset_server::Handle;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, Config, SimpleQueryMessage};

/// Build an adapter with fallback, an `events` table, and a shallow cache on `query`.
///
/// Row 1 is 10:00 US Central Standard Time and row 2 is 10:00 Australian Central Standard Time,
/// so `'2026-01-15 10:00 CST'` matches exactly one of them depending on `timezone_abbreviations`.
async fn setup(
    query: &str,
    options: &str,
) -> (Client, Client, Config, Handle, TestShutdownSender<PostgreSQLAdapter>) {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts.clone()).await;
    conn.simple_query(
        "CREATE TABLE events (id INT PRIMARY KEY, at TIMESTAMPTZ, duration INTERVAL)",
    )
    .await
    .unwrap();
    conn.simple_query(
        "INSERT INTO events (id, at, duration) VALUES \
         (1, '2026-01-15 16:00:00+00', '2 hours'), \
         (2, '2026-01-15 00:30:00+00', '1 day 2 hours')",
    )
    .await
    .unwrap();
    conn.simple_query(&format!("CREATE SHALLOW CACHE {options} FROM {query}"))
        .await
        .unwrap();
    let mut upstream_opts = psql_helpers::upstream_config();
    upstream_opts.dbname(opts.get_dbname().unwrap());
    let upstream_conn = psql_helpers::connect(upstream_opts).await;
    (conn, upstream_conn, opts, handle, shutdown_tx)
}

async fn assert_filled(conn: &Client) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
}

async fn assert_hit(conn: &Client) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );
}

/// Run `query` over the simple protocol and return its first column as wire text.
async fn simple_texts(conn: &Client, query: &str) -> Vec<String> {
    conn.simple_query(query)
        .await
        .unwrap()
        .iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .collect()
}

/// Run `query` over the simple protocol and return its first column as integers.
async fn simple_ids(conn: &Client, query: &str) -> Vec<i32> {
    simple_texts(conn, query)
        .await
        .iter()
        .map(|text| text.parse().unwrap())
        .collect()
}

/// Run `query` over the extended protocol and return its first column as integers.
async fn extended_ids(conn: &Client, query: &str) -> Vec<i32> {
    conn.query(query, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn timezone_abbreviations_partitions_entries() {
    const QUERY: &str = "SELECT id FROM events WHERE at = '2026-01-15 10:00 CST' ORDER BY id";
    let (a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    assert_eq!(simple_ids(&a, QUERY).await, vec![1]);
    assert_filled(&a).await;
    assert_eq!(simple_ids(&a, QUERY).await, vec![1]);
    assert_hit(&a).await;

    let b = psql_helpers::connect(opts).await;
    b.simple_query("SET timezone_abbreviations = 'Australia'")
        .await
        .unwrap();
    assert_eq!(simple_ids(&b, QUERY).await, vec![2]);
    assert_filled(&b).await;
    assert_eq!(simple_ids(&b, QUERY).await, vec![2]);
    assert_hit(&b).await;

    assert_eq!(simple_ids(&a, QUERY).await, vec![1]);
    assert_hit(&a).await;

    // Extended protocol entries are keyed the same way.
    assert_eq!(extended_ids(&b, QUERY).await, vec![2]);
    assert_filled(&b).await;
    assert_eq!(extended_ids(&a, QUERY).await, vec![1]);
    assert_filled(&a).await;
    assert_eq!(extended_ids(&b, QUERY).await, vec![2]);
    assert_hit(&b).await;

    b.simple_query("SET timezone_abbreviations = DEFAULT")
        .await
        .unwrap();
    assert_eq!(simple_ids(&b, QUERY).await, vec![1]);
    assert_hit(&b).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn intervalstyle_partitions_entries() {
    const QUERY: &str = "SELECT duration FROM events WHERE id = 2";
    let (a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    assert_eq!(simple_texts(&a, QUERY).await, vec!["1 day 02:00:00"]);
    assert_filled(&a).await;

    let b = psql_helpers::connect(opts).await;
    b.simple_query("SET intervalstyle = 'iso_8601'")
        .await
        .unwrap();
    assert_eq!(simple_texts(&b, QUERY).await, vec!["P1DT2H"]);
    assert_filled(&b).await;
    assert_eq!(simple_texts(&b, QUERY).await, vec!["P1DT2H"]);
    assert_hit(&b).await;

    assert_eq!(simple_texts(&a, QUERY).await, vec!["1 day 02:00:00"]);
    assert_hit(&a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn intervalstyle_survives_refresh() {
    const QUERY: &str = "SELECT duration FROM events ORDER BY id DESC LIMIT 1";
    let (_a, upstream, opts, _handle, shutdown_tx) =
        setup(QUERY, "POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS").await;

    let b = psql_helpers::connect(opts).await;
    b.simple_query("SET intervalstyle = 'iso_8601'")
        .await
        .unwrap();
    assert_eq!(simple_texts(&b, QUERY).await, vec!["P1DT2H"]);
    assert_filled(&b).await;

    upstream
        .simple_query(
            "INSERT INTO events (id, at, duration) VALUES (3, '2026-01-16 00:00:00+00', '3 days')",
        )
        .await
        .unwrap();

    eventually!(run_test: {
        let texts = simple_texts(&b, QUERY).await;
        let info = last_query_info(&b).await;
        AssertUnwindSafe(move || (info, texts))
    }, then_assert: |result| {
        let (info, texts) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(texts, vec!["P3D"]);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn timezone_abbreviations_survives_refresh() {
    const QUERY: &str = "SELECT id FROM events WHERE at = '2026-01-15 10:00 CST' ORDER BY id";
    let (_a, upstream, opts, _handle, shutdown_tx) =
        setup(QUERY, "POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS").await;

    let b = psql_helpers::connect(opts).await;
    b.simple_query("SET timezone_abbreviations = 'Australia'")
        .await
        .unwrap();
    assert_eq!(simple_ids(&b, QUERY).await, vec![2]);
    assert_filled(&b).await;

    upstream
        .simple_query("INSERT INTO events (id, at) VALUES (3, '2026-01-15 00:30:00+00')")
        .await
        .unwrap();

    eventually!(run_test: {
        let ids = simple_ids(&b, QUERY).await;
        let info = last_query_info(&b).await;
        AssertUnwindSafe(move || (info, ids))
    }, then_assert: |result| {
        let (info, ids) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(ids, vec![2, 3]);
    });

    shutdown_tx.shutdown().await;
}
