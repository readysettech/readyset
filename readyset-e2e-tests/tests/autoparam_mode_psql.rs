//! Postgres coverage for caches that keep their author's literals inline.
//!
//! The values that pick such a cache are matched when the read runs, the bound ones alongside the
//! spelled-out ones. That has to hold for each way Postgres can run a parameterized read: the
//! simple protocol, a named prepared statement, and the unnamed extended-protocol form drivers
//! send when they pipeline Parse/Bind/Execute in one round trip.

use readyset_adapter::backend::MigrationMode;
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::{Adapter, TestBuilder, sleep, wait_for_schema_generation_change};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_client_test_helpers::TestShutdownSender;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage};

const T: &str = "CREATE TABLE t (id int, status text, v int); \
                 INSERT INTO t (id, status, v) VALUES \
                 (1, 'active', 10), (1, 'archived', 20), (2, 'active', 30);";

/// Where the previous statement went, with the adapter's reason.
async fn last_target(client: &Client) -> (QueryDestination, String) {
    let rows = client.simple_query("EXPLAIN LAST STATEMENT").await.unwrap();
    let row = rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("EXPLAIN LAST STATEMENT row");
    let destination = QueryDestination::try_from(row.get(0).expect("destination")).unwrap();
    (destination, row.get(1).unwrap_or_default().to_owned())
}

async fn assert_last_target_was(client: &Client, expected: QueryDestination) {
    let (destination, reason) = last_target(client).await;
    assert_eq!(destination, expected, "{reason}");
}

/// The first column of every row a simple-protocol query returns.
fn first_column(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Out-of-band migration keeps a plain SELECT from creating a cache of its own, so what routes a
/// read is only ever the explicit CREATE CACHE.
async fn adapter(db_name: &str) -> (Client, Handle, TestShutdownSender<PostgreSQLAdapter>) {
    readyset_tracing::init_test_logging();
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut cfg = readyset_client_test_helpers::psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let upstream = readyset_client_test_helpers::psql_helpers::connect(cfg).await;
    for stmt in T.split(';').filter(|s| !s.trim().is_empty()) {
        upstream.simple_query(stmt).await.unwrap();
    }

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;

    let client = readyset_client_test_helpers::psql_helpers::connect(rs_opts).await;
    sleep().await;
    (client, handle, shutdown_tx)
}

const CREATE_KEPT: &str = "CREATE CACHE kept WITH (AUTOPARAM OFF) \
                           FROM SELECT v FROM t WHERE id = $1 AND status = 'active'";
const READ: &str = "SELECT v FROM t WHERE id = $1 AND status = 'active'";

/// A named prepared statement reaches a cache that kept its literals inline. `tokio_postgres`
/// names every statement it prepares, so this is the named path.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn a_named_prepared_read_reaches_a_per_cache_off_cache() {
    let (client, _handle, shutdown_tx) = adapter("autoparam_psql_named").await;

    client.simple_query(CREATE_KEPT).await.unwrap();
    sleep().await;

    let stmt = client.prepare(READ).await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 10);
    assert_last_target_was(&client, QueryDestination::Readyset(Some("kept".into()))).await;

    shutdown_tx.shutdown().await;
}

/// The same read through the simple protocol, which carries its values as text.
///
/// `Client::query` prepares even for a `&str`, so the ad-hoc path is only reached through
/// `simple_query`.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn an_adhoc_read_reaches_a_per_cache_off_cache() {
    let (client, _handle, shutdown_tx) = adapter("autoparam_psql_adhoc").await;

    client.simple_query(CREATE_KEPT).await.unwrap();
    sleep().await;

    let messages = client
        .simple_query("SELECT v FROM t WHERE id = 1 AND status = 'active'")
        .await
        .unwrap();
    let values: Vec<&str> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["10"]);
    assert_last_target_was(&client, QueryDestination::Readyset(Some("kept".into()))).await;

    shutdown_tx.shutdown().await;
}

/// A statement binding the position the cache kept inline is matched by the value it binds: the
/// value the cache kept reaches it, another goes upstream.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn a_prepared_read_binding_a_kept_position_is_matched_by_its_value() {
    let (client, _handle, shutdown_tx) = adapter("autoparam_psql_bind").await;

    client.simple_query(CREATE_KEPT).await.unwrap();
    sleep().await;

    let stmt = client
        .prepare("SELECT v FROM t WHERE id = $1 AND status = $2")
        .await
        .unwrap();
    let rows = client.query(&stmt, &[&1i32, &"active"]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 10);
    assert_last_target_was(&client, QueryDestination::Readyset(Some("kept".into()))).await;

    let rows = client.query(&stmt, &[&1i32, &"archived"]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_last_target_was(&client, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// A schema change taking every status takes every entry with it, so all are filed again.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn a_kept_literal_cache_survives_a_new_type() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_psql_new_type";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut cfg = readyset_client_test_helpers::psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let upstream = readyset_client_test_helpers::psql_helpers::connect(cfg).await;
    for stmt in T.split(';').filter(|s| !s.trim().is_empty()) {
        upstream.simple_query(stmt).await.unwrap();
    }

    let query_status_cache: &'static QueryStatusCache =
        Box::leak(Box::new(QueryStatusCache::new().style(MigrationStyle::Explicit)));
    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .query_status_cache(query_status_cache)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = readyset_client_test_helpers::psql_helpers::connect(rs_opts).await;
    sleep().await;

    client.simple_query(CREATE_KEPT).await.unwrap();
    let read = "SELECT v FROM t WHERE id = 1 AND status = 'active'";
    let served = QueryDestination::Readyset(Some("kept".into()));
    eventually!(run_test: {
        client.simple_query(read).await.unwrap();
        last_target(&client).await
    }, then_assert: |(destination, reason)| {
        assert_eq!(destination, served, "{reason}");
    });

    let filed = query_status_cache.inline_literal_caches_generation();
    let generation = handle.schema_catalog().await.unwrap().generation;
    upstream
        .simple_query("CREATE TYPE mood AS ENUM ('fine')")
        .await
        .unwrap();
    wait_for_schema_generation_change(&mut handle, generation).await;
    eventually! {
        query_status_cache.inline_literal_caches_generation() > filed
    }
    eventually!(
        message: "the kept cache should be filed again after the schema change".to_string(),
        { query_status_cache.may_have_inline_literal_caches() }
    );

    // The server kept the cache, so the read has to reach it again.
    let caches = first_column(&client.simple_query("SHOW CACHES").await.unwrap());
    assert_eq!(caches.len(), 1, "a new type drops no cache");
    eventually!(run_test: {
        let values = first_column(&client.simple_query(read).await.unwrap());
        (values, last_target(&client).await.0)
    }, then_assert: |(values, destination)| {
        assert_eq!(values, vec!["10"]);
        assert_eq!(destination, served);
    });

    shutdown_tx.shutdown().await;
}
