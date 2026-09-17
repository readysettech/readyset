use std::assert_matches;

use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{
    self, BinaryParam, PostgreSQLAdapter, TextParam, last_query_info,
};
use readyset_client_test_helpers::{Adapter, TestBuilder, TestShutdownSender, sleep};
use readyset_server::Handle;
use test_utils::{tags, upstream};
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

const SETUP: &str = "
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE enum_events (id INT PRIMARY KEY, m mood);
    INSERT INTO enum_events (id, m) VALUES (1, 'happy'), (2, 'sad'), (3, 'happy');
";

const EQ: &str = "SELECT id FROM enum_events WHERE m = $1 ORDER BY id";
const IN: &str = "SELECT id FROM enum_events WHERE m IN ($1, $2) ORDER BY id";

/// Build an adapter with fallback over a fresh database holding the enum table, with a deep
/// cache on each of `queries`.
async fn setup(db_name: &str, queries: &[&str]) -> (Client, Handle, TestShutdownSender<PostgreSQLAdapter>) {
    readyset_tracing::init_test_logging();
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream.simple_query(SETUP).await.unwrap();

    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    sleep().await;
    for query in queries {
        conn.simple_query(&format!("CREATE CACHE FROM {query}"))
            .await
            .unwrap();
    }
    sleep().await;
    (conn, handle, shutdown_tx)
}

async fn ids(conn: &Client, query: &str, params: &[&(dyn ToSql + Sync)]) -> Vec<i32> {
    conn.query(query, params)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn assert_served_by_readyset(conn: &Client) {
    let info = last_query_info(conn).await;
    assert_matches!(info.destination, QueryDestination::Readyset(..), "{info:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn text_format_enum_param_is_served_by_deep_cache() {
    let (conn, _handle, shutdown_tx) = setup("enum_params_text", &[EQ, IN]).await;

    assert_eq!(ids(&conn, EQ, &[&TextParam("happy")]).await, vec![1, 3]);
    assert_served_by_readyset(&conn).await;

    let params: [&(dyn ToSql + Sync); 2] = [&TextParam("happy"), &TextParam("sad")];
    assert_eq!(ids(&conn, IN, &params).await, vec![1, 2, 3]);
    assert_served_by_readyset(&conn).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn binary_format_enum_param_is_served_by_deep_cache() {
    let (conn, _handle, shutdown_tx) = setup("enum_params_binary", &[EQ]).await;

    // The binary send format of an enum is its label.
    assert_eq!(ids(&conn, EQ, &[&BinaryParam(b"happy")]).await, vec![1, 3]);
    assert_served_by_readyset(&conn).await;

    shutdown_tx.shutdown().await;
}
