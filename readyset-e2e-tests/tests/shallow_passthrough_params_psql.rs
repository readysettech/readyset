//! Shallow cache reads whose bound parameters have no native `DfValue` representation, such as
//! a Postgres enum or `cidr`, must fill from upstream and then serve from the cache.

use std::assert_matches;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::psql_helpers::{
    self, PostgreSQLAdapter, TextParam, last_query_info,
};
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

const SETUP: &str = "
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE enum_events (id INT PRIMARY KEY, m mood);
    INSERT INTO enum_events (id, m) VALUES (1, 'happy'), (2, 'sad'), (3, 'happy');
    CREATE TABLE network_values (id INT PRIMARY KEY, net cidr);
    INSERT INTO network_values (id, net) VALUES (1, '192.168.1.0/24');
";

/// Build an adapter with fallback, the test tables, and a shallow cache on each of `queries`.
async fn setup(queries: &[&str]) -> (Client, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    conn.simple_query(SETUP).await.unwrap();
    for query in queries {
        conn.simple_query(&format!("CREATE SHALLOW CACHE FROM {query}"))
            .await
            .unwrap();
    }
    (conn, handle, shutdown_tx)
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

async fn ids(conn: &Client, query: &str, params: &[&(dyn ToSql + Sync)]) -> Vec<i32> {
    conn.query(query, params)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn eq(conn: &Client, query: &str, params: &[&(dyn ToSql + Sync)]) -> Vec<bool> {
    conn.query(query, params)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn enum_text_param_fills_then_hits() {
    const EQ: &str = "SELECT id FROM enum_events WHERE m = $1 ORDER BY id";
    const IN: &str = "SELECT id FROM enum_events WHERE m IN ($1, $2) ORDER BY id";
    let (conn, _handle, shutdown_tx) = setup(&[EQ, IN]).await;

    assert_eq!(ids(&conn, EQ, &[&TextParam("happy")]).await, vec![1, 3]);
    assert_filled(&conn).await;
    assert_eq!(ids(&conn, EQ, &[&TextParam("happy")]).await, vec![1, 3]);
    assert_hit(&conn).await;

    let params: [&(dyn ToSql + Sync); 2] = [&TextParam("happy"), &TextParam("sad")];
    assert_eq!(ids(&conn, IN, &params).await, vec![1, 2, 3]);
    assert_filled(&conn).await;
    assert_eq!(ids(&conn, IN, &params).await, vec![1, 2, 3]);
    assert_hit(&conn).await;

    shutdown_tx.shutdown().await;
}

const CIDR_EQ: &str = "SELECT (net = $1::cidr) AS eq FROM network_values WHERE id = $2";

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn cidr_text_param_fills_then_hits() {
    let (conn, _handle, shutdown_tx) = setup(&[CIDR_EQ]).await;

    let params: [&(dyn ToSql + Sync); 2] = [&TextParam("192.168.1.0/24"), &1i32];
    assert_eq!(eq(&conn, CIDR_EQ, &params).await, vec![true]);
    assert_filled(&conn).await;
    assert_eq!(eq(&conn, CIDR_EQ, &params).await, vec![true]);
    assert_hit(&conn).await;

    shutdown_tx.shutdown().await;
}
