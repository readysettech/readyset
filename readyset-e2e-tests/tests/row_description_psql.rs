//! The RowDescription metadata Readyset sends must match upstream Postgres. This covers the type
//! modifier of each column on the proxied, shallow cache, and deep cache paths, and the type size
//! of an enum column, which a client only sees through the simple-query protocol.

use std::assert_matches;
use std::panic::AssertUnwindSafe;

use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, sleep};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage, Statement};

const SETUP: &str = "
    CREATE TYPE typmod_mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE typmod (
        id INT PRIMARY KEY,
        num_ps NUMERIC(10,2),
        num NUMERIC,
        vc VARCHAR(20),
        ch CHAR(10),
        txt TEXT,
        mood typmod_mood
    );
    INSERT INTO typmod VALUES (1, 123.45, 1.5, 'hello', 'hello', 'hello', 'happy');
";

const DEEP: &str = "SELECT id, num_ps, num, vc, ch, txt, mood FROM typmod WHERE id = $1";
const DEEP_SIMPLE: &str = "SELECT id, num_ps, num, vc, ch, txt, mood FROM typmod WHERE id = 1";
const SHALLOW: &str = "SELECT num_ps, vc, ch, mood, id FROM typmod WHERE id = $1";
const PROXIED: &str =
    "SELECT id, num_ps, num, vc, ch, txt, mood, random() FROM typmod WHERE id = $1";

fn modifiers(stmt: &Statement) -> Vec<i32> {
    stmt.columns().iter().map(|c| c.type_modifier()).collect()
}

/// Type OID, type size, and type modifier of each column of the first row of a simple query.
async fn simple_fields(conn: &Client, query: &str) -> Vec<(u32, i16, i32)> {
    let messages = conn.simple_query(query).await.unwrap();
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("query returned no rows");
    row.fields()
        .iter()
        .map(|f| (f.type_oid(), f.type_size(), f.type_modifier()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn row_description_matches_upstream() {
    readyset_tracing::init_test_logging();
    let db_name = "row_description_psql";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream.simple_query(SETUP).await.unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    sleep().await;
    conn.simple_query(&format!("CREATE CACHE FROM {DEEP}"))
        .await
        .unwrap();
    conn.simple_query(&format!("CREATE SHALLOW CACHE FROM {SHALLOW}"))
        .await
        .unwrap();
    sleep().await;

    let expected = modifiers(&upstream.prepare(DEEP).await.unwrap());
    assert!(
        expected.iter().any(|m| *m != -1),
        "upstream reported no type modifiers: {expected:?}"
    );
    let stmt = conn.prepare(DEEP).await.unwrap();
    conn.query(&stmt, &[&1i32]).await.unwrap();
    let info = last_query_info(&conn).await;
    assert_matches!(info.destination, QueryDestination::Readyset(_), "{info:?}");
    assert_eq!(modifiers(&stmt), expected, "deep cache");

    let expected = simple_fields(&upstream, DEEP_SIMPLE).await;
    assert_eq!(expected.last().unwrap().1, 4, "enum type size: {expected:?}");
    let actual = simple_fields(&conn, DEEP_SIMPLE).await;
    let info = last_query_info(&conn).await;
    assert_matches!(info.destination, QueryDestination::Readyset(_), "{info:?}");
    assert_eq!(actual, expected, "deep cache, simple query");

    let expected = modifiers(&upstream.prepare(PROXIED).await.unwrap());
    let stmt = conn.prepare(PROXIED).await.unwrap();
    conn.query(&stmt, &[&1i32]).await.unwrap();
    let info = last_query_info(&conn).await;
    assert_matches!(info.destination, QueryDestination::Upstream, "{info:?}");
    assert_eq!(modifiers(&stmt), expected, "proxied");

    let expected = modifiers(&upstream.prepare(SHALLOW).await.unwrap());
    let stmt = conn.prepare(SHALLOW).await.unwrap();
    conn.query(&stmt, &[&1i32]).await.unwrap();
    eventually!(run_test: {
        conn.query(&stmt, &[&1i32]).await.unwrap();
        let info = last_query_info(&conn).await;
        AssertUnwindSafe(move || info)
    }, then_assert: |result| {
        let info = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(_), "{info:?}");
    });
    assert_eq!(modifiers(&stmt), expected, "shallow cache");

    shutdown_tx.shutdown().await;
}
