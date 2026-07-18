//! Regression test for REA-6745: `BEGIN`/`START TRANSACTION` isolation-level and
//! read-only modifiers were silently dropped because the adapter forwarded a
//! re-serialized modifier-free AST (`stmt.to_string()`) to upstream instead of
//! the client's original text. Forwarding the raw query preserves them.

use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use tokio_postgres::{Client, SimpleQueryMessage};
use test_utils::{tags, upstream};

/// First column of the first row of a simple query.
async fn scalar(conn: &Client, query: &str) -> String {
    let messages = conn.simple_query(query).await.expect("simple query");
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            return row.get(0).expect("column present").to_string();
        }
    }
    panic!("no row returned for {query}");
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn begin_preserves_isolation_and_read_only() {
    readyset_tracing::init_test_logging();

    let (opts, _handle, shutdown_tx) =
        TestBuilder::new(BackendBuilder::new().require_authentication(false))
            .fallback(true)
            .build::<PostgreSQLAdapter>()
            .await;
    let conn = psql_helpers::connect(opts).await;

    // Combined-form BEGIN with an isolation-level modifier must reach upstream.
    conn.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    assert_eq!(
        scalar(&conn, "SHOW transaction_isolation").await,
        "repeatable read",
    );
    assert_eq!(scalar(&conn, "SHOW transaction_read_only").await, "off");
    conn.simple_query("COMMIT").await.unwrap();

    conn.simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();
    assert_eq!(
        scalar(&conn, "SHOW transaction_isolation").await,
        "serializable",
    );
    conn.simple_query("COMMIT").await.unwrap();

    // READ ONLY modifier must reach upstream too.
    conn.simple_query("BEGIN READ ONLY").await.unwrap();
    assert_eq!(scalar(&conn, "SHOW transaction_read_only").await, "on");
    conn.simple_query("COMMIT").await.unwrap();

    shutdown_tx.shutdown().await;
}
