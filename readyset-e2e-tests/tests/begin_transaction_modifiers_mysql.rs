//! Regression test for REA-6745 (MySQL): the access-mode modifier on
//! `START TRANSACTION` (e.g. `READ ONLY`) was silently dropped because the adapter
//! forwarded a re-serialized modifier-free AST (`stmt.to_string()`) to upstream
//! instead of the client's original text. Forwarding the raw query preserves it, so
//! a read-only transaction rejects writes upstream. MySQL sets the isolation level
//! with a separate `SET TRANSACTION`, so the access mode is the modifier at risk on
//! `START TRANSACTION` itself.

use mysql_async::Conn;
use mysql_async::prelude::*;
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use test_utils::{tags, upstream};

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn start_transaction_preserves_read_only() {
    readyset_tracing::init_test_logging();

    let (opts, _handle, shutdown_tx) =
        TestBuilder::new(BackendBuilder::new().require_authentication(false))
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY)")
        .await
        .unwrap();

    // The READ ONLY access-mode modifier must reach upstream: a write inside the
    // transaction is rejected. If it were dropped (plain START TRANSACTION), the
    // INSERT would succeed.
    conn.query_drop("START TRANSACTION READ ONLY").await.unwrap();
    let write = conn.query_drop("INSERT INTO t (id) VALUES (1)").await;
    assert!(
        write.is_err(),
        "a write in a READ ONLY transaction must be rejected upstream",
    );
    conn.query_drop("ROLLBACK").await.unwrap();

    // Contrast: the same write in a READ WRITE transaction is accepted and commits,
    // confirming the rejection above is the access mode rather than a broken path.
    conn.query_drop("START TRANSACTION READ WRITE")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id) VALUES (2)")
        .await
        .unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    shutdown_tx.shutdown().await;
}
