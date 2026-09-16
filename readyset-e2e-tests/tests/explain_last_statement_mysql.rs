use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::QueryInfo;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::{TestBuilder, sleep};
use test_utils::{tags, upstream};

async fn explain_last_statement(conn: &mut Conn) -> QueryInfo {
    conn.query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn explain_last_statement_is_repeatable() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) =
        TestBuilder::new(BackendBuilder::new().require_authentication(false))
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (x INT)").await.unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO t (x) VALUES (1)").await.unwrap();

    let first = explain_last_statement(&mut conn).await;
    assert_eq!(first.destination, QueryDestination::Upstream);
    let second = explain_last_statement(&mut conn).await;
    assert_eq!(second, first);

    shutdown_tx.shutdown().await;
}
