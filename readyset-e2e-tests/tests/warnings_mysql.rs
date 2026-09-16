use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use readyset_adapter::backend::MigrationMode;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use test_utils::{tags, upstream};

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn proxied_statements_report_upstream_warnings() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE probe (id INT PRIMARY KEY, val VARCHAR(50))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO probe VALUES (1, 'hello')")
        .await
        .unwrap();

    // Comparing an INT column against a non-numeric string warns about the truncated conversion.
    let mut result = conn
        .query_iter("SELECT id, val FROM probe WHERE id = '1abc'")
        .await
        .unwrap();
    let rows: Vec<Row> = result.collect().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(result.warnings(), 1);

    let result = conn
        .query_iter("INSERT IGNORE INTO probe (id, val) VALUES (1, 'a'), (100, 'b'), (100, 'c')")
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    assert_eq!(result.warnings(), 2);
    assert_eq!(result.info(), "Records: 3  Duplicates: 2  Warnings: 2");

    let result = conn
        .exec_iter(
            "INSERT IGNORE INTO probe (id, val) VALUES (?, ?)",
            (1, "dup"),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 0);
    assert_eq!(result.warnings(), 1);

    shutdown_tx.shutdown().await;
}
