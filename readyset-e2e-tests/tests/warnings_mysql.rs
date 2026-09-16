use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{MySQLAdapter, last_query_info};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
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
    drop(result);

    // MySQL keeps the diagnostics area across SHOW WARNINGS, so repeating it reports the same
    // warning.
    for _ in 0..2 {
        let warnings: Vec<(String, u32, String)> = conn.query("SHOW WARNINGS").await.unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].0, "Warning");
        assert_eq!(warnings[0].1, 1062);
    }

    // A statement Readyset serves itself raises no warnings, even though the upstream
    // connection's diagnostics area still holds the one from the INSERT.
    eventually!({
        conn.query_drop("CREATE CACHE FROM SELECT id, val FROM probe WHERE id = ?")
            .await
            .is_ok()
    });
    eventually!({
        let rows: Vec<Row> = conn
            .query("SELECT id, val FROM probe WHERE id = 1")
            .await
            .unwrap();
        rows.len() == 1
            && matches!(
                last_query_info(&mut conn).await.destination,
                QueryDestination::Readyset(_)
            )
    });
    let warnings: Vec<Row> = conn.query("SHOW WARNINGS").await.unwrap();
    assert!(warnings.is_empty());

    shutdown_tx.shutdown().await;
}
