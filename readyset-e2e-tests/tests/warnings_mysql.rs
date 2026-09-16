use std::assert_matches;
use std::time::Duration;

use mysql_async::prelude::{Protocol, Queryable};
use mysql_async::{Conn, QueryResult, Row};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// MySQL's code for a duplicate key.
const DUP_ENTRY: u32 = 1062;

async fn show_warnings(conn: &mut Conn) -> Vec<(String, u32, String)> {
    conn.query("SHOW WARNINGS").await.unwrap()
}

/// Read a result to its end and return its rows with the warning count it reported.
async fn rows_and_warnings<P>(mut result: QueryResult<'_, 'static, P>) -> (Vec<Row>, u16)
where
    P: Protocol,
{
    let rows = result.collect().await.unwrap();
    (rows, result.warnings())
}

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
    let result = conn
        .query_iter("SELECT id, val FROM probe WHERE id = '1abc'")
        .await
        .unwrap();
    let (rows, warnings) = rows_and_warnings(result).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings, 1);

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
        let warnings = show_warnings(&mut conn).await;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].0, "Warning");
        assert_eq!(warnings[0].1, DUP_ENTRY);
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
    assert!(show_warnings(&mut conn).await.is_empty());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn shallow_hits_replay_upstream_warnings() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(opts).await.unwrap();
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some("noria")))
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE probe (id INT PRIMARY KEY, val VARCHAR(50))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO probe VALUES (1, 'hello')")
        .await
        .unwrap();
    conn.query_drop(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH EVERY 2 SECONDS
         FROM SELECT id, val, RAND() FROM probe WHERE id = ?",
    )
    .await
    .unwrap();

    // Comparing an INT column against a non-numeric string warns about the truncated conversion,
    // once per evaluation, which the prepared form does twice.
    let query = "SELECT id, val, RAND() FROM probe WHERE id = '1abc'";
    let (_, text_count) = rows_and_warnings(upstream.query_iter(query).await.unwrap()).await;
    let text_warnings = show_warnings(&mut upstream).await;
    let prepared = "SELECT id, val, RAND() FROM probe WHERE id = ?";
    let (_, binary_count) =
        rows_and_warnings(upstream.exec_iter(prepared, ("1abc",)).await.unwrap()).await;
    let binary_warnings = show_warnings(&mut upstream).await;
    assert!(
        text_count > 0 && binary_count > 0,
        "text_count = {text_count}, binary_count = {binary_count}"
    );

    // The fill stores the warnings the upstream raised with the entry.
    let (rows, warnings) = rows_and_warnings(conn.query_iter(query).await.unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings, text_count);
    assert_eq!(show_warnings(&mut conn).await, text_warnings);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(_)
    );

    // A hit replays the stored warnings.
    conn.query_drop("INSERT IGNORE INTO probe VALUES (1, 'dup')")
        .await
        .unwrap();
    let (rows, warnings) = rows_and_warnings(conn.query_iter(query).await.unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings, text_count);
    assert_eq!(show_warnings(&mut conn).await, text_warnings);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(_)
    );
    let hit = rows.into_iter().next().unwrap();

    // The same through the binary protocol, which goes to a separate cache key.
    let (rows, warnings) =
        rows_and_warnings(conn.exec_iter(prepared, ("1abc",)).await.unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings, binary_count);
    assert_eq!(show_warnings(&mut conn).await, binary_warnings);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(_)
    );
    conn.query_drop("INSERT IGNORE INTO probe VALUES (1, 'dup')")
        .await
        .unwrap();
    let (rows, warnings) =
        rows_and_warnings(conn.exec_iter(prepared, ("1abc",)).await.unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings, binary_count);
    assert_eq!(show_warnings(&mut conn).await, binary_warnings);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(_)
    );

    // A refresh stores the warnings again. RAND() shows the entry was replaced.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (rows, warnings) = rows_and_warnings(conn.query_iter(query).await.unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0].get::<f64, _>(2), hit.get::<f64, _>(2));
    assert_eq!(warnings, text_count);
    assert_eq!(show_warnings(&mut conn).await, text_warnings);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow(_)
    );

    shutdown_tx.shutdown().await;
}
