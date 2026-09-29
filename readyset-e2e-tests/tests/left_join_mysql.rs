use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::consts::ColumnFlags;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, derive_test_name};
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn left_join_right_columns_are_nullable() {
    readyset_tracing::init_test_logging();
    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(&test_name)))
        .await
        .unwrap();
    upstream
        .query_drop(
            "CREATE TABLE t (id int NOT NULL PRIMARY KEY, grp int);
             CREATE TABLE note (t_id int NOT NULL, body varchar(10) NOT NULL);
             INSERT INTO t VALUES (1, 1), (2, 1);
             INSERT INTO note VALUES (1, 'x');
             CREATE TABLE memo (t_id int NOT NULL, body varchar(10) NOT NULL DEFAULT '');
             CREATE TABLE other (grp int NOT NULL, body varchar(10) NOT NULL DEFAULT '');
             INSERT INTO memo VALUES (1, 'x');
             INSERT INTO other VALUES (1, 'y');
             CREATE VIEW v AS
                 SELECT t.grp AS grp, memo.body AS body FROM t LEFT JOIN memo ON t.id = memo.t_id
                 UNION ALL SELECT grp, body FROM other",
        )
        .await
        .unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .set_topk(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs = Conn::new(opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();

    let flags = |rows: &[Row]| {
        let flags = ColumnFlags::NOT_NULL_FLAG | ColumnFlags::NO_DEFAULT_VALUE_FLAG;
        rows[0]
            .columns_ref()
            .iter()
            .map(|column| column.flags() & flags)
            .collect::<Vec<_>>()
    };
    let cells = |rows: &[Row]| {
        rows.iter()
            .map(|row| row.clone().unwrap())
            .collect::<Vec<_>>()
    };
    for query in [
        "SELECT t.id, note.t_id, note.body FROM t LEFT JOIN note ON t.id = note.t_id
         WHERE t.grp = ? ORDER BY t.id",
        "SELECT t.id, note.t_id, note.body FROM t LEFT JOIN note ON t.id = note.t_id
         WHERE t.grp = ? ORDER BY t.id LIMIT 10",
        "SELECT body FROM v WHERE grp = ? ORDER BY body",
    ] {
        eventually!(run_test: {
            rs.query_drop(format!("CREATE CACHE FROM {query}")).await.map_err(|e| e.to_string())
        }, then_assert: |result| result.unwrap());
        let text_query = query.replace('?', "1");
        let expected: Vec<Row> = upstream.exec(query, (1,)).await.unwrap();
        let expected_text: Vec<Row> = upstream.query(&text_query).await.unwrap();
        eventually!(run_test: {
            let actual: Vec<Row> = rs.exec(query, (1,)).await.unwrap();
            let info = last_query_info(&mut rs).await;
            let actual_text: Vec<Row> = rs.query(&text_query).await.unwrap();
            let text_info = last_query_info(&mut rs).await;
            AssertUnwindSafe(move || (actual, info, actual_text, text_info))
        }, then_assert: |result| {
            let (actual, info, actual_text, text_info) = result();
            assert_matches!(info.destination, QueryDestination::Readyset(..), "{info:?}");
            assert_matches!(text_info.destination, QueryDestination::Readyset(..), "{text_info:?}");
            assert_eq!(flags(&expected), flags(&actual));
            assert_eq!(cells(&expected), cells(&actual));
            assert_eq!(flags(&expected_text), flags(&actual_text));
            assert_eq!(cells(&expected_text), cells(&actual_text));
        });
    }
    shutdown_tx.shutdown().await;
}

/// One replicated write delivers right rows whose join keys are 1, 2, 1, so the rows under key 1
/// do not arrive together.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn left_join_interleaved_binlog_batch() {
    readyset_tracing::init_test_logging();
    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(&test_name)))
        .await
        .unwrap();
    upstream
        .query_drop(
            "CREATE TABLE l (id INT PRIMARY KEY, k INT);
         CREATE TABLE r (id INT PRIMARY KEY, k INT, v INT);
         INSERT INTO l VALUES (1, 1)",
        )
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .partial(false)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();
    let query = "SELECT l.id, r.v FROM l LEFT JOIN r ON l.k = r.k WHERE l.id = ?";
    eventually!(run_test: {
        rs.query_drop(format!("CREATE CACHE FROM {query}")).await
            .map_err(|e| e.to_string())
    }, then_assert: |result| result.unwrap());
    for (write, expected) in [
        ("", vec![(1i32, None)]),
        (
            "INSERT INTO r VALUES (1, 1, 10), (2, 2, 20), (3, 1, 30)",
            vec![(1, Some(10)), (1, Some(30))],
        ),
        ("DELETE FROM r WHERE id IN (1, 2, 3)", vec![(1, None)]),
    ] {
        if !write.is_empty() {
            upstream.query_drop(write).await.unwrap();
        }
        let mut oracle: Vec<(i32, Option<i32>)> = upstream.exec(query, (1,)).await.unwrap();
        oracle.sort();
        assert_eq!(oracle, expected, "upstream after {write}");
        eventually!(attempts: 40, sleep: std::time::Duration::from_millis(50), run_test: {
            let mut actual: Vec<(i32, Option<i32>)> = rs.exec(query, (1,)).await.unwrap();
            actual.sort();
            let info = last_query_info(&mut rs).await;
            AssertUnwindSafe(move || (actual, info))
        }, then_assert: |result| {
            let (actual, info) = result();
            assert_matches!(info.destination, QueryDestination::Readyset(..), "{info:?}");
            assert_eq!(actual, oracle, "cached rows after {write}");
        });
    }
    shutdown_tx.shutdown().await;
}
