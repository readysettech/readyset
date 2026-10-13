use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::consts::ColumnFlags;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name, sleep};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage};

#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn union_all_column_metadata_mysql() {
    readyset_tracing::init_test_logging();
    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(&test_name)))
        .await
        .unwrap();
    upstream
        .query_drop(
            "CREATE TABLE t (
                 a int NOT NULL AUTO_INCREMENT PRIMARY KEY, b varchar(10) NOT NULL, k int
             );
             CREATE TABLE u (a int NOT NULL, b varchar(10), k int);
             CREATE TABLE note (t_id int NOT NULL, body varchar(10) NOT NULL);
             INSERT INTO t VALUES (1, 'x', 1), (2, 'y', 1);
             INSERT INTO u VALUES (3, NULL, 1);
             INSERT INTO note VALUES (1, 'n');
             CREATE VIEW same_table AS
                 SELECT a, b, k FROM t WHERE a = 1 UNION ALL SELECT a, b, k FROM t WHERE a = 2;
             CREATE VIEW two_tables AS SELECT a, b, k FROM t UNION ALL SELECT a, b, k FROM u;
             CREATE VIEW with_left_join AS
                 SELECT t.k AS k, note.body AS body FROM t LEFT JOIN note ON t.a = note.t_id
                 UNION ALL SELECT t_id AS k, body FROM note",
        )
        .await
        .unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs = Conn::new(opts).await.unwrap();
    rs.query_drop(format!("USE {test_name}")).await.unwrap();

    let flags = |rows: &[Row]| {
        let flags = ColumnFlags::NOT_NULL_FLAG
            | ColumnFlags::PRI_KEY_FLAG
            | ColumnFlags::UNIQUE_KEY_FLAG
            | ColumnFlags::AUTO_INCREMENT_FLAG
            | ColumnFlags::NO_DEFAULT_VALUE_FLAG;
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
        "SELECT a, b FROM same_table WHERE k = ? ORDER BY a",
        "SELECT a, b FROM two_tables WHERE k = ? ORDER BY a",
        "SELECT body FROM with_left_join WHERE k = ? ORDER BY body",
    ] {
        eventually!(run_test: {
            rs.query_drop(format!("CREATE CACHE FROM {query}")).await.map_err(|e| e.to_string())
        }, then_assert: |result| result.unwrap());
        let text_query = query.replace('?', "1");
        let expected: Vec<Row> = upstream.exec(query, (1,)).await.unwrap();
        let expected_text: Vec<Row> = upstream.query(&text_query).await.unwrap();
        eventually!(run_test: {
            let actual: Vec<Row> = rs.exec(query, (1,)).await.unwrap();
            let info = mysql_helpers::last_query_info(&mut rs).await;
            let actual_text: Vec<Row> = rs.query(&text_query).await.unwrap();
            let text_info = mysql_helpers::last_query_info(&mut rs).await;
            AssertUnwindSafe(move || (actual, info, actual_text, text_info))
        }, then_assert: |result| {
            let (actual, info, actual_text, text_info) = result();
            assert_matches!(info.destination, QueryDestination::Readyset(..), "{info:?}");
            assert_matches!(text_info.destination, QueryDestination::Readyset(..), "{text_info:?}");
            assert_eq!(flags(&expected), flags(&actual), "{query}");
            assert_eq!(cells(&expected), cells(&actual), "{query}");
            assert_eq!(flags(&expected_text), flags(&actual_text), "{query}");
            assert_eq!(cells(&expected_text), cells(&actual_text), "{query}");
        });
    }
    shutdown_tx.shutdown().await;
}

/// The type OID and type modifier of each column of the first row of a simple query.
async fn types(conn: &Client, query: &str) -> Vec<(u32, i32)> {
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
        .map(|f| (f.type_oid(), f.type_modifier()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn union_all_column_metadata_psql() {
    readyset_tracing::init_test_logging();
    let db_name = "union_all_column_metadata_psql";
    PostgreSQLAdapter::recreate_database(db_name).await;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(
            "CREATE TABLE t (a int PRIMARY KEY, b varchar(10) NOT NULL, n numeric(8,2), k int);
             CREATE TABLE u (a int NOT NULL, b varchar(10), n numeric(8,2), k int);
             CREATE TABLE w (a int NOT NULL, b varchar(20), n numeric(6,1), k int);
             INSERT INTO t VALUES (1, 'x', 1.5, 1), (2, 'y', 2.5, 1);
             INSERT INTO u VALUES (3, 'z', 3.5, 1);
             INSERT INTO w VALUES (4, 'v', 4.5, 1);
             CREATE VIEW same_table AS SELECT a, b, n, k FROM t UNION ALL SELECT a, b, n, k FROM t;
             CREATE VIEW same_types AS SELECT a, b, n, k FROM t UNION ALL SELECT a, b, n, k FROM u;
             CREATE VIEW other_types AS
                 SELECT a, b, n, k FROM t UNION ALL SELECT a, b, n, k FROM w;",
        )
        .await
        .unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts).await;
    sleep().await;

    // The table OID and column number of a view column are not compared: Readyset has no view OID
    for query in [
        "SELECT a, b, n FROM same_table WHERE k = $1 ORDER BY a",
        "SELECT a, b, n FROM same_types WHERE k = $1 ORDER BY a",
        "SELECT a, b, n FROM other_types WHERE k = $1 ORDER BY a",
    ] {
        conn.simple_query(&format!("CREATE CACHE FROM {query}"))
            .await
            .unwrap();
        let simple_query = query.replace("$1", "1");
        let expected = types(&upstream, &simple_query).await;
        eventually!(run_test: {
            let actual = types(&conn, &simple_query).await;
            let info = psql_helpers::last_query_info(&conn).await;
            AssertUnwindSafe(move || (actual, info))
        }, then_assert: |result| {
            let (actual, info) = result();
            assert_matches!(info.destination, QueryDestination::Readyset(..), "{info:?}");
            assert_eq!(actual, expected, "{query}");
        });
    }
    shutdown_tx.shutdown().await;
}
