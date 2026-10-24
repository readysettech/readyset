use std::panic::AssertUnwindSafe;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn backtick_in_table_name_is_snapshotted() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    MySQLAdapter::recreate_database(&db).await;
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(&db)))
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE plain (id INT PRIMARY KEY, v INT)",
        "CREATE TABLE `tick``x` (id INT PRIMARY KEY, v INT)",
        "INSERT INTO plain VALUES (1, 1)",
        "INSERT INTO `tick``x` VALUES (2, 2)",
    ] {
        upstream.query_drop(sql).await.unwrap();
    }

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&db)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();

    for (select, expected) in [
        ("SELECT id, v FROM plain", (1, 1)),
        ("SELECT id, v FROM `tick``x`", (2, 2)),
    ] {
        eventually!(run_test: {
            let _ = rs.query_drop(format!("CREATE CACHE FROM {select}")).await;
            let rows: Result<Vec<(i32, i32)>, _> = rs.query(select).await;
            let destination = mysql_helpers::last_query_info(&mut rs).await.destination;
            AssertUnwindSafe(move || (rows, destination))
        }, then_assert: |res| {
            let (rows, destination) = res();
            assert_eq!(rows.unwrap(), [expected]);
            assert!(matches!(destination, QueryDestination::Readyset(..)));
        });
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn double_quote_in_table_name_is_snapshotted() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db).await;
    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(&db);
    let upstream = psql_helpers::connect(upstream_config).await;
    for sql in [
        r#"CREATE TABLE plain (id INT PRIMARY KEY, v INT)"#,
        r#"CREATE TABLE "dq""x" (id INT PRIMARY KEY, v INT)"#,
        r#"INSERT INTO plain VALUES (1, 1)"#,
        r#"INSERT INTO "dq""x" VALUES (2, 2)"#,
    ] {
        upstream.simple_query(sql).await.unwrap();
    }

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&db)
        .parsing_preset(ParsingPreset::for_prod())
        .build::<PostgreSQLAdapter>()
        .await;
    let rs = psql_helpers::connect(opts).await;

    for (select, expected) in [
        (r#"SELECT id, v FROM plain"#, (1, 1)),
        (r#"SELECT id, v FROM "dq""x""#, (2, 2)),
    ] {
        eventually!(run_test: {
            let _ = rs.simple_query(&format!("CREATE CACHE FROM {select}")).await;
            let rows: Result<Vec<(i32, i32)>, _> = rs
                .query(select, &[])
                .await
                .map(|rows| rows.iter().map(|r| (r.get(0), r.get(1))).collect());
            let destination = psql_helpers::last_query_info(&rs).await.destination;
            AssertUnwindSafe(move || (rows, destination))
        }, then_assert: |res| {
            let (rows, destination) = res();
            assert_eq!(rows.unwrap(), [expected]);
            assert!(matches!(destination, QueryDestination::Readyset(..)));
        });
    }
    shutdown_tx.shutdown().await;
}
