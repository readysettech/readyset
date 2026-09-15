//! A replicated `DROP DATABASE` must remove Readyset's copy of the schema.

use std::panic::AssertUnwindSafe;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Both CREATE TABLEs arrive through the binlog, so the recreate is a same-body no-op: the shape
/// under which pre-drop rows would survive. Only sqlparser parses DROP DATABASE, and the
/// TestBuilder default preset does not prefer it.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn drop_database_discards_pre_drop_rows() {
    readyset_tracing::init_test_logging();
    // Drop a schema no connection selects: dropping a connection's default db unsets it.
    let db = "drop_database_mysql";
    let dropped = "drop_database_mysql_dropped";
    mysql_helpers::recreate_database(db).await;
    mysql_helpers::recreate_database(dropped).await;
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db)
        .replication_tables(format!("{dropped}.*,{db}.*"))
        .parsing_preset(ParsingPreset::for_prod())
        .build::<MySQLAdapter>()
        .await;
    let mut rs = Conn::new(opts).await.unwrap();
    let mut upstream = Conn::new(mysql_helpers::upstream_config().db_name(Some(db)))
        .await
        .unwrap();

    let create_table = format!("CREATE TABLE {dropped}.t (g INT, x INT)");
    upstream.query_drop(&create_table).await.unwrap();
    upstream
        .query_drop(format!("INSERT INTO {dropped}.t VALUES (1, 30)"))
        .await
        .unwrap();
    let select = format!("SELECT g, x FROM {dropped}.t");
    let create_cache = format!("CREATE CACHE FROM {select}");
    eventually!(run_test: {
        let _ = rs.query_drop(&create_cache).await;
        let rows: Result<Vec<(i32, i32)>, _> = rs.query(&select).await;
        AssertUnwindSafe(move || rows)
    }, then_assert: |rows| assert_eq!(rows().unwrap(), [(1, 30)]));

    for sql in [
        format!("DROP DATABASE {dropped}"),
        format!("CREATE DATABASE {dropped}"),
        create_table,
        format!("INSERT INTO {dropped}.t VALUES (2, 99)"),
    ] {
        upstream.query_drop(sql).await.unwrap();
    }
    eventually!(run_test: {
        // The drop takes the cache with the base; the recreated table needs a new one.
        let _ = rs.query_drop(&create_cache).await;
        let rows: Result<Vec<(i32, i32)>, _> = rs.query(&select).await;
        AssertUnwindSafe(move || rows)
    }, then_assert: |rows| {
        let rows = rows().unwrap();
        assert!(rows.contains(&(2, 99)), "replication has not caught up yet");
        assert_eq!(rows, [(2, 99)], "a pre-drop row survived DROP DATABASE");
    });

    shutdown_tx.shutdown().await;
    for name in [db, dropped] {
        upstream
            .query_drop(format!("DROP DATABASE {name}"))
            .await
            .unwrap();
    }
}
