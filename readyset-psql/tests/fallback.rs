use std::panic::AssertUnwindSafe;

use chrono::NaiveDate;
use postgres_types::private::BytesMut;
use readyset_adapter::backend::{MigrationMode, UnsupportedSetMode};
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::psql_helpers::{upstream_config, PostgreSQLAdapter};
use readyset_client_test_helpers::{sleep, Adapter, TestBuilder};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use serial_test::serial;

mod common;
use common::connect;
use postgres_types::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Client, CommandCompleteContents, SimpleQueryMessage};

async fn setup() -> (tokio_postgres::Config, Handle, ShutdownSender) {
    TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    client
        .simple_query("INSERT INTO cats (id) VALUES (1)")
        .await
        .unwrap();

    sleep().await;
    sleep().await;

    let result = client
        .query_one("SELECT cats.id FROM cats WHERE cats.id = 1", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn delete_case_sensitive() {
    for (opts, _handle, shutdown_tx) in [
        TestBuilder::default().build::<PostgreSQLAdapter>().await,
        setup().await,
    ] {
        let conn = connect(opts).await;
        conn.simple_query(r#"CREATE TABLE "Cats" (id int PRIMARY KEY, "ID" int)"#)
            .await
            .unwrap();
        sleep().await;

        conn.simple_query(r#"INSERT INTO cats (id) VALUES (1)"#)
            .await
            .unwrap_err();

        conn.simple_query(r#"INSERT INTO "cats" (id) VALUES (1)"#)
            .await
            .unwrap_err();

        conn.simple_query(r#"INSERT INTO Cats (id) VALUES (1)"#)
            .await
            .unwrap_err();

        conn.simple_query(r#"INSERT INTO "Cats" (id, "Id") VALUES (1, 2)"#)
            .await
            .unwrap_err();

        conn.simple_query(r#"INSERT INTO "Cats" (iD, "ID") VALUES (1, 2)"#)
            .await
            .unwrap();

        sleep().await;

        let row = conn
            .query_opt(
                r#"SELECT "Cats".id FROM "Cats" WHERE "Cats".id = 1 and "Cats"."ID" = 2"#,
                &[],
            )
            .await
            .unwrap();
        assert!(row.is_some());

        {
            let res = conn
                .simple_query(r#"DELETE FROM "Cats" WHERE "Cats".Id = 1"#)
                .await
                .unwrap();
            let deleted = res.first().unwrap();
            assert!(matches!(
                deleted,
                SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
            ));
            sleep().await;
        }

        let row = conn
            .query_opt(r#"SELECT "Cats".id FROM "Cats" WHERE "Cats".id = 1"#, &[])
            .await
            .unwrap();
        assert!(row.is_none());

        shutdown_tx.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn prepare_typed_insert() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;

    // Simulate JDBC's behavior of sending the types it wants to send regardless of the types the
    // backend responded with by just blindly serializing an i64 as an INT4
    #[derive(Debug, Clone, Copy)]
    struct WrongLengthInt(i64);

    impl ToSql for WrongLengthInt {
        fn to_sql(
            &self,
            _: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
            self.0.to_sql(&Type::INT8, out)
        }

        accepts!(INT4, INT8);
        to_sql_checked!();
    }

    conn.simple_query("create table t1 (x int4)").await.unwrap();
    let stmt = conn
        .prepare_typed("insert into t1 (x) values ($1)", &[Type::INT8])
        .await
        .unwrap();
    conn.execute(&stmt, &[&WrongLengthInt(1)]).await.unwrap();

    let stmt = conn
        .prepare_typed("select count(*) from t1 where x = $1", &[Type::INT8])
        .await
        .unwrap();
    let res = conn.query(&stmt, &[&1i64]).await.unwrap();
    assert_eq!(res.get(0).unwrap().get::<_, i64>(0), 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unsupported_query_ad_hoc() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;
    let result = match client
        .simple_query("SELECT relname FROM pg_class WHERE oid = 'pg_type'::regclass")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_owned(),
        _ => panic!(),
    };

    assert_eq!(result, "pg_type");

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn prepare_execute_fallback() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();

    client
        .execute("INSERT INTO cats (id) VALUES (1)", &[])
        .await
        .unwrap();
    sleep().await;

    // params in subqueries will likely always go to fallback
    let res = client
        .query(
            "SELECT id FROM (SELECT id FROM cats WHERE id = $1) sq",
            &[&1i32],
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get::<_, i32>(0), 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn proxy_unsupported_sets() {
    let (config, _handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .unsupported_set_mode(UnsupportedSetMode::Proxy)
            .require_authentication(false),
    )
    .fallback(true)
    .build::<PostgreSQLAdapter>()
    .await;
    let client = connect(config).await;

    client.simple_query("SET DateStyle = 'DMY'").await.unwrap();

    let res = client
        .query("SELECT '05-03-2022'::date", &[])
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(
        res[0].get::<_, NaiveDate>(0),
        NaiveDate::from_ymd(2022, 3, 5)
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn proxy_unsupported_type() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    #[derive(PartialEq, Eq, Debug, ToSql, FromSql)]
    struct Comp {
        x: i32,
        y: i32,
    }

    client
        .simple_query(
            r#"CREATE TYPE "Comp" AS (x int, y int);
               CREATE TABLE t (x "Comp");"#,
        )
        .await
        .unwrap();

    let exec_res = client
        .query_one(r#"SELECT '(1,2)'::"Comp""#, &[])
        .await
        .unwrap()
        .get::<_, Comp>(0);
    assert_eq!(exec_res, Comp { x: 1, y: 2 });

    let simple_res = match client
        .simple_query(r#"SELECT '(1,2)'::"Comp""#)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(row) => row,
        _ => panic!(),
    };
    assert_eq!(simple_res.get(0).unwrap(), "(1,2)");

    shutdown_tx.shutdown().await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn schema_resolution_with_unreplicated_tables() {
    readyset_tracing::init_test_logging();
    let (config, mut handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    // s2 will exist in readyset
    client
        .simple_query(
            "CREATE SCHEMA s1; CREATE SCHEMA s2;
             CREATE TABLE s2.t (x INT)",
        )
        .await
        .unwrap();

    sleep().await;

    handle
        .set_failpoint("parse-sql-type", "2*return(fail)")
        .await;

    // s1 and the insert into it will fail to parse, so it will only exist upstream
    client
        .simple_query("CREATE TABLE s1.t (x INT)")
        .await
        .unwrap();

    sleep().await;

    client
        .simple_query("INSERT INTO s1.t (x) VALUES (1)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO s2.t (x) VALUES (2)")
        .await
        .unwrap();
    client
        .simple_query("SET search_path = s1, s2")
        .await
        .unwrap();

    // we should be selecting from s1, which is upstream
    let result = client
        .query_one("SELECT x FROM t", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 1);

    // Now drop the non-replicated table, and make sure the next query reads from the second table,
    // against readyset

    client.simple_query("DROP TABLE s1.t").await.unwrap();

    sleep().await;

    // we should be selecting from s2 now
    let result = client
        .query_one("SELECT x FROM t", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 2);

    assert!(last_statement_matches("readyset", "ok", &client).await);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn generated_columns() {
    // Tests that we handle tables that have generated columns by not snapshotting them and falling
    // back to upstream
    readyset_tracing::init_test_logging();

    let mut upstream_config = upstream_config();
    upstream_config.dbname("noria");
    let fallback_conn = connect(upstream_config).await;
    let res = fallback_conn
        .simple_query("DROP TABLE IF EXISTS calc_columns CASCADE")
        .await
        .expect("create failed");
    assert!(matches!(res[0], SimpleQueryMessage::CommandComplete(_)));

    let res = fallback_conn
        .simple_query(
            "CREATE TABLE calc_columns(
        col1 numeric,
        col2 numeric,
        col3 numeric GENERATED ALWAYS AS (col1 + col2) STORED,
        col4 bool GENERATED ALWAYS AS (col1 = col2) STORED
    )",
        )
        .await
        .expect("create failed");
    assert!(matches!(res[0], SimpleQueryMessage::CommandComplete(_)));

    let res = fallback_conn
        .simple_query("INSERT INTO calc_columns (col1, col2) VALUES(1, 2)")
        .await
        .expect("populate failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    ));

    let res = fallback_conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");

    // CommandComplete and 1 row should be returned
    assert_eq!(res.len(), 2);

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::upstream_url("noria"))
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = connect(opts).await;
    // Check that we see the existing insert
    let res = conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");
    // CommandComplete and the 1 inserted rows
    assert_eq!(res.len(), 2);

    // This should fail, since we don't have a base table for calc_columns, as it was ignored in the
    // initial snapshot
    conn.simple_query("CREATE CACHE FROM SELECT * from calc_columns")
        .await
        .expect_err("create cache should have failed");

    // There shouldnt be any caches
    let res = conn
        .simple_query("SHOW CACHES")
        .await
        .expect("show caches failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
    ));

    // Inserting will go to upstream
    let populate_gen_columns_readyset = "INSERT INTO calc_columns (col1, col2) VALUES(3, 4);";
    let res = conn
        .simple_query(populate_gen_columns_readyset)
        .await
        .expect("populate failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    ));

    // We should immediately see the inserted data via the upstream connection
    // because the write synchronously falls back to upstream
    let res = fallback_conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");
    assert!(matches!(
        res[2],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 2, .. })
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unsuported_numeric_scale() {
    // Tests that we handle tables that have NUMERIC values with scales > 28
    // by not snapshotting them and falling back to upstream
    readyset_tracing::init_test_logging();

    let mut upstream_config = upstream_config();
    upstream_config.dbname("noria");
    let fallback_conn = connect(upstream_config).await;
    let res = fallback_conn
        .simple_query("DROP TABLE IF EXISTS t CASCADE")
        .await
        .expect("create failed");
    assert!(matches!(res[0], SimpleQueryMessage::CommandComplete(_)));

    let res = fallback_conn
        .simple_query("CREATE TABLE t (c NUMERIC)")
        .await
        .expect("create failed");
    assert!(matches!(res[0], SimpleQueryMessage::CommandComplete(_)));

    let res = fallback_conn
        .simple_query("INSERT INTO t VALUES(0.00000000000000000000000000001)") // scale=29
        .await
        .expect("populate failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    ));

    let res = fallback_conn
        .simple_query("SELECT * FROM t")
        .await
        .expect("select failed");
    // CommandComplete and 1 row should be returned
    assert_eq!(res.len(), 2);

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::upstream_url("noria"))
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = connect(opts).await;
    // Check that we see the existing insert
    let res = conn
        .simple_query("SELECT * FROM t")
        .await
        .expect("select failed");
    // CommandComplete and the 1 inserted rows
    assert_eq!(res.len(), 2);

    // This should fail, since we don't have a base table, as it was ignored in the
    // initial snapshot
    conn.simple_query("CREATE CACHE FROM SELECT * from t")
        .await
        .expect_err("create cache should have failed");

    // There shouldnt be any caches
    let res = conn
        .simple_query("SHOW CACHES")
        .await
        .expect("show caches failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
    ));

    // Inserting will go to upstream
    let res = conn
        .simple_query("INSERT INTO t VALUES(0)")
        .await
        .expect("populate failed");
    assert!(matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    ));

    // We should immediately see the inserted data via the upstream connection
    // because the write synchronously falls back to upstream
    let res = fallback_conn
        .simple_query("SELECT * FROM t")
        .await
        .expect("select failed");
    assert!(matches!(
        res[2],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 2, .. })
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "ENG-2548 Test reproduces client panic due to known bug"]
async fn add_column_then_read() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (id int)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO cats (id) VALUES (1)")
        .await
        .unwrap();

    client
        .simple_query("ALTER TABLE cats ADD COLUMN meow VARCHAR")
        .await
        .unwrap();

    let result = client
        .query_one("SELECT * FROM cats", &[])
        .await
        .unwrap()
        .get::<_, Option<String>>(1);
    assert_eq!(result, None);

    shutdown_tx.shutdown().await;
}

#[ignore = "ENG-2575 Test reproduces client error due to known bug"]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn drop_column_then_read() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (id INT, meow VARCHAR)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO cats VALUES (1, 'purr')")
        .await
        .unwrap();

    client
        .simple_query("ALTER TABLE cats DROP COLUMN meow")
        .await
        .unwrap();

    // Due to ENG-2575, this currently returns a DbError with reason:
    // "encode error: internal error: incorrect DataRow transfer format length"
    let result = client
        .query_one("SELECT * FROM cats", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);

    assert_eq!(result, 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn deletion_propagation_after_alter() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (id INT)")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE dogs (id INT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO cats VALUES (1)")
        .await
        .unwrap();

    // Note that although we don't actually write or read rows in the "dogs" table, running the
    // ALTER against a separate table from the one we're querying seems to be necessary to
    // replicate this bug.
    client
        .simple_query("ALTER TABLE dogs ADD COLUMN bark VARCHAR")
        .await
        .unwrap();

    // I'd rather use eventually! but can't use it twice in the same function due to weird
    // namespacing issues that I haven't figured out yet, so sleep() will have to do here for now.
    sleep().await;

    let result = client
        .query_one("SELECT * FROM cats", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);

    assert_eq!(result, 1);

    client
        .simple_query("DELETE FROM cats WHERE id = 1")
        .await
        .unwrap();

    eventually!(run_test: {
        let result = client
            .query_one("SELECT * FROM cats", &[])
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        result().unwrap_err();
    });

    shutdown_tx.shutdown().await;
}

#[ignore = "ENG-3041 Test reproduces write drop due to known bug"]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn write_propagation_after_alter_and_drop() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE cats (meow INT)")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE dogs (woof INT)")
        .await
        .unwrap();

    sleep().await;

    client
        .simple_query("ALTER TABLE cats RENAME COLUMN meow TO purr")
        .await
        .unwrap();

    client.simple_query("DROP TABLE cats").await.unwrap();

    client
        .simple_query("INSERT INTO dogs VALUES (1)")
        .await
        .unwrap();

    eventually!(run_test: {
        client
            .query("SELECT woof FROM dogs", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>()
    }, then_assert: |result| {
        assert_eq!(result, vec![1]);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn rename_column_then_create_view() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE t (i int);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (i) VALUES (1)")
        .await
        .unwrap();
    client
        .simple_query("ALTER TABLE t RENAME COLUMN i TO j;")
        .await
        .unwrap();
    client
        .simple_query("CREATE VIEW v AS SELECT * FROM t;")
        .await
        .unwrap();

    eventually! {
        let res = client
            .query("SELECT * FROM v;", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<_, i32>(0))
            .collect::<Vec<_>>();

        last_statement_matches("readyset", "ok", &client).await
            && res == vec![1]
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn alter_enum_after_drop() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TYPE et AS ENUM ('a')")
        .await
        .unwrap();

    client.simple_query("CREATE TABLE t (e et)").await.unwrap();

    client.simple_query("DROP TABLE t").await.unwrap();

    client
        .simple_query("ALTER TYPE et ADD VALUE 'b'")
        .await
        .unwrap();

    // The rest of the test is not technically needed for triggering the bug, but without it the
    // test will appear to pass. Creating and querying the table will not cause the test to fail,
    // but it at least causes the test to hang.
    client
        .simple_query("CREATE TABLE t2 (id INT)")
        .await
        .unwrap();

    sleep().await;

    client.simple_query("SELECT * FROM t2").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[ignore = "ENG-2823 Test reproduces error due to known bug"]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn alter_enum_rename_value() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TYPE et AS ENUM ('a')")
        .await
        .unwrap();

    client.simple_query("CREATE TABLE t (e et)").await.unwrap();

    client
        .simple_query("ALTER TYPE et RENAME VALUE 'a' TO 'b'")
        .await
        .unwrap();

    client
        .simple_query("CREATE CACHE ALWAYS FROM SELECT e FROM t")
        .await
        .unwrap();

    eventually!(run_test: {
        let res = client.simple_query("SELECT e FROM t").await;
        AssertUnwindSafe(|| res)
    }, then_assert: |res| res().unwrap());

    shutdown_tx.shutdown().await;
}

#[allow(dead_code)]
async fn last_statement_matches(dest: &str, status: &str, client: &Client) -> bool {
    match &client
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .expect("explain query failed")[0]
    {
        SimpleQueryMessage::Row(row) => {
            let dest_col = row.get(0).expect("should have 2 cols");
            let status_col = row.get(1).expect("should have 2 cols");
            dest_col.contains(dest) && status_col.contains(status)
        }
        _ => panic!("should have 1 row"),
    }
}

#[cfg(feature = "failure_injection")]
async fn setup_for_replication_failure(client: &Client) {
    client
        .simple_query("DROP TABLE IF EXISTS cats CASCADE")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    client
        .simple_query("CREATE VIEW cats_view AS SELECT id FROM cats ORDER BY id ASC")
        .await
        .unwrap();
    sleep().await;

    client
        .simple_query("INSERT INTO cats (id) VALUES (1)")
        .await
        .unwrap();

    sleep().await;
    sleep().await;

    assert!(last_statement_matches("readyset", "ok", &client).await);
    client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats")
        .await
        .unwrap();
    client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats_view")
        .await
        .unwrap();
    sleep().await;

    let result = client
        .query_one("SELECT * FROM cats", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 1);
    let result = client
        .query_one("SELECT * FROM cats_view", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 1);
}

#[cfg(feature = "failure_injection")]
async fn assert_table_ignored(client: &Client) {
    let res = client.simple_query("SHOW CACHES").await.unwrap();
    let res = res.first().unwrap();
    assert!(matches!(
        res,
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
    ));

    client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats")
        .await
        .expect_err("should fail to create cache now that table is ignored");

    for source in ["cats", "cats_view"] {
        let result = client
            .simple_query(&format!("SELECT * FROM {source}"))
            .await
            .unwrap();
        let c1 = match result.get(0).expect("should have 2 rows") {
            SimpleQueryMessage::Row(r) => r.get(0).expect("should have 1 col"),
            _ => panic!("should be a row"),
        };
        let c2 = match result.get(1).expect("should have 2 rows") {
            SimpleQueryMessage::Row(r) => r.get(0).expect("should have 1 col"),
            _ => panic!("should be a row"),
        };

        let mut results = vec![c1, c2];
        results.sort();
        assert_eq!(results, vec!["1", "2"]);
        assert!(last_statement_matches("readyset_then_upstream", "view destroyed", &client).await);
    }
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn handle_action_replication_failure_ignores_table() {
    replication_failure_ignores_table(readyset_client::failpoints::REPLICATION_HANDLE_ACTION).await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn next_action_replication_failure_ignores_table() {
    replication_failure_ignores_table(
        readyset_client::failpoints::POSTGRES_REPLICATION_NEXT_ACTION,
    )
    .await;
}

#[cfg(feature = "failure_injection")]
async fn replication_failure_ignores_table(failpoint: &str) {
    readyset_tracing::init_test_logging();
    use nom_sql::Relation;
    use readyset_errors::ReadySetError;

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;

    // Tests that if a table fails replication due to a TableError, it is dropped and we stop
    // replicating it going forward
    let client = connect(config).await;

    setup_for_replication_failure(&client).await;

    let err_to_inject = ReadySetError::TableError {
        table: Relation {
            schema: Some("public".into()),
            name: "cats".into(),
        },
        source: Box::new(ReadySetError::Internal("failpoint injected".to_string())),
    };

    handle
        .set_failpoint(
            failpoint,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
            ),
        )
        .await;

    client
        .simple_query("INSERT INTO cats (id) VALUES (2)")
        .await
        .unwrap();

    sleep().await;

    assert_table_ignored(&client).await;

    shutdown_tx.shutdown().await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn handle_action_replication_failure_retries_if_failed_to_drop() {
    replication_failure_retries_if_failed_to_drop(
        readyset_client::failpoints::REPLICATION_HANDLE_ACTION,
    )
    .await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn next_action_replication_failure_retries_if_failed_to_drop() {
    replication_failure_retries_if_failed_to_drop(
        readyset_client::failpoints::POSTGRES_REPLICATION_NEXT_ACTION,
    )
    .await;
}

#[cfg(feature = "failure_injection")]
async fn replication_failure_retries_if_failed_to_drop(failpoint: &str) {
    readyset_tracing::init_test_logging();
    use std::time::Duration;

    use nom_sql::Relation;
    use readyset_errors::ReadySetError;
    use tracing::info;

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<PostgreSQLAdapter>()
        .await;

    // Tests that if a table fails replication due to a TableError, and we fail to drop it as part
    // of ignoring it, we resnapshot and succeed on a retry.
    // replicating it going forward
    let client = connect(config).await;

    setup_for_replication_failure(&client).await;
    info!("setup complete");

    // Inject an error in replication
    let err_to_inject = ReadySetError::TableError {
        table: Relation {
            schema: Some("public".into()),
            name: "cats".into(),
        },
        source: Box::new(ReadySetError::Internal("failpoint injected".to_string())),
    };

    handle
        .set_failpoint(
            failpoint,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
            ),
        )
        .await;

    let err_to_inject = ReadySetError::ResnapshotNeeded;

    // Inject an error in handling the previous error
    handle
        .set_failpoint(
            "ignore-table-fail-dropping-table",
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
            ),
        )
        .await;

    client
        .simple_query("INSERT INTO cats (id) VALUES (2)")
        .await
        .unwrap();

    // There isn't a great way to assert that we resnapshotted here, so confirmed we did via log
    // inspection

    // We have to sleep extra here since we have a sleep between resnapshots
    tokio::time::sleep(Duration::from_secs(5)).await;
    sleep().await;

    let expected = vec![1, 2];
    let mut result: Vec<u32> = client
        .simple_query("SELECT * FROM cats")
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
            _ => None,
        })
        .collect();
    // sort because return order isn't guaranteed
    result.sort();
    assert_eq!(result, expected);

    // dont sort because cats_view uses order by
    let result: Vec<u32> = client
        .simple_query("SELECT * FROM cats_view")
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(result, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn replication_of_other_tables_succeeds_even_after_error() {
    readyset_tracing::init_test_logging();
    use std::time::Duration;

    let (config, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::upstream_url("noria"))
        .migration_mode(MigrationMode::InRequestPath)
        .build::<PostgreSQLAdapter>()
        .await;

    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats CASCADE")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS cats2 CASCADE")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats (id SERIAL PRIMARY KEY, cuteness int)")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats2 (id SERIAL PRIMARY KEY, ts TIMESTAMP)")
        .await
        .unwrap();

    sleep().await;
    sleep().await;

    client
        .simple_query(
            "INSERT INTO cats (cuteness) VALUES (100); INSERT INTO cats2 (ts) VALUES ('infinity')",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    sleep().await;

    let result: Vec<u32> = client
        .simple_query("SELECT * FROM cats")
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(result, [1]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn drop_cache_implicit_caching() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::upstream_url("noria"))
        .migration_mode(MigrationMode::InRequestPath)
        .build::<PostgreSQLAdapter>()
        .await;

    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats CASCADE")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE cats (id SERIAL PRIMARY KEY, cuteness int)")
        .await
        .unwrap();

    // Allow table to be replicated
    sleep().await;
    sleep().await;

    // Cache an ad-hoc and prepared query
    eventually! {
        let _ = client
            .simple_query("SELECT * FROM cats;")
            .await
            .unwrap();

        last_statement_matches("readyset", "ok", &client).await
    }

    eventually! {
        let _ = client.query("SELECT id FROM cats;", &[]).await.unwrap();

        last_statement_matches("readyset", "ok", &client).await
    }

    // Obtain both cache names
    let caches: Vec<String> = client
        .simple_query("SHOW CACHES")
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();

    // Drop both caches
    let _ = client
        .simple_query(&format!("DROP CACHE {};", caches[0]))
        .await
        .unwrap();
    let _ = client
        .simple_query(&format!("DROP CACHE {};", caches[1]))
        .await
        .unwrap();

    // Ensure that we go successfully to upstream. This indicates that we did not attempt to
    // re-cache the query
    eventually! {
        let _ = client
            .simple_query("SELECT * FROM cats;")
            .await
            .unwrap();

        last_statement_matches("upstream", "ok", &client).await
    }

    eventually! {
        let _ = client.query("SELECT id FROM cats;", &[]).await.unwrap();

        last_statement_matches("upstream", "ok", &client).await
    }

    // Let's make sure we can re-cache the queries
    let _ = client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats;")
        .await
        .unwrap();
    let _ = client
        .simple_query("CREATE CACHE FROM SELECT id FROM cats;")
        .await
        .unwrap();

    eventually! {
        let _ = client
            .simple_query("SELECT * FROM cats;")
            .await
            .unwrap();

        last_statement_matches("readyset", "ok", &client).await
    }

    eventually! {
        let _ = client.query("SELECT id FROM cats;", &[]).await.unwrap();

        last_statement_matches("readyset", "ok", &client).await
    }

    shutdown_tx.shutdown().await;
}
