use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use assert_matches::assert_matches;
use chrono::NaiveDate;
use postgres_types::private::BytesMut;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::{MigrationMode, UnsupportedSetMode};
use readyset_adapter::query_status_cache::MigrationStyle;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{PostgreSQLAdapter, connect, upstream_config};
use readyset_client_test_helpers::{TestBuilder, sleep};
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_util::eventually;
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::shutdown::ShutdownSender;
use regex::Regex;
use test_utils::{tags, upstream};

use crate::common::setup_standalone_with_authority;
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use tokio_postgres::{Client, CommandCompleteContents, SimpleQueryMessage};

async fn setup() -> (tokio_postgres::Config, Handle, ShutdownSender) {
    TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

macro_rules! assert_last_statement_matches {
    ($table:expr_2021, $dest:expr_2021, $status:expr_2021, $client:expr_2021) => {
        let (matches, err) = last_statement_matches($dest, $status, $client).await;
        assert!(
            matches,
            "EXPLAIN LAST STATEMENT mismatch for query involving table {}: {}",
            $table, err
        );
    };
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn delete_case_sensitive() {
    for (opts, _handle, shutdown_tx) in [
        TestBuilder::default()
            .replicate(false)
            .build::<PostgreSQLAdapter>()
            .await,
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
            assert_matches!(
                deleted,
                SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
            );
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
    eventually!(run_test: {
        let result = conn.query(&stmt, &[&1i64]).await.unwrap();
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        assert_eq!(result().first().unwrap().get::<_, i64>(0), 1);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
        NaiveDate::from_ymd_opt(2022, 3, 5).unwrap()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn schema_resolution_with_unreplicated_tables() {
    readyset_tracing::init_test_logging();
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    // s2.t will exist in readyset
    client
        .simple_query(
            "CREATE SCHEMA s1; CREATE SCHEMA s2;
             CREATE TABLE s2.t (x INT)",
        )
        .await
        .unwrap();

    sleep().await;

    // s1.t uses an unsupported type, so it will only exist upstream
    client
        .simple_query("CREATE TABLE s1.t (x INET)")
        .await
        .unwrap();

    sleep().await;

    client
        .simple_query("INSERT INTO s1.t (x) VALUES ('127.0.0.1')")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO s2.t (x) VALUES (42)")
        .await
        .unwrap();
    client
        .simple_query("SET search_path = s1, s2")
        .await
        .unwrap();

    // we should be selecting from s1, which is upstream
    let result = client
        .query_one("SELECT CAST(x AS TEXT) FROM t", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(result, "127.0.0.1/32");

    // Now drop the non-replicated table, and make sure the next query reads from the second table,
    // against readyset

    client.simple_query("DROP TABLE s1.t").await.unwrap();

    sleep().await;

    // we should be selecting from s2 now
    let result = client
        .query_one("SELECT CAST(x AS TEXT) FROM t", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(result, "42");

    assert_last_statement_matches!("t", "readyset", "ok", &client);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
    assert_matches!(res[0], SimpleQueryMessage::CommandComplete(_));

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
    assert_matches!(res[0], SimpleQueryMessage::CommandComplete(_));

    let res = fallback_conn
        .simple_query("INSERT INTO calc_columns (col1, col2) VALUES(1, 2)")
        .await
        .expect("populate failed");
    assert_matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    );

    let res = fallback_conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");

    // CommandComplete and 1 row should be returned
    assert_eq!(res.len(), 2);

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
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
    assert_matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
    );

    // Inserting will go to upstream
    let populate_gen_columns_readyset = "INSERT INTO calc_columns (col1, col2) VALUES(3, 4);";
    let res = conn
        .simple_query(populate_gen_columns_readyset)
        .await
        .expect("populate failed");
    assert_matches!(
        res[0],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    );

    // We should immediately see the inserted data via the upstream connection
    // because the write synchronously falls back to upstream
    let res = fallback_conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");
    assert_matches!(
        res[2],
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 2, .. })
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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

    eventually!(
        run_test: {
            client
                .query("SELECT * FROM cats", &[])
                .await
                .unwrap()
                .first()
                .map(|row| row.get(0))
        },
        then_assert: |result| {
            assert_eq!(result, Some(1));
        }
    );

    client
        .simple_query("DELETE FROM cats WHERE id = 1")
        .await
        .unwrap();

    eventually!(client.query_one("SELECT * FROM cats", &[]).await.is_err());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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

        last_statement_matches("readyset", "ok", &client).await.0
            && res == vec![1]
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn insert_enum_value_appended_after_create_table() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TYPE et AS ENUM ('a')")
        .await
        .unwrap();

    client.simple_query("CREATE TABLE t1 (e et)").await.unwrap();

    client.query("SELECT * FROM t1", &[]).await.unwrap();

    client
        .simple_query("ALTER TYPE et ADD VALUE 'b'")
        .await
        .unwrap();

    client.simple_query("CREATE TABLE t2 (e et)").await.unwrap();

    // Due to the bug documented in REA-3108, this used to fail with "decode error: unknown enum
    // variant: b". Note that in order to trigger this bug we had to:
    //  - Create a table using the enum type *and* select from it prior to altering the type (though
    //    this doesn't have to be the same table we later insert into)
    //  - Specifically insert the enum value that was added in the ALTER TYPE statement
    //  - Insert using a parameter, not a hardcoded query (hence the use of `query_raw` here)
    // This turned out to be caused by an integration with a client library that cached types from
    // upstream queries and didn't update the cached definitions after the type was altered.
    let params: Vec<DfValue> = vec!["b".into()];
    client
        .query_raw("INSERT INTO t2 VALUES ($1)", &params)
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn insert_array_of_enum_value_appended_after_create_table() {
    // This test is based off insert_enum_value_appended_after_create_table but inserts an array of
    // enum values instead of a single value, which triggers a similar bug to the one that the
    // former test did. The array version of the bug that this test reproduces is documented in
    // REA-3143.

    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TYPE et AS ENUM ('a')")
        .await
        .unwrap();

    client.simple_query("CREATE TABLE t1 (e et)").await.unwrap();

    client.query("SELECT * FROM t1", &[]).await.unwrap();

    client
        .simple_query("ALTER TYPE et ADD VALUE 'b'")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE t2 (e et[])")
        .await
        .unwrap();

    let params: Vec<DfValue> = vec![DfValue::from(vec![DfValue::from("b")])];
    client
        .query_raw("INSERT INTO t2 VALUES ($1)", &params)
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

async fn last_statement_matches(dest: &str, status: &str, client: &Client) -> (bool, String) {
    match &client
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .expect("explain query failed")[0]
    {
        SimpleQueryMessage::Row(row) => {
            let dest_col = row.get(0).expect("should have 2 cols");
            let status_col = row.get(1).expect("should have 2 cols");
            if !dest_col.contains(dest) {
                return (
                    false,
                    format!(
                        r#"dest column was expected to contain "{dest}", but was: "{dest_col}" (status: "{status_col}")"#
                    ),
                );
            }
            if !Regex::new(status).unwrap().is_match(status_col) {
                return (
                    false,
                    format!(
                        r#"status column was expected to contain "{status}", but was: "{status_col}" (dest: "{dest_col}")"#
                    ),
                );
            }
            (true, "".to_string())
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

    assert_last_statement_matches!("cats", "upstream", "ok", client);
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
    client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats")
        .await
        .expect_err("should fail to create cache now that table is ignored");
    client
        .simple_query("CREATE CACHE FROM SELECT * FROM cats_view")
        .await
        .expect_err("should fail to create cache now that table is ignored");

    for source in ["cats", "cats_view"] {
        let result = client
            .simple_query(&format!("SELECT * FROM {source}"))
            .await
            .unwrap();
        let c1 = match result.first().expect("should have 2 rows") {
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
        assert_last_statement_matches!(source, "upstream", "view destroyed|ok", client);
    }
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn handle_action_replication_failure_ignores_table() {
    replication_failure_ignores_table(readyset_util::failpoints::REPLICATION_HANDLE_ACTION).await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn next_action_replication_failure_ignores_table() {
    replication_failure_ignores_table(readyset_util::failpoints::POSTGRES_REPLICATION_NEXT_ACTION)
        .await;
}

#[cfg(feature = "failure_injection")]
async fn replication_failure_ignores_table(failpoint: &str) {
    readyset_tracing::init_test_logging();
    use readyset_errors::ReadySetError;
    use readyset_sql::ast::Relation;

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
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

    // Check That we don't see any 'Last replicator error' rows
    let results = client
        .simple_query("SHOW READYSET STATUS")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| {
            if let SimpleQueryMessage::Row(r) = m {
                r.get(0).map(String::from)
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    assert!(results.iter().all(|r| !r.contains("Last replicator error")));

    shutdown_tx.shutdown().await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn handle_action_replication_failure_retries_if_failed_to_drop() {
    replication_failure_retries_if_failed_to_drop(
        readyset_util::failpoints::REPLICATION_HANDLE_ACTION,
    )
    .await;
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn next_action_replication_failure_retries_if_failed_to_drop() {
    replication_failure_retries_if_failed_to_drop(
        readyset_util::failpoints::POSTGRES_REPLICATION_NEXT_ACTION,
    )
    .await;
}

#[cfg(feature = "failure_injection")]
async fn replication_failure_retries_if_failed_to_drop(failpoint: &str) {
    readyset_tracing::init_test_logging();

    use readyset_errors::ReadySetError;
    use readyset_sql::ast::Relation;
    use tracing::info;

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
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
            failpoints::IGNORE_TABLE_FAIL_DROPPING_TABLE,
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

    let expected = vec![1, 2];

    // There isn't a great way to assert that we resnapshotted here, so just retry
    eventually!(
        run_test: {
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
            result
        },
        then_assert: |result| {
            assert_eq!(result, expected);
        }
    );

    eventually!(
        run_test: {
            // dont sort because cats_view uses order by
            client
                .simple_query("SELECT * FROM cats_view")
                .await
                .unwrap()
                .iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                    _ => None,
                })
                .collect::<Vec<u32>>()
        },
        then_assert: |result| {
            assert_eq!(result, expected);
        }
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn replication_of_other_tables_succeeds_even_after_error() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
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

    eventually!(
        run_test: {
            client
                .simple_query("SELECT * FROM cats")
                .await
                .unwrap()
                .iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                    _ => None,
                })
                .collect::<Vec<u32>>()
        },
        then_assert: |result| {
            assert_eq!(result, [1]);
        }
    );

    let destination = match client
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_owned(),
        _ => panic!(),
    };

    assert_matches!(
        destination.as_str().try_into(),
        Ok(QueryDestination::Readyset(Some(_)))
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
#[ignore = "REA-3933 (see comments on ticket)"]
async fn show_proxied_queries_show_caches_query_text_matches() {
    readyset_tracing::init_test_logging();
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("CREATE TABLE t (id INT)")
        .await
        .unwrap();

    let (cache_name, cached_query_text) = eventually! {
        run_test: {
            client.simple_query("SELECT id FROM t").await.unwrap();
            client.simple_query("SHOW CACHES").await.unwrap()
        },
        then_assert: |cached_queries| {
            match cached_queries.into_iter().next().unwrap() {
                SimpleQueryMessage::Row(row) => {
                    (row.get(1).unwrap().to_owned(), row.get(2).unwrap().to_owned())
                },
                _ => panic!(),
            }
        }
    };

    client
        .simple_query(&format!("DROP CACHE {cache_name}"))
        .await
        .unwrap();

    let proxied_queries = client.simple_query("SHOW PROXIED QUERIES").await.unwrap();
    let proxied_query_text = match proxied_queries.first().unwrap() {
        SimpleQueryMessage::Row(row) => row.get(1).unwrap(),
        _ => panic!(),
    };

    assert_eq!(proxied_query_text, cached_query_text);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn insert_delete_a_record_in_the_same_transaction() {
    readyset_tracing::init_test_logging();
    let (config, _handle, shutdown_tx) = setup().await;
    let mut client = connect(config).await;
    client.simple_query("create table t(a int)").await.unwrap();
    {
        let transaction = client.transaction().await.unwrap();
        // Begin transaction
        transaction.batch_execute("BEGIN").await.unwrap();

        transaction
            .execute("INSERT INTO t VALUES($1)", &[&1i32])
            .await
            .unwrap();
        transaction.execute("delete from t", &[]).await.unwrap();

        // Commit the transaction
        transaction.batch_execute("COMMIT").await.unwrap();
    }

    // Check if all the records have been deleted
    let rows = client.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 0);
    shutdown_tx.shutdown().await;
}

// Tests that we correctly replicate the events that occur while we are handling a resnapshot
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn pgsql_test_replication_after_resnapshot() {
    use std::time::Duration;

    readyset_tracing::init_test_logging();

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;
    let mut upstream_config = upstream_config();
    upstream_config.dbname("noria");
    let upstream = connect(upstream_config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();

    // This failpoint is placed after we've created the replication slot and before we've started
    // replication, which means any data we insert into the upstream database while the replicator
    // is paused there will not be present in our initial snapshot
    handle
        .set_failpoint(
            readyset_util::failpoints::POSTGRES_SNAPSHOT_START,
            "sleep(5000)",
        )
        .await;

    // This will trigger a resnapshot
    client
        .simple_query("ALTER TABLE cats ADD COLUMN a int")
        .await
        .unwrap();

    // Sleep until the resnapshot has started
    tokio::time::sleep(Duration::from_secs(5)).await;

    // The replicator is sleeping at the failpoint, so we know a replication slot has been created,
    // which means any data added here won't be reflected in the re-snapshot
    for i in 0..10 {
        client
            .simple_query(&format!("INSERT INTO cats (id) VALUES ({i})"))
            .await
            .unwrap();
    }

    let upstream_results: Vec<DfValue> = upstream
        .query("SELECT * FROM cats", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, DfValue>(0))
        .collect();

    eventually!(run_test: {
        let result = client
            .query("SELECT * FROM cats", &[])
            .await;
        AssertUnwindSafe(|| result)
    }, then_assert: |result| {
        let cached_results: Vec<DfValue> = result()
            .unwrap()
            .into_iter()
            .map(|row| row.get::<_, DfValue>(0))
            .collect();

        assert_eq!(upstream_results, cached_results);
    });

    shutdown_tx.shutdown().await;
}

// Tests that we end up with the correct data if replication restarts after we've partially applied
// a commit
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn start_replication_in_middle_of_commit() {
    use std::time::Duration;

    use readyset_errors::ReadySetError;

    readyset_tracing::init_test_logging();
    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::InRequestPath)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats1 CASCADE")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS cats2 CASCADE")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats1 (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats2 (id int, PRIMARY KEY(id))")
        .await
        .unwrap();

    sleep().await;

    // Add a delay between our handling of replication actions to ensure that the transaction is
    // only partially applied to the base tables before we are able to trigger an error
    handle
        .set_failpoint(readyset_util::failpoints::UPSTREAM, "sleep(50)")
        .await;

    // Execute a transaction with many entries. We alternate inserts between two tables to ensure
    // that a replication action is returned for every insert (typically, replication actions are
    // batched when they originate from the same table)
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..10 {
        if i % 2 == 0 {
            client
                .simple_query(&format!("INSERT INTO cats1 (id) VALUES ({i})"))
                .await
                .unwrap();
        } else {
            client
                .simple_query(&format!("INSERT INTO cats2 (id) VALUES ({i})"))
                .await
                .unwrap();
        }
    }
    client.simple_query("COMMIT;").await.unwrap();

    // Sleep to ensure that some of the inserts from above are applied to the base tables
    sleep().await;

    // By now, only some of insert events will have been applied to base tables. We trigger an error
    // here to prompt a restart of the replication loop. Since we are restarting the replication
    // loop in the middle of a commit, we'll be starting replication at an LSN in the middle of a
    // commit, which is exactly what we want to test here
    handle
        .set_failpoint(
            readyset_util::failpoints::UPSTREAM,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&ReadySetError::ReplicationFailed("error".into()))
                    .expect("failed to serialize error")
            ),
        )
        .await;

    // Sleep to wait out the replicator's error timeout period and allow the replicator to catch up
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut upstream_config = upstream_config();
    upstream_config.dbname("noria");
    let upstream = connect(upstream_config).await;

    let cached_results_cats1: Vec<DfValue> = client
        .query("SELECT * FROM cats1", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, DfValue>(0))
        .collect();

    let upstream_results_cats1: Vec<DfValue> = upstream
        .query("SELECT * FROM cats1", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, DfValue>(0))
        .collect();

    let cached_results_cats2: Vec<DfValue> = client
        .query("SELECT * FROM cats2", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, DfValue>(0))
        .collect();

    let upstream_results_cats2: Vec<DfValue> = upstream
        .query("SELECT * FROM cats2", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, DfValue>(0))
        .collect();

    // Check to make sure the results are the same between the upstream database and ReadySet
    assert_eq!(cached_results_cats1, upstream_results_cats1);
    assert_eq!(cached_results_cats2, upstream_results_cats2);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn column_metadata() {
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;
    client.simple_query("create table t(a int)").await.unwrap();
    let table_oid = client
        .query_one("select 't'::regclass::oid", &[])
        .await
        .unwrap()
        .get::<_, u32>(0);

    let cached_stmt = client.prepare("select a from t").await.unwrap();
    assert_eq!(cached_stmt.columns()[0].table_oid(), Some(table_oid));
    assert_eq!(cached_stmt.columns()[0].column_id(), Some(1));

    let proxied_stmt = client.prepare("select a, now() from t").await.unwrap();
    assert_eq!(proxied_stmt.columns()[0].table_oid(), Some(table_oid));
    assert_eq!(proxied_stmt.columns()[0].column_id(), Some(1));

    shutdown_tx.shutdown().await;
}

// Tests that we correctly replicate the events that occur while we are handling a resnapshot with a
// subsequent catchup period
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn pgsql_test_replication_after_resnapshot_with_catchup() {
    readyset_tracing::init_test_logging();

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .migration_mode(MigrationMode::InRequestPath)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats1")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS cats2")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats1 (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats2 (id int, PRIMARY KEY(id))")
        .await
        .unwrap();

    // This failpoint is placed after we've created the replication slot and before we've started
    // replication, which means any data we insert into the upstream database while the replicator
    // is paused there will not be present in our initial snapshot
    handle
        .set_failpoint(
            readyset_util::failpoints::POSTGRES_SNAPSHOT_START,
            "sleep(5000)",
        )
        .await;

    // This will cause `cats1` to be re-snapshotted. Since `cats2` is not being re-snapshotted here,
    // once the snapshot is completed, `cats1` and `cats2` will have different replication offsets,
    // which will prompt us to enter catchup mode
    client
        .simple_query("ALTER TABLE cats1 ADD COLUMN a int")
        .await
        .unwrap();

    // Sleep until the re-snapshot has started
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Since a replication slot has been created and that slot has a Postgres snapshot and an
    // associated consistent point, any data written after the creation of the slot won't be
    // reflected in ReadySet's re-snapshot using this slot. In other words, any data added here will
    // only be replicated as part of the "catchup" that occurs after the re-snapshot finishes.
    //
    // Only the cats1 table is going to be re-snapshotted, so we perform writes here to ensure that
    // the min and max replication offsets across the schema/our tables are different. This prompts
    // us to enter "catchup" mode after the resnapshot is finished.
    client
        .simple_query("INSERT INTO cats2 (id) VALUES (1)")
        .await
        .unwrap();

    eventually!(run_test: {
        let res = client
            .simple_query("SELECT * FROM cats2")
            .await;
        AssertUnwindSafe(|| res)
    }, then_assert: |result| {
        let res: Vec<u32> = result()
            .unwrap()
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                _ => None,
            })
            .collect();

        assert_eq!(res, [1]);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn create_view_then_drop_table_then_create_view_with_same_name() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS t1 CASCADE")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS t2 CASCADE")
        .await
        .unwrap();

    let queries = [
        r#"CREATE TABLE "a" (aa int)"#,
        r#"CREATE TABLE "t2" (id int)"#,
        r#"CREATE VIEW "v" AS SELECT * FROM "a""#,
        r#"CREATE CACHE ALWAYS FROM SELECT * FROM "v""#,
        r#"DROP TABLE "a" CASCADE"#,
        r#"CREATE VIEW "v" AS SELECT * FROM "t2""#,
        r#"CREATE CACHE ALWAYS FROM SELECT * FROM "v""#,
    ];

    for query in queries {
        eventually!(run_test: {
            let result = client
                .simple_query(query)
                .await;
            AssertUnwindSafe(|| result)
        }, then_assert: |result| {
            result().unwrap();
        });
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn create_view_then_drop_view_then_create_view_with_same_name() {
    readyset_tracing::init_test_logging();

    let (config, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS t1 CASCADE")
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS t2 CASCADE")
        .await
        .unwrap();

    let queries = [
        r#"CREATE TABLE "a" (aa int)"#,
        r#"CREATE TABLE "t2" (id int)"#,
        r#"CREATE VIEW "v" AS SELECT * FROM "a""#,
        r#"CREATE CACHE ALWAYS FROM SELECT * FROM "v""#,
        r#"DROP VIEW v"#,
        r#"CREATE VIEW "v" AS SELECT * FROM "t2""#,
        r#"CREATE CACHE ALWAYS FROM SELECT * FROM "v""#,
    ];

    for query in queries {
        eventually!(run_test: {
            let result = client
                .simple_query(query)
                .await;
            AssertUnwindSafe(|| result)
        }, then_assert: |result| {
            result().unwrap();
        });
    }

    shutdown_tx.shutdown().await;
}

// Tests that we correctly perform a full resnapshot when our replication slot becomes invalidated
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn start_replication_invalidated_replication_slot() {
    use readyset_errors::ReadySetError;

    readyset_tracing::init_test_logging();

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();

    // Insert some data
    for i in 0..5 {
        client
            .simple_query(&format!("INSERT INTO cats (id) VALUES ({i})"))
            .await
            .unwrap();
    }

    // Set a failpoint that gets triggered when we go to pull the next event off the WAL
    let err_to_inject = ReadySetError::ReplicationFailed("err injected".into());
    handle
        .set_failpoint(
            readyset_util::failpoints::POSTGRES_NEXT_WAL_EVENT,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
            ),
        )
        .await;

    // Set a failpoint that gets triggered when we try to start replication again after the previous
    // failure
    let err_to_inject = ReadySetError::FullResnapshotNeeded;
    handle
        .set_failpoint(
            readyset_util::failpoints::POSTGRES_START_REPLICATION,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
            ),
        )
        .await;

    // Insert another piece of data to trigger the errors
    client
        .simple_query("INSERT INTO cats (id) VALUES (5)")
        .await
        .unwrap();

    // The full resnapshot should complete successfully
    eventually!(run_test: {
        let res = client
            .simple_query("SELECT * FROM cats")
            .await;
        AssertUnwindSafe(|| res)
    }, then_assert: |result| {
        let res: Vec<u32> = result()
            .unwrap()
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                _ => None,
            })
            .collect();

        assert_eq!(res, [0, 1, 2, 3, 4, 5]);
    });

    shutdown_tx.shutdown().await;
}

// Tests that we create a new replication slot and resnapshot if the slot is deleted while the
// replicator is down
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn recreate_replication_slot() {
    use readyset_errors::ReadySetError;

    readyset_tracing::init_test_logging();

    let (config, mut handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .replication_server_id("recreate_replication_slot".to_string())
        .build::<PostgreSQLAdapter>()
        .await;

    let client = connect(config).await;

    client
        .simple_query("DROP TABLE IF EXISTS cats")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE cats (id int);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO cats (id) VALUES (1);")
        .await
        .unwrap();

    // Add a failpoint that will cause the replicator to fail while starting up until the failpoint
    // is disabled
    handle
        .set_failpoint(
            readyset_util::failpoints::START_INNER_POSTGRES,
            &format!(
                "return({})",
                serde_json::ser::to_string(&ReadySetError::ReplicationFailed("error".into()))
                    .expect("failed to serialize error")
            ),
        )
        .await;

    // Add a failpoint to trigger a replicator restart
    handle
        .set_failpoint(
            readyset_util::failpoints::UPSTREAM,
            &format!(
                "1*return({})",
                serde_json::ser::to_string(&ReadySetError::ReplicationFailed("error".into()))
                    .expect("failed to serialize error")
            ),
        )
        .await;

    // Insert another row to trigger the failpoint
    client
        .simple_query("INSERT INTO cats (id) VALUES (2);")
        .await
        .unwrap();

    // Wait for the replicator's error loop to start
    eventually!(run_test: {
        let res = client
            .simple_query("SHOW READYSET STATUS")
            .await;
        AssertUnwindSafe(|| res)
    }, then_assert: |result| {
        let res: Vec<String> = result()
            .unwrap()
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                _ => None,
            })
            .collect();

        assert!(res.contains(&"Last replicator error".to_owned()));
    });

    // Delete the replication slot while the replicator is waiting to restart
    eventually!(
        client
            .simple_query("SELECT pg_drop_replication_slot('readyset_recreate_replication_slot')")
            .await
            .is_ok()
    );

    // Insert another row while the replicator is waiting to restart
    client
        .simple_query("INSERT INTO cats (id) VALUES (3);")
        .await
        .unwrap();

    // Start the replicator back up
    handle
        .set_failpoint(readyset_util::failpoints::START_INNER_POSTGRES, "off")
        .await;

    eventually!(run_test: {
        let res = client
            .simple_query("SELECT * FROM cats")
            .await;
        AssertUnwindSafe(|| res)
    }, then_assert: |result| {
        let res: Vec<u32> = result()
            .unwrap()
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                _ => None,
            })
            .collect();

        assert_eq!(res, [1, 2, 3]);
    });

    shutdown_tx.shutdown().await;
}

// Tests that a cache with a name can still be queried after it is cleared from the query status
// cache
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn named_cache_queryable_after_being_cleared() {
    readyset_tracing::init_test_logging();
    let prefix = "named_cache_queryable_after_being_cleared";

    let storage_dir = {
        let (builder, authority, storage_dir) = setup_standalone_with_authority(prefix, None);
        let (config, handle, shutdown_tx) = builder
            .replicate(true)
            .fallback(true)
            .migration_style(MigrationStyle::Explicit)
            .migration_mode(MigrationMode::OutOfBand)
            .replication_server_id("named_cache_queryable_after_being_cleared".to_string())
            .build::<PostgreSQLAdapter>()
            .await;

        let conn = connect(config).await;

        conn.simple_query("DROP TABLE IF EXISTS t CASCADE")
            .await
            .unwrap();
        conn.simple_query("CREATE TABLE t (x int)").await.unwrap();
        // TODO(mvzink): Remove sleep once REA-6109 is fixed.
        sleep().await;
        eventually!(
            conn.simple_query("CREATE CACHE test FROM SELECT * FROM t WHERE x = 1")
                .await
                .is_ok()
        );
        conn.simple_query("SELECT * FROM t WHERE x = 1")
            .await
            .unwrap();
        let destination = match conn
            .simple_query("EXPLAIN LAST STATEMENT")
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
        {
            SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_owned(),
            _ => panic!(),
        };

        assert_eq!(
            destination.as_str().try_into(),
            Ok(QueryDestination::Readyset(Some("test".into())))
        );

        shutdown_tx.shutdown().await;
        sleep().await;
        drop(handle);

        // Workers are cleared from the authority only when `Authority::drop` is called, so we
        // need to wait for all the other `Arc<Authority>`s to be dropped. Without this, the
        // next server we start up could end up registering the worker from the old server in
        // addition to the new one, which would cause connection issues, since the old worker
        // is no longer running.
        while Arc::strong_count(&authority) > 1 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        storage_dir
    };

    let (builder, _authority, _tempdir) =
        setup_standalone_with_authority(prefix, Some(storage_dir));

    let (config, _handle, shutdown_tx) = builder
        .replicate(true)
        .recreate_database(false)
        .migration_style(MigrationStyle::Explicit)
        .migration_mode(MigrationMode::OutOfBand)
        .replication_server_id("named_cache_queryable_after_being_cleared".to_string())
        .build::<PostgreSQLAdapter>()
        .await;

    let conn = connect(config).await;

    eventually!(run_test: {
        conn.simple_query("SELECT * FROM t WHERE x = 1")
            .await
            .unwrap();

        let destination = match conn
            .simple_query("EXPLAIN LAST STATEMENT")
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
        {
            SimpleQueryMessage::Row(row) => row.get(0).unwrap().to_owned(),
            _ => panic!(),
        };
        AssertUnwindSafe(|| destination)
    },
    then_assert: |destination| {
        assert_eq!(
            destination().try_into(),
            Ok(QueryDestination::Readyset(Some("test".into())))
        );
    });

    shutdown_tx.shutdown().await;
}

#[cfg(feature = "failure_injection")]
mod failure_injection_tests {
    // TODO: move the above cfg failure_injection tests into this mod

    use std::sync::atomic::{AtomicBool, Ordering};

    use lazy_static::lazy_static;
    use readyset_adapter::query_status_cache::MigrationStyle;
    use readyset_client::consensus::{Authority, AuthorityControl, CacheDDLRequest};
    use readyset_data::Dialect;
    use readyset_errors::ReadySetError;
    use readyset_util::failpoints;
    use tokio_postgres::Client;
    use tracing::debug;

    use super::*;
    // `super::` instead of `crate::` — see test-discovery crate documentation
    use super::Handle;

    async fn assert_query_hits_readyset(conn: &Client, query: &'static str) {
        eventually! {
            run_test: {
                conn.simple_query(query).await.unwrap();
                let row: Vec<String> = match conn
                    .simple_query("EXPLAIN LAST STATEMENT")
                    .await
                    .unwrap()
                    .first()
                    .unwrap()
                {
                    SimpleQueryMessage::Row(r) => {
                        (0..r.len()).map(|i| r.get(i).unwrap().to_owned()).collect()
                    }
                    _ => panic!(),
                };
                AssertUnwindSafe(|| row)
            },
            then_assert: |result| {
                let row = result();
                let destination = QueryDestination::try_from(row.first().unwrap().as_str()).unwrap();
                assert_matches!(destination, QueryDestination::Readyset(_));
                let status = row.get(1).unwrap();
                assert_eq!(status, "ok");
            }
        }
    }

    /// Starts readyset, runs extend recipe with a changelist generated from `queries`, then
    /// injects a leader election that fails to load the controller state and needs to re-create
    /// it.
    async fn setup_reload_controller_state_test(
        prefix: &str,
        queries: &[&str],
    ) -> (
        tokio_postgres::Config,
        Handle,
        Arc<Authority>,
        ShutdownSender,
    ) {
        readyset_tracing::init_test_logging();

        let storage_dir = {
            let (builder, authority, storage_dir) = setup_standalone_with_authority(prefix, None);
            let (config, handle, shutdown_tx) = builder
                .replicate(true)
                .fallback(true)
                .migration_style(MigrationStyle::Explicit)
                .replication_server_id(prefix.to_string())
                .build::<PostgreSQLAdapter>()
                .await;

            let conn = connect(config).await;
            for query in queries {
                debug!(%query, "Running Query");
                let _res = conn.simple_query(query).await;
                // give it some time to propagate
                sleep().await;
            }

            let err_to_inject =
                ReadySetError::SerializationFailed("Backwards Incompatibility Injected".into());

            fail::cfg(
                failpoints::LOAD_CONTROLLER_STATE,
                &format!(
                    "1*return({})",
                    serde_json::ser::to_string(&err_to_inject).expect("failed to serialize error")
                ),
            )
            .expect("failed to set failpoint");

            shutdown_tx.shutdown().await;
            sleep().await;
            drop(handle);

            // Workers are cleared from the authority only when `Authority::drop` is called, so we
            // need to wait for all the other `Arc<Authority>`s to be dropped. Without this, the
            // next server we start up could end up registering the worker from the old server in
            // addition to the new one, which would cause connection issues, since the old worker
            // is no longer running.
            while Arc::strong_count(&authority) > 1 {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            storage_dir
        };

        let (builder, authority, _tempdir) =
            setup_standalone_with_authority(prefix, Some(storage_dir));

        let (config, handle, shutdown_tx) = builder
            .replicate(true)
            .fallback(true)
            .recreate_database(false)
            .migration_style(MigrationStyle::Explicit)
            .replication_server_id(prefix.to_string())
            .build::<PostgreSQLAdapter>()
            .await;

        (config, handle, authority, shutdown_tx)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn caches_recreated_after_backwards_incompatible_upgrade() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE test_query FROM SELECT * FROM users;",
        ];
        let (config, mut handle, _authority, shutdown_tx) =
            setup_reload_controller_state_test("caches_recreated", &queries).await;

        let queries = handle.views().await.unwrap();
        assert!(queries.contains_key(&"test_query".into()));

        let client = connect(config).await;
        assert_query_hits_readyset(&client, "SELECT * FROM users").await;

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn caches_recreated_using_rewritten_query() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE test_query FROM SELECT * FROM users WHERE id = 1;",
        ];
        let (config, mut handle, _authority, shutdown_tx) =
            setup_reload_controller_state_test("caches_recreated_rewritten", &queries).await;

        eventually!(matches!(handle.leader_ready().await, Ok(true)));

        let queries = handle.views().await.unwrap();
        assert!(queries.contains_key(&"test_query".into()));

        let client = connect(config).await;
        assert_query_hits_readyset(&client, "SELECT * FROM users WHERE id = 2").await;

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn dropped_caches_not_recreated() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE dropped_query FROM SELECT * FROM users;",
            "CREATE CACHE cached_query FROM SELECT * FROM users where id = 1;",
            "DROP CACHE dropped_query",
        ];
        let (config, mut handle, _authority, shutdown_tx) =
            setup_reload_controller_state_test("caches_not_recreated", &queries).await;

        let queries = handle.views().await.unwrap();
        assert!(!queries.contains_key(&"dropped_query".into()));
        assert!(queries.contains_key(&"cached_query".into()));

        let client = connect(config).await;
        assert_query_hits_readyset(&client, "SELECT * FROM users WHERE id = 2").await;

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn dropped_then_recreated_cache_recreated() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE dropped_query FROM SELECT * FROM users;",
            "DROP CACHE dropped_query",
            "CREATE CACHE cached_query FROM SELECT * FROM users;",
            "DROP CACHE cached_query;",
            "CREATE CACHE cached_query FROM SELECT * FROM users",
        ];

        let (config, mut handle, _authority, shutdown_tx) =
            setup_reload_controller_state_test("caches_dropped_then_recreated", &queries).await;

        let queries = handle.views().await.unwrap();
        assert!(!queries.contains_key(&"dropped_query".into()));
        assert!(queries.contains_key(&"cached_query".into()));

        let client = connect(config).await;
        assert_query_hits_readyset(&client, "SELECT * FROM users").await;

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn caches_added_if_extend_recipe_times_out() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE test_query FROM SELECT * FROM users;",
        ];

        // This is set to be larger than  EXTEND_RECIPE_MAX_SYNC_TIME, which is 5 seconds
        // The `create cache` is the 3rd extend_recipe run
        fail::cfg(failpoints::EXTEND_RECIPE, "2*off->1*sleep(6000)").expect("failed at failing");

        let (config, mut handle, authority, shutdown_tx) =
            setup_reload_controller_state_test("extend_recipe_timeout", &queries).await;

        let cache_ddl_requests = authority.cache_ddl_requests().await.unwrap();
        let expected = CacheDDLRequest {
            unparsed_stmt: "CREATE CACHE test_query FROM SELECT * FROM users;".to_string(),
            schema_search_path: vec!["postgres".into(), "public".into()],
            dialect: Dialect::DEFAULT_POSTGRESQL,
        };
        assert_eq!(expected, *cache_ddl_requests.first().unwrap());

        let queries = handle.views().await.unwrap();
        assert!(queries.contains_key(&"test_query".into()));

        let client = connect(config).await;
        assert_query_hits_readyset(&client, "SELECT * FROM users").await;

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn create_cache_not_added_if_extend_recipe_fails() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE test_query FROM SELECT * FROM idontexist;",
        ];

        let (_config, mut handle, authority, shutdown_tx) =
            setup_reload_controller_state_test("extend_recipe_create_fail", &queries).await;

        let cache_ddl_requests = authority.cache_ddl_requests().await.unwrap();
        assert!(cache_ddl_requests.is_empty());

        let queries = handle.views().await.unwrap();
        assert!(queries.is_empty());

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn drop_cache_not_added_if_drop_fails() {
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "DROP CACHE idontexist;",
        ];

        let (_config, mut handle, authority, shutdown_tx) =
            setup_reload_controller_state_test("extend_recipe_drop_fail", &queries).await;

        let cache_ddl_requests = authority.cache_ddl_requests().await.unwrap();
        assert!(cache_ddl_requests.is_empty());

        let queries = handle.views().await.unwrap();
        assert!(queries.is_empty());

        shutdown_tx.shutdown().await;
    }

    // Tests that the replicator successfully checks whether the replication slot exists upon
    // restarting
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn replication_slot_exists_check() {
        use readyset_errors::ReadySetError;

        readyset_tracing::init_test_logging();

        let (config, mut handle, shutdown_tx) = TestBuilder::default()
            .replication_server_id("readyset_123".into())
            .fallback(true)
            .build::<PostgreSQLAdapter>()
            .await;

        // Add a failpoint to trigger a replicator restart
        handle
            .set_failpoint(
                readyset_util::failpoints::UPSTREAM,
                &format!(
                    "1*return({})",
                    serde_json::ser::to_string(&ReadySetError::ReplicationFailed("error".into()))
                        .expect("failed to serialize error")
                ),
            )
            .await;

        let client = connect(config).await;

        client
            .simple_query("DROP TABLE IF EXISTS cats")
            .await
            .unwrap();
        client
            .simple_query("CREATE TABLE cats (id int);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO cats (id) VALUES (1);")
            .await
            .unwrap();

        // If we sucessfully replicate the above changes, we know the replicator has successfully
        // started and thus successfully checked for the existence of the replication offset
        eventually!(run_test: {
            let res = client
                .simple_query("SELECT * FROM cats")
                .await;
            AssertUnwindSafe(|| res)
        }, then_assert: |result| {
            let res: Vec<u32> = result()
                .unwrap()
                .iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().parse().unwrap()),
                    _ => None,
                })
                .collect();

            assert_eq!(res, [1]);
        });

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow)]
    #[upstream(postgres13, postgres15)]
    async fn backwards_incompatible_upgrade_doesnt_resnapshot() {
        // If we make a backwards-incompatible change to the serialization of the controller state,
        // we shouldn't have to resnapshot the actual data in the base tables (assuming the schema
        // hasn't changed since we stopped running)
        lazy_static! {
            pub static ref SAW_RESNAPSHOT: AtomicBool = AtomicBool::new(false);
        }

        // The test should not hit "Snapshotting table", because we create the table after the
        // initial snapshot finishes.
        fail::cfg_callback(failpoints::POSTGRES_SNAPSHOT_TABLE, move || {
            SAW_RESNAPSHOT.store(true, Ordering::SeqCst);
        })
        .expect("failed to configure failpoint");
        // We expect to see neither a partial nor a full re-snapshot
        fail::cfg_callback(failpoints::POSTGRES_PARTIAL_RESNAPSHOT, move || {
            SAW_RESNAPSHOT.store(true, Ordering::SeqCst);
        })
        .expect("failed to configure failpoint");
        fail::cfg_callback(failpoints::POSTGRES_FULL_RESNAPSHOT, move || {
            SAW_RESNAPSHOT.store(true, Ordering::SeqCst);
        })
        .expect("failed to configure failpoint");
        let queries = [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);",
            "CREATE CACHE test_query FROM SELECT * FROM users;",
        ];

        let (_config, mut handle, _authority, shutdown_tx) =
            setup_reload_controller_state_test("caches_recreated_no_resnapshot", &queries).await;

        // Add some sleep time because the replicator is a background task and we want to make sure
        // it has had a chance to hit the resnapshot error if it is going to.
        sleep().await;

        assert!(
            !SAW_RESNAPSHOT.load(Ordering::SeqCst),
            "We should not have re-snapshotted"
        );

        let queries = handle.views().await.unwrap();
        assert!(queries.contains_key(&"test_query".into()));
        shutdown_tx.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn drop_and_recreate_demo_cache() {
    // This tests dropping and recreating the cached used by the readyset demo script.
    // There was an error returned at one point in doing this, so this test is an attempt to block
    // a breaking change to the demo script at CI time.
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .set_mixed_comparisons(false)
        .build::<PostgreSQLAdapter>()
        .await;

    let conn = connect(opts).await;

    let queries = [
        "CREATE TABLE public.title_basics (
            tconst text NOT NULL,
            titletype text,
            primarytitle text,
            originaltitle text,
            isadult boolean,
            startyear integer,
            endyear integer,
            runtimeminutes integer,
            genres text
        );",
        "CREATE TABLE public.title_ratings (
            tconst text NOT NULL,
            averagerating numeric,
            numvotes integer
        );",
        "CREATE CACHE FROM
           SELECT count(*)
             FROM title_ratings
             JOIN title_basics
               ON title_ratings.tconst = title_basics.tconst
            WHERE title_basics.startyear = 2000
              AND title_ratings.averagerating > 5;",
        "DROP CACHE q_bccd97aea07c545f;",
        "CREATE CACHE FROM
           SELECT count(*)
             FROM title_ratings
             JOIN title_basics
               ON title_ratings.tconst = title_basics.tconst
            WHERE title_basics.startyear = 2000
              AND title_ratings.averagerating > 5;",
    ];

    for query in queries {
        let _res = conn.simple_query(query).await.expect("query failed");
        // give it some time to propagate
        sleep().await;
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn drop_all_proxied_queries() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = connect(opts).await;

    conn.simple_query("DROP TABLE IF EXISTS t CASCADE")
        .await
        .unwrap();
    conn.simple_query("CREATE TABLE t (x int, y int)")
        .await
        .unwrap();

    // Wait for the DDL to propagate to ReadySet
    eventually!(
        conn.simple_query("CREATE CACHE FROM SELECT * FROM t")
            .await
            .is_ok()
    );

    conn.simple_query("SELECT * FROM t WHERE x = 1")
        .await
        .unwrap();

    // Make sure the query we just proxied is present in SHOW PROXIED QUERIES
    let command = conn
        .simple_query("SHOW PROXIED QUERIES")
        .await
        .unwrap()
        .into_iter()
        .next_back()
        .unwrap();
    assert_matches!(
        command,
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
    );

    conn.simple_query("DROP ALL PROXIED QUERIES").await.unwrap();

    let command = conn
        .simple_query("SHOW PROXIED QUERIES")
        .await
        .unwrap()
        .into_iter()
        .next_back()
        .unwrap();
    assert_matches!(
        command,
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres15)]
async fn numeric_inf_nan() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;

    conn.simple_query("create table numer (a numeric);")
        .await
        .unwrap();

    eventually!(
        conn.simple_query("create cache from select * from numer")
            .await
            .is_ok()
    );

    conn.simple_query("insert into numer (a) values (1.0), ('NaN'), ('Infinity'), ('-Infinity')")
        .await
        .unwrap();

    sleep().await;

    let command = conn
        .simple_query("SELECT * from numer")
        .await
        .unwrap()
        .into_iter()
        .next_back()
        .unwrap();

    assert_last_statement_matches!("numer", "upstream", "view destroyed|ok", &conn);

    // We expect to see all rows because the table was dropped since we don't support NaN/Infinity
    // and the query should be proxied
    assert_matches!(
        command,
        SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 4, .. })
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres13, postgres15)]
async fn real_type() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;

    conn.simple_query("DROP TABLE IF EXISTS t CASCADE")
        .await
        .unwrap();
    conn.simple_query("CREATE TABLE t (f1 real)").await.unwrap();
    conn.simple_query("INSERT INTO t (f1) VALUES (1004.3)")
        .await
        .unwrap();

    eventually!(
        conn.simple_query("CREATE CACHE FROM SELECT * FROM t WHERE f1 <> 1004.3")
            .await
            .is_ok()
    );

    eventually! {
        let len = conn
            .query("SELECT * FROM t WHERE f1 <> 1004.3", &[])
            .await
            .unwrap()
            .len();
        len == 0
    }

    shutdown_tx.shutdown().await;
}

/// Tests that ROLLBACK TO SAVEPOINT does not end transaction state tracking,
/// and that we proxy the TO SAVEPOINT clause correctly to upstream.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn rollback_to_savepoint_preserves_transaction() {
    readyset_tracing::init_test_logging();
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client.simple_query("CREATE TABLE t (x int)").await.unwrap();
    sleep().await;

    client
        .simple_query("CREATE CACHE FROM SELECT * FROM t")
        .await
        .unwrap();

    eventually! {
        let _ = client.query("SELECT * FROM t", &[]).await.unwrap();
        last_statement_matches("readyset", "", &client).await.0
    }

    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    client.simple_query("SAVEPOINT sp1").await.unwrap();
    client
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .unwrap();
    client
        .simple_query("ROLLBACK TO SAVEPOINT sp1")
        .await
        .unwrap();

    // Verify ROLLBACK TO SAVEPOINT was proxied correctly: row 2 should be rolled back
    let rows: Vec<i32> = client
        .query("SELECT * FROM t", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, vec![1], "row 2 should have been rolled back");

    // Verify we're still in a transaction (query was proxied, not served from cache)
    assert_last_statement_matches!("t", "upstream", "", &client);

    client.simple_query("COMMIT").await.unwrap();

    // After commit, should hit cache again
    let _ = client.query("SELECT * FROM t", &[]).await.unwrap();
    assert_last_statement_matches!("t", "readyset", "", &client);

    shutdown_tx.shutdown().await;
}
