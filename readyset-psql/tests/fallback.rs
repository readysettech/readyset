use chrono::NaiveDate;
use readyset_adapter::backend::{MigrationMode, UnsupportedSetMode};
use readyset_adapter::BackendBuilder;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client_test_helpers::psql_helpers::{upstream_config, PostgreSQLAdapter};
use readyset_client_test_helpers::{sleep, Adapter, TestBuilder};
use readyset_server::Handle;
use serial_test::serial;

mod common;
use common::connect;
use postgres_types::{FromSql, ToSql};
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn setup() -> (tokio_postgres::Config, Handle) {
    TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table() {
    let (config, _handle) = setup().await;
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
    assert_eq!(result, 1)
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn delete_case_sensitive() {
    for (opts, _handle) in [
        TestBuilder::default().build::<PostgreSQLAdapter>().await,
        setup().await,
    ] {
        let conn = connect(opts).await;
        conn.simple_query(r#"CREATE TABLE "Cats" (id int PRIMARY KEY, "ID" int)"#)
            .await
            .unwrap();
        sleep().await;

        assert!(conn
            .simple_query(r#"INSERT INTO cats (id) VALUES (1)"#)
            .await
            .is_err());

        assert!(conn
            .simple_query(r#"INSERT INTO "cats" (id) VALUES (1)"#)
            .await
            .is_err());

        assert!(conn
            .simple_query(r#"INSERT INTO Cats (id) VALUES (1)"#)
            .await
            .is_err());

        assert!(conn
            .simple_query(r#"INSERT INTO "Cats" (id, "Id") VALUES (1, 2)"#)
            .await
            .is_err());

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
            assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
            sleep().await;
        }

        let row = conn
            .query_opt(r#"SELECT "Cats".id FROM "Cats" WHERE "Cats".id = 1"#, &[])
            .await
            .unwrap();
        assert!(row.is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unsupported_query_ad_hoc() {
    let (config, _handle) = setup().await;
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
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore] // needs proper detection of reads vs writes through fallback
async fn prepare_execute_fallback() {
    let (config, _handle) = setup().await;
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
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn proxy_unsupported_sets() {
    let (config, _handle) = TestBuilder::new(
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
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn proxy_unsupported_type() {
    let (config, _handle) = setup().await;
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
    let dropped = res.first().unwrap();
    assert!(matches!(dropped, SimpleQueryMessage::CommandComplete(_)));
    sleep().await;
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
    let created = res.first().unwrap();
    assert!(matches!(created, SimpleQueryMessage::CommandComplete(_)));
    sleep().await;
    let res = fallback_conn
        .simple_query("INSERT INTO calc_columns (col1, col2) VALUES(1, 2)")
        .await
        .expect("populate failed");
    let inserted = res.first().unwrap();
    assert!(matches!(inserted, SimpleQueryMessage::CommandComplete(1)));
    sleep().await;
    let res = fallback_conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");

    // CommandComplete and 1 row should be returned
    assert_eq!(res.len(), 2);
    sleep().await;

    let (opts, _handle) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::url())
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
    let caches = res.first().unwrap();
    assert!(matches!(caches, SimpleQueryMessage::CommandComplete(0)));

    // Inserting will go to upstream
    let populate_gen_columns_readyset = "INSERT INTO calc_columns (col1, col2) VALUES(3, 4);";
    let res = conn
        .simple_query(populate_gen_columns_readyset)
        .await
        .expect("populate failed");
    sleep().await;
    let inserted = &res[0];
    assert!(matches!(inserted, SimpleQueryMessage::CommandComplete(1)));

    // We should see the inserted data
    let res = conn
        .simple_query("SELECT * from calc_columns")
        .await
        .expect("select failed");
    let selected = &res[2];
    assert!(matches!(*selected, SimpleQueryMessage::CommandComplete(2)));
}

/// Handle [TOASTed](https://www.postgresql.org/docs/current/storage-toast.html) tables by skipping
/// them during snapshot, and falling back to upstream.
/// ReadySet psql replication specifically doesn't support updating a row containing TOASTed values,
/// where one or more TOASTed values is unchanged.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn toast() {
    readyset_tracing::init_test_logging();

    let mut upstream_config = upstream_config();
    upstream_config.dbname("noria");
    let fallback_conn = connect(upstream_config).await;
    let res = fallback_conn
        .simple_query("DROP TABLE IF EXISTS t CASCADE")
        .await
        .expect("drop failed");
    let dropped = res.first().unwrap();
    assert!(matches!(dropped, SimpleQueryMessage::CommandComplete(_)));
    sleep().await;
    let res = fallback_conn
        .simple_query("CREATE TABLE t (col1 INT PRIMARY KEY, col2 TEXT)")
        .await
        .expect("create failed");
    let created = res.first().unwrap();
    assert!(matches!(created, SimpleQueryMessage::CommandComplete(_)));
    sleep().await;
    fallback_conn
        .query(
            "INSERT INTO t VALUES (0, $1)",
            &[&rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(9000)
                .map(char::from)
                .collect::<String>()],
        )
        .await
        .expect("populate failed");
    sleep().await;
    let res = fallback_conn
        .simple_query("SELECT * FROM t")
        .await
        .expect("select failed");

    // CommandComplete and 1 row should be returned
    assert_eq!(res.len(), 2);
    sleep().await;

    let (opts, _handle) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::url())
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

    // This should fail, since we don't have a base table for t, as it was ignored in the
    // initial snapshot
    conn.simple_query("CREATE CACHE FROM SELECT * FROM t")
        .await
        .expect_err("create cache should have failed");

    // There shouldn't be any caches
    let res = conn
        .simple_query("SHOW CACHES")
        .await
        .expect("show caches failed");
    let caches = res.first().unwrap();
    assert!(matches!(caches, SimpleQueryMessage::CommandComplete(0)));

    // Inserting will go to upstream
    let populate_gen_columns_readyset = "INSERT INTO t VALUES (1, 'hi');";
    let res = conn
        .simple_query(populate_gen_columns_readyset)
        .await
        .expect("populate failed");
    sleep().await;
    let inserted = &res[0];
    assert!(matches!(inserted, SimpleQueryMessage::CommandComplete(1)));

    // We should see the inserted data
    let res = conn
        .simple_query("SELECT * FROM t")
        .await
        .expect("select failed");
    let selected = &res[2];
    assert!(matches!(*selected, SimpleQueryMessage::CommandComplete(2)));
    assert!(last_statement_matches("upstream", "ok", &conn).await);
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
