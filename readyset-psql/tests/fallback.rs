use chrono::NaiveDate;
use readyset_adapter::backend::{MigrationMode, UnsupportedSetMode};
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::psql_helpers::{upstream_config, PostgreSQLAdapter};
use readyset_client_test_helpers::{sleep, Adapter, TestBuilder};
use readyset_server::Handle;
use serial_test::serial;

mod common;
use common::connect;
use postgres_types::{FromSql, ToSql};
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

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn schema_resolution_with_unreplicated_tables() {
    readyset_tracing::init_test_logging();
    let (config, mut handle) = setup().await;
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

    assert!(last_statement_matches("readyset", "ok", &client).await)
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
    assert!(matches!(res, SimpleQueryMessage::CommandComplete(0)));

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
async fn replication_failure_ignores_table() {
    readyset_tracing::init_test_logging();
    use nom_sql::Relation;
    use readyset_client::ReadySetError;

    let (config, mut handle) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::url())
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
            readyset_client::failpoints::REPLICATION_ACTION,
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
}

#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn replication_failure_retries_if_failed_to_drop() {
    readyset_tracing::init_test_logging();
    use std::time::Duration;

    use nom_sql::Relation;
    use readyset_client::ReadySetError;
    use readyset_tracing::info;

    let (config, mut handle) = TestBuilder::default()
        .recreate_database(false)
        .fallback_url(PostgreSQLAdapter::url())
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
            readyset_client::failpoints::REPLICATION_ACTION,
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
}
