use chrono::NaiveDate;
use readyset_client::backend::UnsupportedSetMode;
use readyset_client::BackendBuilder;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::{sleep, TestBuilder};
use readyset_server::Handle;

mod common;
use common::connect;
use tokio_postgres::SimpleQueryMessage;

async fn setup() -> (tokio_postgres::Config, Handle) {
    TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

#[tokio::test(flavor = "multi_thread")]
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
