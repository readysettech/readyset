use noria_client_test_helpers::psql_helpers::setup_w_fallback;
use noria_client_test_helpers::sleep;
use serial_test::serial;

mod common;
use common::connect;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table() {
    let (config, _handle) = setup_w_fallback().await;
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
async fn unsupported_query_ad_hoc() {
    let (config, _handle) = setup_w_fallback().await;
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
    let (config, _handle) = setup_w_fallback().await;
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
