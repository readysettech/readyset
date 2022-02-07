use mysql_async::prelude::*;
use noria_client::BackendBuilder;
use noria_client_test_helpers::mysql_helpers::MySQLAdapter;
use noria_client_test_helpers::{self, sleep};
use noria_server::Handle;
use serial_test::serial;

async fn setup() -> (mysql_async::Opts, Handle) {
    noria_client_test_helpers::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
        true,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row: Option<(i32,)> = conn
        .query_first("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(row, Some((1,)))
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore] // alter table not supported yet
async fn add_column() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row: Option<(i32,)> = conn
        .query_first("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(row, Some((1,)));

    conn.query_drop("ALTER TABLE Cats ADD COLUMN name TEXT;")
        .await
        .unwrap();
    conn.query_drop("UPDATE Cats SET name = 'Whiskers' WHERE id = 1;")
        .await
        .unwrap();
    sleep().await;

    let row: Option<(i32, String)> = conn
        .query_first("SELECT Cats.id, Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(row, Some((1, "Whiskers".to_owned())));
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn json_column_insert_read() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .await
        .unwrap();
    sleep().await;
    sleep().await;

    let rows: Vec<(i32, String)> = conn
        .query("SELECT * FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\":\"Mr. Mistoffelees\"}".to_string())]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn json_column_partial_update() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"xyz\"}')")
        .await
        .unwrap();
    conn.query_drop("UPDATE Cats SET data = JSON_REMOVE(data, '$.name') WHERE id = 1")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, String)> = conn
        .query("SELECT * FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "{}".to_string())]);
}

// TODO: remove this once we support range queries again
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn range_query() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE cats (id int PRIMARY KEY, cuteness int)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO cats (id, cuteness) values (1, 10)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT id, cuteness FROM cats WHERE cuteness > ?", vec![5])
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10)]);
}

// TODO: remove this once we support aggregates on parametrized IN
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn aggregate_in() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE cats (id int PRIMARY KEY, cuteness int)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO cats (id, cuteness) values (1, 10), (2, 8)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32,)> = conn
        .exec(
            "SELECT sum(cuteness) FROM cats WHERE id IN (?, ?)",
            vec![1, 2],
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(18,)]);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn set_autocommit() {
    let (opts, _handle) = setup().await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    // We do not support SET autocommit = 0;
    assert!(conn
        .query_drop("SET @@SESSION.autocommit = 1;")
        .await
        .is_ok());
    assert!(conn
        .query_drop("SET @@SESSION.autocommit = 0;")
        .await
        .is_err());
    assert!(conn.query_drop("SET @@LOCAL.autocommit = 1;").await.is_ok());
    assert!(conn
        .query_drop("SET @@LOCAL.autocommit = 0;")
        .await
        .is_err());
}
