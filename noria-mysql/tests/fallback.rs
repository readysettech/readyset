use mysql::prelude::*;
use noria_client::BackendBuilder;
use noria_client_test_helpers::mysql_helpers::MySQLAdapter;
use noria_client_test_helpers::{self, sleep};

use serial_test::serial;

fn setup() -> mysql::Opts {
    noria_client_test_helpers::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
        true,
    )
}

#[test]
#[serial]
fn create_table() {
    let mut conn = mysql::Conn::new(setup()).unwrap();

    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row: Option<(i32,)> = conn
        .query_first("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(row, Some((1,)))
}

#[test]
#[serial]
#[ignore] // alter table not supported yet
fn add_column() {
    let mut conn = mysql::Conn::new(setup()).unwrap();

    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row: Option<(i32,)> = conn
        .query_first("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(row, Some((1,)));

    conn.query_drop("ALTER TABLE Cats ADD COLUMN name TEXT;")
        .unwrap();
    conn.query_drop("UPDATE Cats SET name = 'Whiskers' WHERE id = 1;")
        .unwrap();
    sleep();

    let row: Option<(i32, String)> = conn
        .query_first("SELECT Cats.id, Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(row, Some((1, "Whiskers".to_owned())));
}

#[test]
#[serial]
fn json_column_insert_read() {
    let mut conn = mysql::Conn::new(setup()).unwrap();

    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .unwrap();
    sleep();
    sleep();

    let rows: Vec<(i32, String)> = conn.query("SELECT * FROM Cats WHERE Cats.id = 1").unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\":\"Mr. Mistoffelees\"}".to_string())]
    );
}

#[test]
#[serial]
fn json_column_partial_update() {
    let mut conn = mysql::Conn::new(setup()).unwrap();

    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"xyz\"}')")
        .unwrap();
    conn.query_drop("UPDATE Cats SET data = JSON_REMOVE(data, '$.name') WHERE id = 1")
        .unwrap();
    sleep();

    let rows: Vec<(i32, String)> = conn.query("SELECT * FROM Cats WHERE Cats.id = 1").unwrap();
    assert_eq!(rows, vec![(1, "{}".to_string())]);
}

// TODO: remove this once we support range queries again
#[test]
#[serial]
fn range_query() {
    let mut conn = mysql::Conn::new(setup()).unwrap();

    conn.query_drop("CREATE TABLE cats (id int PRIMARY KEY, cuteness int)")
        .unwrap();
    conn.query_drop("INSERT INTO cats (id, cuteness) values (1, 10)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT id, cuteness FROM cats WHERE cuteness > ?", vec![5])
        .unwrap();
    assert_eq!(rows, vec![(1, 10)]);
}

// TODO: remove this once we support aggregates on parametrized IN
#[test]
#[serial]
fn aggregate_in() {
    let mut conn = mysql::Conn::new(setup()).unwrap();
    conn.query_drop("CREATE TABLE cats (id int PRIMARY KEY, cuteness int)")
        .unwrap();
    conn.query_drop("INSERT INTO cats (id, cuteness) values (1, 10), (2, 8)")
        .unwrap();
    sleep();

    let rows: Vec<(i32,)> = conn
        .exec(
            "SELECT sum(cuteness) FROM cats WHERE id IN (?, ?)",
            vec![1, 2],
        )
        .unwrap();
    assert_eq!(rows, vec![(18,)]);
}

#[test]
#[serial]
fn set_autocommit() {
    let mut conn = mysql::Conn::new(setup()).unwrap();
    // We do not support SET autocommit = 0;
    assert!(conn.query_drop("SET @@SESSION.autocommit = 1;").is_ok());
    assert!(conn.query_drop("SET @@SESSION.autocommit = 0;").is_err());
    assert!(conn.query_drop("SET @@LOCAL.autocommit = 1;").is_ok());
    assert!(conn.query_drop("SET @@LOCAL.autocommit = 0;").is_err());
}
