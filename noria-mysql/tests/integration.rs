use std::convert::TryFrom;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use mysql_async::prelude::*;
use noria::status::ReadySetStatus;
use noria_client::backend::noria_connector::ReadBehavior;
use noria_client::backend::QueryInfo;
use noria_client_metrics::QueryDestination;
use noria_client_test_helpers::mysql_helpers::{last_query_info, setup, setup_with_read_behavior};
use noria_client_test_helpers::sleep;
use noria_errors::ReadySetError;

#[tokio::test(flavor = "multi_thread")]
async fn delete_basic() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_some());

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep().await;
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_only_constraint() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep().await;
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_multiple() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    for i in 1..4 {
        conn.query_drop(format!("INSERT INTO Cats (id) VALUES ({})", i))
            .await
            .unwrap();
        sleep().await;
    }

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep().await;
    }

    for i in 1..3 {
        let query = format!("SELECT Cats.id FROM Cats WHERE Cats.id = {}", i);
        let row = conn.query_first::<mysql::Row, _>(query).await.unwrap();
        assert!(row.is_none());
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 3")
        .await
        .unwrap();
    assert!(row.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    // `id` can't be both 1 and 2!
    let deleted = conn
        .query_iter("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
        .await
        .unwrap();
    assert_eq!(deleted.affected_rows(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_and() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep().await;
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_or() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep().await;
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_other_column() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"Bob\"")
        .await
        .unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_no_keys() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("DELETE FROM Cats WHERE 1 = 1")
        .await
        .unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_compound_primary_key() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .await
    .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .await
        .unwrap();
    sleep().await;

    {
        let q = "DELETE FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let deleted = conn.query_iter(q).await.unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep().await;
    }

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let row = conn.query_first::<mysql::Row, _>(q).await.unwrap();
    assert!(row.is_none());

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let uid: i32 = conn.query_first(q).await.unwrap().unwrap();
    assert_eq!(uid, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn delete_multi_compound_primary_key() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .await
    .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .await
        .unwrap();
    sleep().await;

    {
        let q = "DELETE FROM Vote WHERE (Vote.aid = 1 AND Vote.uid = 2) OR (Vote.aid = 1 AND Vote.uid = 3)";
        let deleted = conn.query_iter(q).await.unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep().await;
    }

    for _ in 2..4 {
        let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let row = conn.query_first::<mysql::Row, _>(q).await.unwrap();
        assert!(row.is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic_prepared() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .exec_iter(
                "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = ?",
                (1,),
            )
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));

    {
        let updated = conn
            .exec_iter(
                "UPDATE Cats SET Cats.name = ? WHERE Cats.id = ?",
                ("Bob", 1),
            )
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_compound_primary_key() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .await
    .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Vote (aid, uid, reason) VALUES (1, 2, \"okay\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid, reason) VALUES (1, 3, \"still okay\")")
        .await
        .unwrap();
    sleep().await;

    {
        let q = "UPDATE Vote SET Vote.reason = \"better\" WHERE Vote.aid = 1 AND Vote.uid = 2";
        let updated = conn.query_iter(q).await.unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let name: String = conn.query_first(q).await.unwrap().unwrap();
    assert_eq!(name, String::from("better"));

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let name: String = conn.query_first(q).await.unwrap().unwrap();
    assert_eq!(name, String::from("still okay"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_only_constraint() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_pkey() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let query = "UPDATE Cats SET Cats.name = \"Rusty\", Cats.id = 10 WHERE Cats.id = 1";
        let updated = conn.query_iter(query).await.unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 10")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
    let old_row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(old_row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_separate() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty II\" WHERE Cats.id = 1")
            .await
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep().await;
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty II"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_no_keys() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE 1 = 1";
    conn.query_drop(query).await.unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
async fn update_other_column() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.name = \"Bob\"";
    conn.query_drop(query).await.unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn update_no_changes() {
    // ignored because we currently *always* return 1 row(s) affected.
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    let updated = conn
        .query_iter("UPDATE Cats SET Cats.name = \"Bob\" WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(updated.affected_rows(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_bogus() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 AND Cats.id = 2")
        .await
        .unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_pkey() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .await
        .unwrap();
    sleep().await;

    {
        let deleted = conn
            .exec_iter("DELETE FROM Cats WHERE id = ?", (1,))
            .await
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }
    sleep().await;

    let names: Vec<String> = conn.query("SELECT Cats.name FROM Cats").await.unwrap();
    assert_eq!(names, vec!["Jane".to_owned()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn select_collapse_where_in() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .await
        .unwrap();
    sleep().await;

    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2)")
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .exec("SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?)", (1, 2))
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    // some lookups give empty results
    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2, 3)")
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .exec(
            "SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?, ?)",
            (1, 2, 3),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    // also track another parameter
    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.name = 'Bob' AND Cats.id IN (1, 2)")
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));

    let names: Vec<String> = conn
        .exec(
            "SELECT Cats.name FROM Cats WHERE Cats.name = ? AND Cats.id IN (?, ?)",
            ("Bob", 1, 2),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_in() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Robert\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Janeway\")")
        .await
        .unwrap();
    sleep().await;

    let names: Vec<(String,)> = conn
        .exec(
            "SELECT name from Cats where id in (?, ?) and name in (?, ?)",
            (1, 2, "Bob", "Jane"),
        )
        .await
        .unwrap();

    assert_eq!(names, vec![("Bob".to_owned(),), ("Jane".to_owned(),)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_select() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<mysql::Row> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn strings() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x TEXT)").await.unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x) VALUES ('foo')")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(String,)> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, vec![("foo".to_string(),)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_select() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<mysql::Row> = conn
        .exec("SELECT test.* FROM test WHERE x = ?", (4,))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // results actually arrive as integer values since prepared statements use the binary protocol
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec![4.into(), 2.into()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn create_view() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE VIEW testview AS SELECT test.* FROM test")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<mysql::Row> = conn.query("SELECT testview.* FROM testview").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    let rows: Vec<mysql::Row> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_view_rev() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("SELECT test.* FROM test").await.unwrap();
    sleep().await;

    conn.query_drop("CREATE VIEW testview AS SELECT test.* FROM test")
        .await
        .unwrap();
    sleep().await;

    assert_eq!(
        conn.query::<mysql::Row, _>("SELECT testview.* FROM testview")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn prepare_ranged_query_non_partial() {
    let (opts, _handle) = setup(false).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .await
        .unwrap();
    sleep().await;

    let res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ?", (1i32,))
        .await
        .unwrap();
    assert_eq!(res, vec![(4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
#[should_panic]
async fn prepare_conflicting_ranged_query() {
    let (opts, _handle) = setup(false).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .await
        .unwrap();
    sleep().await;

    // panics because you can't mix and match range operators like this yet
    let _res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ? AND x < ?", (1i32, 5i32))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn prepare_ranged_query_partial() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .await
        .unwrap();
    sleep().await;

    let res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ?", (1i32,))
        .await
        .unwrap();
    assert_eq!(res, vec![(4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn absurdly_simple_select() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).await.unwrap();
    rows.sort_by_key(|(a, _)| *a);
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ad_hoc_unparametrized_select() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x = 4")
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x = 2")
        .await
        .unwrap();
    assert_eq!(rows, vec![(2, 4)]);

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE 1 = x")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 3)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ad_hoc_unparametrized_where_in() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x IN (1, 2)")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 3), (2, 4)]);

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x = 4")
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_unparametrized_select() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x = 4", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x = 2", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(2, 4)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_basic() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).await.unwrap();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY x DESC", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY y ASC", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2), (1, 3), (2, 4)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY y DESC", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(2, 4), (1, 3), (4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_limit_basic() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).await.unwrap();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY x DESC LIMIT 3", ())
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // why doesn't this work?
async fn exec_insert() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    conn.exec_drop("INSERT INTO posts (id, number) VALUES (?, 1)", (5,))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn design_doc_topk_with_preload() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 1)", id))
            .await
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 2)", id))
            .await
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 3)", id))
            .await
            .unwrap();
    }
    sleep().await;

    let simple_topk = conn
        .prep("SELECT * FROM posts WHERE number = ? ORDER BY id DESC LIMIT 3")
        .await
        .unwrap();

    let problem_topk = conn
        .prep("SELECT * FROM posts WHERE number < ? ORDER BY id DESC LIMIT 3")
        .await
        .unwrap();

    eprintln!("doing normal topk");
    // normal topk behaviour (sanity check)
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (1,)).await.unwrap();
    assert_eq!(rows, vec![(9, 1), (8, 1), (7, 1)]);
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (2,)).await.unwrap();
    assert_eq!(rows, vec![(10, 2), (4, 2), (2, 2)]);
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (3,)).await.unwrap();
    assert_eq!(rows, vec![(11, 3), (3, 3), (0, 3)]);

    eprintln!("doing bad topk");
    // problematic new topk behaviour
    let rows: Vec<(i32, i32)> = conn.exec(&problem_topk, (3,)).await.unwrap();
    assert_eq!(rows, vec![(10, 2), (9, 1), (8, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn design_doc_topk() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 1)", id))
            .await
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 2)", id))
            .await
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 3)", id))
            .await
            .unwrap();
    }
    sleep().await;

    let problem_topk = conn
        .prep("SELECT * FROM posts WHERE number < ? ORDER BY id DESC LIMIT 3")
        .await
        .unwrap();

    let rows: Vec<(i32, i32)> = conn.exec(&problem_topk, (3,)).await.unwrap();
    assert_eq!(rows, vec![(10, 2), (9, 1), (8, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ilike() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO notes (id, title) VALUES (1, 'foo')")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (2, 'bar')")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (3, 'baz')")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (4, 'BAZ')")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, String)> = conn
        .exec(
            "SELECT id, title FROM notes WHERE title ILIKE ? ORDER BY id ASC",
            ("%a%",),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (2, "bar".to_string()),
            (3, "baz".to_string()),
            (4, "BAZ".to_string())
        ]
    );

    let with_other_constraint: Vec<(i32, String)> = conn
        .exec(
            "SELECT id, title FROM notes WHERE title ILIKE ? AND id >= ? ORDER BY id ASC",
            ("%a%", 3),
        )
        .await
        .unwrap();
    assert_eq!(
        with_other_constraint,
        vec![(3, "baz".to_string()), (4, "BAZ".to_string()),]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn key_type_coercion() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, title TEXT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO posts (id, title) VALUES (1, 'hi')")
        .await
        .unwrap();

    let same_type_result: (u32, String) = conn
        .exec_first("SELECT id, title FROM posts WHERE id = ?", (1,))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(same_type_result, (1, "hi".to_owned()));

    let float_to_int_result: (u32, String) = conn
        .exec_first("SELECT id, title FROM posts WHERE id = ?", (1f32,))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(float_to_int_result, (1, "hi".to_owned()));
}

#[tokio::test(flavor = "multi_thread")]
async fn write_timestamps() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int primary key, created_at TIMESTAMP)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO posts (id, created_at) VALUES (1, '2020-01-23 17:08:24')")
        .await
        .unwrap();
    let result: (u32, NaiveDateTime) = conn
        .exec_first("SELECT id, created_at FROM posts WHERE id = ?", (1,))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.0, 1);
    assert_eq!(
        result.1,
        NaiveDate::from_ymd(2020, 1, 23).and_hms(17, 08, 24)
    );

    conn.query_drop("UPDATE posts SET created_at = '2021-01-25 17:08:24' WHERE id = 1")
        .await
        .unwrap();
    let result: (u32, NaiveDateTime) = conn
        .exec_first("SELECT id, created_at FROM posts WHERE id = ?", (1,))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.0, 1);
    assert_eq!(
        result.1,
        NaiveDate::from_ymd(2021, 1, 25).and_hms(17, 08, 24)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_time_type() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE daily_events (start_time time, end_time time)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO daily_events (start_time, end_time) VALUES ('08:00:00', '09:00:00')",
    )
    .await
    .unwrap();

    conn.exec_drop(
        "INSERT INTO daily_events (start_time, end_time) VALUES (?, ?)",
        (
            NaiveTime::from_hms(10, 30, 00),
            NaiveTime::from_hms(10, 45, 15),
        ),
    )
    .await
    .unwrap();

    let mut res: Vec<(NaiveTime, NaiveTime)> = conn
        .query("SELECT start_time, end_time FROM daily_events")
        .await
        .unwrap();
    assert_eq!(res.len(), 2);
    res.sort();
    assert_eq!(
        res,
        vec![
            (
                NaiveTime::from_hms(8, 00, 00),
                NaiveTime::from_hms(9, 00, 00),
            ),
            (
                NaiveTime::from_hms(10, 30, 00),
                NaiveTime::from_hms(10, 45, 15),
            )
        ]
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_keyed_state() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (id int, a int, b int, c int, d int, e int, f int, g int)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (id, a, b, c, d, e, f, g) VALUES (1, 2, 3, 4, 5, 6, 7, 8)")
        .await
        .unwrap();

    let result: (u32, u32, u32, u32, u32, u32, u32, u32,) = conn
        .exec_first("SELECT * FROM test WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ? AND f = ? AND g = ?", (2, 3, 4, 5, 6, 7, 8,))
        .await.unwrap().unwrap();
    assert_eq!(result, (1, 2, 3, 4, 5, 6, 7, 8,));
}

#[tokio::test(flavor = "multi_thread")]
// Test is ignored due to query reuse issue https://app.clubhouse.io/readysettech/story/380.
#[ignore]
async fn reuse_similar_query() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x = ?", (4,))
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    // This query is not identical to the one above, but Noria is expected to rewrite it and then
    // reuse the same underlying view.
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE NOT x != ?", (4,))
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_quoted_string() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data TEXT)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, String)> = conn
        .query("SELECT * FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\": \"Mr. Mistoffelees\"}".to_string())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn json_column_insert_read() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, String)> = conn
        .query("SELECT * FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\": \"Mr. Mistoffelees\"}".to_string())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_graphviz() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    let res: mysql_async::Row = conn
        .query_first("EXPLAIN GRAPHVIZ;")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.columns().as_ref().len(), 1);
    assert_eq!(
        res.columns().as_ref().first().unwrap().name_str(),
        "GRAPHVIZ"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_last_statement() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("SELECT * FROM test").await.unwrap();
    sleep().await;

    let destination: QueryInfo = conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.destination, QueryDestination::Noria);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_query_cache_where_in() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT);").await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHED QUERY test AS SELECT id FROM t WHERE id IN (?);")
        .await
        .unwrap();
    sleep().await;

    let queries: Vec<(String, String)> = conn.query("SHOW CACHED QUERIES;").await.unwrap();
    assert!(queries.iter().any(|(query_name, _)| query_name == "test"));

    conn.query_drop("CREATE CACHED QUERY test AS SELECT id FROM t WHERE id IN (?, ?);")
        .await
        .unwrap();
    sleep().await;
    let new_queries: Vec<(String, String)> = conn.query("SHOW CACHED QUERIES;").await.unwrap();
    assert_eq!(new_queries.len(), queries.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn show_readyset_status() {
    let (opts, _handle) = setup(true).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    let ret: Vec<mysql::Row> = conn.query("SHOW READYSET STATUS;").await.unwrap();
    assert!(ReadySetStatus::try_from(ret).is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_nonblocking_select() {
    let (opts, _handle) = setup_with_read_behavior(ReadBehavior::NonBlocking).await;
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let res: Result<Vec<mysql_async::Row>, _> =
        conn.exec("SELECT * FROM test WHERE x = 4", ()).await;
    assert_eq!(
        last_query_info(&mut conn).await.noria_error,
        ReadySetError::ReaderMissingKey.to_string()
    );
    assert!(res.is_err());

    // Long enough to wait for upquery.
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).await.unwrap();
    rows.sort_by_key(|(a, _)| *a);
    assert_eq!(rows, vec![(4, 2)]);
}
