use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use mysql::prelude::*;
use noria::status::ReadySetStatus;
use noria_client_test_helpers::sleep;
use std::convert::TryFrom;

mod common;
use common::setup;

#[test]
fn delete_basic() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_some());

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_only_constraint() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_multiple() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    for i in 1..4 {
        conn.query_drop(format!("INSERT INTO Cats (id) VALUES ({})", i))
            .unwrap();
        sleep();
    }

    {
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep();
    }

    for i in 1..3 {
        let query = format!("SELECT Cats.id FROM Cats WHERE Cats.id = {}", i);
        let row = conn.query_first::<mysql::Row, _>(query).unwrap();
        assert!(row.is_none());
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 3")
        .unwrap();
    assert!(row.is_some());
}

#[test]
fn delete_bogus() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    // `id` can't be both 1 and 2!
    let deleted = conn
        .query_iter("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap();
    assert_eq!(deleted.affected_rows(), 0);
}

#[test]
fn delete_bogus_valid_and() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_bogus_valid_or() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query_iter("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_other_column() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"Bob\"")
        .unwrap_err();
}

#[test]
fn delete_no_keys() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.query_drop("DELETE FROM Cats WHERE 1 = 1").unwrap_err();
}

#[test]
fn delete_compound_primary_key() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .unwrap();
    sleep();

    {
        let q = "DELETE FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let deleted = conn.query_iter(q).unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let row = conn.query_first::<mysql::Row, _>(q).unwrap();
    assert!(row.is_none());

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let uid: i32 = conn.query_first(q).unwrap().unwrap();
    assert_eq!(uid, 3);
}

#[test]
#[ignore]
fn delete_multi_compound_primary_key() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .unwrap();
    sleep();

    {
        let q = "DELETE FROM Vote WHERE (Vote.aid = 1 AND Vote.uid = 2) OR (Vote.aid = 1 AND Vote.uid = 3)";
        let deleted = conn.query_iter(q).unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep();
    }

    for _ in 2..4 {
        let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let row = conn.query_first::<mysql::Row, _>(q).unwrap();
        assert!(row.is_none());
    }
}

#[test]
fn update_basic() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
}

#[test]
fn update_basic_prepared() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .exec_iter(
                "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = ?",
                (1,),
            )
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));

    {
        let updated = conn
            .exec_iter(
                "UPDATE Cats SET Cats.name = ? WHERE Cats.id = ?",
                ("Bob", 1),
            )
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Bob"));
}

#[test]
fn update_compound_primary_key() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Vote (aid, uid, reason) VALUES (1, 2, \"okay\")")
        .unwrap();
    conn.query_drop("INSERT INTO Vote (aid, uid, reason) VALUES (1, 3, \"still okay\")")
        .unwrap();
    sleep();

    {
        let q = "UPDATE Vote SET Vote.reason = \"better\" WHERE Vote.aid = 1 AND Vote.uid = 2";
        let updated = conn.query_iter(q).unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let name: String = conn.query_first(q).unwrap().unwrap();
    assert_eq!(name, String::from("better"));

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let name: String = conn.query_first(q).unwrap().unwrap();
    assert_eq!(name, String::from("still okay"));
}

#[test]
fn update_only_constraint() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
}

#[test]
fn update_pkey() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let query = "UPDATE Cats SET Cats.name = \"Rusty\", Cats.id = 10 WHERE Cats.id = 1";
        let updated = conn.query_iter(query).unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 10")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
    let old_row = conn
        .query_first::<mysql::Row, _>("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap();
    assert!(old_row.is_none());
}

#[test]
fn update_separate() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    {
        let updated = conn
            .query_iter("UPDATE Cats SET Cats.name = \"Rusty II\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .query_first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty II"));
}

#[test]
fn update_no_keys() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE 1 = 1";
    conn.query_drop(query).unwrap_err();
}

#[test]
fn update_other_column() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.name = \"Bob\"";
    conn.query_drop(query).unwrap_err();
}

#[test]
#[ignore]
fn update_no_changes() {
    // ignored because we currently *always* return 1 row(s) affected.
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    let updated = conn
        .query_iter("UPDATE Cats SET Cats.name = \"Bob\" WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(updated.affected_rows(), 0);
}

#[test]
fn update_bogus() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    conn.query_drop("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap_err();
}

#[test]
fn delete_pkey() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .unwrap();
    sleep();

    {
        let deleted = conn
            .exec_iter("DELETE FROM Cats WHERE id = ?", (1,))
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }
    sleep();

    let names: Vec<String> = conn.query("SELECT Cats.name FROM Cats").unwrap();
    assert_eq!(names, vec!["Jane".to_owned()]);
}

#[test]
fn select_collapse_where_in() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .unwrap();
    sleep();

    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2)")
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .exec("SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?)", (1, 2))
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
        .unwrap()
        .into_iter()
        .map(|mut row: mysql::Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));
}

#[test]
fn multiple_in() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Robert\")")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .unwrap();
    conn.query_drop("INSERT INTO Cats (id, name) VALUES (2, \"Janeway\")")
        .unwrap();
    sleep();

    let names: Vec<(String,)> = conn
        .exec(
            "SELECT name from Cats where id in (?, ?) and name in (?, ?)",
            (1, 2, "Bob", "Jane"),
        )
        .unwrap();

    assert_eq!(names, vec![("Bob".to_owned(),), ("Jane".to_owned(),)]);
}

#[test]
fn basic_select() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    let rows: Vec<mysql::Row> = conn.query("SELECT test.* FROM test").unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );
}

#[test]
fn strings() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x TEXT)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x) VALUES ('foo')")
        .unwrap();
    sleep();

    let rows: Vec<(String,)> = conn.query("SELECT test.* FROM test").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, vec![("foo".to_string(),)]);
}

#[test]
fn prepared_select() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    let rows: Vec<mysql::Row> = conn
        .exec("SELECT test.* FROM test WHERE x = ?", (4,))
        .unwrap();
    assert_eq!(rows.len(), 1);
    // results actually arrive as integer values since prepared statements use the binary protocol
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec![4.into(), 2.into()]]
    );
}

#[test]
fn create_view() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    conn.query_drop("CREATE VIEW testview AS SELECT test.* FROM test")
        .unwrap();
    sleep();

    let rows: Vec<mysql::Row> = conn.query("SELECT testview.* FROM testview").unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    let rows: Vec<mysql::Row> = conn.query("SELECT test.* FROM test").unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );
}

#[test]
#[ignore]
fn create_view_rev() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("SELECT test.* FROM test").unwrap();
    sleep();

    conn.query_drop("CREATE VIEW testview AS SELECT test.* FROM test")
        .unwrap();
    sleep();

    assert_eq!(
        conn.query::<mysql::Row, _>("SELECT testview.* FROM testview")
            .unwrap()
            .len(),
        1
    );
}

#[test]
#[ignore]
fn prepare_ranged_query_non_partial() {
    let mut conn = mysql::Conn::new(setup(false)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .unwrap();
    sleep();

    let res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ?", (1i32,))
        .unwrap();
    assert_eq!(res, vec![(4, 2)]);
}

#[test]
#[ignore]
#[should_panic]
fn prepare_conflicting_ranged_query() {
    let mut conn = mysql::Conn::new(setup(false)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .unwrap();
    sleep();

    // panics because you can't mix and match range operators like this yet
    let _res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ? AND x < ?", (1i32, 5i32))
        .unwrap();
}

#[test]
#[ignore]
fn prepare_ranged_query_partial() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 1)")
        .unwrap();
    sleep();

    let res: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test WHERE y > ?", (1i32,))
        .unwrap();
    assert_eq!(res, vec![(4, 2)]);
}

#[test]
fn absurdly_simple_select() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).unwrap();
    rows.sort_by_key(|(a, _)| *a);
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
}

#[test]
fn ad_hoc_unparametrized_select() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn.query("SELECT x, y FROM test WHERE x = 4").unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    let rows: Vec<(i32, i32)> = conn.query("SELECT x, y FROM test WHERE x = 2").unwrap();
    assert_eq!(rows, vec![(2, 4)]);

    let rows: Vec<(i32, i32)> = conn.query("SELECT x, y FROM test WHERE 1 = x").unwrap();
    assert_eq!(rows, vec![(1, 3)]);
}

#[test]
fn ad_hoc_unparametrized_where_in() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x IN (1, 2)")
        .unwrap();
    assert_eq!(rows, vec![(1, 3), (2, 4)]);

    let rows: Vec<(i32, i32)> = conn.query("SELECT x, y FROM test WHERE x = 4").unwrap();
    assert_eq!(rows, vec![(4, 2)]);
}

#[test]
fn prepared_unparametrized_select() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn.exec("SELECT x, y FROM test WHERE x = 4", ()).unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    let rows: Vec<(i32, i32)> = conn.exec("SELECT x, y FROM test WHERE x = 2", ()).unwrap();
    assert_eq!(rows, vec![(2, 4)]);
}

#[test]
fn order_by_basic() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).unwrap();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test ORDER BY x DESC", ()).unwrap();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
    let rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test ORDER BY y ASC", ()).unwrap();
    assert_eq!(rows, vec![(4, 2), (1, 3), (2, 4)]);
    let rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test ORDER BY y DESC", ()).unwrap();
    assert_eq!(rows, vec![(2, 4), (1, 3), (4, 2)]);
}

#[test]
fn order_by_limit_basic() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).unwrap();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY x DESC LIMIT 3", ())
        .unwrap();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
}

#[test]
#[ignore] // why doesn't this work?
fn exec_insert() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .unwrap();
    sleep();

    conn.exec_drop("INSERT INTO posts (id, number) VALUES (?, 1)", (5,))
        .unwrap();
}

#[test]
#[ignore]
fn design_doc_topk_with_preload() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .unwrap();
    sleep();

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 1)", id))
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 2)", id))
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 3)", id))
            .unwrap();
    }
    sleep();

    let simple_topk = conn
        .prep("SELECT * FROM posts WHERE number = ? ORDER BY id DESC LIMIT 3")
        .unwrap();

    let problem_topk = conn
        .prep("SELECT * FROM posts WHERE number < ? ORDER BY id DESC LIMIT 3")
        .unwrap();

    eprintln!("doing normal topk");
    // normal topk behaviour (sanity check)
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (1,)).unwrap();
    assert_eq!(rows, vec![(9, 1), (8, 1), (7, 1)]);
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (2,)).unwrap();
    assert_eq!(rows, vec![(10, 2), (4, 2), (2, 2)]);
    let rows: Vec<(i32, i32)> = conn.exec(&simple_topk, (3,)).unwrap();
    assert_eq!(rows, vec![(11, 3), (3, 3), (0, 3)]);

    eprintln!("doing bad topk");
    // problematic new topk behaviour
    let rows: Vec<(i32, i32)> = conn.exec(&problem_topk, (3,)).unwrap();
    assert_eq!(rows, vec![(10, 2), (9, 1), (8, 1)]);
}

#[test]
#[ignore]
fn design_doc_topk() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .unwrap();
    sleep();

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 1)", id))
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 2)", id))
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({}, 3)", id))
            .unwrap();
    }
    sleep();

    let problem_topk = conn
        .prep("SELECT * FROM posts WHERE number < ? ORDER BY id DESC LIMIT 3")
        .unwrap();

    let rows: Vec<(i32, i32)> = conn.exec(&problem_topk, (3,)).unwrap();
    assert_eq!(rows, vec![(10, 2), (9, 1), (8, 1)]);
}

#[test]
#[ignore]
fn ilike() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO notes (id, title) VALUES (1, 'foo')")
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (2, 'bar')")
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (3, 'baz')")
        .unwrap();
    conn.query_drop("INSERT INTO notes (id, title) VALUES (4, 'BAZ')")
        .unwrap();
    sleep();

    let rows: Vec<(i32, String)> = conn
        .exec(
            "SELECT id, title FROM notes WHERE title ILIKE ? ORDER BY id ASC",
            ("%a%",),
        )
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
        .unwrap();
    assert_eq!(
        with_other_constraint,
        vec![(3, "baz".to_string()), (4, "BAZ".to_string()),]
    );
}

#[test]
fn key_type_coercion() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE posts (id int, title TEXT)")
        .unwrap();
    conn.query_drop("INSERT INTO posts (id, title) VALUES (1, 'hi')")
        .unwrap();

    let same_type_result: (u32, String) = conn
        .exec_first("SELECT id, title FROM posts WHERE id = ?", (1,))
        .unwrap()
        .unwrap();
    assert_eq!(same_type_result, (1, "hi".to_owned()));

    let float_to_int_result: (u32, String) = conn
        .exec_first("SELECT id, title FROM posts WHERE id = ?", (1f32,))
        .unwrap()
        .unwrap();
    assert_eq!(float_to_int_result, (1, "hi".to_owned()));
}

#[test]
fn write_timestamps() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE posts (id int primary key, created_at TIMESTAMP)")
        .unwrap();
    conn.query_drop("INSERT INTO posts (id, created_at) VALUES (1, '2020-01-23 17:08:24')")
        .unwrap();
    let result: (u32, NaiveDateTime) = conn
        .exec_first("SELECT id, created_at FROM posts WHERE id = ?", (1,))
        .unwrap()
        .unwrap();

    assert_eq!(result.0, 1);
    assert_eq!(
        result.1,
        NaiveDate::from_ymd(2020, 1, 23).and_hms(17, 08, 24)
    );

    conn.query_drop("UPDATE posts SET created_at = '2021-01-25 17:08:24' WHERE id = 1")
        .unwrap();
    let result: (u32, NaiveDateTime) = conn
        .exec_first("SELECT id, created_at FROM posts WHERE id = ?", (1,))
        .unwrap()
        .unwrap();

    assert_eq!(result.0, 1);
    assert_eq!(
        result.1,
        NaiveDate::from_ymd(2021, 1, 25).and_hms(17, 08, 24)
    );
}

#[test]
fn round_trip_time_type() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE daily_events (start_time time, end_time time)")
        .unwrap();
    conn.query_drop(
        "INSERT INTO daily_events (start_time, end_time) VALUES ('08:00:00', '09:00:00')",
    )
    .unwrap();

    conn.exec_drop(
        "INSERT INTO daily_events (start_time, end_time) VALUES (?, ?)",
        (
            NaiveTime::from_hms(10, 30, 00),
            NaiveTime::from_hms(10, 45, 15),
        ),
    )
    .unwrap();

    let mut res: Vec<(NaiveTime, NaiveTime)> = conn
        .query("SELECT start_time, end_time FROM daily_events")
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

#[test]
fn multi_keyed_state() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (id int, a int, b int, c int, d int, e int, f int, g int)")
        .unwrap();
    conn.query_drop("INSERT INTO test (id, a, b, c, d, e, f, g) VALUES (1, 2, 3, 4, 5, 6, 7, 8)")
        .unwrap();

    let result: (u32, u32, u32, u32, u32, u32, u32, u32,) = conn
        .exec_first("SELECT * FROM test WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ? AND f = ? AND g = ?", (2, 3, 4, 5, 6, 7, 8,))
        .unwrap()
        .unwrap();
    assert_eq!(result, (1, 2, 3, 4, 5, 6, 7, 8,));
}

#[test]
// Test is ignored due to query reuse issue https://app.clubhouse.io/readysettech/story/380.
#[ignore]
fn reuse_similar_query() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x = ?", (4,))
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    // This query is not identical to the one above, but Noria is expected to rewrite it and then
    // reuse the same underlying view.
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE NOT x != ?", (4,))
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);
}

#[test]
fn insert_quoted_string() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data TEXT)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .unwrap();
    sleep();

    let rows: Vec<(i32, String)> = conn.query("SELECT * FROM Cats WHERE Cats.id = 1").unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\": \"Mr. Mistoffelees\"}".to_string())]
    );
}

#[test]
fn json_column_insert_read() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, data JSON)")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, data) VALUES (1, '{\"name\": \"Mr. Mistoffelees\"}')")
        .unwrap();
    sleep();

    let rows: Vec<(i32, String)> = conn.query("SELECT * FROM Cats WHERE Cats.id = 1").unwrap();
    assert_eq!(
        rows,
        vec![(1, "{\"name\": \"Mr. Mistoffelees\"}".to_string())]
    );
}

#[test]
fn explain_graphviz() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    let res = conn.query_iter("EXPLAIN GRAPHVIZ;").unwrap();
    assert_eq!(res.columns().as_ref().len(), 1);
    assert_eq!(
        res.columns().as_ref().first().unwrap().name_str(),
        "GRAPHVIZ"
    );
}

#[test]
fn explain_last_statement() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    conn.query_drop("SELECT * FROM test").unwrap();
    sleep();

    let destination: String = conn.query_first("EXPLAIN LAST STATEMENT").unwrap().unwrap();
    assert_eq!(&destination[..], "noria");
}

#[test]
fn create_query_cache_where_in() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    conn.query_drop("CREATE TABLE t (id INT);").unwrap();
    sleep();

    conn.query_drop("CREATE CACHED QUERY test AS SELECT id FROM t WHERE id IN (?);")
        .unwrap();
    sleep();

    let queries: Vec<(String, String)> = conn.query("SHOW CACHED QUERIES;").unwrap();
    assert!(queries.iter().any(|(query_name, _)| query_name == "test"));

    conn.query_drop("CREATE CACHED QUERY test AS SELECT id FROM t WHERE id IN (?, ?);")
        .unwrap();
    sleep();
    let new_queries: Vec<(String, String)> = conn.query("SHOW CACHED QUERIES;").unwrap();
    assert_eq!(new_queries.len(), queries.len());
}

#[test]
fn show_readyset_status() {
    let mut conn = mysql::Conn::new(setup(true)).unwrap();
    let ret: Vec<mysql::Row> = conn.query("SHOW READYSET STATUS;").unwrap();
    assert!(ReadySetStatus::try_from(ret).is_ok());
}
