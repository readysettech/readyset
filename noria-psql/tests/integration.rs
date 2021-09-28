use chrono::{NaiveDate, NaiveDateTime};
use noria_client::test_helpers::{self, sleep};
use noria_client::BackendBuilder;
use postgres::{NoTls, SimpleQueryMessage};

mod common;
use common::PostgreSQLAdapter;

pub fn setup(partial: bool) -> postgres::Config {
    test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        false,
        partial,
    )
}

#[test]
fn delete_basic() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .unwrap();
    sleep();

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_some());

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep();
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_only_constraint() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep();
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_multiple() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    for i in 1..4 {
        conn.simple_query(&format!("INSERT INTO Cats (id) VALUES ({})", i))
            .unwrap();
        sleep();
    }

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(2)));
        sleep();
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_none());

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 2", &[])
        .unwrap();
    assert!(row.is_none());

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 3", &[])
        .unwrap();
    assert!(row.is_some());
}

#[test]
fn delete_bogus() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    // `id` can't be both 1 and 2!
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(0)));
        sleep();
    }
}

#[test]
fn delete_bogus_valid_and() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .unwrap();
    sleep();

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_some());

    // Not that it makes much sense, but we should support this regardless...
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep();
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_bogus_valid_or() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .unwrap();
    sleep();

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_some());

    // Not that it makes much sense, but we should support this regardless...
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep();
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn delete_other_column() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    assert!(matches!(
        conn.simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = 'Bob'"),
        Err(_)
    ));
}

#[test]
fn delete_no_keys() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    assert!(matches!(
        conn.simple_query("DELETE FROM Cats WHERE 1 = 1"),
        Err(_)
    ));
}

#[test]
fn delete_compound_primary_key() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .unwrap();
    conn.simple_query("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .unwrap();
    sleep();

    {
        let res = conn
            .simple_query("DELETE FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2")
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep();
    }

    let row = conn
        .query_opt(
            "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2",
            &[],
        )
        .unwrap();
    assert!(row.is_none());

    let uid: i32 = conn
        .query_one(
            "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(3, uid);
}

#[test]
fn update_basic() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));
}

#[test]
fn update_basic_prepared() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = $1",
                &[&1],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = $1 WHERE Cats.id = $2",
                &[&"Bob", &1],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

#[test]
fn update_compound_primary_key() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 2, 'okay')")
        .unwrap();
    conn.simple_query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 3, 'still okay')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute(
                "UPDATE Vote SET Vote.reason = 'better' WHERE Vote.aid = 1 AND Vote.uid = 2",
                &[],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one(
            "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("better"));

    let name: String = conn
        .query_one(
            "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("still okay"));
}

#[test]
fn update_only_constraint() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));
}

#[test]
fn update_pkey() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty', Cats.id = 10 WHERE Cats.id = 1",
                &[],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 10", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));

    let old_row = conn
        .query_opt("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap();
    assert!(old_row.is_none());
}

#[test]
fn update_separate() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty II' WHERE Cats.id = 1",
                &[],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty II"));
}

#[test]
fn update_no_keys() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE 1 = 1";
    assert!(matches!(conn.simple_query(query), Err(_)));
}

#[test]
fn update_other_column() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.name = 'Bob'";
    assert!(matches!(conn.simple_query(query), Err(_)));
}

#[test]
fn update_bogus() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1 AND Cats.id = 2";
    assert!(matches!(conn.simple_query(query), Err(_)));
}

#[test]
fn select_collapse_where_in() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .unwrap();
    conn.simple_query("INSERT INTO Cats (id, name) VALUES (2, 'Jane')")
        .unwrap();
    sleep();

    // NOTE: It seems that Noria may require WHERE IN prepared statements to contain at least one
    // parameter. For that reason, simple_query is used instead.
    let names: Vec<String> = conn
        .simple_query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2)")
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .query(
            "SELECT Cats.name FROM Cats WHERE Cats.id IN ($1, $2)",
            &[&1, &2],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    // some lookups give empty results
    let names: Vec<String> = conn
        .simple_query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2, 3)")
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .query(
            "SELECT Cats.name FROM Cats WHERE Cats.id IN ($1, $2, $3)",
            &[&1, &2, &3],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    // also track another parameter
    let names: Vec<String> = conn
        .simple_query("SELECT Cats.name FROM Cats WHERE Cats.name = 'Bob' AND Cats.id IN (1, 2)")
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));

    let names: Vec<String> = conn
        .query(
            "SELECT Cats.name FROM Cats WHERE Cats.name = $1 AND Cats.id IN ($2, $3)",
            &[&"Bob".to_string(), &1, &2],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));
}

#[test]
fn basic_select() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    // Test binary format response.
    let rows = conn.query("SELECT test.* FROM test", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);

    // Test text format response.
    let rows = conn.simple_query("SELECT test.* FROM test").unwrap();
    let row = match rows.first().unwrap() {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!(),
    };
    assert_eq!(row.get(0).unwrap(), "4");
    assert_eq!(row.get(1).unwrap(), "2");
}

#[test]
fn strings() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x TEXT)").unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x) VALUES ('foo')")
        .unwrap();
    sleep();

    // Test binary format response.
    let rows = conn.query("SELECT test.* FROM test", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, String>(0), "foo".to_string());

    // Test text format response.
    let rows = conn.simple_query("SELECT test.* FROM test").unwrap();
    let row = match rows.first().unwrap() {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!(),
    };
    assert_eq!(row.get(0).unwrap(), "foo");
}

#[test]
fn prepared_select() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    let rows = conn
        .query("SELECT test.* FROM test WHERE x = $1", &[&4])
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);
}

#[test]
fn select_quoting_names() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x INT, y INT)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    // Test SELECT using unquoted names.
    let rows = conn.query("SELECT x AS foo FROM test", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, i32>(0), 4);

    // Test SELECT using quoted names.
    let rows = conn
        .query("SELECT \"x\" AS \"foo\" FROM \"test\"", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, i32>(0), 4);
}

#[test]
fn create_view() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    sleep();

    conn.simple_query("CREATE VIEW testview AS SELECT test.* FROM test")
        .unwrap();
    sleep();

    let rows = conn.query("SELECT testview.* FROM testview", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);

    let rows = conn.query("SELECT test.* FROM test", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);
}

#[test]
fn absurdly_simple_select() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows = conn.query("SELECT * FROM test", &[]).unwrap();
    let mut rows: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
}

#[test]
fn order_by_basic() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let mut rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY x DESC", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY y ASC", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (1, 3), (2, 4)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY y DESC", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(2, 4), (1, 3), (4, 2)]);
}

#[test]
fn order_by_limit_basic() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .unwrap();
    sleep();

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let mut rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY x DESC LIMIT 3", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
}

#[test]
fn write_timestamps() {
    let opts = setup(true);
    let mut conn = opts.connect(NoTls).unwrap();
    conn.simple_query("CREATE TABLE posts (id int primary key, created_at TIMESTAMP)")
        .unwrap();
    conn.simple_query("INSERT INTO posts (id, created_at) VALUES (1, '2020-01-23 17:08:24')")
        .unwrap();

    // Test binary format response.
    let row = conn
        .query_one("SELECT id, created_at FROM posts WHERE id = $1", &[&1])
        .unwrap();
    assert_eq!(row.get::<usize, i32>(0), 1);
    assert_eq!(
        row.get::<usize, NaiveDateTime>(1),
        NaiveDate::from_ymd(2020, 1, 23).and_hms(17, 08, 24)
    );

    // Test text format response.
    let rows = conn
        .simple_query("SELECT id, created_at FROM posts")
        .unwrap();
    let row = match rows.first().unwrap() {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!(),
    };
    assert_eq!(row.get(0).unwrap(), "1");
    assert_eq!(row.get(1).unwrap(), "2020-01-23 17:08:24");

    {
        let updated = conn
            .execute(
                "UPDATE posts SET created_at = '2021-01-25 17:08:24' WHERE id = 1",
                &[],
            )
            .unwrap();
        assert_eq!(updated, 1);
        sleep();
    }

    let row = conn
        .query_one("SELECT id, created_at FROM posts WHERE id = $1", &[&1])
        .unwrap();
    assert_eq!(row.get::<usize, i32>(0), 1);
    assert_eq!(
        row.get::<usize, NaiveDateTime>(1),
        NaiveDate::from_ymd(2021, 1, 25).and_hms(17, 08, 24)
    );
}
