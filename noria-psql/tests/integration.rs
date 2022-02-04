use chrono::{NaiveDate, NaiveDateTime};
use noria_client_test_helpers::psql_helpers::{setup, setup_w_fallback};
use noria_client_test_helpers::{self, sleep};
use tokio_postgres::SimpleQueryMessage;

mod common;
use common::connect;

#[tokio::test(flavor = "multi_thread")]
async fn delete_basic() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_some());

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_only_constraint() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_multiple() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    for i in 1..4 {
        conn.simple_query(&format!("INSERT INTO Cats (id) VALUES ({})", i))
            .await
            .unwrap();
        sleep().await;
    }

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(2)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 2", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 3", &[])
        .await
        .unwrap();
    assert!(row.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    // `id` can't be both 1 and 2!
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(0)));
        sleep().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_and() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_some());

    // Not that it makes much sense, but we should support this regardless...
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_or() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_some());

    // Not that it makes much sense, but we should support this regardless...
    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_other_column() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    assert!(matches!(
        conn.simple_query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = 'Bob'")
            .await,
        Err(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_no_keys() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    assert!(matches!(
        conn.simple_query("DELETE FROM Cats WHERE 1 = 1").await,
        Err(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_compound_primary_key() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .await
    .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .await
        .unwrap();
    sleep().await;

    {
        let res = conn
            .simple_query("DELETE FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt(
            "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2",
            &[],
        )
        .await
        .unwrap();
    assert!(row.is_none());

    let uid: i32 = conn
        .query_one(
            "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(3, uid);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic_prepared() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = $1",
                &[&1],
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = $1 WHERE Cats.id = $2",
                &[&"Bob", &1],
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_compound_primary_key() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query(
        "CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))",
    )
    .await
    .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 2, 'okay')")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 3, 'still okay')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute(
                "UPDATE Vote SET Vote.reason = 'better' WHERE Vote.aid = 1 AND Vote.uid = 2",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one(
            "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("better"));

    let name: String = conn
        .query_one(
            "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("still okay"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_only_constraint() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_pkey() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty', Cats.id = 10 WHERE Cats.id = 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 10", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty"));

    let old_row = conn
        .query_opt("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(old_row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_separate() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    {
        let updated = conn
            .execute("UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1", &[])
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    {
        let updated = conn
            .execute(
                "UPDATE Cats SET Cats.name = 'Rusty II' WHERE Cats.id = 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let name: String = conn
        .query_one("SELECT Cats.name FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Rusty II"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_no_keys() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE 1 = 1";
    assert!(matches!(conn.simple_query(query).await, Err(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_other_column() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.name = 'Bob'";
    assert!(matches!(conn.simple_query(query).await, Err(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_bogus() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.id = 1 AND Cats.id = 2";
    assert!(matches!(conn.simple_query(query).await, Err(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn select_collapse_where_in() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, name) VALUES (1, 'Bob')")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO Cats (id, name) VALUES (2, 'Jane')")
        .await
        .unwrap();
    sleep().await;

    // NOTE: It seems that Noria may require WHERE IN prepared statements to contain at least one
    // parameter. For that reason, simple_query is used instead.
    let names: Vec<String> = conn
        .simple_query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2)")
        .await
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
        .await
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
        .await
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
        .await
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
        .await
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
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_select() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    // Test binary format response.
    let rows = conn.query("SELECT test.* FROM test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);

    // Test text format response.
    let rows = conn.simple_query("SELECT test.* FROM test").await.unwrap();
    let row = match rows.first().unwrap() {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!(),
    };
    assert_eq!(row.get(0).unwrap(), "4");
    assert_eq!(row.get(1).unwrap(), "2");
}

#[tokio::test(flavor = "multi_thread")]
async fn strings() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x TEXT)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x) VALUES ('foo')")
        .await
        .unwrap();
    sleep().await;

    // Test binary format response.
    let rows = conn.query("SELECT test.* FROM test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, String>(0), "foo".to_string());

    // Test text format response.
    let rows = conn.simple_query("SELECT test.* FROM test").await.unwrap();
    let row = match rows.first().unwrap() {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!(),
    };
    assert_eq!(row.get(0).unwrap(), "foo");
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_select() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows = conn
        .query("SELECT test.* FROM test WHERE x = $1", &[&4])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn select_quoting_names() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x INT, y INT)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    // Test SELECT using unquoted names.
    let rows = conn.query("SELECT x AS foo FROM test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, i32>(0), 4);

    // Test SELECT using quoted names.
    let rows = conn
        .query("SELECT \"x\" AS \"foo\" FROM \"test\"", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get::<usize, i32>(0), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_view() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("CREATE VIEW testview AS SELECT test.* FROM test")
        .await
        .unwrap();
    sleep().await;

    let rows = conn
        .query("SELECT testview.* FROM testview", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);

    let rows = conn.query("SELECT test.* FROM test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<usize, i32>(0), 4);
    assert_eq!(row.get::<usize, i32>(1), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn absurdly_simple_select() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let rows = conn.query("SELECT * FROM test", &[]).await.unwrap();
    let mut rows: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_basic() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY x DESC", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY y ASC", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (1, 3), (2, 4)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY y DESC", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(2, 4), (1, 3), (4, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_limit_basic() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (1, 3)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO test (x, y) VALUES (2, 4)")
        .await
        .unwrap();
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT * FROM test ORDER BY x DESC LIMIT 3", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i32>(1)))
        .collect();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_timestamps() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE posts (id int primary key, created_at TIMESTAMP)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO posts (id, created_at) VALUES (1, '2020-01-23 17:08:24')")
        .await
        .unwrap();

    // Test binary format response.
    let row = conn
        .query_one("SELECT id, created_at FROM posts WHERE id = $1", &[&1])
        .await
        .unwrap();
    assert_eq!(row.get::<usize, i32>(0), 1);
    assert_eq!(
        row.get::<usize, NaiveDateTime>(1),
        NaiveDate::from_ymd(2020, 1, 23).and_hms(17, 08, 24)
    );

    // Test text format response.
    let rows = conn
        .simple_query("SELECT id, created_at FROM posts")
        .await
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
            .await
            .unwrap();
        assert_eq!(updated, 1);
        sleep().await;
    }

    let row = conn
        .query_one("SELECT id, created_at FROM posts WHERE id = $1", &[&1])
        .await
        .unwrap();
    assert_eq!(row.get::<usize, i32>(0), 1);
    assert_eq!(
        row.get::<usize, NaiveDateTime>(1),
        NaiveDate::from_ymd(2021, 1, 25).and_hms(17, 08, 24)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_case_sensitive() {
    for (opts, _handle) in [setup(true).await, setup_w_fallback().await] {
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
async fn delete_case_insensitive() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE CaTs.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_some());

    {
        let res = conn
            .simple_query("DELETE FROM Cats WHERE Cats.id = 1")
            .await
            .unwrap();
        let deleted = res.first().unwrap();
        assert!(matches!(deleted, SimpleQueryMessage::CommandComplete(1)));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT CatS.iD FROM Cats WHERE CatS.Id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_graphviz() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    let res = conn.simple_query("EXPLAIN GRAPHVIZ").await.unwrap();
    let row = match res.first().unwrap() {
        SimpleQueryMessage::Row(row) => row,
        _ => panic!("Expected row"),
    };
    assert_eq!(row.columns().len(), 1);
    assert_eq!(row.columns().first().unwrap().name(), "GRAPHVIZ");
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn unordered_params_are_unsupported() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, val int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (1, 2, 'Alice')")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (2, 1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    let name: String = conn
        .query_one(
            "SELECT Cats.name FROM Cats WHERE Cats.id = $1 AND Cats.val = $2",
            &[&1, &2],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Alice"));

    let name: String = conn
        .query_one(
            "SELECT Cats.name FROM Cats WHERE Cats.id = $2 AND Cats.val = $1",
            &[&1, &2],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn reusing_params_is_unsupported() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, val int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (1, 1, 'Alice')")
        .await
        .unwrap();
    sleep().await;

    let name: String = conn
        .query_one(
            "SELECT Cats.name FROM Cats WHERE Cats.id = $1 AND Cats.val = $1",
            &[&1],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn placeholder_numbering_does_not_break_postgres() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, val int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (1, 2, 'Alice')")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (2, 1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    let name: String = conn
        .query_one(
            "SELECT Cats.name FROM Cats WHERE Cats.val = 1 AND Cats.id IN ($1, $2)",
            &[&1, &2],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // remove ignore when ENG-929 is fixed
async fn placeholder_numbering_does_not_break_postgres_ignore() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, val int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (1, 2, 'Alice')")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (2, 1, 'Bob')")
        .await
        .unwrap();
    sleep().await;

    let name: String = conn
        .query_one(
            "SELECT Cats.name FROM Cats WHERE Cats.id = $2 AND Cats.val = $1",
            &[&1, &2],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(name, String::from("Bob"));
}

/// Only tests that the query succeeds. Correctness of the query is left to the MySQL
/// integration test.
#[tokio::test(flavor = "multi_thread")]
async fn show_readyset_status() {
    let (opts, _handle) = setup(true).await;
    let conn = connect(opts).await;
    assert!(conn.simple_query("SHOW READYSET STATUS;").await.is_ok())
}
