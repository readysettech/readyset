use chrono::{NaiveDate, NaiveDateTime};
use readyset_client::consensus::AuthorityControl;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::{self, sleep, TestBuilder};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use tokio_postgres::{CommandCompleteContents, SimpleQueryMessage};

mod common;
use common::connect;

use crate::common::setup_standalone_with_authority;

async fn setup() -> (tokio_postgres::Config, Handle, ShutdownSender) {
    TestBuilder::default().build::<PostgreSQLAdapter>().await
}

#[tokio::test(flavor = "multi_thread")]
async fn two_columns_with_same_name() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;

    conn.simple_query("create table t1 (x int);").await.unwrap();

    conn.simple_query("insert into t1 (x) values (1);")
        .await
        .unwrap();

    conn.simple_query("create table t2 (x int);").await.unwrap();

    conn.simple_query("insert into t2 (x) values (2);")
        .await
        .unwrap();

    sleep().await;

    let ad_hoc_res = match conn
        .simple_query("SELECT t1.x, t2.x FROM t1, t2")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(r) => (r.get(0).unwrap().to_owned(), r.get(1).unwrap().to_owned()),
        _ => panic!(),
    };
    assert_eq!(ad_hoc_res, ("1".to_owned(), "2".to_owned()));

    let exec_res = conn
        .query_one("SELECT t1.x, t2.x FROM t1, t2", &[])
        .await
        .unwrap();
    assert_eq!(
        (exec_res.get::<_, i32>(0), exec_res.get::<_, i32>(1)),
        (1, 2)
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_only_constraint() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_multiple() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 2, .. })
        ));
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 0, .. })
        ));
        sleep().await;
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_and() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_or() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT Cats.id FROM Cats WHERE Cats.id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_other_column() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_no_keys() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    assert!(matches!(
        conn.simple_query("DELETE FROM Cats WHERE 1 = 1").await,
        Err(_)
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_compound_primary_key() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic_prepared() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_compound_primary_key() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_only_constraint() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_pkey() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_separate() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_no_keys() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE 1 = 1";
    assert!(matches!(conn.simple_query(query).await, Err(_)));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_other_column() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = 'Rusty' WHERE Cats.name = 'Bob'";
    assert!(matches!(conn.simple_query(query).await, Err(_)));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_bogus() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_collapse_where_in() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    // NOTE: It seems that ReadySet may require WHERE IN prepared statements to contain at least one
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn strings() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_quoting_names() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_view() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn absurdly_simple_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_one() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE t (x int)").await.unwrap();
    conn.simple_query("INSERT INTO t (x) VALUES (1)")
        .await
        .unwrap();

    sleep().await;

    let res = conn
        .query("SELECT 1 FROM t", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect::<Vec<i64>>();
    assert_eq!(res, vec![1]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_limit_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_timestamps() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        NaiveDate::from_ymd(2020, 1, 23).and_hms(17, 8, 24)
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
        NaiveDate::from_ymd(2021, 1, 25).and_hms(17, 8, 24)
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_case_insensitive() {
    let (opts, _handle, shutdown_tx) = setup().await;
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
        assert!(matches!(
            deleted,
            SimpleQueryMessage::CommandComplete(CommandCompleteContents { rows: 1, .. })
        ));
        sleep().await;
    }

    let row = conn
        .query_opt("SELECT CatS.iD FROM Cats WHERE CatS.Id = 1", &[])
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_graphviz() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    let res = conn.simple_query("EXPLAIN GRAPHVIZ").await.unwrap();
    let row = match res.first().unwrap() {
        SimpleQueryMessage::Row(row) => row,
        _ => panic!("Expected row"),
    };
    assert_eq!(row.columns().len(), 1);
    assert_eq!(row.columns().first().unwrap().name(), "GRAPHVIZ");

    shutdown_tx.shutdown().await;
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn unordered_params_are_unsupported() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn reusing_params_is_unsupported() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn placeholder_numbering_does_not_break_postgres() {
    let (opts, _handle, shutdown_tx) = setup().await;
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // remove ignore when ENG-929 is fixed
async fn placeholder_numbering_does_not_break_postgres_ignore() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE Cats (id int PRIMARY KEY, val int, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (1, 2, 'Alice')")
        .await
        .unwrap();
    sleep().await;

    conn.simple_query("INSERT INTO Cats (id, val, name) VALUES (2, 1, 'Bob)")
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

    shutdown_tx.shutdown().await;
}

/// Only tests that the query succeeds. Correctness of the query is left to the MySQL
/// integration test.
#[tokio::test(flavor = "multi_thread")]
async fn show_readyset_status() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("SHOW READYSET STATUS;").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn show_readyset_version() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("SHOW READYSET VERSION;").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_qualifier() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE public.t (a int)")
        .await
        .unwrap();
    conn.simple_query("SELECT public.t.a from public.t")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_search_path() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE s1.t (a int)")
        .await
        .unwrap();
    conn.simple_query("CREATE TABLE s2.t (b int)")
        .await
        .unwrap();
    conn.simple_query("CREATE TABLE s2.t2 (c int)")
        .await
        .unwrap();

    // Schema search path: [s1, s2]
    conn.simple_query("SET search_path = s1, s2").await.unwrap();
    conn.simple_query("SELECT a FROM t").await.unwrap();
    conn.simple_query("SELECT c FROM t2").await.unwrap();

    // Schema search path: [s2, s1]
    conn.simple_query("SET search_path = s2, s1").await.unwrap();
    conn.simple_query("SELECT b FROM t").await.unwrap();
    conn.simple_query("SELECT c FROM t2").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn view_schema_resolution() {
    readyset_tracing::init_test_logging();
    let (config, _handle, shutdown_tx) = setup().await;
    let client = connect(config).await;

    client
        .simple_query("SET search_path = s1,s2;")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE s1.t (x int);")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE s2.t (x int);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO s1.t (x) VALUES (1);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO s2.t (x) VALUES (2);")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW s2.v AS SELECT x FROM s2.t;")
        .await
        .unwrap();

    eventually! {
        let s2_res = client
            .query_one("SELECT x FROM v", &[])
            .await
            .unwrap()
            .get::<_, i32>(0);
        s2_res == 2
    }

    // Once we insert a view earlier in the schema search path, caches should start reading from
    // that view instead
    client
        .simple_query("CREATE VIEW s1.v AS SELECT x FROM s1.t;")
        .await
        .unwrap();

    sleep().await;

    // The cache has been dropped, so run the query once to clear the view cache and re-run CREATE
    // CACHE
    let _ = client.simple_query("SELECT x FROM v").await;

    client
        .simple_query("CREATE CACHE FROM SELECT x FROM v")
        .await
        .unwrap();

    let _ = client.simple_query("SELECT x FROM v").await;

    eventually! {
        let s1_res: i32 = client
            .query_one("SELECT x FROM v", &[])
            .await
            .unwrap()
            .get(0);
        s1_res == 1
    }

    shutdown_tx.shutdown().await;
}

/// Tests that two queries that are syntactically equivalent, but semantically different due to
/// different search paths, are executed as separate queries
#[tokio::test(flavor = "multi_thread")]
async fn same_query_different_search_path() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = setup().await;
    let conn = connect(opts).await;
    conn.simple_query("CREATE TABLE s1.t (a int)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO s1.t (a) values (1)")
        .await
        .unwrap();
    conn.simple_query("CREATE TABLE s2.t (a int)")
        .await
        .unwrap();
    conn.simple_query("INSERT INTO s2.t (a) values (2)")
        .await
        .unwrap();

    // Schema search path: [s1, s2]
    conn.simple_query("SET search_path = s1, s2").await.unwrap();
    assert_eq!(
        conn.query_one("SELECT a FROM t", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );

    // Schema search path: [s2, s1]
    conn.simple_query("SET search_path = s2, s1").await.unwrap();
    assert_eq!(
        conn.query_one("SELECT a FROM t", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        2
    );

    shutdown_tx.shutdown().await;
}

mod multiple_create_and_drop {
    use itertools::Itertools;
    use readyset_util::eventually;
    use tokio_postgres::Client;

    use crate::common::connect;
    use crate::setup;

    async fn create_query(conn: &Client, query_name: &str, query: &str) {
        conn.simple_query(&format!("CREATE CACHE {query_name} FROM {query}"))
            .await
            .unwrap();
    }

    async fn drop_query(conn: &Client, query_name: &str) {
        conn.simple_query(&format!("DROP CACHE {query_name}"))
            .await
            .unwrap();
    }

    async fn insert_values(conn: &Client, s1: Vec<i32>, s2: Vec<i32>, result: &mut Vec<i32>) {
        conn.simple_query(&format!(
            "INSERT INTO s1.t (a) values ({})",
            s1.iter().join("), (")
        ))
        .await
        .unwrap();
        conn.simple_query(&format!(
            "INSERT INTO s2.t (a) values ({})",
            s2.iter().join("), (")
        ))
        .await
        .unwrap();

        for val1 in &s1 {
            for val2 in &s2 {
                if val1 == val2 {
                    result.push(*val1);
                }
            }
        }
    }

    #[ignore = "REA-3159"]
    #[tokio::test(flavor = "multi_thread")]
    async fn same_query_name() {
        readyset_tracing::init_test_logging();
        let (opts, _handle, shutdown_tx) = setup().await;
        let conn = connect(opts).await;

        conn.simple_query("CREATE TABLE s1.t (a int)")
            .await
            .unwrap();
        conn.simple_query("CREATE TABLE s2.t (a int)")
            .await
            .unwrap();

        let mut results = Vec::new();
        let query_name = "q";
        let query = "SELECT s1.t.a FROM s1.t JOIN s2.t ON s1.t.a = s2.t.a";

        insert_values(&conn, vec![1, 2, 3], vec![2, 3, 4], &mut results).await;
        create_query(&conn, query_name, query).await;

        eventually!(
            run_test: { conn.query(query, &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>() },
            then_assert: |r| assert_eq!(r, results)
        );

        drop_query(&conn, query_name).await;
        insert_values(&conn, vec![10, 11, 12], vec![11, 13, 14], &mut results).await;
        create_query(&conn, query_name, query).await;

        eventually!(
            run_test: {
                conn.query(query, &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>()
            },
            then_assert: |r| assert_eq!(r, results)
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn different_query_name() {
        readyset_tracing::init_test_logging();
        let (opts, _handle, shutdown_tx) = setup().await;
        let conn = connect(opts).await;

        conn.simple_query("CREATE TABLE s1.t (a int)")
            .await
            .unwrap();
        conn.simple_query("CREATE TABLE s2.t (a int)")
            .await
            .unwrap();

        let mut results = Vec::new();
        let query = "SELECT s1.t.a FROM s1.t JOIN s2.t ON s1.t.a = s2.t.a";

        insert_values(&conn, vec![1, 2, 3], vec![2, 3, 4], &mut results).await;
        create_query(&conn, "q1", query).await;

        eventually!(
            run_test: {
                conn.query(query, &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>()
            },
            then_assert: |r| assert_eq!(r, results)
        );

        drop_query(&conn, "q1").await;
        insert_values(&conn, vec![10, 11, 12], vec![11, 13, 14], &mut results).await;
        create_query(&conn, "q2", query).await;

        eventually!(
            run_test: {
                conn.query(query, &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>()
            },
            then_assert: |r| assert_eq!(r, results)
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_cache_concurrently() {
        use tokio_postgres::{SimpleQueryMessage, SimpleQueryRow};
        fn extract_single_value(mut rows: Vec<SimpleQueryMessage>) -> SimpleQueryRow {
            match rows.swap_remove(0) {
                SimpleQueryMessage::Row(r) => r,
                _ => panic!(),
            }
        }

        readyset_tracing::init_test_logging();
        let (opts, _handle, shutdown_tx) = setup().await;
        let conn = connect(opts).await;

        conn.simple_query("CREATE TABLE t (a int)").await.unwrap();
        let valid_cache = extract_single_value(
            conn.simple_query("CREATE CACHE CONCURRENTLY FROM SELECT a FROM t")
                .await
                .unwrap(),
        );

        let invalid_cache = extract_single_value(
            conn.simple_query("CREATE CACHE CONCURRENTLY FROM SELECT b FROM t")
                .await
                .unwrap(),
        );

        eventually!(
            run_test: {
                extract_single_value(
                    conn.simple_query(&format!(
                        "SHOW READYSET MIGRATION STATUS {}",
                        valid_cache.get(0).unwrap()
                    ))
                    .await
                    .unwrap()
                )
            },
            then_assert: |completed| assert_eq!(completed.get(0).unwrap(), "Completed")
        );
        eventually!(
            run_test: {
                extract_single_value(
                    conn.simple_query(&format!(
                        "SHOW READYSET MIGRATION STATUS {}",
                        invalid_cache.get(0).unwrap()
                    ))
                    .await
                    .unwrap(),
                )
            },
            then_assert: |failed| assert_eq!(&failed.get(0).unwrap()[..6], "Failed")
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_cache_concurrently_always() {
        use tokio_postgres::{SimpleQueryMessage, SimpleQueryRow};
        fn extract_single_value(mut rows: Vec<SimpleQueryMessage>) -> SimpleQueryRow {
            match rows.swap_remove(0) {
                SimpleQueryMessage::Row(r) => r,
                _ => panic!(),
            }
        }

        readyset_tracing::init_test_logging();
        let (opts, _handle, shutdown_tx) = setup().await;
        let conn = connect(opts).await;

        conn.simple_query("CREATE TABLE t (a int)").await.unwrap();
        let valid_cache = extract_single_value(
            conn.simple_query("CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT a FROM t")
                .await
                .unwrap(),
        );

        let invalid_cache = extract_single_value(
            conn.simple_query("CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT b FROM t")
                .await
                .unwrap(),
        );

        eventually!(
            run_test: {
                extract_single_value(
                    conn.simple_query(&format!(
                        "SHOW READYSET MIGRATION STATUS {}",
                        valid_cache.get(0).unwrap()
                    ))
                    .await
                    .unwrap()
                )
            },
            then_assert: |completed| assert_eq!(completed.get(0).unwrap(), "Completed")
        );
        eventually!(
            run_test: {
                extract_single_value(
                    conn.simple_query(&format!(
                        "SHOW READYSET MIGRATION STATUS {}",
                        invalid_cache.get(0).unwrap()
                    ))
                    .await
                    .unwrap(),
                )
            },
            then_assert: |failed| assert_eq!(&failed.get(0).unwrap()[..6], "Failed")
        );

        shutdown_tx.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn caches_go_in_authority_list() {
    readyset_tracing::init_test_logging();

    let (config, _handle, authority, shutdown_tx) =
        setup_standalone_with_authority("caches_go_in_authority_list", None, true).await;

    let queries = [
        "CREATE TABLE t (x int);",
        "CREATE CACHE q FROM SELECT x FROM t;",
    ];

    let conn = connect(config).await;
    for query in queries {
        let _res = conn.simple_query(query).await.expect("query failed");
        // give it some time to propagate
        sleep().await;
    }

    let res = authority.cache_ddl_requests().await.unwrap();
    let unparsed_stmt = res.get(0).unwrap();
    assert_eq!(unparsed_stmt, "CREATE CACHE q FROM SELECT x FROM t;");

    shutdown_tx.shutdown().await;
}
