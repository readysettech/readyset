use std::env;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use mysql_async::prelude::*;
use mysql_async::{params, Conn, OptsBuilder, Row, Value};
use readyset_adapter::backend::noria_connector::ReadBehavior;
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_adapter::proxied_queries_reporter::ProxiedQueriesReporter;
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_adapter::BackendBuilder;
use readyset_client::status::CurrentStatus;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{last_query_info, MySQLAdapter};
use readyset_client_test_helpers::{sleep, TestBuilder};
use readyset_errors::ReadySetError;
use readyset_server::Handle;
use readyset_telemetry_reporter::{TelemetryEvent, TelemetryInitializer, TelemetryReporter};
use readyset_util::shutdown::ShutdownSender;
use readyset_util::{eventually, retry_with_exponential_backoff};
use regex::Regex;
use test_utils::{skip_flaky_finder, tags};

async fn setup_with_mysql_flags<F>(set: F) -> (mysql_async::Opts, Handle, ShutdownSender)
where
    F: Fn(TestBuilder) -> TestBuilder,
{
    readyset_tracing::init_test_logging();
    let mut users = std::collections::HashMap::new();
    users.insert("root".to_string(), "noria".to_string());

    let builder = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(false)
            .users(users),
    );

    set(builder).build::<MySQLAdapter>().await
}

async fn setup_with_mysql() -> (mysql_async::Opts, Handle, ShutdownSender) {
    setup_with_mysql_flags(std::convert::identity).await
}

async fn setup() -> (mysql_async::Opts, Handle, ShutdownSender) {
    readyset_tracing::init_test_logging();
    TestBuilder::default()
        .replicate(false)
        .build::<MySQLAdapter>()
        .await
}

async fn setup_telemetry() -> (TelemetryReporter, mysql_async::Opts, Handle, ShutdownSender) {
    let (sender, reporter) = TelemetryInitializer::test_init();

    let backend = BackendBuilder::new()
        .require_authentication(false)
        .migration_mode(MigrationMode::OutOfBand)
        .telemetry_sender(sender);
    let (opts, handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate(false)
        .build::<MySQLAdapter>()
        .await;

    (reporter, opts, handle, shutdown_tx)
}

fn mysql_url() -> String {
    format!(
        "mysql://root:noria@{}:{}/noria",
        env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
    )
}

async fn connect() -> Conn {
    retry_with_exponential_backoff!(
        {  Conn::from_url(mysql_url()).await },
        retries: 10,
        delay: 100,
        backoff: 2,
    )
    .unwrap()
}

async fn query_one(conn: &mut Conn, query: &str) -> Vec<Vec<Value>> {
    conn.query(query)
        .await
        .unwrap()
        .into_iter()
        .map(|x: Row| x.unwrap())
        .collect()
}

#[allow(dead_code)] // no idea why rust says this is unused
async fn query_both(
    c1: &mut Conn,
    c2: &mut Conn,
    query: &str,
) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    (query_one(c1, query).await, query_one(c2, query).await)
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_join_key() {
    // This used to trigger a bug involving weak indexes. See issue #179 for more info.

    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE a (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE b (id INT, a_id INT, PRIMARY KEY (id))")
        .await
        .unwrap();

    conn.query_drop("INSERT INTO b VALUES (1, 99)")
        .await
        .unwrap();

    conn.query_drop("SELECT a.id FROM a JOIN b on b.a_id = a.id WHERE a.id = 42")
        .await
        .unwrap();

    conn.query_drop("INSERT INTO b VALUES (2, 99)")
        .await
        .unwrap();

    conn.query_drop("DELETE FROM b WHERE id = 1").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_columns_with_same_name() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("create table t1 (x int);").await.unwrap();

    conn.query_drop("insert into t1 (x) values (1);")
        .await
        .unwrap();

    conn.query_drop("create table t2 (x int);").await.unwrap();

    conn.query_drop("insert into t2 (x) values (2);")
        .await
        .unwrap();

    sleep().await;

    let ad_hoc_res = conn
        .query_first::<(i32, i32), _>("SELECT t1.x, t2.x FROM t1, t2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ad_hoc_res, (1, 2));

    let exec_res = conn
        .exec_first::<(i32, i32), _, _>("SELECT t1.x, t2.x FROM t1, t2", ())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exec_res, (1, 2));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
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
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_only_constraint() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_multiple() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    for i in 1..4 {
        conn.query_drop(format!("INSERT INTO Cats (id) VALUES ({i})"))
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
        let query = format!("SELECT Cats.id FROM Cats WHERE Cats.id = {i}");
        let row = conn.query_first::<Row, _>(query).await.unwrap();
        assert!(row.is_none());
    }

    let row = conn
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 3")
        .await
        .unwrap();
    assert!(row.is_some());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_and() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
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
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_bogus_valid_or() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO Cats (id) VALUES (1)")
        .await
        .unwrap();
    sleep().await;

    let row = conn
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
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
        .query_first::<Row, _>("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_other_column() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"Bob\"")
        .await
        .unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_no_keys() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("DELETE FROM Cats WHERE 1 = 1")
        .await
        .unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_compound_primary_key() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
    let row = conn.query_first::<Row, _>(q).await.unwrap();
    assert!(row.is_none());

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let uid: i32 = conn.query_first(q).await.unwrap().unwrap();
    assert_eq!(uid, 3);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn delete_multi_compound_primary_key() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        let row = conn.query_first::<Row, _>(q).await.unwrap();
        assert!(row.is_none());
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_basic_prepared() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_compound_primary_key() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_only_constraint() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_pkey() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        .query_first::<Row, _>("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .await
        .unwrap();
    assert!(old_row.is_none());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_separate() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_no_keys() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE 1 = 1";
    conn.query_drop(query).await.unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_other_column() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .await
        .unwrap();
    sleep().await;

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.name = \"Bob\"";
    conn.query_drop(query).await.unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn update_no_changes() {
    // ignored because we currently *always* return 1 row(s) affected.
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_bogus() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_pkey() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_collapse_where_in() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "Bob"));
    assert!(names.iter().any(|s| s == "Jane"));

    let names: Vec<String> = conn
        .exec("SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?)", (1, 2))
        .await
        .unwrap()
        .into_iter()
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
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
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
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
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
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
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
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
        .map(|mut row: Row| row.take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "Bob"));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_in() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
            "SELECT name from Cats where id in (?, ?) and name in (?, ?) ORDER BY name",
            (1, 2, "Bob", "Jane"),
        )
        .await
        .unwrap();

    assert_eq!(names, vec![("Bob".to_owned(),), ("Jane".to_owned(),)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<Row> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn strings() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x TEXT)").await.unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x) VALUES ('foo')")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(String,)> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows, vec![("foo".to_string(),)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<Row> = conn
        .exec("SELECT test.* FROM test WHERE x = ?", (4,))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // results actually arrive as integer values since prepared statements use the binary protocol
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec![4.into(), 2.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_view() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    let rows: Vec<Row> = conn.query("SELECT testview.* FROM testview").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    let rows: Vec<Row> = conn.query("SELECT test.* FROM test").await.unwrap();
    assert_eq!(rows.len(), 1);
    // NOTE(malte): the row contains strings (!) because non-prepared statements are executed via
    // the MySQL text protocol, and we receive the result as `Bytes`.
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn create_view_rev() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        conn.query::<Row, _>("SELECT testview.* FROM testview")
            .await
            .unwrap()
            .len(),
        1
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn prepare_ranged_query_non_partial() {
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate(false)
        .partial(false)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
#[should_panic]
async fn prepare_conflicting_ranged_query() {
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate(false)
        .partial(false)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn prepare_ranged_query_partial() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn absurdly_simple_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ad_hoc_unparameterized_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ad_hoc_unparameterized_where_in() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        .query("SELECT x, y FROM test WHERE x IN (1, 2) ORDER BY x")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 3), (2, 4)]);

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT x, y FROM test WHERE x = 4")
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_unparameterized_select() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_limit_basic() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // why doesn't this work?
async fn exec_insert() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    conn.exec_drop("INSERT INTO posts (id, number) VALUES (?, 1)", (5,))
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exec_insert_multiple() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (x int)").await.unwrap();
    sleep().await;

    conn.exec_drop("INSERT INTO t (x) VALUES (?), (?)", (1, 2))
        .await
        .unwrap();

    let res = conn.query::<(i32,), _>("SELECT x FROM t").await.unwrap();
    assert_eq!(res, vec![(1,), (2,)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn design_doc_topk_with_preload() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 1)"))
            .await
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 2)"))
            .await
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 3)"))
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn design_doc_topk() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .await
        .unwrap();
    sleep().await;

    for id in 5..10 {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 1)"))
            .await
            .unwrap();
    }
    for id in &[10, 4, 2, 1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 2)"))
            .await
            .unwrap();
    }
    for id in &[11, 3, 0, -1] {
        conn.query_drop(format!("INSERT INTO posts (id, number) VALUES ({id}, 3)"))
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ilike() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn key_type_coercion() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_timestamps() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
        NaiveDate::from_ymd_opt(2020, 1, 23)
            .unwrap()
            .and_hms_opt(17, 8, 24)
            .unwrap()
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
        NaiveDate::from_ymd_opt(2021, 1, 25)
            .unwrap()
            .and_hms_opt(17, 8, 24)
            .unwrap()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_time_type() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
            NaiveTime::from_hms_opt(10, 30, 00).unwrap(),
            NaiveTime::from_hms_opt(10, 45, 15).unwrap(),
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
                NaiveTime::from_hms_opt(8, 00, 00).unwrap(),
                NaiveTime::from_hms_opt(9, 00, 00).unwrap(),
            ),
            (
                NaiveTime::from_hms_opt(10, 30, 00).unwrap(),
                NaiveTime::from_hms_opt(10, 45, 15).unwrap(),
            )
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_keyed_state() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (id int, a int, b int, c int, d int, e int, f int, g int)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test (id, a, b, c, d, e, f, g) VALUES (1, 2, 3, 4, 5, 6, 7, 8)")
        .await
        .unwrap();

    eventually!(run_test: {
        let result = conn
            .exec_first("SELECT * FROM test WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ? AND f = ? AND g = ?", (2, 3, 4, 5, 6, 7, 8,))
            .await;
        AssertUnwindSafe(move || result)
    }, then_assert: |result| {
        assert_eq!(result().unwrap(), Some((1, 2, 3, 4, 5, 6, 7, 8,)));
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reuse_similar_query() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x BETWEEN ? AND ?", (4, 5))
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    // This query is not identical to the one above, but ReadySet is expected to rewrite it and then
    // reuse the same underlying view.
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT x, y FROM test WHERE x >= ? AND x <= ?", (4, 5))
        .await
        .unwrap();
    assert_eq!(rows, vec![(4, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_quoted_string() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "REA-4099"]
async fn json_column_insert_read() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_graphviz() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_last_statement() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
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
    assert_matches!(destination.destination, QueryDestination::Readyset(_));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_query_cache_where_in() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT);").await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE test FROM SELECT id FROM t WHERE id IN (?);")
        .await
        .unwrap();
    sleep().await;

    let queries: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES;").await.unwrap();
    assert!(queries
        .iter()
        .any(|(_, query_name, _, properties, _)| query_name == "test"
            && !properties.contains("always")));

    conn.query_drop("CREATE CACHE test FROM SELECT id FROM t WHERE id IN (?, ?);")
        .await
        .unwrap();
    sleep().await;
    let new_queries: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES;").await.unwrap();
    assert_eq!(new_queries.len(), queries.len());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn show_caches_with_always() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT);").await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE ALWAYS test_always FROM SELECT id FROM t WHERE id IN (?, ?);")
        .await
        .unwrap();
    sleep().await;
    let queries: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES;").await.unwrap();
    assert!(queries.iter().any(
        |(_, query_name, _, properties, _)| query_name == "test_always"
            && properties.contains("always")
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn show_readyset_status() {
    let (opts, _handle, shutdown_tx) = setup_with_mysql().await;
    let mut conn = Conn::new(opts).await.unwrap();
    let mut ret: Vec<Row> = conn.query("SHOW READYSET STATUS;").await.unwrap();

    let valid_timestamp = |s: String| {
        if s == "NULL" {
            true
        } else {
            NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S UTC").is_ok()
        }
    };

    let valid_binlog = |s: String| {
        if s == "NULL" {
            true
        } else {
            let re = Regex::new(r"^binlog\.\d{6}\:\d{1,}$").unwrap();
            re.is_match(&s)
        }
    };

    // NOTE: If this readyset extension has changed, verify the new behavior is correct then update
    // the expected values below
    assert_eq!(ret.len(), 9);
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Database Connection");
    assert_eq!(row.get::<String, _>(1).unwrap(), "Connected");
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Connection Count");
    assert_eq!(row.get::<String, _>(1).unwrap(), "0");
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Status");
    assert_eq!(
        row.get::<String, _>(1).unwrap(),
        CurrentStatus::Online.to_string()
    );
    let row = ret.remove(0);
    assert_eq!(
        row.get::<String, _>(0).unwrap(),
        "Maximum Replication Offset"
    );
    assert!(valid_binlog(row.get::<String, _>(1).unwrap()));
    let row = ret.remove(0);
    assert_eq!(
        row.get::<String, _>(0).unwrap(),
        "Minimum Replication Offset"
    );
    assert!(valid_binlog(row.get::<String, _>(1).unwrap()));
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Last started Controller");
    assert!(valid_timestamp(row.get::<String, _>(1).unwrap()));
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Last completed snapshot");
    assert!(valid_timestamp(row.get::<String, _>(1).unwrap()));
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Last started replication");
    assert!(valid_timestamp(row.get::<String, _>(1).unwrap()));
    let row = ret.remove(0);
    assert_eq!(row.get::<String, _>(0).unwrap(), "Enabled Features");
    assert_eq!(row.get::<String, _>(1).unwrap(), "None");
    readyset_maintenance_mode(&mut conn).await;
    shutdown_tx.shutdown().await;
}

async fn readyset_maintenance_mode(conn: &mut Conn) {
    conn.query_drop("ALTER READYSET ENTER MAINTENANCE MODE;")
        .await
        .unwrap();
    sleep().await;
    let ret: Vec<Row> = conn.query("SHOW READYSET STATUS;").await.unwrap();
    assert_eq!(ret.len(), 9);
    // find the row with "Status"
    let row = ret
        .iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Status")
        .unwrap();
    assert_eq!(
        row.get::<String, _>(1).unwrap(),
        CurrentStatus::MaintenanceMode.to_string()
    );
    conn.query_drop("ALTER READYSET EXIT MAINTENANCE MODE;")
        .await
        .unwrap();
    sleep().await;
    let ret: Vec<Row> = conn.query("SHOW READYSET STATUS;").await.unwrap();
    assert_eq!(ret.len(), 9);
    let row = ret
        .iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Status")
        .unwrap();
    assert_eq!(
        row.get::<String, _>(1).unwrap(),
        CurrentStatus::Online.to_string()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn show_readyset_version() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop("SHOW READYSET VERSION;")
        .await
        .expect("should be OK");

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_nonblocking_select() {
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .replicate(false)
        .read_behavior(ReadBehavior::NonBlocking)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(opts).await.unwrap();
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
    res.unwrap_err();

    // Long enough to wait for upquery.
    sleep().await;

    let mut rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).await.unwrap();
    rows.sort_by_key(|(a, _)| *a);
    assert_eq!(rows, vec![(4, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn switch_database_with_use() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE s1.t (a int)").await.unwrap();
    conn.query_drop("CREATE TABLE s2.t (b int)").await.unwrap();
    conn.query_drop("CREATE TABLE s2.t2 (c int)").await.unwrap();

    conn.query_drop("USE s1;").await.unwrap();
    conn.query_drop("SELECT a FROM t").await.unwrap();

    conn.query_drop("USE s2;").await.unwrap();
    conn.query_drop("SELECT b FROM t").await.unwrap();
    conn.query_drop("SELECT c FROM t2").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_default_db_in_connection_opts() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let opts = OptsBuilder::from_opts(opts).db_name(Some("s1"));

    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE t (a int)").await.unwrap();
    conn.query_drop("SELECT a from s1.t").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[skip_flaky_finder]
async fn test_show_proxied_queries_telemetry() {
    readyset_tracing::init_test_logging();
    let (mut reporter, opts, _handle, shutdown_tx) = setup_telemetry().await;

    let mut conn = Conn::new(opts).await.unwrap();

    assert!(reporter
        .check_event(TelemetryEvent::ShowProxiedQueries)
        .await
        .is_empty());

    conn.query_drop("SHOW PROXIED QUERIES").await.unwrap();
    reporter
        .test_run_once(&mut tokio::time::interval(Duration::from_nanos(1)))
        .await;

    assert_eq!(
        1,
        reporter
            .check_event(TelemetryEvent::ShowProxiedQueries)
            .await
            .len()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_show_caches_queries_telemetry() {
    readyset_tracing::init_test_logging();
    let (mut reporter, opts, _handle, shutdown_tx) = setup_telemetry().await;

    let mut conn = Conn::new(opts).await.unwrap();

    assert!(reporter
        .check_event(TelemetryEvent::ShowCaches)
        .await
        .is_empty());

    conn.query_drop("SHOW CACHES").await.unwrap();
    reporter
        .test_run_once(&mut tokio::time::interval(Duration::from_nanos(1)))
        .await;

    assert_eq!(
        1,
        reporter.check_event(TelemetryEvent::ShowCaches).await.len()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Flaky test (REA-2878)"]
async fn test_proxied_queries_telemetry() {
    readyset_tracing::init_test_logging();
    // This variation on setup_telemetry sets up a periodic reporter for proxied queries.
    let (mut reporter, opts, _handle, shutdown_tx) = {
        let query_status_cache = Box::leak(Box::new(
            QueryStatusCache::new().style(MigrationStyle::Explicit),
        ));
        let proxied_queries_reporter = Arc::new(ProxiedQueriesReporter::new(query_status_cache));
        let (telemetry_sender, mut reporter) = TelemetryInitializer::test_init();
        reporter
            .register_periodic_reporter(proxied_queries_reporter)
            .await;

        let backend = BackendBuilder::new()
            .require_authentication(false)
            .migration_mode(MigrationMode::OutOfBand)
            .telemetry_sender(telemetry_sender.clone());

        let (opts, handle, shutdown_tx) = TestBuilder::new(backend)
            .replicate(false)
            .query_status_cache(query_status_cache)
            .migration_mode(MigrationMode::OutOfBand)
            .replicate(true)
            .build::<MySQLAdapter>()
            .await;
        (reporter, opts, handle, shutdown_tx)
    };

    let mut conn = Conn::new(opts).await.unwrap();
    assert!(reporter
        .check_event(TelemetryEvent::ProxiedQuery)
        .await
        .is_empty());

    conn.query_drop("CREATE TABLE t1 (a int)").await.unwrap();
    sleep().await;

    // Since explicit migration mode is on, this will be proxied.
    let q = "SELECT * from t1";
    conn.query_drop(q).await.unwrap();
    sleep().await;
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );

    let mut interval = tokio::time::interval(Duration::from_millis(1));

    // Run once to get the new event, and once to run the periodic reporter
    reporter.test_run_once(&mut interval).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    reporter.test_run_once(&mut interval).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    let telemetry = reporter
        .check_event(TelemetryEvent::ProxiedQuery)
        .await
        .first()
        .cloned()
        .expect("should have 1 element");
    sleep().await;

    assert_eq!(
        telemetry.proxied_query,
        Some("SELECT * FROM `anon_id_0`".to_string())
    );

    // We don't actually run dry run migrations, so just check that this field is populated
    // with its initial value
    assert_eq!(telemetry.migration_status, Some("pending".to_string()));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn datetime_nanosecond_precision_text_protocol() {
    let mut direct_mysql = connect().await;
    direct_mysql.query_drop("SET sql_mode='';
             DROP TABLE IF EXISTS dt_nano_text_protocol CASCADE;
             CREATE TABLE dt_nano_text_protocol (col1 DATETIME, col2 DATETIME(2), col3 DATETIME(4), col4 DATETIME(6));
             INSERT INTO dt_nano_text_protocol VALUES ('2021-01-01 00:00:00', '2021-01-01 00:00:00.00', '2021-01-01 00:00:00.0000', '2021-01-01 00:00:00.000000');
             INSERT INTO dt_nano_text_protocol VALUES ('2021-01-01 00:00:00', '2021-01-01 00:00:00.01', '2021-01-01 00:00:00.0001', '2021-01-01 00:00:00.000001');
             INSERT INTO dt_nano_text_protocol VALUES ('0000-00-00 00:00:00', '0000-00-00 00:00:00.00', '0000-00-00 00:00:00.0000', '0000-00-00 00:00:00.000000');")
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM dt_nano_text_protocol")
        .await
        .unwrap();
    sleep().await;

    let mut my_rows: Vec<(String, String, String, String)> = direct_mysql
        .query("SELECT * FROM dt_nano_text_protocol")
        .await
        .unwrap();
    let mut rs_rows: Vec<(String, String, String, String)> = conn
        .query("SELECT * FROM dt_nano_text_protocol")
        .await
        .unwrap();
    my_rows.sort();
    rs_rows.sort();
    assert_eq!(rs_rows, my_rows);

    direct_mysql.query_drop("INSERT INTO dt_nano_text_protocol VALUES ('2021-01-02 00:00:00', '2021-01-02 00:00:00.00', '2021-01-02 00:00:00.0000', '2021-01-02 00:00:00.000000');").await.unwrap();
    direct_mysql.query_drop("INSERT INTO dt_nano_text_protocol VALUES ('2021-01-02 00:00:00', '2021-01-02 00:00:00.01', '2021-01-02 00:00:00.0001', '2021-01-02 00:00:00.000001');").await.unwrap();

    sleep().await;

    eventually!(run_test: {
        let mut my_rows: Vec<(String, String, String, String)> = direct_mysql
        .query("SELECT * FROM dt_nano_text_protocol")
        .await
        .unwrap();

        let mut rs_rows: Vec<(String, String, String, String)> = conn
            .query("SELECT * FROM dt_nano_text_protocol")
            .await
            .unwrap();

        my_rows.sort();
        rs_rows.sort();
        AssertUnwindSafe(move || (rs_rows, my_rows))
    },then_assert: |results| {
        let (rs_rows, my_rows) = results();
        assert_eq!(rs_rows, my_rows)
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn datetime_nanosecond_precision_binary_protocol() {
    let mut direct_mysql = connect().await;
    direct_mysql.query_drop("SET sql_mode='';
             DROP TABLE IF EXISTS dt_nano_bin_protocol CASCADE;
             CREATE TABLE dt_nano_bin_protocol (ID INT PRIMARY KEY, col1 DATETIME, col2 DATETIME(2), col3 DATETIME(4), col4 DATETIME(6));
             INSERT INTO dt_nano_bin_protocol VALUES (1, '2021-01-01 00:00:00', '2021-01-01 00:00:00.00', '2021-01-01 00:00:00.0000', '2021-01-01 00:00:00.000000');
             INSERT INTO dt_nano_bin_protocol VALUES (2, '2021-01-01 00:00:00', '2021-01-01 00:00:00.01', '2021-01-01 00:00:00.0001', '2021-01-01 00:00:00.000001');
             INSERT INTO dt_nano_bin_protocol VALUES (3, '0000-00-00 00:00:00', '0000-00-00 00:00:00.00', '0000-00-00 00:00:00.0000', '0000-00-00 00:00:00.000000');")
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM dt_nano_bin_protocol WHERE ID = ?")
        .await
        .unwrap();
    sleep().await;

    for id in 1..=3 {
        let my_rows: Row = direct_mysql
            .exec_first("SELECT * FROM dt_nano_bin_protocol WHERE ID = ?", (id,))
            .await
            .unwrap()
            .unwrap();
        let rs_rows: Row = conn
            .exec_first("SELECT * FROM dt_nano_bin_protocol WHERE ID = ?", (id,))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rs_rows.unwrap_raw(), my_rows.unwrap_raw())
    }

    direct_mysql.query_drop("INSERT INTO dt_nano_bin_protocol VALUES (4, '2021-01-02 00:00:00', '2021-01-02 00:00:00.00', '2021-01-02 00:00:00.0000', '2021-01-02 00:00:00.000000');").await.unwrap();
    direct_mysql.query_drop("INSERT INTO dt_nano_bin_protocol VALUES (5, '2021-01-02 00:00:00', '2021-01-02 00:00:00.01', '2021-01-02 00:00:00.0001', '2021-01-02 00:00:00.000001');").await.unwrap();

    sleep().await;

    for id in 4..=5 {
        eventually!(run_test: {
            let my_rows: Row = direct_mysql
                .exec_first("SELECT * FROM dt_nano_bin_protocol WHERE ID = ?", (id,))
                .await
                .unwrap()
                .unwrap();
            let rs_rows: Row = conn
                .exec_first("SELECT * FROM dt_nano_bin_protocol WHERE ID = ?", (id,))
                .await
                .unwrap()
                .unwrap();
            AssertUnwindSafe(move || (rs_rows, my_rows))
        },then_assert: |results| {
            let (rs_rows, my_rows) = results();
            assert_eq!(rs_rows.unwrap_raw(), my_rows.unwrap_raw())
        });
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn datetime_binary_protocol() {
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    let mut direct_mysql = connect().await;
    direct_mysql.query_drop("SET sql_mode='';").await.unwrap();
    direct_mysql
        .query_drop("DROP TABLE IF EXISTS dt_bin_protocol CASCADE;")
        .await
        .unwrap();
    direct_mysql.query_drop("CREATE TABLE dt_bin_protocol (ID INT PRIMARY KEY, col1 DATETIME(6), col2 DATETIME(6), col3 DATETIME(6), col4 DATETIME(6));").await.unwrap();
    direct_mysql.query_drop("INSERT INTO dt_bin_protocol VALUES (1, '0000-00-00 00:00:00.000000', '2021-01-01 00:00:00.000000', '2021-01-01 00:00:01.0000000', '2021-01-01 00:00:01.000001');")
        .await
        .unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM dt_bin_protocol WHERE ID = ?")
        .await
        .unwrap();
    sleep().await;
    let rs_rows: Row = conn
        .exec_first("SELECT * FROM dt_bin_protocol WHERE ID = 1", ())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rs_rows.unwrap_raw(),
        vec![
            Some(Value::Int(1)),
            Some(Value::Date(0, 0, 0, 0, 0, 0, 0)),
            Some(Value::Date(2021, 1, 1, 0, 0, 0, 0)),
            Some(Value::Date(2021, 1, 1, 0, 0, 1, 0)),
            Some(Value::Date(2021, 1, 1, 0, 0, 1, 1))
        ]
    );
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn timestamp_binary_protocol() {
    let mut direct_mysql = connect().await;
    direct_mysql.query_drop("
             SET SESSION time_zone = '+05:00';
             DROP TABLE IF EXISTS ts_bin_protocol CASCADE;
             CREATE TABLE ts_bin_protocol (ID INT PRIMARY KEY, col1 TIMESTAMP, col2 TIMESTAMP(2), col3 TIMESTAMP(4), col4 TIMESTAMP(6));
             INSERT INTO ts_bin_protocol VALUES (1, '2021-01-01 00:00:00', '2021-01-01 00:00:00.00', '2021-01-01 00:00:00.0000', '2021-01-01 00:00:00.000000');
             INSERT INTO ts_bin_protocol VALUES (2, '2021-01-01 00:00:00', '2021-01-01 00:00:00.01', '2021-01-01 00:00:00.0001', '2021-01-01 00:00:00.000001');")
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    sleep().await;

    direct_mysql.query_drop("INSERT INTO ts_bin_protocol VALUES (3, '2021-01-02 00:00:00', '2021-01-02 00:00:00.00', '2021-01-02 00:00:00.0000', '2021-01-02 00:00:00.000000');").await.unwrap();
    direct_mysql.query_drop("INSERT INTO ts_bin_protocol VALUES (4, '2021-01-02 00:00:00', '2021-01-02 00:00:00.01', '2021-01-02 00:00:00.0001', '2021-01-02 00:00:00.000001');").await.unwrap();
    direct_mysql
        .query_drop("SET SESSION time_zone = SYSTEM;")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT * FROM ts_bin_protocol WHERE ID = ?")
        .await
        .unwrap();
    sleep().await;

    for id in 1..=4 {
        eventually!(run_test: {

        let my_rows: Row = direct_mysql
            .exec_first("SELECT * FROM ts_bin_protocol WHERE ID = ?", (id,))
            .await
            .unwrap()
            .unwrap();
        let rs_rows: Row = conn
            .exec_first("SELECT * FROM ts_bin_protocol WHERE ID = ?", (id,))
            .await
            .unwrap()
            .unwrap();
            AssertUnwindSafe(move || (rs_rows, my_rows))
        },then_assert: |results| {
            let (rs_rows, my_rows) = results();
            assert_eq!(rs_rows.unwrap_raw(), my_rows.unwrap_raw())
        });
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn timestamp_text_protocol() {
    let mut direct_mysql = connect().await;
    direct_mysql.query_drop("
             SET SESSION time_zone = '+05:00';
             DROP TABLE IF EXISTS ts_text_protocol CASCADE;
             CREATE TABLE ts_text_protocol (ID INT PRIMARY KEY, col1 TIMESTAMP, col2 TIMESTAMP(2), col3 TIMESTAMP(4), col4 TIMESTAMP(6));
             INSERT INTO ts_text_protocol VALUES (1, '2021-01-01 00:00:00', '2021-01-01 00:00:00.00', '2021-01-01 00:00:00.0000', '2021-01-01 00:00:00.000000');
             INSERT INTO ts_text_protocol VALUES (2, '2021-01-01 00:00:00', '2021-01-01 00:00:00.01', '2021-01-01 00:00:00.0001', '2021-01-01 00:00:00.000001');")
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    sleep().await;

    direct_mysql.query_drop("INSERT INTO ts_text_protocol VALUES (3, '2021-01-02 00:00:00', '2021-01-02 00:00:00.00', '2021-01-02 00:00:00.0000', '2021-01-02 00:00:00.000000');").await.unwrap();
    direct_mysql.query_drop("INSERT INTO ts_text_protocol VALUES (4, '2021-01-02 00:00:00', '2021-01-02 00:00:00.01', '2021-01-02 00:00:00.0001', '2021-01-02 00:00:00.000001');").await.unwrap();
    direct_mysql
        .query_drop("SET SESSION time_zone = SYSTEM;")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT * FROM ts_text_protocol WHERE ID = ?")
        .await
        .unwrap();
    sleep().await;

    for id in 1..=4 {
        eventually!(run_test: {

        let q = format!("SELECT * FROM ts_text_protocol WHERE ID = {id}");
        let my_rows: Vec<(String, String, String, String, String)> = direct_mysql
            .query(&q)
            .await
            .unwrap();
        let rs_rows: Vec<(String, String, String, String, String)> = conn
            .query(&q)
            .await
            .unwrap();
            AssertUnwindSafe(move || (rs_rows, my_rows))
        },then_assert: |results| {
            let (rs_rows, my_rows) = results();
            assert_eq!(rs_rows, my_rows)
        });
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn date_only_text_protocol() {
    readyset_tracing::init_test_logging();
    let mut direct_mysql = connect().await;
    direct_mysql
        .query_drop(
            "DROP TABLE IF EXISTS date_text_protocol CASCADE;
             CREATE TABLE date_text_protocol (col1  DATE);
             INSERT INTO date_text_protocol VALUES ('2021-01-01');",
        )
        .await
        .unwrap();

    let (rs_opts, _rs_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = Conn::new(rs_opts).await.unwrap();
    sleep().await;

    conn.query_drop("CREATE CACHE FROM SELECT * FROM date_text_protocol WHERE col1 = ?")
        .await
        .unwrap();
    sleep().await;

    eventually!(run_test: {
        let my_rows: Vec<(String,)> = direct_mysql
            .query("SELECT * FROM date_text_protocol WHERE col1 = '2021-01-01'")
            .await
            .unwrap();
        let rs_rows: Vec<(String,)> = conn
            .query("SELECT * FROM date_text_protocol WHERE col1 = '2021-01-01'")
            .await
            .unwrap();
            AssertUnwindSafe(move || (rs_rows, my_rows))
    }, then_assert: |results| {
        let (rs_rows, my_rows) = results();
        assert_eq!(rs_rows, my_rows)
    });

    direct_mysql
        .query_drop("INSERT INTO date_text_protocol VALUES ('2021-01-02');")
        .await
        .unwrap();

    eventually!(run_test: {
        let my_rows: Vec<(String,)> = direct_mysql
            .query("SELECT * FROM date_text_protocol WHERE col1 = '2021-01-02'")
            .await
            .unwrap();
        let rs_rows: Vec<(String,)> = conn
            .query("SELECT * FROM date_text_protocol WHERE col1 = '2021-01-02'")
            .await
            .unwrap();
            AssertUnwindSafe(move || (rs_rows, my_rows))
    }, then_assert: |results| {
        let (rs_rows, my_rows) = results();
        assert_eq!(rs_rows, my_rows)
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_char_column_padding_binary_collation() {
    readyset_tracing::init_test_logging();
    let mut my_conn = connect().await;
    my_conn
        .query_drop(
            "DROP TABLE IF EXISTS char_binary_padding CASCADE;
             CREATE TABLE char_binary_padding (col1 CHAR(5) COLLATE 'binary');
             INSERT INTO char_binary_padding VALUES ('foo'), ('¥'), ('');",
        )
        .await
        .unwrap();

    let (rs_opts, _rs_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = Conn::new(rs_opts).await.unwrap();
    sleep().await;

    let vals: Vec<(&str, usize)> = vec![
        ("foo", 0),
        ("foo\0\0", 1),
        ("¥", 0),
        ("¥\0\0\0", 1),
        ("", 0),
        ("\0\0\0\0\0", 1),
    ];
    for (i, (val, count)) in vals.into_iter().enumerate() {
        // Test with binary protocol
        eventually!(run_test: {
            let my_rows: Vec<Vec<u8>> = my_conn
                .exec("SELECT * FROM char_binary_padding WHERE col1 = ?", (val.as_bytes(),))
                .await
                .unwrap();
            let rs_rows: Vec<Vec<u8>> = rs_conn
                .exec("SELECT * FROM char_binary_padding WHERE col1 = ?", (val.as_bytes(),))
                .await
                .unwrap();
                AssertUnwindSafe(move || (my_rows, rs_rows))
        }, then_assert: |results| {
            let (my_rows, rs_rows) = results();
            assert_eq!(my_rows, rs_rows, "[{i}] Expected {count} row(s) matching {val:?}");
            assert_eq!(my_rows.len(), count);
        });
        // Test with text protocol
        eventually!(run_test: {
            let my_rows: Vec<Vec<u8>> = my_conn
                .query(dbg!(format!("SELECT * FROM char_binary_padding WHERE col1 = '{val}'")))
                .await
                .unwrap();
            let rs_rows: Vec<Vec<u8>> = rs_conn
                .query(format!("SELECT * FROM char_binary_padding WHERE col1 = '{val}'"))
                .await
                .unwrap();
                AssertUnwindSafe(move || (my_rows, rs_rows))
        }, then_assert: |results| {
            let (my_rows, rs_rows) = results();
            assert_eq!(my_rows, rs_rows, "[{i}] Expected {count} row(s) matching {val:?}");
            assert_eq!(my_rows.len(), count);
        });
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_binary_column_padding() {
    readyset_tracing::init_test_logging();
    let mut my_conn = connect().await;
    my_conn
        .query_drop(
            "DROP TABLE IF EXISTS binary_padding CASCADE;
             CREATE TABLE binary_padding (col1 BINARY(5));
             INSERT INTO binary_padding VALUES ('foo'), ('¥'), ('');",
        )
        .await
        .unwrap();

    let (rs_opts, _rs_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = Conn::new(rs_opts).await.unwrap();
    sleep().await;

    let vals: Vec<(&str, usize)> = vec![
        ("foo", 0),
        ("foo\0\0", 1),
        ("¥", 0),
        ("¥\0\0\0", 1),
        ("", 0),
        ("\0\0\0\0\0", 1),
    ];
    for (i, (val, count)) in vals.into_iter().enumerate() {
        // Test with binary protocol
        eventually!(run_test: {
            let my_rows: Vec<Vec<u8>> = my_conn
                .exec("SELECT col1 FROM binary_padding WHERE col1 = ?", (val.as_bytes(),))
                .await
                .unwrap();
            let rs_rows: Vec<Vec<u8>> = rs_conn
                .exec("SELECT col1 FROM binary_padding WHERE col1 = ?", (val.as_bytes(),))
                .await
                .unwrap();
                AssertUnwindSafe(move || (my_rows, rs_rows))
        }, then_assert: |results| {
            let (my_rows, rs_rows) = results();
            assert_eq!(my_rows, rs_rows, "[{i}] Expected {count} row(s) matching {val:?}");
            assert_eq!(my_rows.len(), count);
        });
        // Test with text protocol
        eventually!(run_test: {
            let my_rows: Vec<(Vec<u8>, Option<bool>)> = my_conn
                .query(format!("SELECT col1, col1 = '{val}' FROM binary_padding WHERE col1 = '{val}'"))
                .await
                .unwrap();
            let rs_rows: Vec<(Vec<u8>, Option<bool>)> = rs_conn
                .query(format!("SELECT col1, col1 = '{val}' FROM binary_padding WHERE col1 = '{val}'"))
                .await
                .unwrap();
                AssertUnwindSafe(move || (my_rows, rs_rows))
        }, then_assert: |results| {
            let (my_rows, rs_rows) = results();
            assert_eq!(my_rows, rs_rows, "[{i}] Expected {count} row(s) matching {val:?}");
            assert_eq!(my_rows.len(), count);
        });
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_case_expr_then_expr() {
    let mut direct_mysql = connect().await;
    let (opts, _handle, shutdown_tx) = setup_with_mysql_flags(|b| b.recreate_database(false)).await;
    let mut conn = Conn::new(opts).await.unwrap();
    sleep().await;

    direct_mysql
        .query_drop("DROP TABLE IF EXISTS test_case_expr_then_expr CASCADE;")
        .await
        .unwrap();
    direct_mysql
        .query_drop(
            "CREATE TABLE test_case_expr_then_expr (id INT PRIMARY KEY, col1 INT, col2 INT);",
        )
        .await
        .unwrap();
    direct_mysql
        .query_drop("INSERT INTO test_case_expr_then_expr VALUES (1, 1, 2);")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT CASE col1 WHEN 1 THEN col2 ELSE col1 END AS t FROM test_case_expr_then_expr WHERE id = ?")
        .await
        .unwrap();
    sleep().await;

    let rs_rows: Vec<String> = conn
        .query("SELECT CASE col1 WHEN 1 THEN col2 ELSE col1 END AS t FROM test_case_expr_then_expr WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(rs_rows, vec![(String::from("2"))]);

    shutdown_tx.shutdown().await;
}

async fn populate_all_data_types(direct_mysql: &mut Conn, table_name: &str, add_sample_data: bool) {
    // Drop existing table if any
    direct_mysql
        .query_drop(format!("DROP TABLE IF EXISTS {table_name} CASCADE;"))
        .await
        .unwrap();
    // Create table
    direct_mysql
        .query_drop(
            format!("CREATE TABLE {table_name} (
        -- Numeric Data Types (Signed and Unsigned Integers)
        col_tinyint TINYINT NOT NULL,                  -- Signed (-128 to 127)
        col_tinyint_unsigned TINYINT UNSIGNED NOT NULL, -- Unsigned (0 to 255)

        col_smallint SMALLINT NOT NULL,                 -- Signed (-32,768 to 32,767)
        col_smallint_unsigned SMALLINT UNSIGNED NOT NULL, -- Unsigned (0 to 65,535)

        col_mediumint MEDIUMINT NOT NULL,               -- Signed (-8,388,608 to 8,388,607)
        col_mediumint_unsigned MEDIUMINT UNSIGNED NOT NULL, -- Unsigned (0 to 16,777,215)

        col_int INT NOT NULL,                           -- Signed (-2,147,483,648 to 2,147,483,647)
        col_int_unsigned INT UNSIGNED NOT NULL,         -- Unsigned (0 to 4,294,967,295)

        col_bigint BIGINT NOT NULL,                     -- Signed (-2^63 to 2^63-1)
        col_bigint_unsigned BIGINT UNSIGNED NOT NULL,   -- Unsigned (0 to 2^64-1)

        col_decimal DECIMAL(12,2) NOT NULL,             -- Fixed-point exact decimal (e.g., 99999999.99)
        col_numeric NUMERIC(10,2) NOT NULL,             -- Synonym for DECIMAL

        col_float FLOAT NOT NULL,                       -- 32-bit floating point (approximate value)
        col_double DOUBLE NOT NULL,                     -- 64-bit floating point (approximate value)
        col_real REAL NOT NULL,                         -- Synonym for DOUBLE

        col_bit BIT(8) NOT NULL,                        -- Bit field (e.g., 8-bit binary)
        col_boolean BOOLEAN NOT NULL,                   -- Alias for TINYINT(1) (0 = false, 1 = true)

        -- Date and Time Data Types
        col_date DATE NOT NULL,
        col_datetime DATETIME(2) NOT NULL,
        col_timestamp TIMESTAMP NOT NULL,
        col_time TIME NOT NULL,

        -- String Data Types
        col_char CHAR(10) NOT NULL,
        col_varchar VARCHAR(255) NOT NULL,

        col_text TEXT NOT NULL,
        col_tinytext TINYTEXT NOT NULL,
        col_mediumtext MEDIUMTEXT NOT NULL,
        col_longtext LONGTEXT NOT NULL,

        col_blob BLOB NOT NULL,
        col_tinyblob TINYBLOB NOT NULL,
        col_mediumblob MEDIUMBLOB NOT NULL,
        col_longblob LONGBLOB NOT NULL,

        col_enum ENUM('small', 'medium', 'large') NOT NULL,
        col_json JSON NOT NULL
    );")
        )
        .await
        .unwrap();
    if add_sample_data {
        // Insert test data
        direct_mysql
            .query_drop(format!(
                "INSERT INTO {table_name} (
col_tinyint, col_tinyint_unsigned,
col_smallint, col_smallint_unsigned,
col_mediumint, col_mediumint_unsigned,
col_int, col_int_unsigned,
col_bigint, col_bigint_unsigned,

col_decimal, col_numeric,
col_float, col_double, col_real,
col_bit, col_boolean,

col_date, col_datetime, col_timestamp, col_time,

col_char, col_varchar,

col_text, col_tinytext, col_mediumtext, col_longtext,

col_blob, col_tinyblob, col_mediumblob, col_longblob,

col_enum, col_json
) VALUES (
-- Integer types
-128, 255,
-32768, 65535,
-8388608, 16777215,
-2147483648, 4294967295,
-9223372036854775808, 18446744073709551615,

-- Fixed-point and floating types
1234567890.12, 98765.43,
3.14, 2.71828, 1.618,

-- Bit and boolean
b'10101010', TRUE,

-- Date and time
'2025-03-20',
'2025-03-20 12:34:56.78',
CURRENT_TIMESTAMP,
'23:59:59',

-- Strings
'char_data',
'This is a varchar string.',

'This is some long TEXT content...',
'tiny text!',
'This is a medium text field. It can hold more data than tinytext.',
'This is a LONGTEXT. It can store a LOT of characters — up to 4GB!',

'blob data here',
'tb',
'medium blob example',
'long blob contents here',

'medium',
'{{\"name\": \"Alice\", \"roles\": [\"admin\", \"editor\"], \"active\": true}}'
);"
            ))
            .await
            .unwrap();
    }

    sleep().await;
}

async fn test_column_definition_verify(
    direct_mysql: &mut Conn,
    rs_conn: &mut Conn,
    table_name: &str,
) {
    // Verify results
    let direct_rows: Vec<Row> = direct_mysql
        .query(format!("SELECT * FROM {table_name}"))
        .await
        .unwrap();
    let rs_rows: Vec<Row> = rs_conn
        .query(format!("SELECT * FROM {table_name}"))
        .await
        .unwrap();

    let direct_columns = direct_rows[0].columns();
    let rs_columns = rs_rows[0].columns();

    // Compare columns
    for (direct_column, rs_column) in direct_columns.iter().zip(rs_columns.iter()) {
        let column_name = String::from_utf8_lossy(direct_column.name_ref());
        assert_eq!(
            direct_column.column_length(),
            rs_column.column_length(),
            "Column length mismatch for column: {column_name}"
        );
        assert_eq!(
            direct_column.character_set(),
            rs_column.character_set(),
            "Character set mismatch for column: {column_name}"
        );
        assert_eq!(
            direct_column.column_type(),
            rs_column.column_type(),
            "Column type mismatch for column: {column_name}"
        );
        assert_eq!(
            direct_column.flags(),
            rs_column.flags(),
            "Column flags mismatch for column: {column_name}"
        );
        assert_eq!(
            direct_column.decimals(),
            rs_column.decimals(),
            "Column decimals mismatch for column: {column_name}"
        );
        assert_eq!(
            direct_column.name_ref(),
            rs_column.name_ref(),
            "Column name mismatch for column: {column_name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_column_definition_upstream_readyset_snapshot() {
    readyset_tracing::init_test_logging();
    let mut direct_mysql = connect().await;
    populate_all_data_types(&mut direct_mysql, "all_data_types", true).await;

    // Setup ReadySet connection after table creation
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(rs_opts).await.unwrap();

    test_column_definition_verify(&mut direct_mysql, &mut conn, "all_data_types").await;
    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_column_definition_upstream_readyset_replication() {
    readyset_tracing::init_test_logging();
    let mut direct_mysql = connect().await;

    // Setup ReadySet connection before table creation
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = Conn::new(rs_opts).await.unwrap();

    populate_all_data_types(&mut direct_mysql, "all_data_types", true).await;
    test_column_definition_verify(&mut direct_mysql, &mut conn, "all_data_types").await;
    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn text_citext_default_coercion_minimal_row_base_replication() {
    readyset_tracing::init_test_logging();
    let mut direct_mysql = connect().await;

    direct_mysql
        .query_drop("DROP TABLE IF EXISTS text_citext_default_coercion CASCADE;")
        .await
        .unwrap();
    direct_mysql
        .query_drop("CREATE TABLE text_citext_default_coercion (id INT PRIMARY KEY, col1 TEXT);")
        .await
        .unwrap();

    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .build::<MySQLAdapter>()
        .await;
    sleep().await;
    let mut conn = Conn::new(rs_opts).await.unwrap();
    direct_mysql
        .query_drop("SET SESSION binlog_row_image = MINIMAL;")
        .await
        .unwrap();
    direct_mysql
        .query_drop("INSERT INTO text_citext_default_coercion (id) VALUES (1);")
        .await
        .unwrap();
    sleep().await;
    conn.query_drop("CREATE CACHE FROM SELECT * FROM text_citext_default_coercion WHERE id = ?")
        .await
        .unwrap();
    let rs_row: Row = conn
        .query_first("SELECT * FROM text_citext_default_coercion WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    let direct_row: Row = direct_mysql
        .query_first("SELECT * FROM text_citext_default_coercion WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rs_row.len(), 2);
    assert_eq!(rs_row[0], direct_row[0]);
    assert_eq!(rs_row[1], direct_row[1]);
    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_default_value_not_null_for_replication() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    sleep().await;
    let mut direct_mysql = connect().await;
    direct_mysql
        .query_drop("SET SESSION binlog_row_image = MINIMAL; SET SESSION sql_mode = '';")
        .await
        .unwrap();
    populate_all_data_types(&mut direct_mysql, "all_data_types_not_null", false).await;
    direct_mysql
        .query_drop("INSERT INTO all_data_types_not_null () VALUES ();")
        .await
        .unwrap();

    let mut conn = Conn::new(rs_opts).await.unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT * FROM all_data_types_not_null")
        .await
        .unwrap();
    let rs_row: Row = conn
        .query_first("SELECT * FROM all_data_types_not_null")
        .await
        .unwrap()
        .unwrap();
    let direct_row: Row = direct_mysql
        .query_first("SELECT * FROM all_data_types_not_null")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rs_row.len(), direct_row.len());
    let row_length = rs_row.len();
    for i in 0..row_length {
        assert_eq!(
            rs_row[i],
            direct_row[i],
            "Value mismatch for column: {}",
            String::from_utf8_lossy(rs_row.columns()[i].name_ref())
        );
    }
    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
async fn create_duplicate_unnamed_caches() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();

    conn.query_drop("CREATE CACHE FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();
    conn.query_drop("DROP ALL CACHES").await.unwrap();
    conn.query_drop("CREATE CACHE FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();

    let caches: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(caches.len(), 1, "unexpected caches: {caches:?}");

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
async fn create_duplicate_named_caches() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();

    conn.query_drop("CREATE CACHE name1 FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE name2 FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE name2 FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();

    let caches: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(caches.len(), 1, "unexpected caches: {caches:?}");

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
async fn create_duplicate_query_id_and_name_caches() {
    let (opts, _handle, shutdown_tx) = setup().await;
    let mut conn = Conn::new(opts).await.unwrap();

    conn.query_drop("CREATE TABLE foo (a INT, b INT)")
        .await
        .unwrap();

    conn.query_drop("CREATE CACHE name1 FROM SELECT b FROM foo")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE name2 FROM SELECT b FROM foo WHERE a = ?")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE name1 FROM SELECT b FROM foo WHERE a = 1")
        .await
        .unwrap();
    conn.query_drop("CREATE CACHE name1 FROM SELECT b FROM foo WHERE a = 3")
        .await
        .unwrap();

    let caches: Vec<(String, String, String, String, String)> =
        conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(caches.len(), 1, "unexpected caches: {caches:?}");

    shutdown_tx.shutdown().await;
}

async fn test_utf8(coll: &str) {
    readyset_tracing::init_test_logging();
    let (rs_opts, _rs_handle, tx) = setup_with_mysql().await;
    sleep().await;
    let mut mysql = connect().await;
    let mut rs = Conn::new(rs_opts).await.unwrap();

    mysql
        .query_drop(format!(
            "create table {coll} (
                 a int primary key,
                 b varchar(10) collate {coll}
             )"
        ))
        .await
        .unwrap();

    mysql
        .query_drop(format!(
            "insert into {coll} values (1, 'e'), (2, 'é'), (3, 'E'), (4, 'e'), (5, 'f')"
        ))
        .await
        .unwrap();
    sleep().await;
    rs.query_drop(format!(
        "create cache from select * from {coll} order by b, a"
    ))
    .await
    .unwrap();
    sleep().await;

    let (m, r) = query_both(
        &mut mysql,
        &mut rs,
        &format!("select * from {coll} order by b, a"),
    )
    .await;
    assert_eq!(m, r);

    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8_ai_ci() {
    test_utf8("utf8mb4_0900_ai_ci").await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8_as_ci() {
    test_utf8("utf8mb4_0900_as_ci").await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8mb4_general_ci() {
    test_utf8("utf8mb4_general_ci").await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8mb4_unicode_ci() {
    test_utf8("utf8mb4_unicode_ci").await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8mb3_unicode_ci() {
    test_utf8("utf8mb3_unicode_ci").await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn test_latin1_swedish_ci() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _rs_handle, tx) = setup_with_mysql().await;
    sleep().await;
    let mut mysql = connect().await;
    let mut rs = Conn::new(rs_opts).await.unwrap();

    mysql
        .query_drop(
            "create table swedish (
                 a int primary key,
                 b varchar(1) character set latin1 collate latin1_swedish_ci
             )",
        )
        .await
        .unwrap();

    for i in 0..=255 {
        mysql
            .query_drop(format!(
                "insert into swedish values ({i}, _latin1 x'{i:02x}')"
            ))
            .await
            .unwrap();
    }
    sleep().await;

    rs.query_drop("create cache from select * from swedish order by b, a")
        .await
        .unwrap();
    rs.query_drop("create cache from select * from swedish where b = ? order by a")
        .await
        .unwrap();
    sleep().await;

    let (m, r) = query_both(&mut mysql, &mut rs, "select * from swedish order by b, a").await;
    assert_eq!(m, r);

    async fn run(db: &mut Conn, i: u8) -> Vec<(u8, String)> {
        let i = yore::code_pages::CP1252.decode(&[i]).to_string();
        "select * from swedish where b = :i order by a"
            .with(params! {
                "i" => i,
            })
            .run(db)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    for i in 0..=255 {
        let m = run(&mut mysql, i).await;
        let r = run(&mut rs, i).await;
        assert_eq!(m, r, "on codepoint {i}");
    }

    tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql8_upstream)]
async fn test_utf8mb4_bin() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _rs_handle, tx) = setup_with_mysql().await;
    sleep().await;
    let mut mysql = connect().await;
    let mut rs = Conn::new(rs_opts).await.unwrap();

    fn utf8_str_for(i: u32) -> String {
        let mut buf = [0; 8];
        let buf = char::from_u32(i).unwrap().encode_utf8(&mut buf);
        let buf = buf
            .as_bytes()
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>();
        format!("_utf8 x'{}'", buf.join(""))
    }

    mysql
        .query_drop(
            "create table utf8_bin (
                 a int primary key,
                 b varchar(1) collate utf8mb4_bin
             )",
        )
        .await
        .unwrap();

    // this range in UCA has several codepoints that aren't in numerical order
    for i in 16..=255 {
        let utf = utf8_str_for(i);
        mysql
            .query_drop(format!("insert into utf8_bin values ({i}, {utf})"))
            .await
            .unwrap();
    }
    sleep().await;

    rs.query_drop("create cache from select * from utf8_bin order by b, a")
        .await
        .unwrap();
    rs.query_drop("create cache from select * from utf8_bin where b = ? order by a")
        .await
        .unwrap();
    sleep().await;

    let (m, r) = query_both(&mut mysql, &mut rs, "select * from utf8_bin order by b, a").await;
    assert_eq!(m, r);

    async fn run(db: &mut Conn, i: u8) -> Vec<(u8, String)> {
        let i = yore::code_pages::CP1252.decode(&[i]).to_string();
        "select * from utf8_bin where b = :i order by a"
            .with(params! {
                "i" => i,
            })
            .run(db)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    for i in 16..=255 {
        let m = run(&mut mysql, i).await;
        let r = run(&mut rs, i).await;
        assert_eq!(m, r, "on codepoint {i}");
    }

    tx.shutdown().await;
}
