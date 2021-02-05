#[macro_use]
extern crate slog;

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::MysqlIntermediary;
use mysql::prelude::*;
use nom_sql::SelectStatement;
use noria_server::{Builder, ControllerHandle, ZookeeperAuthority};
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

use noria_mysql::backend::noria_connector::NoriaConnector;
use noria_mysql::backend::BackendBuilder;

// Appends a unique ID to deployment strings, to avoid collisions between tests.
struct Deployment {
    name: String,
}

impl Deployment {
    fn new(prefix: &str) -> Self {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!(
            "{}.{}.{}",
            prefix,
            current_time.as_secs(),
            current_time.subsec_nanos()
        );

        Self { name }
    }
}

impl Drop for Deployment {
    fn drop(&mut self) {
        // Remove the ZK data if we created any:
        let zk = ZooKeeper::connect(
            "127.0.0.1:2181",
            Duration::from_secs(3),
            |_: WatchedEvent| {},
        );

        if let Ok(z) = zk {
            let _ = z.delete_recursive(&format!("/{}", self.name));
        }
    }
}

fn sleep() {
    thread::sleep(Duration::from_millis(200));
}

fn zk_addr() -> String {
    format!(
        "{}:{}",
        env::var("ZOOKEEPER_HOST").unwrap_or("127.0.0.1".into()),
        env::var("ZOOKEEPER_PORT").unwrap_or("2181".into()),
    )
}

// Initializes a Noria worker and starts processing MySQL queries against it.
// If `partial` is `false`, disables partial queries.
fn setup(deployment: &Deployment, partial: bool) -> mysql::Opts {
    // Run with VERBOSE=1 for log output.
    let verbose = env::var("VERBOSE")
        .ok()
        .and_then(|v| v.parse().ok())
        .iter()
        .any(|i| i == 1);

    let logger = if verbose {
        noria_server::logger_pls()
    } else {
        slog::Logger::root(slog::Discard, o!())
    };

    let barrier = Arc::new(Barrier::new(2));

    let l = logger.clone();
    let n = deployment.name.clone();
    let b = barrier.clone();
    thread::spawn(move || {
        let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), n)).unwrap();
        let mut builder = Builder::default();
        if !partial {
            builder.disable_partial();
        }
        authority.log_with(l.clone());
        builder.log_with(l);
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        // NOTE(malte): important to assign to a variable here, since otherwise the handle gets
        // dropped immediately and the Noria instance quits.
        let _handle = rt.block_on(builder.start(Arc::new(authority))).unwrap();
        b.wait();
        loop {
            thread::sleep(Duration::from_millis(1000));
        }
    });

    barrier.wait();

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let mut zk_auth =
        ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), deployment.name)).unwrap();
    zk_auth.log_with(logger.clone());

    debug!(logger, "Connecting to Noria...",);
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let ch = rt.block_on(ControllerHandle::new(zk_auth)).unwrap();
    debug!(logger, "Connected!");

    // no need for a barrier here since accept() acts as one
    thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        let s = rt
            .handle()
            .enter(|| tokio::net::TcpStream::from_std(s).unwrap());

        let writer = NoriaConnector::new(ch.clone(), auto_increments.clone(), query_cache.clone());
        let reader = NoriaConnector::new(ch, auto_increments, query_cache);

        let backend = BackendBuilder::new()
            .writer(rt.block_on(writer))
            .reader(rt.block_on(reader))
            .require_authentication(false)
            .build();

        rt.block_on(MysqlIntermediary::run_on_tcp(backend, s))
            .unwrap();
        drop(rt);
    });

    mysql::OptsBuilder::default().tcp_port(addr.port()).into()
}

#[test]
fn delete_basic() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
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
    let d = Deployment::new("delete_only_constraint");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("delete_multiple");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
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
    let d = Deployment::new("delete_bogus");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
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
    let d = Deployment::new("delete_bogus_valid_and");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
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
    let d = Deployment::new("delete_bogus_valid_or");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
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
    let d = Deployment::new("delete_other_column");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"Bob\"")
        .unwrap_err();
}

#[test]
fn delete_no_keys() {
    let d = Deployment::new("delete_no_keys");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("DELETE FROM Cats WHERE 1 = 1").unwrap_err();
}

#[test]
fn delete_compound_primary_key() {
    let d = Deployment::new("delete_compound_primary_key");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("delete_multi_compound_primary_key");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("update_basic");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
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
    let d = Deployment::new("update_basic");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
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
    let d = Deployment::new("update_compound_primary_key");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("update_only_constraint");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("update_pkey");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
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
    let d = Deployment::new("update_separate");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
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
    let d = Deployment::new("update_no_keys");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE 1 = 1";
    conn.query_drop(query).unwrap_err();
}

#[test]
fn update_other_column() {
    let d = Deployment::new("update_no_keys");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.name = \"Bob\"";
    conn.query_drop(query).unwrap_err();
}

#[test]
#[ignore]
fn update_no_changes() {
    // ignored because we currently *always* return 1 row(s) affected.

    let d = Deployment::new("update_no_changes");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("update_bogus");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query_drop("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    conn.query_drop("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap_err();
}

#[test]
fn select_collapse_where_in() {
    let d = Deployment::new("collapsed_where");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
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
fn basic_select() {
    let d = Deployment::new("basic_select");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("strings");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("prepared_select");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("create_view");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("create_view_rev");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
fn prepare_ranged_query_non_partial() {
    let d = Deployment::new("prepare_ranged_query_non_partial");
    let opts = setup(&d, false);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
#[should_panic]
fn prepare_conflicting_ranged_query() {
    let d = Deployment::new("prepare_conflicting_ranged_query");
    let opts = setup(&d, false);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
fn prepare_ranged_query_partial() {
    let d = Deployment::new("prepare_ranged_query_partial");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("absurdly_simple_select");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
fn order_by_basic() {
    let d = Deployment::new("order_by_basic");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).unwrap();
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
    let d = Deployment::new("order_by_limit_basic");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query_drop("INSERT INTO test (x, y) VALUES (4, 2)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (1, 3)")
        .unwrap();
    conn.query_drop("INSERT INTO test (x, y) VALUES (2, 4)")
        .unwrap();
    sleep();

    let rows: Vec<(i32, i32)> = conn.exec("SELECT * FROM test", ()).unwrap();
    assert_eq!(rows, vec![(1, 3), (2, 4), (4, 2)]);
    let rows: Vec<(i32, i32)> = conn
        .exec("SELECT * FROM test ORDER BY x DESC LIMIT 3", ())
        .unwrap();
    assert_eq!(rows, vec![(4, 2), (2, 4), (1, 3)]);
}

#[test]
#[ignore] // why doesn't this work?
fn exec_insert() {
    let d = Deployment::new("exec_insert");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query_drop("CREATE TABLE posts (id int, number int)")
        .unwrap();
    sleep();

    conn.exec_drop("INSERT INTO posts (id, number) VALUES (?, 1)", (5,))
        .unwrap();
}

#[test]
fn design_doc_topk_with_preload() {
    let d = Deployment::new("design_doc_topk_with_preload");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
fn design_doc_topk() {
    let d = Deployment::new("design_doc_topk");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
fn ilike() {
    let d = Deployment::new("ilike");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("key_type_coercion");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
    let d = Deployment::new("insert_timestamps");
    let opts = setup(&d, true);
    let mut conn = mysql::Conn::new(opts).unwrap();
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
