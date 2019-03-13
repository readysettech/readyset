extern crate msql_srv;
extern crate mysql;
extern crate nom_sql;
extern crate noria_server;
#[macro_use]
extern crate slog;
extern crate tokio;
extern crate zookeeper;

extern crate noria_mysql;

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria_server::{Builder, ZookeeperAuthority};
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

use noria_mysql::NoriaBackend;

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

// Initializes a Noria worker and starts processing MySQL queries against it.
fn setup(deployment: &Deployment) -> mysql::Opts {
    let zk_addr = "127.0.0.1:2181";
    // Run with VERBOSE=1 for log output.
    let verbose = match env::var("VERBOSE") {
        Ok(value) => {
            let i: u32 = value.parse().unwrap();
            i == 1
        }
        Err(_) => false,
    };

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
        let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, n)).unwrap();
        let mut builder = Builder::default();
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

    let deployment_id = deployment.name.clone();

    // no need for a barrier here since accept() acts as one
    thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();

        let b = NoriaBackend::new(
            zk_addr,
            &deployment_id,
            auto_increments,
            query_cache,
            false,
            true,
            true,
            logger,
        );

        MysqlIntermediary::run_on_tcp(b, s).unwrap();
    });

    let mut builder = mysql::OptsBuilder::default();
    builder.tcp_port(addr.port());
    builder.into()
}

#[test]
fn delete_basic() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1").unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_only_constraint() {
    let d = Deployment::new("delete_only_constraint");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1").unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_multiple() {
    let d = Deployment::new("delete_multiple");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    for i in 1..4 {
        conn.query(format!("INSERT INTO Cats (id) VALUES ({})", i))
            .unwrap();
        sleep();
    }

    {
        let deleted = conn
            .query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep();
    }

    for i in 1..3 {
        let query = format!("SELECT Cats.id FROM Cats WHERE Cats.id = {}", i);
        let row = conn.query(query).unwrap().next();
        assert!(row.is_none());
    }

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 3")
        .unwrap()
        .next();
    assert!(row.is_some());
}

#[test]
fn delete_bogus() {
    let d = Deployment::new("delete_bogus");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    // `id` can't be both 1 and 2!
    let deleted = conn
        .query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap();
    assert_eq!(deleted.affected_rows(), 0);
}

#[test]
fn delete_bogus_valid_and() {
    let d = Deployment::new("delete_bogus_valid_and");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_bogus_valid_or() {
    let d = Deployment::new("delete_bogus_valid_or");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn
            .query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let row = conn
        .query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_other_column() {
    let d = Deployment::new("delete_other_column");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"Bob\"")
        .unwrap_err();
}

#[test]
fn delete_no_keys() {
    let d = Deployment::new("delete_no_keys");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("DELETE FROM Cats WHERE 1 = 1").unwrap_err();
}

#[test]
fn delete_compound_primary_key() {
    let d = Deployment::new("delete_compound_primary_key");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .unwrap();
    conn.query("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .unwrap();
    sleep();

    {
        let q = "DELETE FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let deleted = conn.query(q).unwrap();
        assert_eq!(deleted.affected_rows(), 1);
        sleep();
    }

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let row = conn.query(q).unwrap().next();
    assert!(row.is_none());

    let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let uid: i32 = conn.first(q).unwrap().unwrap();
    assert_eq!(uid, 3);
}

#[test]
#[ignore]
fn delete_multi_compound_primary_key() {
    let d = Deployment::new("delete_multi_compound_primary_key");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Vote (aid, uid) VALUES (1, 2)")
        .unwrap();
    conn.query("INSERT INTO Vote (aid, uid) VALUES (1, 3)")
        .unwrap();
    sleep();

    {
        let q = "DELETE FROM Vote WHERE (Vote.aid = 1 AND Vote.uid = 2) OR (Vote.aid = 1 AND Vote.uid = 3)";
        let deleted = conn.query(q).unwrap();
        assert_eq!(deleted.affected_rows(), 2);
        sleep();
    }

    for _ in 2..4 {
        let q = "SELECT Vote.uid FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
        let row = conn.query(q).unwrap().next();
        assert!(row.is_none());
    }
}

#[test]
fn update_basic() {
    let d = Deployment::new("update_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("\"Rusty\""));
}

#[test]
fn update_basic_prepared() {
    let d = Deployment::new("update_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .prep_exec(
                "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = ?",
                (1,),
            )
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("\"Rusty\""));

    {
        let updated = conn
            .prep_exec(
                "UPDATE Cats SET Cats.name = ? WHERE Cats.id = ?",
                ("Bob", 1),
            )
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Bob"));
}

#[test]
fn update_compound_primary_key() {
    let d = Deployment::new("update_compound_primary_key");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Vote (aid int, uid int, reason VARCHAR(255), PRIMARY KEY(aid, uid))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 2, \"okay\")")
        .unwrap();
    conn.query("INSERT INTO Vote (aid, uid, reason) VALUES (1, 3, \"still okay\")")
        .unwrap();
    sleep();

    {
        let q = "UPDATE Vote SET Vote.reason = \"better\" WHERE Vote.aid = 1 AND Vote.uid = 2";
        let updated = conn.query(q).unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 2";
    let name: String = conn.first(q).unwrap().unwrap();
    assert_eq!(name, String::from("\"better\""));

    let q = "SELECT Vote.reason FROM Vote WHERE Vote.aid = 1 AND Vote.uid = 3";
    let name: String = conn.first(q).unwrap().unwrap();
    assert_eq!(name, String::from("\"still okay\""));
}

#[test]
fn update_only_constraint() {
    let d = Deployment::new("update_only_constraint");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    // Note that this doesn't have `id int PRIMARY KEY` like the other tests:
    conn.query("CREATE TABLE Cats (id int, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("\"Rusty\""));
}

#[test]
fn update_pkey() {
    let d = Deployment::new("update_pkey");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let query = "UPDATE Cats SET Cats.name = \"Rusty\", Cats.id = 10 WHERE Cats.id = 1";
        let updated = conn.query(query).unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 10")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("\"Rusty\""));
    let old_row = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(old_row.is_none());
}

#[test]
fn update_separate() {
    let d = Deployment::new("update_separate");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn
            .query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    {
        let updated = conn
            .query("UPDATE Cats SET Cats.name = \"Rusty II\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
        sleep();
    }

    let name: String = conn
        .first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("\"Rusty II\""));
}

#[test]
fn update_no_keys() {
    let d = Deployment::new("update_no_keys");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE 1 = 1";
    conn.query(query).unwrap_err();
}

#[test]
fn update_other_column() {
    let d = Deployment::new("update_no_keys");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.name = \"Bob\"";
    conn.query(query).unwrap_err();
}

#[test]
#[ignore]
fn update_no_changes() {
    // ignored because we currently *always* return 1 row(s) affected.

    let d = Deployment::new("update_no_changes");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    let updated = conn
        .query("UPDATE Cats SET Cats.name = \"Bob\" WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(updated.affected_rows(), 0);
}

#[test]
fn update_bogus() {
    let d = Deployment::new("update_bogus");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    conn.query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap_err();
}

#[test]
fn select_collapse_where_in() {
    let d = Deployment::new("collapsed_where");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    conn.query("INSERT INTO Cats (id, name) VALUES (2, \"Jane\")")
        .unwrap();
    sleep();

    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2)")
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "\"Bob\""));
    assert!(names.iter().any(|s| s == "\"Jane\""));

    let names: Vec<String> = conn
        .prep_exec("SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?)", (1, 2))
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "\"Bob\""));
    assert!(names.iter().any(|s| s == "\"Jane\""));

    // some lookups give empty results
    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.id IN (1, 2, 3)")
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "\"Bob\""));
    assert!(names.iter().any(|s| s == "\"Jane\""));

    let names: Vec<String> = conn
        .prep_exec(
            "SELECT Cats.name FROM Cats WHERE Cats.id IN (?, ?, ?)",
            (1, 2, 3),
        )
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|s| s == "\"Bob\""));
    assert!(names.iter().any(|s| s == "\"Jane\""));

    // also track another parameter
    let names: Vec<String> = conn
        .query("SELECT Cats.name FROM Cats WHERE Cats.name = 'Bob' AND Cats.id IN (1, 2)")
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "\"Bob\""));

    let names: Vec<String> = conn
        .prep_exec(
            "SELECT Cats.name FROM Cats WHERE Cats.name = ? AND Cats.id IN (?, ?)",
            ("Bob", 1, 2),
        )
        .unwrap()
        .map(|row| row.unwrap().take::<String, _>(0).unwrap())
        .collect();
    assert_eq!(names.len(), 1);
    assert!(names.iter().any(|s| s == "\"Bob\""));
}

#[test]
fn basic_select() {
    let d = Deployment::new("basic_select");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query("INSERT INTO test (x, y) VALUES (4, 2)").unwrap();
    sleep();

    assert_eq!(conn.query("SELECT test.* FROM test").unwrap().count(), 1);
}

#[test]
fn create_view() {
    let d = Deployment::new("create_view");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query("INSERT INTO test (x, y) VALUES (4, 2)").unwrap();
    sleep();

    conn.query("CREATE VIEW testview AS SELECT test.* FROM test")
        .unwrap();
    sleep();

    let rows: Vec<_> = conn
        .query("SELECT testview.* FROM testview")
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec!["4".into(), "2".into()]]
    );

    let rows: Vec<_> = conn
        .query("SELECT test.* FROM test")
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(),
        vec![vec![4.into(), 2.into()]]
    );
}

#[test]
#[ignore]
fn create_view_rev() {
    let d = Deployment::new("create_view_rev");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE test (x int, y int)").unwrap();
    sleep();

    conn.query("SELECT test.* FROM test").unwrap();
    sleep();

    conn.query("CREATE VIEW testview AS SELECT test.* FROM test")
        .unwrap();
    sleep();

    assert_eq!(
        conn.query("SELECT testview.* FROM testview")
            .unwrap()
            .count(),
        1
    );
}
