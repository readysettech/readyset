extern crate distributary;
extern crate msql_srv;
extern crate mysql;
extern crate nom_sql;
#[macro_use]
extern crate slog;

extern crate mysoupql;

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

use distributary::{ControllerBuilder, ZookeeperAuthority};
use msql_srv::MysqlIntermediary;
use nom_sql::ColumnSpecification;

use mysoupql::SoupBackend;

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

fn sleep() {
    thread::sleep(Duration::from_millis(200));
}

// Initializes a souplet and starts processing MySQL queries against it.
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
        distributary::logger_pls()
    } else {
        slog::Logger::root(slog::Discard, o!())
    };

    let l = logger.clone();
    let n = deployment.name.clone();
    thread::spawn(move || {
        let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, n));
        let mut builder = ControllerBuilder::default();
        authority.log_with(l.clone());
        builder.log_with(l);
        builder.build(Arc::new(authority)).wait();
    });

    let query_counter = Arc::new(AtomicUsize::new(0));
    let schemas: Arc<Mutex<HashMap<String, Vec<ColumnSpecification>>>> =
        Arc::new(Mutex::new(HashMap::default()));
    let auto_increments: Arc<Mutex<HashMap<String, u64>>> =
        Arc::new(Mutex::new(HashMap::default()));
    let soup = SoupBackend::new(
        zk_addr,
        &deployment.name,
        schemas,
        auto_increments,
        query_counter,
        logger,
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        MysqlIntermediary::run_on_tcp(soup, s).unwrap();
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

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1").unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
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
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 2")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 2);
    }

    for i in 1..3 {
        let query = format!("SELECT Cats.id FROM Cats WHERE Cats.id = {}", i);
        let row = conn.query(query).unwrap().next();
        assert!(row.is_none());
    }

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 3")
        .unwrap()
        .next();
    assert!(row.is_some());
}

#[test]
fn delete_bogus() {
    let d = Deployment::new("delete_multiple");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    // `id` can't be both 1 and 2!
    let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 2")
        .unwrap();
    assert_eq!(deleted.affected_rows(), 0);
}

#[test]
fn delete_bogus_valid_and() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1 AND Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_bogus_valid_or() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id) VALUES (1)").unwrap();
    sleep();

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        // Not that it makes much sense, but we should support this regardless...
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.id = 1")
            .unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }

    let row = conn.query("SELECT Cats.id FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}

#[test]
fn delete_non_key() {
    let d = Deployment::new("delete_multiple");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    assert!(
        conn.query("DELETE FROM Cats WHERE Cats.id = 1 OR Cats.name = \"bob\"")
            .is_err()
    );
}

#[test]
fn update_basic() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn.query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
    }

    let name: String = conn.first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
}

#[test]
fn update_pkey() {
    let d = Deployment::new("delete_basic");
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
    }

    let name: String = conn.first("SELECT Cats.name FROM Cats WHERE Cats.id = 10")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty"));
    let old_row = conn.query("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(old_row.is_none());
}

#[test]
fn update_separate() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    {
        let updated = conn.query("UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
    }

    {
        let updated = conn.query("UPDATE Cats SET Cats.name = \"Rusty II\" WHERE Cats.id = 1")
            .unwrap();
        assert_eq!(updated.affected_rows(), 1);
    }

    let name: String = conn.first("SELECT Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .unwrap();
    assert_eq!(name, String::from("Rusty II"));
}

#[test]
fn update_multiple() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    for i in 1..4 {
        let query = format!("INSERT INTO Cats (id, name) VALUES ({}, \"Bob\")", i);
        conn.query(query).unwrap();
        sleep();
    }

    {
        let query = "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 OR Cats.id = 2";
        let updated = conn.query(query).unwrap();
        assert_eq!(updated.affected_rows(), 2);
    }

    for i in 1..3 {
        let query = format!("SELECT Cats.id, Cats.name FROM Cats WHERE Cats.id = {}", i);
        let (id, name): (usize, String) = conn.first(query).unwrap().unwrap();
        assert_eq!(i, id);
        assert_eq!(name, "Rusty");
    }

    let name: String = conn.first("SELECT Cats.name FROM Cats WHERE Cats.id = 3")
        .unwrap()
        .unwrap();
    assert_eq!(name, "Bob");
}

#[test]
fn update_no_changes() {
    let d = Deployment::new("delete_basic");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    let updated = conn.query("UPDATE Cats SET Cats.name = \"Bob\" WHERE Cats.id = 1")
        .unwrap();
    assert_eq!(updated.affected_rows(), 0);
}

#[test]
fn update_bogus() {
    let d = Deployment::new("delete_multiple");
    let opts = setup(&d);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY(id))")
        .unwrap();
    sleep();

    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    // `id` can't be both 1 and 2!
    let deleted = conn.query(
        "UPDATE Cats SET Cats.name = \"Rusty\" WHERE Cats.id = 1 AND Cats.id = 2",
    ).unwrap();
    assert_eq!(deleted.affected_rows(), 0);
}
