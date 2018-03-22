extern crate distributary;
extern crate msql_srv;
extern crate mysql;
extern crate nom_sql;

extern crate mysoupql;

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

use distributary::{ControllerBuilder, ZookeeperAuthority};
use msql_srv::MysqlIntermediary;
use nom_sql::ColumnSpecification;

use mysoupql::SoupBackend;

struct TestName {
    name: String,
}

impl TestName {
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

fn setup(test: &TestName) -> mysql::Opts {
    let zk_addr = "127.0.0.1:2181";
    let logger = distributary::logger_pls();
    let l = logger.clone();
    let n = test.name.clone();
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
        &test.name,
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
    let test = TestName::new("delete_basic");
    let opts = setup(&test);
    let mut conn = mysql::Conn::new(opts).unwrap();
    conn.query("CREATE TABLE Cats (id int PRIMARY KEY, name VARCHAR(255), PRIMARY KEY (id))")
        .unwrap();

    sleep();
    conn.query("INSERT INTO Cats (id, name) VALUES (1, \"Bob\")")
        .unwrap();
    sleep();

    let row = conn.query("SELECT Cats.id, Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_some());

    {
        let deleted = conn.query("DELETE FROM Cats WHERE Cats.id = 1").unwrap();
        assert_eq!(deleted.affected_rows(), 1);
    }

    let row = conn.query("SELECT Cats.id, Cats.name FROM Cats WHERE Cats.id = 1")
        .unwrap()
        .next();
    assert!(row.is_none());
}
