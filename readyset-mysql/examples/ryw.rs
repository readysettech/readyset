use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nom_sql::SelectStatement;
use readyset::consensus::Authority;
use readyset::{ControllerHandle, ZookeeperAuthority};
use readyset_client::backend::noria_connector::{self, NoriaConnector, ReadBehavior};
use readyset_client::backend::{BackendBuilder, QueryResult};
use readyset_client::query_status_cache::QueryStatusCache;
use readyset_client::{Backend, UpstreamDatabase};
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};

/// This example demonstrates setting ReadySet up with a separate MySQL database.
/// Run `ryw-setup.sh` once ReadySet is running to configure all of the
/// needed components of the system before running this script.
#[tokio::main]
async fn main() {
    // This is the URL to connect to the setup alternate mysql db
    // MUST be able to read information_schema.innodb_trx table. Here, has root access.
    let mysql_url = "mysql://root:debezium@127.0.0.1:3306/inventory";

    //Construct the Reader (Direct to ReadySet)
    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let zk_addr = "127.0.0.1:2181";
    let deployment = "ryw";
    let zk_auth = Authority::from(
        ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment))
            .await
            .unwrap(),
    );

    let mut ch = ControllerHandle::new(zk_auth).await;

    // Construct the Writer (to an underlying DB)
    let mysql_url = String::from(mysql_url);
    let upstream = Some(MySqlUpstream::connect(mysql_url.clone()).await.unwrap());

    let noria = NoriaConnector::new(
        ch.clone(),
        auto_increments,
        query_cache,
        None,
        ReadBehavior::Blocking,
    )
    .await;
    let query_status_cache = Box::leak(Box::new(QueryStatusCache::new()));

    let mut b: Backend<_, MySqlQueryHandler> = BackendBuilder::new()
        .require_authentication(false)
        .enable_ryw(true)
        .build(noria, upstream, query_status_cache);

    // Install Query/Recipe to ReadySet (must match the underlying mysql database structure)
    let test_sql_string = "
            CREATE TABLE employees (
                emp_no      INT             NOT NULL,
                first_name  VARCHAR(14)     NOT NULL,
                last_name   VARCHAR(16)     NOT NULL,
                gender      VARCHAR(1)      NOT NULL,
                PRIMARY KEY (emp_no)
            );

            QUERY testQuery : \
                SELECT * FROM employees;
        ";

    ch.extend_recipe(test_sql_string.parse().unwrap())
        .await
        .unwrap();
    println!("Current Ticket: {:?}", b.ticket());

    // Make some writes to underlying DB (could also make writes via mysql shell)
    // Current ticket will always be default until TimestampService is implemented
    let write_res = b
        .query("INSERT INTO employees VALUES (1, 'John', 'Doe', 'M')")
        .await;
    println!("{:?}", write_res);
    println!("New Ticket: {:?}", b.ticket());

    std::thread::sleep(Duration::from_millis(5000));

    let write_res = b
        .query("INSERT INTO employees VALUES (2, 'John3', 'Doe3', 'M')")
        .await;
    println!("{:?}", write_res);
    println!("New Ticket: {:?}", b.ticket());

    // Will block until the writes are through
    let res = b.query("select * from employees;").await;

    match res {
        Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
            data,
            select_schema: _,
        })) => print!("{:#?}", data),
        _ => print!("Select had an issue"),
    };
}
