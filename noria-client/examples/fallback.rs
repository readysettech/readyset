use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, RwLock},
    time::Duration,
};

use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::{
    error::Error, mysql_connector::MySqlConnector, noria_connector::NoriaConnector, BackendBuilder,
    QueryResult, Reader, Writer,
};

/// This example demonstrates setting Noria up with a separate MySQL database.
/// Run `ryw-setup.sh` once noria is running to configure all of the
/// needed components of the system before running this script.
#[tokio::main]
async fn main() {
    println!("Begin!");
    let mysql_url = "mysql://root:mysqlroot@127.0.0.1:3308/inventory";

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let zk_addr = "127.0.0.1:2181";
    let deployment = "fallback";
    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();

    let ch = ControllerHandle::new(zk_auth).await.unwrap();

    let noria_conn = NoriaConnector::new(
        ch.clone(),
        auto_increments.clone(),
        query_cache.clone(),
        None,
    )
    .await;

    let mysql_url = String::from(mysql_url);

    let reader = Reader {
        noria_connector: noria_conn,
        mysql_connector: Some(MySqlConnector::new(mysql_url.clone()).await),
    };

    // Construct the Writer (to an underlying DB)
    let writer = MySqlConnector::new(mysql_url).await;
    let writer = Writer::MySqlConnector(writer);

    let mut b = BackendBuilder::new()
        .reader(reader)
        .writer(writer)
        .require_authentication(false)
        .enable_ryw(true)
        .build();

    let noria_res = b.query("select * from customers;").await;
    let mysql_res = b.query("show tables;").await;

    fn print_res(res: Result<QueryResult, Error>) {
        match res {
            Ok(QueryResult::NoriaSelect {
                data,
                select_schema: _,
            }) => {
                println!("Noria Result:");
                println!("{:#?}", data);
            }
            Ok(QueryResult::MySqlSelect { data }) => {
                println!("MySQL Result:");
                println!("{:#?}", data);
            }
            _ => println!("Select had an issue"),
        };
    }
    print_res(noria_res);
    print_res(mysql_res);
}
