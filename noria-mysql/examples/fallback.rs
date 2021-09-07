use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::{
    backend::{
        noria_connector::{self, NoriaConnector},
        BackendBuilder, QueryResult,
    },
    Backend, UpstreamDatabase,
};
use noria_mysql::{Error, MySqlQueryHandler, MySqlUpstream};

/// This example demonstrates setting Noria up with a separate MySQL database.
/// Run `ryw-setup.sh` once noria is running to configure all of the
/// needed components of the system before running this script.
#[tokio::main]
async fn main() {
    let mysql_url = "mysql://root:mysqlroot@127.0.0.1:3308/inventory";

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let zk_addr = "127.0.0.1:2181";
    let deployment = "fallback";
    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();

    let ch = ControllerHandle::new(zk_auth).await;

    let noria = NoriaConnector::new(ch, auto_increments, query_cache, None).await;

    let mysql_url = String::from(mysql_url);

    let upstream = Some(MySqlUpstream::connect(mysql_url).await.unwrap());

    let mut b: Backend<_, _, MySqlQueryHandler> = BackendBuilder::new()
        .require_authentication(false)
        .enable_ryw(true)
        .build(noria, upstream);

    let noria_res = b.query("select * from customers;").await;
    let mysql_res = b.query("show tables;").await;

    fn print_res(res: Result<QueryResult<MySqlUpstream>, Error>) {
        match res {
            Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
                data,
                select_schema: _,
            })) => {
                println!("Noria Result:");
                println!("{:#?}", data);
            }
            Ok(QueryResult::Upstream(noria_mysql::QueryResult::ReadResult { data, .. })) => {
                println!("MySQL Result:");
                println!("{:#?}", data);
            }
            _ => println!("Select had an issue"),
        };
    }
    print_res(noria_res);
    print_res(mysql_res);
}
