use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use maplit::hashmap;
use nom_sql::SelectStatement;
use noria::consensus::Authority;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::noria_connector::{self, NoriaConnector};
use noria_client::backend::{BackendBuilder, QueryResult};
use noria_client::query_status_cache::QueryStatusCache;
use noria_client::Backend;
use noria_mysql::{MySqlQueryHandler, MySqlUpstream};

#[tokio::main]
async fn main() -> Result<()> {
    let deployment = "myapp".to_owned();
    let zk_addr = "127.0.0.1:2181";

    let zk_auth =
        Authority::from(ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).await?);
    let ch = ControllerHandle::new(zk_auth).await;

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let query_status_cache = Arc::new(QueryStatusCache::new());

    let upstream: Option<MySqlUpstream> = None;
    let noria = NoriaConnector::new(ch, auto_increments, query_cache, None).await;
    let slowlog = false;
    let users: &'static HashMap<String, String> = Box::leak(Box::new(hashmap! {
        "user".to_owned() => "pw".to_owned()
    }));
    let require_authentication = false;

    let mut b: Backend<_, MySqlQueryHandler> = BackendBuilder::new()
        .slowlog(slowlog)
        .users(users.clone())
        .require_authentication(require_authentication)
        .build(noria, upstream, query_status_cache);

    let res = b.query("select * from employees;").await;

    match res {
        Ok(QueryResult::Noria(noria_connector::QueryResult::Select {
            data,
            select_schema: _,
        })) => print!("{:#?}", data),
        _ => print!("Select had an issue"),
    };

    Ok(())
}
