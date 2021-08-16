use anyhow::Result;
use maplit::hashmap;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::{
    noria_connector::NoriaConnector, BackendBuilder, QueryResult, Reader, Writer,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() -> Result<()> {
    let deployment = "myapp".to_owned();
    let zk_addr = "127.0.0.1:2181";

    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment))?;
    let ch = ControllerHandle::new(zk_auth).await?;

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

    let sanitize = true;
    let static_responses = false;
    let writer = {
        let writer = NoriaConnector::new(
            ch.clone(),
            auto_increments.clone(),
            query_cache.clone(),
            None,
        )
        .await;
        Writer::NoriaConnector(writer)
    };
    let mysql_connector = None;
    let noria_connector = NoriaConnector::new(ch, auto_increments, query_cache, None).await;
    let reader = Reader {
        mysql_connector,
        noria_connector,
    };
    let slowlog = false;
    let permissive = false;
    let users: &'static HashMap<String, String> = Box::leak(Box::new(hashmap! {
        "user".to_owned() => "pw".to_owned()
    }));
    let require_authentication = false;

    let mut b = BackendBuilder::new()
        .sanitize(sanitize)
        .static_responses(static_responses)
        .slowlog(slowlog)
        .permissive(permissive)
        .users(users.clone())
        .require_authentication(require_authentication)
        .build(writer, reader);

    let res = b.query("select * from employees;").await;

    match res {
        Ok(QueryResult::NoriaSelect {
            data,
            select_schema: _,
        }) => print!("{:#?}", data),
        _ => print!("Select had an issue"),
    };

    Ok(())
}
