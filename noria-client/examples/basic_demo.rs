use anyhow::Result;
use maplit::hashmap;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::mysql_connector::MySqlConnector;
use noria_client::backend::noria_connector::{self, NoriaConnector};
use noria_client::backend::{BackendBuilder, QueryResult, Reader, Writer};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() -> Result<()> {
    let deployment = "myapp".to_owned();
    let zk_addr = "127.0.0.1:2181";

    let zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment))?;
    let ch = ControllerHandle::new(zk_auth).await;

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

    let static_responses = false;
    let writer = {
        Writer::Noria(
            NoriaConnector::new(
                ch.clone(),
                auto_increments.clone(),
                query_cache.clone(),
                None,
            )
            .await,
        )
    };
    let mysql_connector: Option<MySqlConnector> = None;
    let noria_connector = NoriaConnector::new(ch, auto_increments, query_cache, None).await;
    let reader = Reader {
        upstream: mysql_connector,
        noria_connector,
    };
    let slowlog = false;
    let users: &'static HashMap<String, String> = Box::leak(Box::new(hashmap! {
        "user".to_owned() => "pw".to_owned()
    }));
    let require_authentication = false;

    let mut b = BackendBuilder::new()
        .static_responses(static_responses)
        .slowlog(slowlog)
        .users(users.clone())
        .require_authentication(require_authentication)
        .build(writer, reader);

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
