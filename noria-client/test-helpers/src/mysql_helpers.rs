use std::env;
use std::fmt::Display;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use mysql_srv::MysqlIntermediary;
use noria_client::backend::noria_connector::ReadBehavior;
use noria_client::backend::{BackendBuilder, MigrationMode, QueryInfo};
use noria_client::query_status_cache::QueryStatusCache;
use noria_mysql::{Backend, MySqlQueryHandler, MySqlUpstream};
use noria_server::Handle;
use tokio::net::TcpStream;

use crate::Adapter;

/// Retrieves where the query executed by parsing the row returned by
/// EXPLAIN LAST STATEMENT.
pub async fn last_query_info(conn: &mut Conn) -> QueryInfo {
    conn.query_first::<'_, QueryInfo, _>("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap()
}

pub async fn recreate_database<N>(dbname: N)
where
    N: Display,
{
    let mut management_db = mysql_async::Conn::new(
        mysql_async::OptsBuilder::default()
            .user(Some("root"))
            .pass(Some("noria"))
            .ip_or_hostname(env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()))
            .tcp_port(
                env::var("MYSQL_TCP_PORT")
                    .unwrap_or_else(|_| "3306".into())
                    .parse()
                    .unwrap(),
            ),
    )
    .await
    .unwrap();
    management_db
        .query_drop(format!("DROP DATABASE IF EXISTS {}", dbname))
        .await
        .unwrap();
    management_db
        .query_drop(format!("CREATE DATABASE {}", dbname))
        .await
        .unwrap();
}

pub struct MySQLAdapter;

impl MySQLAdapter {
    pub fn url_with_db(db: &str) -> String {
        format!(
            "mysql://root:noria@{}:{}/{}",
            env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
            db
        )
    }
}

#[async_trait]
impl Adapter for MySQLAdapter {
    type ConnectionOpts = mysql_async::Opts;
    type Upstream = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::MySQL;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts {
        mysql_async::OptsBuilder::default().tcp_port(port).into()
    }

    fn url() -> String {
        MySQLAdapter::url_with_db("noria")
    }

    async fn recreate_database() {
        recreate_database("noria").await;
    }

    async fn run_backend(
        backend: noria_client::Backend<Self::Upstream, Self::Handler>,
        s: TcpStream,
    ) {
        MysqlIntermediary::run_on_tcp(Backend::new(backend), s)
            .await
            .unwrap()
    }
}

// Initializes a Noria worker and starts processing MySQL queries against it.
// If `partial` is `false`, disables partial queries.
pub async fn setup(partial: bool) -> (mysql_async::Opts, Handle) {
    crate::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        false,
        partial,
        true,
        ReadBehavior::Blocking,
    )
    .await
}

pub async fn setup_with_read_behavior(read_behavior: ReadBehavior) -> (mysql_async::Opts, Handle) {
    crate::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        false,
        true,
        true,
        read_behavior,
    )
    .await
}

pub async fn query_cache_setup(
    query_status_cache: &'static QueryStatusCache,
    fallback: bool,
    migration_mode: MigrationMode,
) -> (mysql_async::Opts, Handle) {
    crate::setup_inner::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        if fallback {
            Some(MySQLAdapter::url())
        } else {
            None
        },
        true,
        true,
        query_status_cache,
        migration_mode,
        true,  // recreate database.
        false, //allow unsupported set
        ReadBehavior::Blocking,
    )
    .await
}
