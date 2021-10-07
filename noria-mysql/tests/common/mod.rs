use async_trait::async_trait;
use mysql::prelude::Queryable;
use std::env;
use std::fmt::Display;
use tokio::net::TcpStream;

use msql_srv::MysqlIntermediary;
use noria_client::backend::BackendBuilder;
use noria_client::test_helpers;

use noria_mysql::{Backend, MySqlQueryHandler, MySqlUpstream};

pub fn recreate_database<N>(dbname: N)
where
    N: Display,
{
    let mut management_db = mysql::Conn::new(
        mysql::OptsBuilder::default()
            .user(Some("root"))
            .pass(Some("noria"))
            .ip_or_hostname(Some(
                env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            ))
            .tcp_port(
                env::var("MYSQL_TCP_PORT")
                    .unwrap_or_else(|_| "3306".into())
                    .parse()
                    .unwrap(),
            ),
    )
    .unwrap();
    management_db
        .query_drop(format!("DROP DATABASE IF EXISTS {}", dbname))
        .unwrap();
    management_db
        .query_drop(format!("CREATE DATABASE {}", dbname))
        .unwrap();
}

pub struct MySQLAdapter;
#[async_trait]
impl test_helpers::Adapter for MySQLAdapter {
    type ConnectionOpts = mysql::Opts;
    type Upstream = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::MySQL;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts {
        mysql::OptsBuilder::default().tcp_port(port).into()
    }

    fn url() -> String {
        format!(
            "mysql://root:noria@{}:{}/noria",
            env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
        )
    }

    fn recreate_database() {
        recreate_database("noria");
    }

    async fn run_backend(
        backend: noria_client::Backend<Self::Upstream, Self::Handler>,
        s: TcpStream,
    ) {
        MysqlIntermediary::run_on_tcp(Backend(backend), s)
            .await
            .unwrap()
    }
}

// Initializes a Noria worker and starts processing MySQL queries against it.
// If `partial` is `false`, disables partial queries.
#[allow(dead_code)] // not all test files use this function
pub fn setup(partial: bool) -> mysql::Opts {
    test_helpers::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        false,
        partial,
        true,
    )
}
