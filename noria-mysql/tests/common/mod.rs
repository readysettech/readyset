use async_trait::async_trait;
use mysql::prelude::Queryable;
use std::env;
use tokio::net::TcpStream;

use msql_srv::MysqlIntermediary;
use noria::consensus::Authority;
use noria_client::backend::BackendBuilder;
use noria_client::test_helpers::{self, Deployment};

use noria_mysql::{Backend, MySqlUpstream};

pub struct MySQLAdapter;
#[async_trait]
impl test_helpers::Adapter for MySQLAdapter {
    type ConnectionOpts = mysql::Opts;
    type Upstream = MySqlUpstream;

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
            .query_drop("DROP DATABASE IF EXISTS noria")
            .unwrap();
        management_db.query_drop("CREATE DATABASE noria").unwrap();
    }

    async fn run_backend<A>(backend: noria_client::Backend<A, Self::Upstream>, s: TcpStream)
    where
        A: 'static + Authority,
    {
        MysqlIntermediary::run_on_tcp(Backend(backend), s)
            .await
            .unwrap()
    }
}

// Initializes a Noria worker and starts processing MySQL queries against it.
// If `partial` is `false`, disables partial queries.
#[allow(dead_code)] // not all test files use this function
pub fn setup(deployment: &Deployment, partial: bool) -> mysql::Opts {
    test_helpers::setup::<MySQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        deployment,
        false,
        partial,
    )
}
