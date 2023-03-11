use std::env;
use std::fmt::Display;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_srv::MySqlIntermediary;
use readyset_adapter::backend::QueryInfo;
use readyset_mysql::{Backend, MySqlQueryHandler, MySqlUpstream};
use tokio::net::TcpStream;

use crate::Adapter;

/// Retrieves where the query executed by parsing the row returned by
/// EXPLAIN LAST STATEMENT.
pub async fn last_query_info(conn: &mut impl Queryable) -> QueryInfo {
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

    const EXPR_DIALECT: readyset_data::Dialect = readyset_data::Dialect::DEFAULT_MYSQL;

    fn connection_opts_with_port(db: Option<&str>, port: u16) -> Self::ConnectionOpts {
        mysql_async::OptsBuilder::default()
            .tcp_port(port)
            .db_name(db)
            .into()
    }

    fn upstream_url(db_name: &str) -> String {
        MySQLAdapter::url_with_db(db_name)
    }

    async fn recreate_database(db_name: &str) {
        recreate_database(db_name).await;
    }

    async fn run_backend(
        backend: readyset_adapter::Backend<Self::Upstream, Self::Handler>,
        s: TcpStream,
    ) {
        MySqlIntermediary::run_on_tcp(
            Backend {
                noria: backend,
                enable_statement_logging: false,
            },
            s,
            false,
        )
        .await
        .unwrap()
    }
}
