use std::env;

use async_trait::async_trait;
use readyset_client::Backend;
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{sleep, Adapter};

pub fn upstream_config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .user(&env::var("PGUSER").unwrap_or_else(|_| "postgres".into()))
        .password(
            env::var("PGPASSWORD")
                .unwrap_or_else(|_| "noria".into())
                .as_bytes(),
        )
        .host(&env::var("PGHOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        );
    config
}

pub struct PostgreSQLAdapter;
#[async_trait]
impl Adapter for PostgreSQLAdapter {
    type ConnectionOpts = tokio_postgres::Config;
    type Upstream = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::PostgreSQL;

    fn connection_opts_with_port(db_name: &str, port: u16) -> Self::ConnectionOpts {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(port).dbname(db_name);
        config
    }

    fn upstream_url(db_name: &str) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
            env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()),
            env::var("PGHOST").unwrap_or_else(|_| "localhost".into()),
            env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
            db_name
        )
    }

    async fn recreate_database(db_name: &str) {
        let mut config = upstream_config();

        let (client, connection) = config.dbname("postgres").connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        while let Err(error) = client
            .simple_query(&format!("DROP DATABASE IF EXISTS {db_name}"))
            .await
        {
            error!(%error, "Error dropping database");
            sleep().await
        }

        client
            .simple_query(&format!("CREATE DATABASE {db_name}"))
            .await
            .unwrap();
    }

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream) {
        psql_srv::run_backend(readyset_psql::Backend(backend), s).await
    }
}
