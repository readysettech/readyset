use std::env;

use async_trait::async_trait;
use noria_client::backend::noria_connector::ReadBehavior;
use noria_client::backend::BackendBuilder;
use noria_client::Backend;
use noria_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use noria_server::Handle;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{sleep, Adapter};

pub async fn setup_w_fallback() -> (tokio_postgres::Config, Handle) {
    crate::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
        true,
        ReadBehavior::Blocking,
    )
    .await
}

pub async fn setup(partial: bool) -> (tokio_postgres::Config, Handle) {
    crate::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        false,
        partial,
        true,
        ReadBehavior::Blocking,
    )
    .await
}

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

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(port).dbname("noria");
        config
    }

    fn url() -> String {
        format!(
            "postgresql://{}:{}@{}:{}/noria",
            env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
            env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()),
            env::var("PGHOST").unwrap_or_else(|_| "localhost".into()),
            env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
        )
    }

    async fn recreate_database() {
        let mut config = upstream_config();

        let (client, connection) = config.dbname("postgres").connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        while let Err(error) = client.simple_query("DROP DATABASE IF EXISTS noria").await {
            error!(%error, "Error dropping database");
            sleep().await
        }

        client.simple_query("CREATE DATABASE noria").await.unwrap();
    }

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream) {
        psql_srv::run_backend(noria_psql::Backend(backend), s).await
    }
}
