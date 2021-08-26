use async_trait::async_trait;
use std::env;
use tokio::net::TcpStream;

use noria::consensus::Authority;
use noria_client::backend::BackendBuilder;
use noria_client::test_helpers::{self, Deployment};
use noria_client::Backend;

use noria_psql::PostgreSqlUpstream;

pub struct PostgreSQLAdapter;
#[async_trait]
impl test_helpers::Adapter for PostgreSQLAdapter {
    type ConnectionOpts = postgres::Config;
    type Upstream = PostgreSqlUpstream;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::PostgreSQL;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts {
        let mut config = postgres::Config::new();
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

    async fn run_backend<A>(backend: Backend<A, Self::Upstream>, s: TcpStream)
    where
        A: 'static + Authority,
    {
        psql_srv::run_backend(noria_psql::Backend(backend), s).await
    }
}

pub fn setup(deployment: &Deployment, partial: bool) -> postgres::Config {
    test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        deployment,
        false,
        partial,
    )
}
