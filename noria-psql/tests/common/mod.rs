use async_trait::async_trait;
use noria_client::backend::BackendBuilder;
use postgres::NoTls;
use std::env;
use tokio::net::TcpStream;

use noria_client::test_helpers;
use noria_client::Backend;

use noria_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};

#[allow(dead_code)]
pub fn setup_w_fallback() -> postgres::Config {
    test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
        true,
    )
}

pub struct PostgreSQLAdapter;
#[async_trait]
impl test_helpers::Adapter for PostgreSQLAdapter {
    type ConnectionOpts = postgres::Config;
    type Upstream = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    const DIALECT: nom_sql::Dialect = nom_sql::Dialect::PostgreSQL;
    const MIRROR_DDL: bool = true;

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

    fn recreate_database() {
        let mut config = postgres::Config::new();
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

        let mut management_db = config.dbname("postgres").connect(NoTls).unwrap();
        while management_db
            .simple_query("DROP DATABASE IF EXISTS noria")
            .is_err()
        {
            test_helpers::sleep()
        }

        management_db.simple_query("CREATE DATABASE noria").unwrap();
    }

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream) {
        psql_srv::run_backend(noria_psql::Backend(backend), s).await
    }
}
