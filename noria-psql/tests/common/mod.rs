use async_trait::async_trait;
use postgres::NoTls;
use std::env;
use tokio::net::TcpStream;

use noria::consensus::Authority;
use noria_client::test_helpers;
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

    fn recreate_database() {
        let mut management_db = postgres::Config::new()
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
            )
            .dbname("postgres")
            .connect(NoTls)
            .unwrap();
        management_db
            .simple_query("DROP DATABASE IF EXISTS noria")
            .unwrap();
        management_db.simple_query("CREATE DATABASE noria").unwrap();
    }

    async fn run_backend<A>(backend: Backend<A, Self::Upstream>, s: TcpStream)
    where
        A: 'static + Authority,
    {
        psql_srv::run_backend(noria_psql::Backend(backend), s).await
    }
}
