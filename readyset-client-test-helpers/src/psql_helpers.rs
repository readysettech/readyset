use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use database_utils::TlsMode;
use mysql_srv::{AuthCache, AuthPlugin};
use readyset_adapter::backend::{QueryDestination, QueryInfo};
use readyset_adapter::Backend;
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use readyset_util::retry_with_exponential_backoff;
use tokio::net::TcpStream;
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use tracing::error;

use crate::{sleep, Adapter};

pub fn upstream_config() -> Config {
    let mut config = Config::new();
    config
        .user(env::var("PGUSER").unwrap_or_else(|_| "postgres".into()))
        .password(
            env::var("PGPASSWORD")
                .unwrap_or_else(|_| "noria".into())
                .as_bytes(),
        )
        .host(env::var("PGHOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        );
    config
}

pub async fn connect(config: Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

pub struct PostgreSQLAdapter;

#[async_trait]
impl Adapter for PostgreSQLAdapter {
    type ConnectionOpts = Config;
    type Upstream = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    const DIALECT: readyset_sql::Dialect = readyset_sql::Dialect::PostgreSQL;

    const EXPR_DIALECT: readyset_data::Dialect = readyset_data::Dialect::DEFAULT_POSTGRESQL;

    fn connection_opts_with_port(db_name: Option<&str>, port: u16) -> Self::ConnectionOpts {
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .dbname(db_name.unwrap_or("noria"));
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

        let (client, connection) = retry_with_exponential_backoff!(
            { config.dbname("postgres").connect(NoTls).await },
            retries: 10,
            delay: 100,
            backoff: 2,
        )
        .unwrap();
        tokio::spawn(connection);

        tokio::time::timeout(Duration::from_secs(60), async move {
            while client
                .simple_query(&format!("CREATE DATABASE {db_name}"))
                .await
                .is_err()
            {
                while let Err(error) = client
                    .simple_query(&format!("DROP DATABASE IF EXISTS {db_name}"))
                    .await
                {
                    error!(%error, "Error dropping database");
                    sleep().await
                }
                sleep().await;
            }
        })
        .await
        .expect("Failed to cleanly drop and recreate database after");
    }

    async fn run_backend(
        backend: Backend<Self::Upstream, Self::Handler>,
        s: TcpStream,
        _auth_plugin: AuthPlugin,
        _auth_cache: Arc<AuthCache>,
    ) {
        psql_srv::run_backend(
            readyset_psql::Backend::new(backend),
            s,
            false,
            None,
            TlsMode::Optional,
        )
        .await
    }
}

/// A parameter sent in text format with exactly these bytes, whatever type the statement gives it.
#[derive(Debug)]
pub struct TextParam(pub &'static str);

impl ToSql for TextParam {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

/// A parameter sent in binary format with exactly these bytes, whatever type the statement gives
/// it.
#[derive(Debug)]
pub struct BinaryParam(pub &'static [u8]);

impl ToSql for BinaryParam {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(self.0);
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Retrieves where the query executed by parsing the row returned by EXPLAIN LAST STATEMENT.
pub async fn last_query_info(conn: &Client) -> QueryInfo {
    let row = match conn
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        SimpleQueryMessage::Row(row) => row,
        _ => panic!("Unexpected SimpleQueryMessage"),
    };

    let destination = QueryDestination::try_from(row.get("Query_destination").unwrap()).unwrap();
    let reason = row.get("Readyset_reason").unwrap().to_owned();

    QueryInfo {
        destination,
        reason,
    }
}
