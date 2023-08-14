use std::str::FromStr;
use std::sync::Arc;

use database_utils::{DatabaseURL, UpstreamConfig};
use nom_sql::Dialect;
use readyset_adapter::backend::{BackendBuilder, NoriaConnector};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::UpstreamDatabase;
use readyset_client::consensus::Authority;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};

/// Represents a ReadySet adapter backend that may be for a MySQL upstream or a Postgres upstream
pub enum Backend {
    MySql(readyset_adapter::backend::Backend<MySqlUpstream, MySqlQueryHandler>),
    PostgreSql(readyset_adapter::backend::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>),
}

impl Backend {
    pub async fn new(
        url: &str,
        noria: NoriaConnector,
        authority: Arc<Authority>,
    ) -> anyhow::Result<Self> {
        let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));

        match DatabaseURL::from_str(url)? {
            DatabaseURL::MySQL(_) => {
                let upstream = MySqlUpstream::connect(UpstreamConfig::from_url(url)).await?;

                Ok(Self::MySql(
                    BackendBuilder::new()
                        .require_authentication(false)
                        .enable_ryw(true)
                        .build(noria, Some(upstream), query_status_cache, authority),
                ))
            }
            DatabaseURL::PostgreSQL(_) => {
                let upstream = PostgreSqlUpstream::connect(UpstreamConfig::from_url(url)).await?;

                Ok(Self::PostgreSql(
                    BackendBuilder::new()
                        .require_authentication(false)
                        .enable_ryw(true)
                        .build(noria, Some(upstream), query_status_cache, authority),
                ))
            }
            DatabaseURL::Vitess(_) => todo!(),
        }
    }

    /// Executes a query against the underlying backend
    pub async fn query(&mut self, query: &str) -> anyhow::Result<()> {
        match self {
            Self::MySql(backend) => {
                backend.query(query).await?;
                Ok(())
            }
            Self::PostgreSql(backend) => {
                backend.query(query).await?;
                Ok(())
            }
        }
    }

    /// Returns the SQL dialect associated with the underlying backend
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::MySql(_) => Dialect::MySQL,
            Self::PostgreSql(_) => Dialect::PostgreSQL,
        }
    }
}
