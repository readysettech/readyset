use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::marker::{Send, Sync};
use std::str::FromStr;

use anyhow::{anyhow, bail};
use derive_more::From;
use futures::{StreamExt, TryStreamExt};
use mysql::prelude::Queryable;
use mysql::OptsBuilder;
use mysql_async as mysql;
use tokio_postgres as pgsql;

use crate::ast::Value;
use crate::runner::TestScript;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
}

impl FromStr for DatabaseType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mysql" => Ok(Self::MySQL),
            "postgresql" => Ok(Self::PostgreSQL),
            _ => bail!("Invalid upstream type `{}`, expected one of `mysql` or `postgresql`"),
        }
    }
}

impl Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::MySQL => f.write_str("MySQL"),
            DatabaseType::PostgreSQL => f.write_str("PostgreSQL"),
        }
    }
}

/// URL for an upstream database
#[derive(Debug, Clone, From)]
#[allow(clippy::large_enum_variant)]
pub enum DatabaseURL {
    MySQL(mysql::Opts),
    PostgreSQL(pgsql::Config),
}

impl FromStr for DatabaseURL {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            Ok(Self::MySQL(mysql::Opts::from_url(s)?))
        } else if s.starts_with("postgresql://") {
            Ok(Self::PostgreSQL(pgsql::Config::from_str(s)?))
        } else {
            Err(anyhow!("Invalid URL format"))
        }
    }
}

impl From<mysql::OptsBuilder> for DatabaseURL {
    fn from(ob: mysql::OptsBuilder) -> Self {
        Self::MySQL(ob.into())
    }
}

impl DatabaseURL {
    pub async fn connect(&self) -> anyhow::Result<DatabaseConnection> {
        match self {
            DatabaseURL::MySQL(opts) => Ok(DatabaseConnection::MySQL(
                mysql::Conn::new(opts.clone()).await?,
            )),
            DatabaseURL::PostgreSQL(config) => {
                let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
                let tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let (client, connection) = config.connect(tls).await?;
                tokio::spawn(connection);
                Ok(DatabaseConnection::PostgreSQL(client))
            }
        }
    }

    pub fn upstream_type(&self) -> DatabaseType {
        match self {
            DatabaseURL::MySQL(_) => DatabaseType::MySQL,
            DatabaseURL::PostgreSQL(_) => DatabaseType::PostgreSQL,
        }
    }

    pub fn db_name(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.db_name(),
            DatabaseURL::PostgreSQL(config) => config.get_dbname(),
        }
    }

    pub fn set_db_name(&mut self, db_name: String) {
        match self {
            DatabaseURL::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone())
                    .db_name(Some(db_name))
                    .into();
            }
            DatabaseURL::PostgreSQL(config) => {
                config.dbname(&db_name);
            }
        }
    }
}

pub enum DatabaseConnection {
    MySQL(mysql::Conn),
    PostgreSQL(pgsql::Client),
}

async fn convert_mysql_results<'a, 't, P>(
    mut results: mysql::QueryResult<'a, 't, P>,
) -> anyhow::Result<Vec<Vec<Value>>>
where
    P: mysql::prelude::Protocol,
{
    results
        .map(|mut r| {
            (0..r.columns().len())
                .map(|c| Value::try_from(r.take::<mysql::Value, _>(c).unwrap()))
                .collect::<Result<Vec<Value>, _>>()
        })
        .await?
        .into_iter()
        .collect()
}

async fn convert_pgsql_results(results: pgsql::RowStream) -> anyhow::Result<Vec<Vec<Value>>> {
    Ok(results
        .map(|r| {
            let r = r?;
            (0..r.len())
                .map(|c| r.try_get(c))
                .collect::<Result<Vec<_>, _>>()
        })
        .try_collect()
        .await?)
}

impl DatabaseConnection {
    pub async fn query_drop<Q>(&mut self, stmt: Q) -> anyhow::Result<()>
    where
        Q: AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.query_drop(stmt).await?),
            DatabaseConnection::PostgreSQL(client) => {
                client.execute(stmt.as_ref(), &[]).await?;
                Ok(())
            }
        }
    }

    pub async fn query<Q>(&mut self, query: Q) -> anyhow::Result<Vec<Vec<Value>>>
    where
        Q: AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.query_iter(query).await?).await
            }
            DatabaseConnection::PostgreSQL(client) => {
                convert_pgsql_results(client.query_raw(query.as_ref(), Vec::<i8>::new()).await?)
                    .await
            }
        }
    }

    pub async fn execute<Q, P>(&mut self, stmt: Q, params: P) -> anyhow::Result<Vec<Vec<Value>>>
    where
        Q: AsRef<str>,
        P: Into<mysql::Params> + IntoIterator,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.exec_iter(stmt.as_ref(), params).await?).await
            }
            DatabaseConnection::PostgreSQL(client) => {
                convert_pgsql_results(client.query_raw(stmt.as_ref(), params).await?).await
            }
        }
    }

    pub async fn run_script(&mut self, script: &TestScript) -> anyhow::Result<()> {
        script
            .run_on_database(&Default::default(), self, None)
            .await
    }
}
