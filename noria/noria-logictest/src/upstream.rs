use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::marker::{Send, Sync};
use std::str::FromStr;

use anyhow::{anyhow, bail};
use derive_more::From;
use futures::{StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::OptsBuilder;
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
            _ => bail!(
                "Invalid upstream type `{}`, expected one of `mysql` or `postgresql`",
                s
            ),
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
    MySQL(mysql_async::Opts),
    PostgreSQL(pgsql::Config),
}

impl FromStr for DatabaseURL {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            Ok(Self::MySQL(mysql_async::Opts::from_url(s)?))
        } else if s.starts_with("postgresql://") {
            Ok(Self::PostgreSQL(pgsql::Config::from_str(s)?))
        } else {
            Err(anyhow!("Invalid URL format"))
        }
    }
}

impl From<mysql_async::OptsBuilder> for DatabaseURL {
    fn from(ob: mysql_async::OptsBuilder) -> Self {
        Self::MySQL(ob.into())
    }
}

impl DatabaseURL {
    pub async fn connect(&self) -> anyhow::Result<DatabaseConnection> {
        match self {
            DatabaseURL::MySQL(opts) => Ok(DatabaseConnection::MySQL(
                mysql_async::Conn::new(opts.clone()).await?,
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
    MySQL(mysql_async::Conn),
    PostgreSQL(pgsql::Client),
}

async fn convert_mysql_results<'a, 't, P>(
    mut results: mysql_async::QueryResult<'a, 't, P>,
) -> anyhow::Result<Vec<Vec<Value>>>
where
    P: mysql_async::prelude::Protocol,
{
    results
        .map(|mut r| {
            (0..r.columns().len())
                .map(|c| Value::try_from(r.take::<mysql_async::Value, _>(c).unwrap()))
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
                client.simple_query(stmt.as_ref()).await?;
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

    pub async fn prepare<Q>(&mut self, query: Q) -> anyhow::Result<DatabaseStatement>
    where
        Q: AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.prep(query).await?.into()),
            DatabaseConnection::PostgreSQL(client) => {
                Ok(client.prepare(query.as_ref()).await?.into())
            }
        }
    }

    pub async fn execute<P>(
        &mut self,
        stmt: impl Into<DatabaseStatement>,
        params: P,
    ) -> anyhow::Result<Vec<Vec<Value>>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
    {
        match stmt.into() {
            DatabaseStatement::Mysql(s) => {
                convert_mysql_results(self.as_mysql_conn()?.exec_iter(s, params).await?).await
            }
            DatabaseStatement::Postgres(s) => {
                convert_pgsql_results(self.as_postgres_conn()?.query_raw(&s, params).await?).await
            }
            DatabaseStatement::Str(s) => self.execute_str(s.as_ref(), params).await,
        }
    }

    async fn execute_str<P>(&mut self, stmt: &str, params: P) -> anyhow::Result<Vec<Vec<Value>>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.exec_iter(stmt, params).await?).await
            }
            DatabaseConnection::PostgreSQL(client) => {
                convert_pgsql_results(client.query_raw(stmt, params).await?).await
            }
        }
    }

    pub async fn run_script(&mut self, script: &TestScript) -> anyhow::Result<()> {
        script
            .run_on_database(&Default::default(), self, None)
            .await
    }

    pub fn as_mysql_conn(&mut self) -> anyhow::Result<&mut mysql_async::Conn> {
        if let DatabaseConnection::MySQL(c) = self {
            Ok(c)
        } else {
            Err(anyhow!(
                "DatabaseConnection is not a MySQL connection variant"
            ))
        }
    }

    pub fn as_postgres_conn(&mut self) -> anyhow::Result<&mut tokio_postgres::Client> {
        if let DatabaseConnection::PostgreSQL(c) = self {
            Ok(c)
        } else {
            Err(anyhow!(
                "DatabaseConnection is not a Postgres connection variant"
            ))
        }
    }
}

/// An enum wrapper around various prepared statement types. Either a mysql_async prepared
/// statement, a tokio_postgres prepared statement, or a plain query string that we would like to
/// both prepare and execute.
pub enum DatabaseStatement {
    /// A MySQL prepared statement returned from a prepare call in `mysql_async`.
    Mysql(mysql_async::Statement),
    /// A PostgreSQL prepared statement returned from a prepare call in `tokio_postgres`.
    Postgres(tokio_postgres::Statement),
    /// A simple query string that a user would like to be both prepared and executed.
    Str(String),
}

impl From<mysql_async::Statement> for DatabaseStatement {
    fn from(s: mysql_async::Statement) -> DatabaseStatement {
        DatabaseStatement::Mysql(s)
    }
}

impl From<tokio_postgres::Statement> for DatabaseStatement {
    fn from(s: tokio_postgres::Statement) -> DatabaseStatement {
        DatabaseStatement::Postgres(s)
    }
}

impl From<&str> for DatabaseStatement {
    fn from(s: &str) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_string())
    }
}

impl From<String> for DatabaseStatement {
    fn from(s: String) -> DatabaseStatement {
        DatabaseStatement::Str(s)
    }
}

impl From<&String> for DatabaseStatement {
    fn from(s: &String) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_owned())
    }
}
