use std::convert::TryFrom;
use std::marker::{Send, Sync};
use std::str;

use derive_more::From;
use futures::{StreamExt, TryStreamExt};
use mysql::prelude::AsQuery;
use mysql_async::prelude::Queryable;
use readyset_errors::ReadySetError;
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::{ConnectionType, DatabaseError};

/// An enum wrapper around either a MySQL or PostgreSQL connection.
pub enum DatabaseConnection {
    /// A MySQL database connection.
    MySQL(mysql_async::Conn),
    /// A PostgreSQL database connection.
    PostgreSQL(
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), ReadySetError>>,
    ),
}

async fn convert_mysql_results<'a, 't, P, V>(
    mut results: mysql::QueryResult<'a, 't, P>,
) -> Result<Vec<Vec<V>>, DatabaseError>
where
    P: mysql::prelude::Protocol,
    V: TryFrom<mysql::Value>,
    <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
{
    results
        .map(|mut r| {
            (0..r.columns().len())
                .map(|c| {
                    V::try_from(r.take::<mysql::Value, _>(c).unwrap())
                        .map_err(|e| DatabaseError::ValueConversion(Box::new(e)))
                })
                .collect::<Result<Vec<V>, _>>()
        })
        .await?
        .into_iter()
        .collect::<Result<Vec<Vec<V>>, _>>()
}

async fn convert_pgsql_results<V>(results: pgsql::RowStream) -> Result<Vec<Vec<V>>, pgsql::Error>
where
    for<'a> V: pgsql::types::FromSql<'a>,
{
    results
        .map(|r| {
            let r = r?;
            (0..r.len())
                .map(|c| r.try_get(c))
                .collect::<Result<Vec<_>, _>>()
        })
        .try_collect()
        .await
}

impl DatabaseConnection {
    /// Executes query_drop for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.query_drop(stmt).await?),
            DatabaseConnection::PostgreSQL(client, _jh) => {
                client.simple_query(stmt.as_ref()).await?;
                Ok(())
            }
        }
    }

    /// Executes query for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn query<Q, V>(&mut self, query: Q) -> Result<Vec<Vec<V>>, DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.query_iter(query).await?).await
            }
            DatabaseConnection::PostgreSQL(client, _jh) => {
                // TODO: We should use simple_query here instead, because query_raw will still
                // prepare. simple_query returns a different result type, so may take some work to
                // get it work properly here.
                convert_pgsql_results(client.query_raw(query.as_ref(), Vec::<i8>::new()).await?)
                    .await
                    .map_err(DatabaseError::PostgreSQL)
            }
        }
    }

    /// Executes prepare for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn prepare<Q>(&mut self, query: Q) -> Result<DatabaseStatement, DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.prep(query).await?.into()),
            DatabaseConnection::PostgreSQL(client, _jh) => {
                Ok(client.prepare(query.as_ref()).await?.into())
            }
        }
    }

    /// Executes a prepared statement for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant. Will also optionally prepare and execute a query string, if
    /// supplied instead.
    pub async fn execute<P, V>(
        &mut self,
        stmt: impl Into<DatabaseStatement>,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError>
    where
        P: IntoIterator,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match stmt.into() {
            DatabaseStatement::MySql(s) => {
                convert_mysql_results(
                    self.as_mysql_conn()
                        .ok_or(DatabaseError::WrongConnection(ConnectionType::MySQL))?
                        .exec_iter(s, convert_mysql_params(params)?)
                        .await?,
                )
                .await
            }
            DatabaseStatement::Postgres(s) => convert_pgsql_results(
                self.as_postgres_conn()
                    .ok_or(DatabaseError::WrongConnection(ConnectionType::PostgreSQL))?
                    .query_raw(&s, params)
                    .await?,
            )
            .await
            .map_err(DatabaseError::PostgreSQL),
            DatabaseStatement::Str(s) => self.execute_str(s.as_ref(), params).await,
        }
    }

    /// Executes a simple query string, and can potentially return an underlying database error.
    /// That might be an underlying mysql or postgres error, depending on the underlying connection
    /// type, or it may be a value conversion error in the case that the caller is using a custom
    /// value type to convert results into.
    pub async fn execute_str<P, V>(
        &mut self,
        stmt: &str,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError>
    where
        P: IntoIterator,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.exec_iter(stmt, convert_mysql_params(params)?).await?)
                    .await
            }
            DatabaseConnection::PostgreSQL(client, _jh) => {
                convert_pgsql_results(client.query_raw(stmt, params).await?)
                    .await
                    .map_err(DatabaseError::PostgreSQL)
            }
        }
    }

    pub fn as_mysql_conn(&mut self) -> Option<&mut mysql_async::Conn> {
        if let DatabaseConnection::MySQL(c) = self {
            Some(c)
        } else {
            None
        }
    }

    pub fn as_postgres_conn(&mut self) -> Option<&mut tokio_postgres::Client> {
        if let DatabaseConnection::PostgreSQL(c, _jh) = self {
            Some(c)
        } else {
            None
        }
    }
}

fn convert_mysql_params<P>(params: P) -> Result<Vec<mysql_async::Value>, DatabaseError>
where
    P: IntoIterator,
    mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
    <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
        std::error::Error + Send + Sync + 'static,
{
    params
        .into_iter()
        .map(mysql_async::Value::try_from)
        .collect::<Result<Vec<mysql_async::Value>, _>>()
        .map_err(|e| DatabaseError::ValueConversion(Box::new(e)))
}

/// An enum wrapper around various prepared statement types. Either a mysql_async prepared
/// statement, a tokio_postgres prepared statement, or a plain query string that we would like to
/// both prepare and execute.
#[derive(From, Clone)]
pub enum DatabaseStatement {
    /// A MySQL prepared statement returned from a prepare call in `mysql_async`.
    MySql(mysql_async::Statement),
    /// A PostgreSQL prepared statement returned from a prepare call in `tokio_postgres`.
    Postgres(tokio_postgres::Statement),
    /// A simple query string that a user would like to be both prepared and executed.
    Str(String),
}

impl From<&str> for DatabaseStatement {
    fn from(s: &str) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_owned())
    }
}

impl From<&String> for DatabaseStatement {
    fn from(s: &String) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_owned())
    }
}
