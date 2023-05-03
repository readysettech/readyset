use std::convert::TryFrom;
use std::marker::{Send, Sync};
use std::str;

use async_trait::async_trait;
use derive_more::From;
use futures::TryStreamExt;
use mysql::prelude::AsQuery;
use mysql_async::prelude::Queryable;
use readyset_errors::ReadySetError;
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::{ConnectionType, DatabaseError};

#[async_trait]
pub trait QueryableConnection: Send {
    /// Executes query_drop for either mysql or postgres, whichever is the underlying
    /// connection variant.
    async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync;

    /// Executes query for either mysql or postgres, whichever is the underlying
    /// connection variant.
    async fn query<Q>(&mut self, query: Q) -> Result<QueryResults, DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync;

    /// Executes a prepared statement for either mysql or postgres. Will also optionally prepare and
    /// execute a query string, if supplied instead.
    async fn execute<P>(
        &mut self,
        stmt: impl Into<DatabaseStatement> + Send,
        params: P,
    ) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static;

    /// Executes a simple query string, and can potentially return an underlying database error.
    /// That might be an underlying mysql or postgres error, depending on the underlying connection
    /// type, or it may be a value conversion error in the case that the caller is using a custom
    /// value type to convert results into.
    async fn execute_str<P>(
        &mut self,
        stmt: &str,
        params: P,
    ) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static;
}

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

#[async_trait]
impl QueryableConnection for DatabaseConnection {
    async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError>
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

    async fn query<Q>(&mut self, query: Q) -> Result<QueryResults, DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(QueryResults::MySql(
                conn.query_iter(query).await?.collect().await?,
            )),
            DatabaseConnection::PostgreSQL(client, _jh) => {
                // TODO: We should use simple_query here instead, because query_raw will still
                // prepare. simple_query returns a different result type, so may take some work to
                // get it work properly here.
                Ok(QueryResults::Postgres(
                    client
                        .query_raw(query.as_ref(), Vec::<i8>::new())
                        .await?
                        .try_collect()
                        .await?,
                ))
            }
        }
    }

    async fn execute<P>(
        &mut self,
        stmt: impl Into<DatabaseStatement> + Send,
        params: P,
    ) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match stmt.into() {
            DatabaseStatement::MySql(s) => Ok(QueryResults::MySql(
                self.as_mysql_conn()?
                    .exec_iter(s, convert_mysql_params(params)?)
                    .await?
                    .collect()
                    .await?,
            )),
            DatabaseStatement::Postgres(s) => Ok(QueryResults::Postgres(
                self.as_postgres_conn()?
                    .query_raw(&s, params)
                    .await?
                    .try_collect()
                    .await?,
            )),
            DatabaseStatement::Str(s) => self.execute_str(s.as_ref(), params).await,
        }
    }

    async fn execute_str<P>(&mut self, stmt: &str, params: P) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(QueryResults::MySql(
                conn.exec_iter(stmt, convert_mysql_params(params)?)
                    .await?
                    .collect()
                    .await?,
            )),
            DatabaseConnection::PostgreSQL(client, _jh) => Ok(QueryResults::Postgres(
                client.query_raw(stmt, params).await?.try_collect().await?,
            )),
        }
    }
}

impl DatabaseConnection {
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

    /// Creates a new transaction using the underlying database connection.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, DatabaseError> {
        match self {
            Self::MySQL(conn) => conn
                .start_transaction(mysql_async::TxOpts::default())
                .await
                .map(Transaction::MySql)
                .map_err(DatabaseError::MySQL),
            Self::PostgreSQL(conn, _jh) => conn
                .transaction()
                .await
                .map(Transaction::Postgres)
                .map_err(DatabaseError::PostgreSQL),
        }
    }

    pub fn as_mysql_conn(&mut self) -> Result<&mut mysql_async::Conn, DatabaseError> {
        if let DatabaseConnection::MySQL(c) = self {
            Ok(c)
        } else {
            Err(DatabaseError::WrongConnection(ConnectionType::MySQL))
        }
    }

    pub fn as_postgres_conn(&mut self) -> Result<&mut tokio_postgres::Client, DatabaseError> {
        if let DatabaseConnection::PostgreSQL(c, _jh) = self {
            Ok(c)
        } else {
            Err(DatabaseError::WrongConnection(ConnectionType::PostgreSQL))
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

/// An enum wrapper around the native Postgres and MySQL result types.
pub enum QueryResults {
    MySql(Vec<mysql_async::Row>),
    Postgres(Vec<pgsql::Row>),
}

impl<V> TryFrom<QueryResults> for Vec<Vec<V>>
where
    V: TryFrom<mysql_async::Value>,
    <V as TryFrom<mysql_async::Value>>::Error: std::error::Error + Send + Sync + 'static,
    for<'a> V: pgsql::types::FromSql<'a>,
{
    type Error = DatabaseError;

    fn try_from(results: QueryResults) -> Result<Self, Self::Error> {
        match results {
            QueryResults::MySql(results) => Ok(results
                .into_iter()
                .map(|mut r| {
                    (0..r.columns().len())
                        .map(|c| {
                            V::try_from(r.take::<mysql::Value, _>(c).unwrap())
                                .map_err(|e| DatabaseError::ValueConversion(Box::new(e)))
                        })
                        .collect::<Result<Vec<V>, _>>()
                })
                .collect::<Result<Vec<Vec<V>>, _>>()?),
            QueryResults::Postgres(results) => Ok(results
                .into_iter()
                .map(|r| {
                    (0..r.len())
                        .map(|c| r.try_get(c))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<Vec<V>>, _>>()
                .map_err(DatabaseError::PostgreSQL)?),
        }
    }
}

/// An enum wrapper around the native Postgres and MySQL transaction types.
pub enum Transaction<'a> {
    MySql(mysql_async::Transaction<'a>),
    Postgres(pgsql::Transaction<'a>),
}

#[async_trait]
impl<'a> QueryableConnection for Transaction<'a> {
    async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
    {
        match self {
            Transaction::MySql(transaction) => Ok(transaction.query_drop(stmt).await?),
            Transaction::Postgres(transaction) => {
                transaction.simple_query(stmt.as_ref()).await?;
                Ok(())
            }
        }
    }

    async fn query<Q>(&mut self, query: Q) -> Result<QueryResults, DatabaseError>
    where
        Q: AsQuery + AsRef<str> + Send + Sync,
    {
        match self {
            Transaction::MySql(transaction) => Ok(QueryResults::MySql(
                transaction.query_iter(query).await?.collect().await?,
            )),
            Transaction::Postgres(transaction) => {
                // TODO: We should use simple_query here instead, because query_raw will still
                // prepare. simple_query returns a different result type, so may take some work to
                // get it work properly here.
                Ok(QueryResults::Postgres(
                    transaction
                        .query_raw(query.as_ref(), Vec::<i8>::new())
                        .await?
                        .try_collect()
                        .await?,
                ))
            }
        }
    }

    async fn execute<'b, P>(
        &'b mut self,
        stmt: impl Into<DatabaseStatement> + Send,
        params: P,
    ) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match stmt.into() {
            DatabaseStatement::MySql(stmt) => Ok(QueryResults::MySql(
                self.as_mysql_transaction()?
                    .exec_iter(stmt, convert_mysql_params(params)?)
                    .await?
                    .collect()
                    .await?,
            )),
            DatabaseStatement::Postgres(stmt) => Ok(QueryResults::Postgres(
                self.as_postgres_transaction()?
                    .query_raw(&stmt, params)
                    .await?
                    .try_collect()
                    .await?,
            )),
            DatabaseStatement::Str(stmt) => self.execute_str(&stmt, params).await,
        }
    }

    async fn execute_str<'b, P>(
        &'b mut self,
        stmt: &str,
        params: P,
    ) -> Result<QueryResults, DatabaseError>
    where
        P: IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        mysql_async::Value: TryFrom<<P as IntoIterator>::Item>,
        <mysql_async::Value as TryFrom<<P as IntoIterator>::Item>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match self {
            Transaction::MySql(transaction) => Ok(QueryResults::MySql(
                transaction
                    .exec_iter(stmt, convert_mysql_params(params)?)
                    .await?
                    .collect()
                    .await?,
            )),
            Transaction::Postgres(transaction) => Ok(QueryResults::Postgres(
                transaction
                    .query_raw(stmt, params)
                    .await?
                    .try_collect()
                    .await?,
            )),
        }
    }
}

impl<'a> Transaction<'a> {
    /// Consumes the transaction, committing the operations.
    pub async fn commit(self) -> Result<(), DatabaseError> {
        match self {
            Self::MySql(transaction) => transaction.commit().await.map_err(DatabaseError::MySQL),
            Self::Postgres(transaction) => transaction
                .commit()
                .await
                .map_err(DatabaseError::PostgreSQL),
        }
    }

    fn as_mysql_transaction<'b>(
        &'b mut self,
    ) -> Result<&'b mut mysql_async::Transaction<'a>, DatabaseError> {
        if let Self::MySql(c) = self {
            Ok(c)
        } else {
            Err(DatabaseError::WrongConnection(ConnectionType::MySQL))
        }
    }

    fn as_postgres_transaction<'b>(
        &'b mut self,
    ) -> Result<&'b mut tokio_postgres::Transaction<'a>, DatabaseError> {
        if let Self::Postgres(c) = self {
            Ok(c)
        } else {
            Err(DatabaseError::WrongConnection(ConnectionType::PostgreSQL))
        }
    }
}
