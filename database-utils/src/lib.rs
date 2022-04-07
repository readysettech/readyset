#![feature(never_type)]

use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::marker::{Send, Sync};
use std::str::FromStr;

use derive_more::From;
use error::{ConnectionType, DatabaseTypeParseError};
use futures::{StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::OptsBuilder;
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::{DatabaseError, DatabaseURLParseError};

pub mod error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
}

/// Parses the strings `"mysql"` and `"postgresql"`, case-insensitively
///
/// # Examples
///
/// ```rust
/// use database_utils::DatabaseType;
///
/// let pg: DatabaseType = "postgresql".parse().unwrap();
/// assert_eq!(pg, DatabaseType::PostgreSQL);
///
/// let my: DatabaseType = "mysql".parse().unwrap();
/// assert_eq!(my, DatabaseType::MySQL);
///
/// assert!("pgsql".parse::<DatabaseType>().is_err());
/// ```
impl FromStr for DatabaseType {
    type Err = DatabaseTypeParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" => Ok(Self::MySQL),
            "postgresql" => Ok(Self::PostgreSQL),
            _ => Err(DatabaseTypeParseError {
                value: s.to_owned(),
            }),
        }
    }
}

/// Displays as either `MySQL` or PostgreSQL
///
/// # Examples
///
/// ```
/// use database_utils::DatabaseType;
///
/// assert_eq!(
///     format!("{}", DatabaseType::PostgreSQL),
///     "PostgreSQL".to_owned()
/// );
/// ```
impl Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::MySQL => f.write_str("MySQL"),
            DatabaseType::PostgreSQL => f.write_str("PostgreSQL"),
        }
    }
}

/// URL for an upstream database.
///
/// [`DatabaseURL`]s can be constructed directly via the [`From`] implementations, or parsed from a
/// database URL using the [`FromStr`] implementation. A [`DatabaseConnection`] can be built from a
/// [`DatabaseURL`] via the [`connect` method](Self::connect).
#[derive(Debug, Clone, From)]
#[allow(clippy::large_enum_variant)]
pub enum DatabaseURL {
    MySQL(mysql_async::Opts),
    PostgreSQL(pgsql::Config),
}

/// Parses URLs starting with either `"mysql://"` or `"postgresql://"`.
///
/// # Examples
///
/// ```
/// use database_utils::{DatabaseType, DatabaseURL};
///
/// let mysql_url: DatabaseURL = "mysql://root:password@localhost:3306/mysql"
///     .parse()
///     .unwrap();
/// assert_eq!(mysql_url.database_type(), DatabaseType::MySQL);
/// ```
///
/// ```
/// use database_utils::{DatabaseType, DatabaseURL};
///
/// let pgsql_url: DatabaseURL = "postgresql://postgres:postgres@localhost:5432/postgres"
///     .parse()
///     .unwrap();
/// assert_eq!(pgsql_url.database_type(), DatabaseType::PostgreSQL);
/// ```
impl FromStr for DatabaseURL {
    type Err = DatabaseURLParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            Ok(Self::MySQL(mysql::Opts::from_url(s)?))
        } else if s.starts_with("postgresql://") {
            Ok(Self::PostgreSQL(pgsql::Config::from_str(s)?))
        } else {
            Err(DatabaseURLParseError::InvalidFormat)
        }
    }
}

impl From<mysql_async::OptsBuilder> for DatabaseURL {
    fn from(ob: mysql_async::OptsBuilder) -> Self {
        Self::MySQL(ob.into())
    }
}

impl DatabaseURL {
    /// Create a new [`DatabaseConnection`] by connecting to the database at this database URL
    pub async fn connect(&self) -> Result<DatabaseConnection, DatabaseError<!>> {
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

    /// Returns the underlying database type, either [`DatabaseType::MySQL`] or
    /// [`DatabaseType::PostgreSQL].
    pub fn database_type(&self) -> DatabaseType {
        match self {
            DatabaseURL::MySQL(_) => DatabaseType::MySQL,
            DatabaseURL::PostgreSQL(_) => DatabaseType::PostgreSQL,
        }
    }

    /// Returns the underlying database nname.
    pub fn db_name(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.db_name(),
            DatabaseURL::PostgreSQL(config) => config.get_dbname(),
        }
    }

    /// Sets the underlying database nname.
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

/// An enum wrapper around either a MySQL or PostgreSQL connection.
pub enum DatabaseConnection {
    /// A MySQL database connection.
    MySQL(mysql_async::Conn),
    /// A PostgreSQL database connection.
    PostgreSQL(pgsql::Client),
}

async fn convert_mysql_results<'a, 't, P, V>(
    mut results: mysql::QueryResult<'a, 't, P>,
) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
where
    P: mysql::prelude::Protocol,
    V: TryFrom<mysql::Value>,
    <V as TryFrom<mysql::Value>>::Error: std::error::Error,
{
    results
        .map(|mut r| {
            (0..r.columns().len())
                .map(|c| {
                    V::try_from(r.take::<mysql::Value, _>(c).unwrap())
                        .map_err(DatabaseError::ValueConversion)
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
    /// DatabaseConnection variant. Since we aren't returning any results, we can't get a
    /// ValueError type as the result of TryFrom<mysql::Value>, so the error type is annotated with
    /// the never type.
    pub async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError<!>>
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

    /// Executes query for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn query<Q, V>(&mut self, query: Q) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        Q: AsRef<str> + Send + Sync,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.query_iter(query).await?).await
            }
            DatabaseConnection::PostgreSQL(client) => {
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
    pub async fn prepare<Q>(&mut self, query: Q) -> Result<DatabaseStatement, DatabaseError<!>>
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

    /// Executes a prepared statement for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant. Will also optionally prepare and execute a query string, if
    /// supplied instead.
    pub async fn execute<P, V>(
        &mut self,
        stmt: impl Into<DatabaseStatement>,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match stmt.into() {
            DatabaseStatement::Mysql(s) => {
                convert_mysql_results(
                    self.as_mysql_conn()
                        .ok_or(DatabaseError::WrongConnection(ConnectionType::MySQL))?
                        .exec_iter(s, params)
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
    async fn execute_str<P, V>(
        &mut self,
        stmt: &str,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.exec_iter(stmt, params).await?).await
            }
            DatabaseConnection::PostgreSQL(client) => {
                convert_pgsql_results(client.query_raw(stmt, params).await?)
                    .await
                    .map_err(DatabaseError::PostgreSQL)
            }
        }
    }

    fn as_mysql_conn(&mut self) -> Option<&mut mysql_async::Conn> {
        if let DatabaseConnection::MySQL(c) = self {
            Some(c)
        } else {
            None
        }
    }

    fn as_postgres_conn(&mut self) -> Option<&mut tokio_postgres::Client> {
        if let DatabaseConnection::PostgreSQL(c) = self {
            Some(c)
        } else {
            None
        }
    }
}

/// An enum wrapper around various prepared statement types. Either a mysql_async prepared
/// statement, a tokio_postgres prepared statement, or a plain query string that we would like to
/// both prepare and execute.
#[derive(From)]
pub enum DatabaseStatement {
    /// A MySQL prepared statement returned from a prepare call in `mysql_async`.
    Mysql(mysql_async::Statement),
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
