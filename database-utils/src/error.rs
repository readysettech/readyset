use std::fmt::{Debug, Display};

use thiserror::Error;
use tokio_postgres::error::SqlState;
use {mysql_async as mysql, tokio_postgres as pgsql};

/// Base error enum for this library. Represents many types of potential errors we may encounter.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    PostgreSQL(#[from] pgsql::Error),

    #[error(transparent)]
    MySQL(#[from] mysql::Error),

    #[error(transparent)]
    NativeTls(#[from] native_tls::Error),

    #[error(transparent)]
    DeadpoolPool(#[from] deadpool_postgres::PoolError),

    #[error(transparent)]
    DeadpoolBuild(#[from] deadpool_postgres::BuildError),

    #[error("Error converting value from result set: {0}")]
    ValueConversion(Box<dyn std::error::Error + Send + Sync>),

    #[error("DatabaseConnection is not a {0} connection variant")]
    WrongConnection(ConnectionType),

    #[error("TLS not supported for MySQL")]
    TlsUnsupported,
}

impl DatabaseError {
    /// Returns true only if the error is a MySQL key already exists error
    pub fn is_mysql_key_already_exists(&self) -> bool {
        matches!(self, Self::MySQL(mysql::Error::Server(e)) if e.code == 1062)
    }

    /// Returns true only if the error is a Postgres unique violation error
    pub fn is_postgres_unique_violation(&self) -> bool {
        matches!(self, Self::PostgreSQL(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION))
    }
}

#[derive(Debug)]
pub enum ConnectionType {
    MySQL,
    PostgreSQL,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ConnectionType::MySQL => "MySQL".to_string(),
            ConnectionType::PostgreSQL => "PostgreSQL".to_string(),
        };
        write!(f, "{}", s)
    }
}

/// Error type for the [`FromStr`] implementation for [`DatabaseURL`]
#[derive(Debug, Error)]
pub enum DatabaseURLParseError {
    #[error("Invalid database URL format; database URLs must start with either mysql:// or postgresql://")]
    InvalidFormat,

    #[error("Invalid database URL format; ReadySet requires that Postgres database URLs contain a database name")]
    MissingPostgresDbName,

    #[error("Invalid database URL format: {0}; Make sure any special characters are percent-encoded. See https://docs.readyset.io/reference/cli/readyset#--upstream-db-url for more details.")]
    PostgreSQL(#[from] pgsql::Error),

    #[error("Invalid database URL format: {0}; Make sure any special characters are percent-encoded. See https://docs.readyset.io/reference/cli/readyset#--upstream-db-url for more details.")]
    MySQL(#[from] mysql::UrlError),
}

/// Error type for the [`FromStr`] implementation for [`DatabaseType`]
#[derive(Debug, Error)]
#[error("Invalid upstream type `{value}`, expected one of `mysql` or `postgres`")]
pub struct DatabaseTypeParseError {
    /// The value that was originally being parsed
    pub value: String,
}
