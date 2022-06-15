use std::error::Error;
use std::fmt::{Debug, Display};

use thiserror::Error;
use {mysql_async as mysql, tokio_postgres as pgsql};

/// Base error enum for this library. Represents many types of potential errors we may encounter.
#[derive(Debug, Error)]
pub enum DatabaseError<ValueError: Debug + Error> {
    #[error(transparent)]
    PostgreSQL(#[from] pgsql::Error),

    #[error(transparent)]
    MySQL(#[from] mysql::Error),

    #[error("Error converting value from result set")]
    ValueConversion(ValueError),

    #[error("DatabaseConnection is not a {0} connection variant")]
    WrongConnection(ConnectionType),
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

    #[error(transparent)]
    PostgreSQL(#[from] pgsql::Error),

    #[error(transparent)]
    MySQL(#[from] mysql::UrlError),
}

/// Error type for the [`FromStr`] implementation for [`DatabaseType`]
#[derive(Debug, Error)]
#[error("Invalid upstream type `{value}`, expected one of `mysql` or `postgres`")]
pub struct DatabaseTypeParseError {
    /// The value that was originally being parsed
    pub value: String,
}
