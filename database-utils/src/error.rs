use std::fmt::{Debug, Display};

use thiserror::Error;
use tokio_postgres::error::SqlState;
use {mysql_async as mysql, tokio_postgres as pgsql};

/// Base error enum for this library. Represents many types of potential errors we may encounter.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("{}", readyset_errors::postgres_err(.0))]
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

    #[error("Error interpolating parameters into query: {0}")]
    Interpolation(Box<dyn std::error::Error + Send + Sync>),

    #[error("Upstream connection is None")]
    UpstreamConnectionNone,

    #[error("Upstream query timeout")]
    UpstreamQueryTimeout,

    #[error("Row missing from result set")]
    RowMissing,

    #[error("Unexpected row in result set")]
    UnexpectedRow,

    #[error("Column missing from result set at index: {0}")]
    ColumnMissing(usize),
}

/// The kind of failure an upstream server returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamFailure {
    /// The server is not serving.
    Down,
    /// The server rejected the credentials.
    Auth,
    /// The request was wrong.
    Semantic,
}

impl UpstreamFailure {
    /// Classify a MySQL driver error.
    pub fn from_mysql(error: &mysql::Error) -> Self {
        match error {
            mysql::Error::Io(_) | mysql::Error::Driver(mysql::DriverError::ConnectionClosed) => {
                Self::Down
            }
            mysql::Error::Driver(mysql::DriverError::UnknownAuthPlugin { .. }) => Self::Auth,
            mysql::Error::Server(e) => Self::from_mysql_code(e.code),
            mysql::Error::Driver(_) | mysql::Error::Other(_) | mysql::Error::Url(_) => {
                Self::Semantic
            }
        }
    }

    /// Classify a MySQL server error code.
    pub fn from_mysql_code(code: u16) -> Self {
        match code {
            // ER_CON_COUNT_ERROR, ER_SERVER_SHUTDOWN, ER_OPTION_PREVENTS_STATEMENT,
            // ER_READ_ONLY_MODE, CR_CONNECTION_ERROR, CR_CONN_HOST_ERROR,
            // CR_SERVER_GONE_ERROR, CR_SERVER_LOST.
            1040 | 1053 | 1290 | 1836 | 2002 | 2003 | 2006 | 2013 => Self::Down,
            // ER_DBACCESS_DENIED_ERROR, ER_ACCESS_DENIED_ERROR, ER_NOT_SUPPORTED_AUTH_MODE,
            // ER_ACCESS_DENIED_NO_PASSWORD_ERROR.
            1044 | 1045 | 1251 | 1698 => Self::Auth,
            _ => Self::Semantic,
        }
    }

    /// Classify a PostgreSQL driver error.
    pub fn from_postgres(error: &pgsql::Error) -> Self {
        if error.is_closed() {
            return Self::Down;
        }
        match error.code() {
            // An error with no SQLSTATE did not come from the server: connect, I/O, or TLS.
            None => Self::Down,
            Some(code)
                if *code == SqlState::ADMIN_SHUTDOWN
                    || *code == SqlState::CRASH_SHUTDOWN
                    || *code == SqlState::CANNOT_CONNECT_NOW
                    || *code == SqlState::TOO_MANY_CONNECTIONS
                    || *code == SqlState::CONNECTION_EXCEPTION
                    || *code == SqlState::CONNECTION_FAILURE =>
            {
                Self::Down
            }
            Some(code)
                if *code == SqlState::INVALID_PASSWORD
                    || *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION =>
            {
                Self::Auth
            }
            Some(_) => Self::Semantic,
        }
    }
}

impl DatabaseError {
    /// The kind of upstream failure this error represents.
    pub fn upstream_failure(&self) -> UpstreamFailure {
        match self {
            Self::MySQL(e) => UpstreamFailure::from_mysql(e),
            Self::PostgreSQL(e) => UpstreamFailure::from_postgres(e),
            Self::DeadpoolPool(deadpool_postgres::PoolError::Backend(e)) => {
                UpstreamFailure::from_postgres(e)
            }
            Self::NativeTls(_)
            | Self::DeadpoolPool(_)
            | Self::UpstreamConnectionNone
            | Self::UpstreamQueryTimeout => UpstreamFailure::Down,
            Self::DeadpoolBuild(_)
            | Self::ValueConversion(_)
            | Self::WrongConnection(_)
            | Self::Interpolation(_)
            | Self::RowMissing
            | Self::UnexpectedRow
            | Self::ColumnMissing(_) => UpstreamFailure::Semantic,
        }
    }

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
        write!(f, "{s}")
    }
}

/// Error type for the [`FromStr`] implementation for [`DatabaseURL`]
#[derive(Debug, Error)]
pub enum DatabaseURLParseError {
    #[error(
        "Invalid database URL format; database URLs must start with either mysql:// or postgresql://"
    )]
    InvalidFormat,

    #[error(
        "Invalid database URL format; Readyset requires that Postgres database URLs contain a database name"
    )]
    MissingPostgresDbName,

    #[error(
        "Invalid database URL format: {}; Make sure any special characters are percent-encoded. See https://docs.readyset.io/reference/cli/readyset#--upstream-db-url for more details.",
        readyset_errors::postgres_err(.0)
    )]
    PostgreSQL(#[from] pgsql::Error),

    #[error(
        "Invalid database URL format: {0}; Make sure any special characters are percent-encoded. See https://docs.readyset.io/reference/cli/readyset#--upstream-db-url for more details."
    )]
    MySQL(#[from] mysql::UrlError),
}

/// Error type for the [`FromStr`] implementation for [`DatabaseType`]
#[derive(Debug, Error)]
#[error("Invalid upstream type `{value}`, expected one of `mysql` or `postgres`")]
pub struct DatabaseTypeParseError {
    /// The value that was originally being parsed
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_error(code: u16) -> mysql::Error {
        mysql::Error::Server(mysql::ServerError {
            code,
            message: String::new(),
            state: String::new(),
        })
    }

    #[test]
    fn mysql_transport_errors_mean_the_server_is_down() {
        let io = mysql::Error::Io(mysql::IoError::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )));
        assert_eq!(UpstreamFailure::from_mysql(&io), UpstreamFailure::Down);
        let closed = mysql::Error::Driver(mysql::DriverError::ConnectionClosed);
        assert_eq!(UpstreamFailure::from_mysql(&closed), UpstreamFailure::Down);
        let plugin = mysql::Error::Driver(mysql::DriverError::UnknownAuthPlugin {
            name: "sha256_password".to_string(),
        });
        assert_eq!(UpstreamFailure::from_mysql(&plugin), UpstreamFailure::Auth);
        let url = mysql::Error::Url(mysql::UrlError::Invalid);
        assert_eq!(UpstreamFailure::from_mysql(&url), UpstreamFailure::Semantic);
    }

    #[test]
    fn database_error_delegates_to_the_driver() {
        let err = DatabaseError::MySQL(server_error(1045));
        assert_eq!(err.upstream_failure(), UpstreamFailure::Auth);
        // ER_NO_SUCH_TABLE is not in any list, so it falls through to the default.
        let err = DatabaseError::MySQL(server_error(1146));
        assert_eq!(err.upstream_failure(), UpstreamFailure::Semantic);
        assert_eq!(
            DatabaseError::UpstreamQueryTimeout.upstream_failure(),
            UpstreamFailure::Down
        );
        assert_eq!(
            DatabaseError::RowMissing.upstream_failure(),
            UpstreamFailure::Semantic
        );
    }
}
