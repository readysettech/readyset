use std::io;

use mysql_srv::MsqlSrvError;
use readyset_adapter::upstream_database::{IsFatalError, UpstreamFailure};
use readyset_errors::ReadySetError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    ReadySet(#[from] ReadySetError),

    #[error(transparent)]
    MySql(#[from] mysql_async::Error),

    #[error(transparent)]
    MsqlSrv(#[from] MsqlSrvError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl Error {
    /// Transforms each error to the closest mysql error.
    /// Sometimes, there is not a good one and UNKNOWN is used.
    pub fn error_kind(&self) -> mysql_srv::ErrorKind {
        // TODO(peter): Implement the rest of this error translation logic.
        /*
        match self {
            MySqlError(_) => ErrorKind::ER_UNKNOWN_ERROR,
            MySqlAsyncError(_) => ErrorKind::ER_UNKNOWN_ERROR,
            NoriaReadError(_) => ErrorKind::ER_ERROR_ON_READ,
            NoriaWriteError(_) => ErrorKind::ER_ERROR_ON_WRITE,
            NoriaRecipeError(_) => ErrorKind::ER_UNKNOWN_ERROR,
            ParseError(_) => ErrorKind::ER_PARSE_ERROR,
            IOError(_) => ErrorKind::ER_IO_WRITE_ERROR,
            UnimplementedError(_) => ErrorKind::ER_NOT_SUPPORTED_YET,
            UnsupportedError(_) => ErrorKind::ER_NOT_SUPPORTED_YET,
            Internal(_) => ErrorKind::ER_INTERNAL_ERROR,
            MissingPreparedStatement => ErrorKind::ER_NEED_REPREPARE,
        }
         */
        match self {
            Self::ReadySet(e) if e.is_transient() => mysql_srv::ErrorKind::ER_QUERY_INTERRUPTED,
            Self::ReadySet(e) if e.caused_by_table_not_found() => {
                mysql_srv::ErrorKind::ER_NO_SUCH_TABLE
            }
            Self::MySql(mysql_async::Error::Server(e)) => e.code.into(),
            Self::MySql(_) => {
                // TODO(peter): We need to translate these to appropriate
                // mysql error codes. Currently mysql_async is only used by fallback.
                mysql_srv::ErrorKind::ER_UNKNOWN_ERROR
            }
            _ => mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
        }
    }
}

impl IsFatalError for Error {
    fn is_fatal(&self) -> bool {
        matches!(self, Self::MySql(e) if e.is_fatal())
    }

    fn upstream_failure(&self) -> UpstreamFailure {
        match self {
            Self::MySql(e) => UpstreamFailure::from_mysql(e),
            Self::ReadySet(e) if e.is_upstream_server_down() => UpstreamFailure::Down,
            Self::Io(_) => UpstreamFailure::Down,
            Self::ReadySet(_) | Self::MsqlSrv(_) => UpstreamFailure::Semantic,
        }
    }
}
