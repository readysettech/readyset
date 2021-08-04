use msql_srv::ErrorKind;
use msql_srv::MsqlSrvError;
use noria::ReadySetError;
use std::io;
use thiserror::Error;

/// An enum of the common error types experiences when reading and writing from the data store.
/// Connectors should all use this error enum in their results.
#[derive(Debug, Error)]
pub enum Error {
    #[error("MySQL error: {0}")]
    MySql(#[from] mysql::error::Error),
    #[error("MySQL error: {0}")]
    MySqlAsync(#[from] mysql_async::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    ReadySet(ReadySetError),
    #[error("MsqlSrvError: {0}")]
    MsqlSrv(#[from] MsqlSrvError),
}

/// Everything in `noria-client` involves doing a Noria RPC call, so this `From` implementation
/// discards a top-level `ReadySetError::RpcFailed`, if there is one.
impl From<ReadySetError> for Error {
    fn from(rse: ReadySetError) -> Error {
        Error::ReadySet(match rse {
            ReadySetError::RpcFailed { source, .. } => *source,
            x => x,
        })
    }
}

impl Error {
    /// Transforms each error to the closest mysql error.
    /// Sometimes, there is not a good one and UNKNOWN is used.
    pub fn error_kind(&self) -> ErrorKind {
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
            Error::MySql(mysql::Error::MySqlError(e)) => e.code.into(),
            Error::MySqlAsync(mysql_async::Error::Server(e)) => e.code.into(),
            Error::MySqlAsync(_) => {
                // TODO(peter): We need to translate these to appropriate
                // mysql error codes. Currently mysql_async is only used by fallback.
                ErrorKind::ER_UNKNOWN_ERROR
            }
            _ => ErrorKind::ER_UNKNOWN_ERROR,
        }
    }
}
