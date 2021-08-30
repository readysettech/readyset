use std::io;

use msql_srv::MsqlSrvError;
use noria::ReadySetError;
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
    pub fn error_kind(&self) -> msql_srv::ErrorKind {
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
            Self::MySql(mysql_async::Error::Server(e)) => e.code.into(),
            Self::MySql(_) => {
                // TODO(peter): We need to translate these to appropriate
                // mysql error codes. Currently mysql_async is only used by fallback.
                msql_srv::ErrorKind::ER_UNKNOWN_ERROR
            }
            _ => msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
        }
    }
}
