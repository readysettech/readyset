use crate::backend::error::Error::*;
use msql_srv::ErrorKind;
use std::{fmt, io};

/// An enum of the common error types experiences when reading and writing from the data store.
/// Connectors should all use this error enum in their results.
#[derive(Debug)]
pub enum Error {
    MySqlError(mysql::error::Error),
    MySqlAsyncError(mysql_async::error::Error),
    NoriaReadError(anyhow::Error),
    NoriaWriteError(anyhow::Error),
    NoriaRecipeError(anyhow::Error),
    ParseError(String),
    IOError(io::Error),
    UnimplementedError(String),
    UnsupportedError(String),
    MissingPreparedStatement,
}

impl Error {
    /// Transforms each error to the closest mysql error.
    /// Sometimes, there is not a good one and UNKNOWN is used.
    pub fn error_kind(&self) -> ErrorKind {
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
            MissingPreparedStatement => ErrorKind::ER_NEED_REPREPARE,
        }
    }

    pub fn message(&self) -> String {
        match self {
            MySqlError(e) => format!(
                "There was an error executing this query in the mysql connector : {:?}",
                e
            ),
            MySqlAsyncError(e) => format!(
                "There was an error executing this query in the mysql connector : {:?}",
                e
            ),
            NoriaReadError(e) => format!("There was an error trying to read from Noria : {:?}", e),
            NoriaWriteError(e) => format!("There was an error trying to write to Noria : {:?}", e),
            NoriaRecipeError(e) => format!(
                "There was an error when trying to install a noria recipe : {:?}",
                e
            ),
            ParseError(e) => format!("There was an error parsing this query : {:?}", e),
            IOError(e) => format!("{:?}", e),
            UnimplementedError(e) => format!("This operation is not supported yet. {:?}", e),
            UnsupportedError(e) => format!(
                "This operation is not currently supported in Noria. {:?}",
                e
            ),
            MissingPreparedStatement => "Could not find this prepared statement.".to_string(),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MySqlError(e) => Some(e),
            MySqlAsyncError(e) => Some(e),
            NoriaReadError(_) => None,
            NoriaWriteError(_) => None,
            NoriaRecipeError(_) => None,
            ParseError(_) => None,
            IOError(e) => Some(e),
            UnimplementedError(_) => None,
            UnsupportedError(_) => None,
            MissingPreparedStatement => None,
        }
    }
}

impl From<mysql::error::Error> for Error {
    fn from(e: mysql::error::Error) -> Self {
        MySqlError(e)
    }
}

impl From<mysql_async::error::Error> for Error {
    fn from(e: mysql_async::error::Error) -> Self {
        MySqlAsyncError(e)
    }
}

impl Into<io::Error> for Error {
    fn into(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self.message())
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        IOError(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}
