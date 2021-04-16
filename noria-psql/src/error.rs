use noria::ReadySetError;
use noria_client::backend as cl;
use psql_srv as ps;

/// A simple wrapper around `noria_client`'s `Error`, facilitating conversion to `psql_srv::Error`.
pub struct Error(cl::error::Error);

impl From<cl::error::Error> for Error {
    fn from(e: cl::error::Error) -> Self {
        Error(e)
    }
}

impl From<Error> for ps::Error {
    fn from(e: Error) -> Self {
        use cl::error::Error::*;
        match e.0 {
            MySql(e) => ps::Error::Unknown(e.to_string()),
            MySqlAsync(e) => ps::Error::Unknown(e.to_string()),
            Io(e) => ps::Error::IoError(e),
            ReadySet(ReadySetError::UnparseableQuery { query }) => ps::Error::ParseError(query),
            ReadySet(ReadySetError::PreparedStatementMissing) => {
                ps::Error::MissingPreparedStatement("unknown".to_string())
            }
            ReadySet(ReadySetError::Unsupported(s)) => ps::Error::Unsupported(s),
            ReadySet(e) => ps::Error::Unknown(e.to_string()),
        }
    }
}
