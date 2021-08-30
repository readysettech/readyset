use noria::ReadySetError;
use psql_srv as ps;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    ReadySet(#[from] ReadySetError),

    #[error(transparent)]
    PostgreSql(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<Error> for ps::Error {
    fn from(e: Error) -> Self {
        use Error::*;
        match e {
            Io(e) => ps::Error::IoError(e),
            ReadySet(ReadySetError::UnparseableQuery { query }) => ps::Error::ParseError(query),
            ReadySet(ReadySetError::PreparedStatementMissing { statement_id }) => {
                ps::Error::MissingPreparedStatement(statement_id.to_string())
            }
            ReadySet(ReadySetError::Unsupported(s)) => ps::Error::Unsupported(s),
            ReadySet(e) => ps::Error::Unknown(e.to_string()),
            PostgreSql(e) => e.into(),
        }
    }
}
