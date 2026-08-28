use std::io;

use psql_srv as ps;
use readyset_adapter::upstream_database::{IsFatalError, UpstreamFailure};
use readyset_errors::ReadySetError;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    ReadySet(#[from] ReadySetError),

    #[error("{}", readyset_errors::postgres_err(.0))]
    PostgreSql(#[from] tokio_postgres::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Tls(#[from] native_tls::Error),
}

impl From<Error> for ps::Error {
    fn from(e: Error) -> Self {
        use Error::*;
        match e {
            Io(e) => ps::Error::IoError(e),
            ReadySet(e) if e.is_transient() => ps::Error::TransientError(e.to_string()),
            ReadySet(e) if e.caused_by_table_not_found() => ps::Error::TableNotFound(e.to_string()),
            ReadySet(ReadySetError::UnparseableQuery(err)) => ps::Error::ParseError(err),
            ReadySet(ReadySetError::PreparedStatementMissing { statement_id }) => {
                ps::Error::MissingPreparedStatement(statement_id.to_string())
            }
            ReadySet(ReadySetError::Unsupported(s)) => ps::Error::Unsupported(s),
            ReadySet(ReadySetError::ConnectionClosed(s)) => ps::Error::ConnectionClosed(s),
            ReadySet(e) => ps::Error::Unknown(e.to_string()),
            PostgreSql(e) if e.is_closed() => {
                error!("upstream connection closed");
                ps::Error::ConnectionClosed("upstream connection closed".into())
            }
            PostgreSql(e) => e.into(),
            Tls(e) => ps::Error::Unknown(e.to_string()),
        }
    }
}

impl IsFatalError for Error {
    fn is_fatal(&self) -> bool {
        // For now we have no way of matching on the inner error kind ofr postgres errors, so
        // we can't detect io, or other fatal errors.
        matches!(self, Self::PostgreSql(e) if e.is_closed())
    }

    fn upstream_failure(&self) -> UpstreamFailure {
        match self {
            Self::PostgreSql(e) => UpstreamFailure::from_postgres(e),
            Self::ReadySet(e) if e.is_upstream_server_down() => UpstreamFailure::Down,
            Self::Io(_) | Self::Tls(_) => UpstreamFailure::Down,
            Self::ReadySet(_) => UpstreamFailure::Semantic,
        }
    }
}
