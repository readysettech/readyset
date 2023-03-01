use std::num::TryFromIntError;

use postgres::error::SqlState;
use postgres_types::Type;
use thiserror::Error;

use crate::codec::{DecodeError, EncodeError};
use crate::message::{BackendMessage, ErrorSeverity, FrontendMessage};

#[derive(Debug, Error)]
pub enum Error {
    #[error("password authentication failed for user \"{0}\"")]
    AuthenticationFailure(String),

    #[error("decode error: {0}")]
    DecodeError(#[from] DecodeError),

    #[error("encode error: {0}")]
    EncodeError(#[from] EncodeError),

    #[error("incorrect format count: {0}")]
    IncorrectFormatCount(usize),

    #[error("internal error: {0}")]
    InternalError(String),

    #[error("invalid integer: {0}")]
    InvalidInteger(#[from] TryFromIntError),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("missing portal: {0}")]
    MissingPortal(String),

    #[error("missing prepared statement: {0}")]
    MissingPreparedStatement(String),

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("unimplemented: {0}")]
    Unimplemented(String),

    #[error("unknown: {0}")]
    Unknown(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("unsupported message: {0}")]
    UnsupportedMessage(FrontendMessage),

    #[error("unsupported type: {0}")]
    UnsupportedType(Type),

    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::error::Error),
}

impl<R> From<Error> for BackendMessage<R> {
    fn from(error: Error) -> Self {
        let sqlstate = match error {
            Error::AuthenticationFailure(_) => SqlState::INVALID_PASSWORD,
            Error::DecodeError(_) => SqlState::IO_ERROR,
            Error::EncodeError(_) => SqlState::IO_ERROR,
            Error::IncorrectFormatCount(_) => SqlState::IO_ERROR,
            Error::InternalError(_) => SqlState::INTERNAL_ERROR,
            Error::InvalidInteger(_) => SqlState::DATATYPE_MISMATCH,
            Error::IoError(_) => SqlState::IO_ERROR,
            Error::MissingPortal(_) => SqlState::UNDEFINED_PSTATEMENT,
            Error::MissingPreparedStatement(_) => SqlState::UNDEFINED_PSTATEMENT,
            Error::ParseError(_) => SqlState::INVALID_PSTATEMENT_DEFINITION,
            Error::Unimplemented(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::Unknown(_) => SqlState::INTERNAL_ERROR,
            Error::Unsupported(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::UnsupportedMessage(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::UnsupportedType(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::PostgresError(ref e) => e.code().cloned().unwrap_or(SqlState::INTERNAL_ERROR),
        };

        BackendMessage::ErrorResponse {
            severity: ErrorSeverity::Error,
            sqlstate,
            message: error.to_string(),
        }
    }
}
