use std::num::TryFromIntError;

use postgres::error::SqlState;
use postgres_types::Type;
use thiserror::Error;

use crate::codec::{DecodeError, EncodeError};
use crate::message::{BackendMessage, ErrorSeverity, FrontendMessage};
use crate::scram;

#[derive(Debug, Error)]
pub enum Error {
    #[error("password authentication failed for user \"{username}\"")]
    AuthenticationFailure { username: String },

    #[error("no user specified in connection")]
    NoUserSpecified,

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

    #[error("invalid authorization specification error: {0}")]
    InvalidAuthorizationSpecification(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("missing portal: {0}")]
    MissingPortal(String),

    #[error("missing prepared statement: {0}")]
    MissingPreparedStatement(String),

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("unexpected message: {0}")]
    UnexpectedMessage(String),

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

    #[error("SCRAM error: {0}")]
    Scram(#[from] scram::Error),

    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::error::Error),

    #[error("connection closed: {0}")]
    ConnectionClosed(String),
}

impl From<Error> for BackendMessage {
    fn from(error: Error) -> Self {
        match error {
            Error::InvalidAuthorizationSpecification(message) => {
                return BackendMessage::error(
                    ErrorSeverity::Fatal,
                    SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                    message,
                )
            }
            Error::ConnectionClosed(message) => {
                return BackendMessage::error(
                    ErrorSeverity::Fatal,
                    SqlState::ADMIN_SHUTDOWN,
                    message,
                )
            }
            Error::PostgresError(ref e) if e.as_db_error().is_some() => {
                let db_error = e.as_db_error().unwrap();
                return BackendMessage::ErrorResponse {
                    severity: match db_error.severity() {
                        "ERROR" => ErrorSeverity::Error,
                        "FATAL" => ErrorSeverity::Fatal,
                        "PANIC" => ErrorSeverity::Panic,
                        _ => ErrorSeverity::Error,
                    },
                    sqlstate: db_error.code().clone(),
                    message: db_error.message().into(),
                    detail: db_error.detail().map(ToOwned::to_owned),
                    hint: db_error.hint().map(ToOwned::to_owned),
                    position: db_error.position().cloned(),
                    where_: db_error.where_().map(ToOwned::to_owned),
                    schema: db_error.schema().map(ToOwned::to_owned),
                    table: db_error.table().map(ToOwned::to_owned),
                    column: db_error.column().map(ToOwned::to_owned),
                    datatype: db_error.datatype().map(ToOwned::to_owned),
                    constraint: db_error.constraint().map(ToOwned::to_owned),
                    file: db_error.file().map(ToOwned::to_owned),
                    line: db_error.line(),
                    routine: db_error.routine().map(ToOwned::to_owned),
                };
            }
            _ => {}
        }

        let sqlstate = match error {
            Error::AuthenticationFailure { .. } | Error::NoUserSpecified => {
                SqlState::INVALID_PASSWORD
            }
            Error::DecodeError(_)
            | Error::EncodeError(_)
            | Error::IncorrectFormatCount(_)
            | Error::IoError(_) => SqlState::IO_ERROR,
            Error::InternalError(_) | Error::Unknown(_) => SqlState::INTERNAL_ERROR,
            Error::InvalidInteger(_) => SqlState::DATATYPE_MISMATCH,
            Error::MissingPortal(_) | Error::MissingPreparedStatement(_) => {
                SqlState::UNDEFINED_PSTATEMENT
            }
            Error::ParseError(_) => SqlState::INVALID_PSTATEMENT_DEFINITION,
            Error::Unimplemented(_)
            | Error::Unsupported(_)
            | Error::UnsupportedMessage(_)
            | Error::UnsupportedType(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::UnexpectedMessage(_) | Error::Scram(_) => SqlState::PROTOCOL_VIOLATION,
            Error::PostgresError(_) => SqlState::INTERNAL_ERROR,
            Error::InvalidAuthorizationSpecification(_) | Error::ConnectionClosed(_) => {
                unreachable!()
            }
        };

        BackendMessage::error(ErrorSeverity::Error, sqlstate, error.to_string())
    }
}
