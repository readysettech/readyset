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
}

impl<R> From<Error> for BackendMessage<R> {
    fn from(error: Error) -> Self {
        let sqlstate = match error {
            Error::AuthenticationFailure { .. } => SqlState::INVALID_PASSWORD,
            Error::NoUserSpecified => SqlState::INVALID_PASSWORD,
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
            Error::UnexpectedMessage(_) => SqlState::PROTOCOL_VIOLATION,
            Error::Unknown(_) => SqlState::INTERNAL_ERROR,
            Error::Unsupported(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::UnsupportedMessage(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::UnsupportedType(_) => SqlState::FEATURE_NOT_SUPPORTED,
            Error::Scram(_) => SqlState::PROTOCOL_VIOLATION,
            Error::PostgresError(ref e) => {
                if let Some(db_error) = e.as_db_error() {
                    return BackendMessage::ErrorResponse {
                        severity: match db_error.severity() {
                            "ERROR" => ErrorSeverity::Error,
                            "FATAL" => ErrorSeverity::Fatal,
                            "PANIC" => ErrorSeverity::Panic,
                            _ => ErrorSeverity::Error,
                        },
                        sqlstate: db_error.code().clone(),
                        message: db_error.message().into(),
                        detail: db_error.detail().map(|s| s.to_owned()),
                        hint: db_error.hint().map(|s| s.to_owned()),
                        position: db_error.position().cloned(),
                        where_: db_error.where_().map(|s| s.to_owned()),
                        schema: db_error.schema().map(|s| s.to_owned()),
                        table: db_error.table().map(|s| s.to_owned()),
                        column: db_error.column().map(|s| s.to_owned()),
                        datatype: db_error.datatype().map(|s| s.to_owned()),
                        constraint: db_error.constraint().map(|s| s.to_owned()),
                        file: db_error.file().map(|s| s.to_owned()),
                        line: db_error.line(),
                        routine: db_error.routine().map(|s| s.to_owned()),
                    };
                } else {
                    SqlState::INTERNAL_ERROR
                }
            }
        };

        BackendMessage::ErrorResponse {
            severity: ErrorSeverity::Error,
            sqlstate,
            message: error.to_string(),
            detail: None,
            hint: None,
            position: None,
            where_: None,
            schema: None,
            table: None,
            column: None,
            datatype: None,
            constraint: None,
            file: None,
            line: None,
            routine: None,
        }
    }
}
