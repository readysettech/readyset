use std::num::TryFromIntError;

use postgres_types::Type;
use thiserror::Error;

use crate::codec::{DecodeError, EncodeError};
use crate::message::FrontendMessage;

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
