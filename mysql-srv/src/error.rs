//! Msql specific error codes

use std::io;

use thiserror::Error;
/// Enum of errors returned from mysql-srv
#[derive(Debug, Error)]
pub enum MsqlSrvError {
    /// Error returned by mysql TryFrom implementations.
    #[error("Value conversion error: invalid type conversion from {src_type} to {target_type}")]
    InvalidConversion {
        /// The type that TryFrom attempts to convert the value into.
        target_type: String,
        /// The original type of the value to be converted.
        src_type: String,
    },
    /// Error generated when converting [u8] to str.
    #[error("Failed to convert bytes to valid UTF8 str")]
    Utf8Error(#[from] std::str::Utf8Error),
    /// Error returned when attempting to create a Date from a [u8] of
    /// incorrect size.
    #[error("Length of Date vector is not valid")]
    InvalidDate,
    /// Error returned when attempting to create a Datetime from a [u8]
    /// of incorrect size.
    #[error("Length of Datetime vector is not valid")]
    InvalidDatetime,
    /// Error returned when attempting to create a Duration from a [u8]
    /// of incorrect size.
    #[error("Length of Duration vector is not valid")]
    InvalidDuration,
    /// Generic error type for i/o errors
    #[error("IoError")]
    IoError(#[from] std::io::Error),
    /// Error returned when an edge case is reached that we do not handle. Used
    /// to replace unimplemented!()
    #[error("Not implemented: {operation}")]
    Unimplemented {
        /// The unimplemented case
        operation: String,
    },
    /// Used to propagate errors from getrandom(). Indicates that the
    /// generation of random data for the auth challenge failed.
    #[error("Failed to generate random bytes for auth challenge data")]
    GetRandomError,
    /// Error returned when attempting to read from an invalid index.
    #[error("Failed to retrieve data from index")]
    IndexingError,
    /// Error returned when a case is unexpectedly reached.
    #[error("This condition was reached unexpectedly and cannot be handled")]
    UnreachableError,
    /// Error from mysql_common indicating that the column type is unknown.
    #[error("Unknown column type")]
    UnknownColumnType(#[from] myc::constants::UnknownColumnType),
    /// Failed to decrypt client password during RSA-based authentication.
    #[error("Failed to decrypt client password: {0}")]
    DecryptionError(String),
    /// Failed to create or convert an RSA private key.
    #[error("RSA key creation failed: {0}")]
    KeyCreationError(String),
    /// Failed to encode a key to PEM format or convert PEM bytes to UTF-8.
    #[error("Key encoding failed: {0}")]
    EncodingError(String),
    /// `AuthKeys::initialize()` was called more than once.
    #[error("Auth keys already initialized")]
    KeyAlreadyInitialized,
    /// An RSA key file on disk could not be read or parsed.
    #[error("Failed to load RSA key from file: {0}")]
    KeyLoadError(String),
}

impl From<MsqlSrvError> for io::Error {
    fn from(e: MsqlSrvError) -> io::Error {
        match e {
            MsqlSrvError::IoError(e) => e,
            _ => std::io::Error::other(format!("{e:?}")),
        }
    }
}

/// io::ErrorKind::Other types
pub enum OtherErrorKind {
    /// Error getting QueryResultWriter
    QueryResultWriterErr,
    /// Error getting PacketWriter
    PacketWriterErr,
    /// Error indexing into data
    IndexErr {
        /// The data being indexed into
        data: String,
        /// The value used for indexing
        index: usize,
        /// The length of the data
        length: usize,
    },
    /// Unexpected case reached
    Unexpected {
        /// Error string to be printed
        error: String,
    },
    /// Returned when generate_auth_data() returns an error
    AuthDataErr,
    /// Generic error type
    GenericErr {
        /// Error string to be printed
        error: String,
    },
}

/// Generate an io::ErrorKind::Other
pub fn other_error(err_kind: OtherErrorKind) -> io::Error {
    match err_kind {
        OtherErrorKind::QueryResultWriterErr => io::Error::other("Failed to get QueryResultWriter"),
        OtherErrorKind::PacketWriterErr => io::Error::other("Failed to get PacketWriter"),
        OtherErrorKind::IndexErr {
            data,
            index,
            length,
        } => io::Error::other(format!(
            "Failed to index into {data} (attempted index: {index}, length: {length})"
        )),
        OtherErrorKind::Unexpected { error } => io::Error::other(error),
        OtherErrorKind::AuthDataErr => io::Error::other("Error generating auth data"),
        OtherErrorKind::GenericErr { error } => io::Error::other(error),
    }
}
