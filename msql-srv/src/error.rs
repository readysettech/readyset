//! Msql specific error codes

use std::io;

use thiserror::Error;
/// Enum of errors returned from msql-srv
#[derive(Debug, Error)]
pub enum MsqlSrvError {
    /// Error returned by msql TryFrom implementations.
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
}

impl From<MsqlSrvError> for io::Error {
    fn from(e: MsqlSrvError) -> io::Error {
        match e {
            MsqlSrvError::IoError(e) => e,
            _ => std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)),
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
        OtherErrorKind::QueryResultWriterErr => {
            io::Error::new(io::ErrorKind::Other, "Failed to get QueryResultWriter")
        }
        OtherErrorKind::PacketWriterErr => {
            io::Error::new(io::ErrorKind::Other, "Failed to get PacketWriter")
        }
        OtherErrorKind::IndexErr {
            data,
            index,
            length,
        } => io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Failed to index into {} (attemped index: {}, length: {})",
                data, index, length
            ),
        ),
        OtherErrorKind::Unexpected { error } => io::Error::new(io::ErrorKind::Other, error),
        OtherErrorKind::AuthDataErr => {
            io::Error::new(io::ErrorKind::Other, "Error generating auth data")
        }
        OtherErrorKind::GenericErr { error } => io::Error::new(io::ErrorKind::Other, error),
    }
}
