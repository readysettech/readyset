use thiserror::Error;

/// Errors that can happen when attempting to access the parts of a reader map
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// No publish has occurred yet, so it's impossible to obtain a read handle to the map
    #[error("The map has not been published yet")]
    NotPublished,

    /// The map has been destroyed, so it's impossible to obtain a write or read handle
    #[error("The map has been destroyed")]
    Destroyed,
}

/// Result type alias for reader map operations
pub type Result<T> = std::result::Result<T, Error>;
