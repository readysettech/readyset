use opentelemetry_sdk::trace::TraceError;
use tracing_subscriber::{filter::ParseError, reload};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("trace error: {0}")]
    Trace(#[from] TraceError),
    #[error("failed to parse filter: {0}")]
    Parse(#[from] ParseError),
    #[error("invalid rotation cadence: {0}")]
    InvalidRotationCadence(String),
    #[error("could not open file for appending: {0}")]
    CouldNotOpenFile(#[from] std::io::Error),
    #[error("log level reload handle not initialized")]
    ReloadHandleNotInitialized,
    #[error("could not set log level: {0}")]
    CouldNotSetLogLevel(#[from] reload::Error),
}
