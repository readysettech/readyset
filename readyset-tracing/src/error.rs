use opentelemetry::trace::TraceError;
use tracing_subscriber::filter::ParseError;

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
}
