use opentelemetry::trace::TraceError;
use tracing_subscriber::filter::ParseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("trace error: {0}")]
    Trace(#[from] TraceError),
    #[error("failed to parse filter: {0}")]
    Parse(#[from] ParseError),
}
