use reqwest::header::InvalidHeaderValue;
use reqwest::StatusCode;
use thiserror::Error;
use tokio::time::error::Elapsed;

/// Errors that can occur when reporting telemetry payloads
#[derive(Debug, Error)]
pub enum ReporterError {
    #[error("Invalid API key: {0}")]
    InvalidAPIKeyHeader(InvalidHeaderValue),

    #[error("Error received from server when sending telemetry payload: {0}")]
    Server(String),

    #[error("Invalid API key")]
    Unauthorized,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("Error sending telemetry payload (status {status}): {body}")]
    HTTPError { status: StatusCode, body: String },

    #[error("Request timed out")]
    Timeout(#[from] Elapsed),

    #[error("Error serializing telemetry payload: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Error making client: {0}")]
    Client(String),
}

/// Result type alias for the telemetry reporter
pub type ReporterResult<T> = std::result::Result<T, ReporterError>;

/// Errors that can occur when sending telemetry payloads from the sender to the reporter

#[derive(Debug, Error)]
pub enum SenderError {
    #[error("Error sending telemetry event {0}")]
    Sender(String),
}

/// Result type alias for the telemetry reporter
pub type SenderResult<T> = std::result::Result<T, SenderError>;
