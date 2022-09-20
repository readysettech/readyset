//! This crate provides a reusable mechanism for reporting telemetry payloads to the
//! ReadySet Segment HTTP source endpoint.
//!
//! In the future, the plan is to extend this with support for things like background reporting,
//! more advanced API token validation, integration with `metrics`, etc.

mod error;

mod reporter;
pub use reporter::*;

mod sender;
pub use sender::*;

mod telemetry;
pub use telemetry::*;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

pub const TELMETRY_CHANNEL_LEN: usize = 1024;

pub struct TelemetryInitializer {}

impl TelemetryInitializer {
    /// Initializes a background task and returns a TelemetrySender handle
    pub fn init(disable_telemetry: bool, api_key: Option<String>) -> TelemetrySender {
        if disable_telemetry {
            return TelemetrySender::new_no_op();
        }

        match api_key {
            Some(api_key) => {
                let (tx, rx) = channel(TELMETRY_CHANNEL_LEN); // Arbitrary number of metrics to allow in queue before dropping them
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                let sender = TelemetrySender::new(tx, shutdown_tx);

                // If the reporter fails to initialize, sends will return errors, which can be
                // either be ignored (similar to no_op behavior) or handled at the time they are
                // sent.
                tokio::spawn(async move {
                    let mut reporter = TelemetryReporter::try_new(rx, Some(api_key), shutdown_rx)
                        .expect("failed to create reporter");
                    reporter.run().await;
                });

                sender
            }
            None => TelemetrySender::new_no_op(),
        }
    }
}
