use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use readyset_tracing::{debug, warn};
use readyset_util::shutdown::ShutdownSender;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use crate::error::{SenderError as Error, SenderResult as Result};
use crate::telemetry::{TelemetryBuilder, TelemetryEvent, *};

/// A struct that can be used to report payloads containing arbitrary telemetry data to the ReadySet
/// telemetry ingress.
#[derive(Debug, Clone)]
pub struct TelemetrySender {
    tx: Option<Sender<(TelemetryEvent, Telemetry)>>,
    shutdown_tx: Arc<Mutex<Option<ShutdownSender>>>,
    no_op: bool,
}

impl TelemetrySender {
    /// Construct a new [`TelemetrySender`]
    pub fn new(tx: Sender<(TelemetryEvent, Telemetry)>, shutdown_tx: ShutdownSender) -> Self {
        Self {
            tx: Some(tx),
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            no_op: false,
        }
    }

    /// Create a new "no-op" telemetry reporter.
    pub fn new_no_op() -> Self {
        Self {
            tx: None,
            shutdown_tx: Arc::new(Mutex::new(None)),
            no_op: true,
        }
    }

    /// Send a telemetry payload to Segment. If the initial request fails for a non-permanent
    /// reason (eg, not a 4XX or IO error), this function will retry with an exponential
    /// backoff, timing out at [`TIMEOUT`].
    pub fn send_event_with_payload(&self, event: TelemetryEvent, payload: Telemetry) -> Result<()> {
        debug!("sending {event:?} with payload {payload:?}");
        if self.no_op {
            debug!("Ignoring ({event:?} {payload:?}) in no-op mode");
            return Ok(());
        }

        match self.tx.as_ref() {
            Some(tx) => tx
                .try_send((event, payload))
                .map_err(|e| Error::Sender(e.to_string())),
            None => Err(Error::Sender("sender missing tx".into())),
        }
    }

    pub fn send_event(&self, event: TelemetryEvent) -> Result<()> {
        self.send_event_with_payload(event, TelemetryBuilder::new().build())
    }

    /// Any event sent after shutdown() is sent will fail
    /// Waits until `timeout` for the TelemetryReporter to ack shutdown completion.
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        match self.shutdown_tx.lock().await.deref_mut().take() {
            Some(shutdown_tx) => tokio::time::timeout(timeout, shutdown_tx.shutdown())
                .await
                .map_err(|_| Error::Sender("graceful shutdown timeout".to_string())),
            None => {
                warn!("graceful shutdown not possible, no shutdown_tx found");
                Ok(())
            }
        }
    }
}
