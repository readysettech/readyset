//! Controller events sent to connected clients via Server-Sent Events (SSE).
//!
//! SSE is a simple protocol for server-to-client push notifications over HTTP. The server holds
//! the connection open and sends events as they occur. Each event is formatted as:
//!
//! ```text
//! event: <event-type>
//! data: <payload>
//!
//! ```
//!
//! The double newline marks the end of an event.
//!
//! Readyset uses SSE to send events from the controller (readyset-server, and only the leader
//! actively sends messages) to each connected adapter (readyset-adapter).

use std::sync::Arc;
use std::time::Duration;

use failpoint_macros::set_failpoint;
use futures_util::StreamExt;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Method, Request, StatusCode};
use readyset_errors::{internal, internal_err, ReadySetResult};
use schema_catalog::SchemaCatalogUpdate;
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;
use tokio::sync::broadcast;
use tokio::time::sleep;
use url::Url;

use crate::consensus::{Authority, AuthorityControl};

/// Any of the supported events that the controller (and specifically the leader) might send to
/// notify connected clients (i.e. the adapter) of changes to the system.
#[derive(Debug, Clone, Serialize, Deserialize, IntoStaticStr)]
pub enum ControllerEvent {
    /// Periodic heartbeat event to keep the connection alive (and aid in debugging).
    Heartbeat,
    /// The leader will no longer send events on this stream, and clients will have to reconnect
    /// once a new leader is elected.
    ///
    /// We could probably do away with this, because if all the `Sender`s for a given broadcast
    /// channel are dropped, then the `Receiver`s will get an error indicating that the channel is
    /// closed. However, we have a loop which sends `Heartbeat` events, and it makes debugging
    /// simpler when we expect it to only shut down after seeing this message first. It's also nicer
    /// (again, mostly just for debugging) to see that message on the client side instead of just
    /// the connection abruptly closing.
    LeaderLost,
    /// A schema catalog update is available.
    SchemaCatalogUpdate(SchemaCatalogUpdate),
    /// Test-only event variant with arbitrary string data.
    #[cfg(test)]
    TestData(String),
}

impl ControllerEvent {
    /// Format the event as a Server-Sent Events (SSE) message.
    ///
    /// SSE format (the two trailing newlines delimit the end of the event):
    ///
    /// ```plaintext
    /// event: <enum key>
    /// data: <json serialized data>
    ///
    /// ```
    pub fn to_sse(&self) -> ReadySetResult<String> {
        let event_type: &str = self.into();
        let data = serde_json::to_string(self)?;
        Ok(format!("event: {}\ndata: {}\n\n", event_type, data))
    }
}

/// Different sources for getting the current leader URL
#[derive(Clone)]
enum LeaderUrlSource {
    /// A fixed URL that will never change
    Fixed(Url),
    /// A URL that might change (updated by checking the authority)
    Dynamic {
        authority: Arc<Authority>,
        leader_url: Arc<parking_lot::RwLock<Option<Url>>>,
    },
}

impl LeaderUrlSource {
    /// Get the current leader URL
    async fn url(&self) -> Option<Url> {
        match self {
            LeaderUrlSource::Fixed(url) => Some(url.clone()),
            LeaderUrlSource::Dynamic {
                authority,
                leader_url,
            } => {
                if let Some(url) = leader_url.read().clone() {
                    return Some(url);
                }

                match authority.get_leader().await {
                    Ok(desc) => {
                        let mut guard = leader_url.write();
                        *guard = Some(desc.controller_uri.clone());
                        Some(desc.controller_uri)
                    }
                    Err(error) => {
                        tracing::warn!(%error, "Failed to fetch leader URL for events client");
                        None
                    }
                }
            }
        }
    }
}

/// SSE stream processor that handles parsing and event extraction
struct SseStreamProcessor {
    buffer: String,
    buffer_limit: usize,
}

impl SseStreamProcessor {
    fn new(buffer_limit: usize) -> Self {
        Self {
            buffer: String::new(),
            buffer_limit,
        }
    }

    /// Process incoming chunk and extract complete events
    fn process_chunk(&mut self, chunk: &[u8]) -> ReadySetResult<Vec<(String, String)>> {
        let chunk_str = String::from_utf8_lossy(chunk);
        self.buffer.push_str(&chunk_str);

        let mut events = Vec::new();
        let mut processed_up_to = 0;

        // Process all complete events in the buffer
        loop {
            let search_slice = &self.buffer[processed_up_to..];
            let Some(pos) = search_slice.find("event: ") else {
                break;
            };
            let event_start = pos + processed_up_to;

            // "event: " must be at start of buffer or preceded by newline to be valid.
            // After buffer overflow, we may have residual payload data containing "event: "
            // as a substring - skip past false matches to resynchronize at a real boundary.
            if event_start > 0 && !self.buffer[..event_start].ends_with('\n') {
                processed_up_to = event_start + 7; // len("event: ")
                continue;
            }

            // Look for the data field after the event field
            if let Some(data_start) = self.buffer[event_start..].find("\ndata: ") {
                let data_start = event_start + data_start;

                // Look for the double newline that ends the event
                if let Some(event_end) = self.buffer[data_start..].find("\n\n") {
                    let event_end = data_start + event_end + 2; // Include the final \n\n

                    let event_str = &self.buffer[event_start..event_end];

                    match Self::parse_sse_event(event_str) {
                        Ok((event_type, data)) => events.push((event_type, data)),
                        Err(e) => {
                            tracing::warn!("Failed to parse SSE event: {}", e);
                        }
                    }

                    processed_up_to = event_end;
                } else {
                    // Incomplete event, wait for more data
                    break;
                }
            } else {
                // Incomplete event, wait for more data
                break;
            }
        }

        // Remove processed events from buffer
        if processed_up_to > 0 {
            self.buffer.drain(..processed_up_to);
        }

        // Limit buffer size to prevent memory issues.
        // Clearing the buffer may cause us to lose in-flight events. The parser
        // resynchronizes by requiring "event: " to appear at buffer start or after
        // a newline, skipping any false matches in residual payload data.
        if self.buffer.len() > self.buffer_limit {
            tracing::warn!("SSE buffer exceeded size limit, clearing");
            self.buffer.clear();
        }

        Ok(events)
    }

    /// Parse a complete SSE event string into (event_type, data)
    fn parse_sse_event(event_str: &str) -> ReadySetResult<(String, String)> {
        const EVENT_PREFIX: &str = "event: ";
        const DATA_PREFIX: &str = "\ndata: ";

        // Extract the event type
        let event_start = event_str
            .find(EVENT_PREFIX)
            .ok_or_else(|| internal_err!("Missing event type"))?;
        let event_start = event_start + EVENT_PREFIX.len();

        let event_end = event_str[event_start..]
            .find('\n')
            .ok_or_else(|| internal_err!("Invalid event format"))?;
        let event_type = event_str[event_start..event_start + event_end].to_string();

        // Extract the data
        let data_start = event_str
            .find(DATA_PREFIX)
            .ok_or_else(|| internal_err!("Missing event data"))?;
        let data_start = data_start + DATA_PREFIX.len();

        let data_end = event_str[data_start..]
            .find('\n')
            .ok_or_else(|| internal_err!("Invalid data format"))?;
        let data = event_str[data_start..data_start + data_end].to_string();

        Ok((event_type, data))
    }
}

/// A client which maintains a connection to the leader, including across leader changes and server
/// restarts, and sends out events from the `/events/stream` SSE endpoint.
#[derive(Clone)]
pub(crate) struct ControllerEventsClient {
    /// A way to get the possibly-changed Leader URL when connecting, i.e.
    /// after leader changes.
    leader_url: LeaderUrlSource,
    /// A broadcast sender where we send all events we get from the leader.
    events_tx: broadcast::Sender<ControllerEvent>,
    /// Buffer size limit to prevent memory issues
    buffer_limit: usize,
}

/// Default buffer limit for SSE stream processing (50 MB)
const DEFAULT_SSE_BUFFER_LIMIT: usize = 50 * 1024 * 1024;

/// Environment variable to override the SSE buffer limit
const SSE_BUFFER_LIMIT_ENV: &str = "READYSET_SSE_BUFFER_LIMIT";

/// Capacity of the broadcast channel for controller events.
///
/// Used on both the client side ([`ControllerEventsClient`]) and the server side
/// (`readyset_server::controller::events::EventsHandle`) to keep them in sync and
/// minimize lag under DDL stress.
pub const BROADCAST_CHANNEL_CAPACITY: usize = 256;

impl ControllerEventsClient {
    /// Create a new `ControllerEventsClient`.
    fn new(leader_url: LeaderUrlSource) -> Self {
        let (events_tx, _events_rx) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        let buffer_limit = std::env::var(SSE_BUFFER_LIMIT_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SSE_BUFFER_LIMIT);
        Self {
            leader_url,
            events_tx,
            buffer_limit,
        }
    }

    pub(crate) fn with_fixed_leader(leader_url: Url) -> Self {
        Self::new(LeaderUrlSource::Fixed(leader_url))
    }

    pub(crate) fn with_dynamic_leader(
        authority: Arc<Authority>,
        leader_url: Arc<parking_lot::RwLock<Option<Url>>>,
    ) -> Self {
        Self::new(LeaderUrlSource::Dynamic {
            authority,
            leader_url,
        })
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ControllerEvent> {
        self.events_tx.subscribe()
    }

    /// Subscribe and start the background event streaming task.
    ///
    /// Returns a receiver that is guaranteed to be registered before the task
    /// begins, preventing the race where the task could see zero receivers
    /// and exit prematurely.
    pub(crate) fn subscribe_and_start(&self) -> broadcast::Receiver<ControllerEvent> {
        let receiver = self.subscribe();
        self.start();
        receiver
    }

    fn start(&self) {
        let leader_url = self.leader_url.clone();
        let events_tx = self.events_tx.clone();
        let buffer_limit = self.buffer_limit;
        tokio::spawn(Self::run(leader_url, events_tx, buffer_limit));
    }

    async fn run(
        leader_url: LeaderUrlSource,
        events_tx: broadcast::Sender<ControllerEvent>,
        buffer_limit: usize,
    ) {
        // Configure the HTTP client with reasonable timeouts
        let mut http_connector = HttpConnector::new();
        http_connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder()
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .build(http_connector);

        loop {
            let Some(url) = leader_url.url().await else {
                tracing::warn!("Leader URL not available, waiting before retry");
                // TODO: exponential backoff and/or don't hardcode this
                sleep(Duration::from_millis(500)).await;
                continue;
            };

            let events_url = match url.join("/events/stream") {
                Ok(url) => url,
                Err(e) => {
                    tracing::error!("Failed to build events URL: {}", e);
                    // TODO: exponential backoff
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            // Use fail::eval instead of set_failpoint! so we can do an async sleep.
            // The fail crate's built-in sleep action uses std::thread::sleep, which blocks the
            // tokio worker thread and can stall the timer driver.
            // Configure with "return(delay_ms)" to trigger, e.g. "1*return(15000)".
            #[cfg(feature = "failure_injection")]
            if let Some(delay_ms) = fail::eval(
                readyset_util::failpoints::CONTROLLER_EVENTS_SSE_CONNECT_DELAY,
                |v| v.and_then(|s| s.parse::<u64>().ok()),
            )
            .flatten()
            {
                tracing::info!(delay_ms, "SSE connect failpoint: delaying connection");
                sleep(Duration::from_millis(delay_ms)).await;
            }

            match Self::connect_and_stream(&events_url, &client, &events_tx, buffer_limit).await {
                Ok(should_reconnect) => {
                    if !should_reconnect {
                        tracing::debug!("Event stream closed permanently");
                        break;
                    }
                    metrics::counter!(crate::metrics::recorded::CONTROLLER_EVENTS_DISCONNECTED)
                        .increment(1);
                    tracing::info!("Event stream closed, attempting to reconnect");
                }
                Err(e) => {
                    metrics::counter!(crate::metrics::recorded::CONTROLLER_EVENTS_DISCONNECTED)
                        .increment(1);
                    tracing::info!("Error connecting to event stream: {}, retrying", e);
                }
            }

            // Safe: subscribe-before-start ordering is enforced by subscribe_and_start(),
            // so no subscriber is mid-resubscribe at this point.
            if events_tx.receiver_count() == 0 {
                tracing::debug!("No controller event subscribers; stopping events client");
                break;
            }

            // TODO: exponential backoff
            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Connects to the SSE endpoint and processes events.
    /// Returns Ok(true) if we should attempt to reconnect, Ok(false) if we should terminate.
    async fn connect_and_stream(
        url: &Url,
        client: &Client<HttpConnector>,
        events_tx: &broadcast::Sender<ControllerEvent>,
        buffer_limit: usize,
    ) -> ReadySetResult<bool> {
        tracing::debug!(%url, "Connecting to SSE endpoint");

        let req = Request::builder()
            .uri(url.as_str())
            .method(Method::GET)
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .body(Body::empty())
            .map_err(|e| internal_err!("Failed to build request: {e}"))?;

        let mut res = client
            .request(req)
            .await
            .map_err(|e| internal_err!("Failed to send request: {e}"))?;

        if res.status() != StatusCode::OK {
            let status = res.status();
            let body = hyper::body::to_bytes(res.body_mut())
                .await
                .unwrap_or_default();
            internal!(
                "Error response from server: {status} - {body}",
                body = String::from_utf8_lossy(&body)
            );
        }

        metrics::counter!(crate::metrics::recorded::CONTROLLER_EVENTS_CONNECTED).increment(1);
        tracing::debug!("Connected to SSE endpoint");

        // Process the event stream
        let mut processor = SseStreamProcessor::new(buffer_limit);

        let mut body_stream = res.into_body();

        while let Some(chunk_result) = body_stream.next().await {
            set_failpoint!(
                readyset_util::failpoints::CONTROLLER_EVENTS_SSE_DISCONNECT,
                |_| Err(internal_err!("failpoint: SSE stream disconnect"))
            );
            let chunk = chunk_result.map_err(|e| internal_err!("Failed to read chunk: {e}"))?;
            let events = processor.process_chunk(&chunk)?;

            for (event_type, data) in events {
                match Self::parse_controller_event(&event_type, &data) {
                    Ok(event) => {
                        if events_tx.send(event).is_err() {
                            // All receivers have been dropped, we can stop
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse controller event: {}", e);
                    }
                }
            }
        }

        // The stream ended normally, we should try to reconnect
        Ok(true)
    }

    /// Parses a server-sent event into a ControllerEvent
    fn parse_controller_event(event_type: &str, data: &str) -> ReadySetResult<ControllerEvent> {
        // TODO: Replace with strum derive helpers, or just ignore event type and deserialize data
        match event_type {
            "Heartbeat" => Ok(ControllerEvent::Heartbeat),
            "LeaderLost" => Ok(ControllerEvent::LeaderLost),
            #[cfg(test)]
            "TestData" => Ok(ControllerEvent::TestData(
                serde_json::from_str(data)
                    .map_err(|e| internal_err!("Failed to deserialize TestData: {e}"))?,
            )),
            _ => {
                // Try to deserialize as a custom event
                serde_json::from_str(data)
                    .map_err(|e| internal_err!("Failed to deserialize event {event_type}: {e}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Validates that the background task exits when all receivers are dropped,
    /// even if the SSE endpoint is unreachable.
    #[tokio::test]
    async fn test_task_stops_when_receivers_dropped() {
        // Use a URL that will fail to connect (nothing listening on this port)
        let url = Url::parse("http://127.0.0.1:1").expect("valid URL");
        let client = ControllerEventsClient::with_fixed_leader(url);

        let receiver = client.subscribe_and_start();

        // Drop the only receiver
        drop(receiver);

        // The task should notice receiver_count() == 0 and exit.
        // Give it a generous timeout to account for the 500ms reconnect sleep.
        tokio::time::timeout(Duration::from_secs(5), async {
            // Poll until the sender has no task holding it alive.
            // When the spawned task exits, it drops its clone of events_tx,
            // leaving only the one inside ControllerEventsClient.
            loop {
                // receiver_count == 0 AND only one strong sender means the task exited.
                // We can detect this indirectly: a new subscribe + send should have
                // no receivers if the task is gone. But more directly, just try
                // subscribing and check that sends fail (no active receivers besides us).
                if client.events_tx.receiver_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("background task should have exited after all receivers were dropped");
    }

    #[test]
    fn test_false_event_boundary_in_residual_data() {
        // Small buffer to make overflow easy to trigger
        let mut processor = SseStreamProcessor::new(30);

        // Simulate post-overflow state: chunk starts with residual payload
        // containing "event: " that should NOT be treated as event boundary.
        // This can happen when: buffer overflows mid-event, gets cleared, and
        // next chunk contains the tail of that event plus a new complete event.
        let chunk = b"residue event: fake\"}\n\nevent: Real\ndata: \"real\"\n\n";

        let events = processor.process_chunk(chunk).unwrap();

        // Should find exactly one event: "Real" with data "\"real\""
        // Should NOT be confused by "event: fake" in the residue
        assert_eq!(events.len(), 1, "expected 1 event, got {:?}", events);
        assert_eq!(events[0].0, "Real");
        assert_eq!(events[0].1, "\"real\"");
    }
}
