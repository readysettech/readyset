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

use readyset_errors::ReadySetResult;
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

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
