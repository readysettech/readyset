use std::time;
use flow::payload::PacketEvent;

/// Events that can occur
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum DebugEventType {
    /// The event relates to the processing of a specific packet.
    PacketEvent(PacketEvent, u64),
}

/// Sent along the debug channel to indicate that some notable event has occurred.
pub struct DebugEvent {
    /// The time when this event happened.
    pub instant: time::Instant,
    /// What the event was.
    pub event: DebugEventType,
}
