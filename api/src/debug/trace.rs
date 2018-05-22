use channel;
use std::time;

#[doc(hidden)]
pub type Tracer = Option<(u64, Option<channel::TraceSender<Event>>)>;

/// Different events that can occur as a packet is being processed.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum PacketEvent {
    /// The packet has been pulled off the input channel.
    ExitInputChannel,
    /// The packet has been received by some domain, and is being handled.
    Handle,
    /// The packet is being processed at some node.
    Process,
    /// The packet has reached some reader node.
    ReachedReader,
    /// The packet has been merged with another, and will no longer trigger events.
    Merged(u64),
}

/// Events that can occur
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    /// The event relates to the processing of a specific packet.
    PacketEvent(PacketEvent, u64),
}

/// Sent along the debug channel to indicate that some notable event has occurred.
#[derive(Serialize, Deserialize)]
pub struct Event {
    /// The time when this event happened, or the last time this event was deserialized.
    #[serde(skip)]
    #[serde(default = "time::Instant::now")]
    pub instant: time::Instant,
    /// What the event was.
    pub event: EventType,
}
