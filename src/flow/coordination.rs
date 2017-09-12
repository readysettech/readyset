use std::net::SocketAddr;

use flow::domain::{self, DomainBuilder};

/// Coordination-layer message wrapper; adds a mandatory `source` field to each message.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CoordinationMessage {
    /// The worker's `SocketAddr` from which this message was sent.
    pub source: SocketAddr,
    /// Message payload.
    pub payload: CoordinationPayload,
}

/// Coordination-layer message payloads.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CoordinationPayload {
    /// Register a new worker.
    Register {
        /// Address of the worker.
        addr: SocketAddr,
        /// Address the worker will be listening on to serve reads.
        read_listen_addr: SocketAddr,
    },
    /// Worker going offline.
    Deregister,
    /// Worker is still alive.
    Heartbeat,
    /// Assign a new domain for a worker to run.
    AssignDomain(DomainBuilder),
    /// Remove a running domain from a worker.
    RemoveDomain,
    /// Domain connectivity gossip.
    DomainBooted((domain::Index, usize), SocketAddr),
}
