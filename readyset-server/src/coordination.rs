use std::net::SocketAddr;

use dataflow::prelude::*;
use serde::{Deserialize, Serialize};

/// Response to `WorkerRequestKind::RunDomain`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunDomainResponse {
    /// The address used by other domains to talk to the newly booted domain.
    pub(crate) external_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct DomainDescriptor {
    replica_address: ReplicaAddress,
    socket_address: SocketAddr,
}

impl DomainDescriptor {
    pub fn new(replica_address: ReplicaAddress, socket_address: SocketAddr) -> Self {
        DomainDescriptor {
            replica_address,
            socket_address,
        }
    }

    pub fn socket_address(&self) -> SocketAddr {
        self.socket_address
    }

    pub fn replica_address(&self) -> ReplicaAddress {
        self.replica_address
    }

    pub fn domain_index(&self) -> DomainIndex {
        self.replica_address().domain_index
    }

    pub fn shard(&self) -> usize {
        self.replica_address().shard
    }
}
