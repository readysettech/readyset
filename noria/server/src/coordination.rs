use dataflow::prelude::*;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Response to `WorkerRequestKind::RunDomain`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunDomainResponse {
    /// The address used by other domains to talk to the newly booted domain.
    pub(crate) external_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct DomainDescriptor {
    id: DomainIndex,
    shard: usize,
    addr: SocketAddr,
}

impl DomainDescriptor {
    pub fn new(id: DomainIndex, shard: usize, addr: SocketAddr) -> Self {
        DomainDescriptor { id, shard, addr }
    }

    pub fn domain(&self) -> DomainIndex {
        self.id
    }

    pub fn shard(&self) -> usize {
        self.shard
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
