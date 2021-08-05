use std::net::SocketAddr;
use url::Url;

use crate::VolumeId;
use dataflow::prelude::*;
use noria::consensus::Epoch;
pub use noria::util::do_noria_rpc;

/// Initial registration request body, sent from workers to controllers.
///
/// (used for the `/worker_rx/register` route)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterPayload {
    /// What the worker thinks the current epoch is.
    pub epoch: Epoch,
    /// URI at which the worker can be reached.
    pub worker_uri: Url,
    /// Socket address the worker listens on for data-plane read queries.
    pub reader_addr: SocketAddr,
    /// The region the worker is located in.
    pub region: Option<String>,
    /// Whether or not this worker is used only to hold reader domains.
    pub reader_only: bool,
    /// The volume associated with this server.
    pub volume_id: Option<VolumeId>,
}

/// Worker heartbeat request body, sent from workers to controllers.
///
/// (used for the `/worker_rx/heartbeat` route)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HeartbeatPayload {
    /// What the worker thinks the current epoch is.
    pub epoch: Epoch,
    /// URI at which the worker can be reached.
    pub worker_uri: Url,
}

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
