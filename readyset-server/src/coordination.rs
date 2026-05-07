use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Response to `WorkerRequestKind::RunDomain`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunDomainResponse {
    /// The address used by other domains to talk to the newly booted domain.
    pub(crate) external_addr: SocketAddr,
}
