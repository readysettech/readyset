//! Trait for interacting with an conensus system (Zookeeper, Consul, etcd) to determine
//! which Noria worker acts as the controller, which Noria workers exist, detecting failed
//! workers which necessitate changes, and storing cluster wide global state.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::{anyhow, Error};
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use url::Url;

mod consul;
mod local;
pub mod zk;
pub use self::consul::ConsulAuthority;
pub use self::local::{LocalAuthority, LocalAuthorityStore};
pub use self::zk::ZookeeperAuthority;
use crate::ControllerDescriptor;

// This should be an associated type on Authority but since Authority will only have one possible
// type inside of Noria, we are using a type alias here instead.
// If Authority ever moves out of Noria, it should become an associated type.
// LeaderPayload must be Serialize + DeserializeOwned + PartialEq
type LeaderPayload = ControllerDescriptor;

pub type VolumeId = String;
pub type WorkerId = String;
pub type AdapterId = String;

/// A response to a `worker_heartbeat`, to inform the worker of its
/// status within the system.
#[derive(Debug, PartialEq)]
pub enum AuthorityWorkerHeartbeatResponse {
    Alive,
    Failed,
}

/// The set of possible results to retrieving the new leader.
#[derive(Debug, PartialEq)]
pub enum GetLeaderResult {
    NewLeader(LeaderPayload),
    Unchanged,
    NoLeader,
}

/// Restriction for how domains containing a particular kind of node can be scheduled onto a
/// particular worker.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub enum NodeTypeSchedulingRestriction {
    /// No restrictions: all domains with or without this node type may be scheduled onto this
    /// worker
    None,
    /// Only domains containing this node type may be scheduled onto this worker
    OnlyWithNodeType,
    /// Domains containing this node type may never be scheduled onto this worker
    NeverWithNodeType,
}

impl Default for NodeTypeSchedulingRestriction {
    fn default() -> Self {
        Self::None
    }
}

/// Configuration for how domains should be scheduled onto a particular worker.
///
/// The [`Default`] value for this struct allows any domain to be scheduled onto any worker.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct WorkerSchedulingConfig {
    /// Identifier for the persistent volume associated with this worker, if any. This is used to
    /// make sure that once a domain with a particular base table is scheduled onto a worker, that
    /// domain will always be scheduled onto a worker with the same persistent volume.
    pub volume_id: Option<VolumeId>,
    /// Configuration for how domains containing or not containing reader nodes may be scheduled
    /// onto this worker
    pub reader_nodes: NodeTypeSchedulingRestriction,
}

/// Initial registration request body, sent from workers to controllers.
/// ///
/// (used for the `/worker_rx/register` route)
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WorkerDescriptor {
    /// URI at which the worker can be reached.
    pub worker_uri: Url,
    /// Socket address the worker listens on for data-plane read queries.
    pub reader_addr: SocketAddr,
    /// True if this worker may become a leader
    pub leader_eligible: bool,
    /// Configuration for how domains should be scheduled onto this worker
    pub domain_scheduling_config: WorkerSchedulingConfig,
}

pub trait UpdateInPlace<E, F, P>: Send + Sync
where
    F: FnMut(Option<&mut P>) -> Result<(), E>,
{
    /// An optimized method to update the controller state in place, the update is performed with
    /// the provided `f` closure.
    fn update_controller_in_place(&self, f: F) -> Result<(), E>;
}

#[async_trait]
#[enum_dispatch]
pub trait AuthorityControl: Send + Sync {
    /// If this [`AuthorityControl`] also implements the optimized [`UpdateInPlace`] trait, returns
    /// self as [`UpdateInPlace`], so caller can use optimized method instead.
    fn as_local<E, F, P>(&self) -> Option<&dyn UpdateInPlace<E, F, P>>
    where
        P: 'static,
        F: FnMut(Option<&mut P>) -> Result<(), E>,
    {
        None
    }

    /// Initializes the authority. This performs any initialization that the authority client
    /// needs to perform with the backend. This should be performed before any other
    /// calls are made to AuthorityControl functions.
    /// TODO(justin): Existing authorities should guarentee authority usage adheres to calling
    /// init() before other functionality.
    async fn init(&self) -> Result<(), Error>;

    /// Attempt to become leader with a specific payload. The payload should be something that can
    /// be deserialized to get the information on how to connect to the leader. If it is successful
    /// the this will return Some(payload), otherwise None and another instance has become leader.
    async fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error>;

    /// Voluntarily give up leadership, allowing another node to become leader. It is extremely
    /// important that the node calling this function is actually the leader.
    async fn surrender_leadership(&self) -> Result<(), Error>;

    /// Returns the payload for the current leader, blocking if there is not currently a leader.
    /// This method is intended for clients to determine the current leader.
    async fn get_leader(&self) -> Result<LeaderPayload, Error>;

    /// Non-blocking call to retrieve a change in the authorities leader state.
    async fn try_get_leader(&self) -> Result<GetLeaderResult, Error>;

    /// Whether the authority can place a watch that can unpark the thread when
    /// there is a change in authority state.
    fn can_watch(&self) -> bool;

    /// Waits for a change in the leader, and returns on a change. This should only
    /// be called if `can_watch` returns True.
    async fn watch_leader(&self) -> Result<(), Error>;

    /// Waits for a change in the workers, and returns on a change. This should only
    /// be called if `can_watch` returns True.
    async fn watch_workers(&self) -> Result<(), Error>;

    /// Do a non-blocking read at the indicated key.
    async fn try_read<P>(&self, path: &str) -> Result<Option<P>, Error>
    where
        P: DeserializeOwned;

    // Temporarily here to support arbitrary introspection into the authority. Will replace with
    // better functions later.
    async fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    async fn read_modify_write<F, P, E>(&self, path: &str, f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send;

    /// Register a worker with their descriptor. Returns a unique identifier that represents this
    /// worker if successful.
    async fn register_worker(&self, payload: WorkerDescriptor) -> Result<Option<WorkerId>, Error>
    where
        WorkerDescriptor: Serialize;

    /// Workers run this function on a regular cadence to confirm current state. Returns a response
    /// for next actions for this particular worker or if it should continue being a worker.
    async fn worker_heartbeat(
        &self,
        id: WorkerId,
    ) -> Result<AuthorityWorkerHeartbeatResponse, Error>;

    /// Retrieves the current set of workers from the authority.
    async fn get_workers(&self) -> Result<HashSet<WorkerId>, Error>;

    /// Retrieves the worker data for a set of workers.
    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> Result<HashMap<WorkerId, WorkerDescriptor>, Error>;

    /// Repeatedly attempts to do update the controller state. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    /// In addition some implementors of the trait may apply the method `u` to the stored copy of
    /// the value only.
    async fn update_controller_state<F, U, P: 'static, E>(
        &self,
        f: F,
        u: U,
    ) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        U: Send + FnMut(&mut P),
        P: Send + Serialize + DeserializeOwned + Clone,
        E: Send;

    /// Register an adapters http port.
    async fn register_adapter(&self, endpoint: SocketAddr) -> Result<Option<AdapterId>, Error>;

    /// Retrieves the current set of adapter endpoints from the authority.
    async fn get_adapters(&self) -> Result<HashSet<SocketAddr>, Error>;
}

/// Enum that dispatches calls to the `AuthorityControl` trait to
/// the respective variant.
#[allow(clippy::large_enum_variant)]
#[enum_dispatch(AuthorityControl)]
pub enum Authority {
    ZookeeperAuthority,
    ConsulAuthority,
    LocalAuthority,
}

/// Enum that mirrors Authority that parses command line arguments.
#[derive(Clone, Debug)]
pub enum AuthorityType {
    Zookeeper,
    Consul,
    Local,
}

impl FromStr for AuthorityType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "zookeeper" => Ok(AuthorityType::Zookeeper),
            "consul" => Ok(AuthorityType::Consul),
            "local" => Ok(AuthorityType::Local),
            other => Err(anyhow!("Invalid authority type: {}", other)),
        }
    }
}

impl ToString for AuthorityType {
    fn to_string(&self) -> String {
        match &self {
            AuthorityType::Zookeeper => "zookeeper".to_string(),
            AuthorityType::Consul => "consul".to_string(),
            AuthorityType::Local => "local".to_string(),
        }
    }
}

impl AuthorityType {
    pub async fn to_authority(&self, addr: &str, deployment: &str) -> Authority {
        match self {
            AuthorityType::Zookeeper => Authority::from(
                ZookeeperAuthority::new(&format!("{}/{}", &addr, &deployment))
                    .await
                    .unwrap(),
            ),
            AuthorityType::Consul => Authority::from(
                ConsulAuthority::new(&format!("http://{}/{}", &addr, &deployment)).unwrap(),
            ),
            AuthorityType::Local => Authority::from(LocalAuthority::new()),
        }
    }
}

/// A wrapper around a gzip compressor
pub(crate) struct Compressor(cloudflare_zlib::Deflate);

impl Compressor {
    fn new() -> Self {
        Compressor(
            cloudflare_zlib::Deflate::new(6, cloudflare_zlib::Z_DEFAULT_STRATEGY, 15)
                .expect("Can't fail with valid params"),
        )
    }

    pub(crate) fn compress(data: &[u8]) -> Vec<u8> {
        let mut comp = Self::new();
        comp.0.compress(data).expect("Can't fail");
        comp.0.finish().expect("Can't fail")
    }
}
