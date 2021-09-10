//! Trait for interacting with an conensus system (Zookeeper, Consul, etcd) to determine
//! which Noria worker acts as the controller, which Noria workers exist, detecting failed
//! workers which necessitate changes, and storing cluster wide global state.

use anyhow::Error;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use url::Url;

mod consul;
mod local;
pub mod zk;
use crate::ControllerDescriptor;

pub use self::consul::ConsulAuthority;
pub use self::local::{LocalAuthority, LocalAuthorityStore};
pub use self::zk::ZookeeperAuthority;

// This should be an associated type on Authority but since Authority will only have one possible
// type inside of Noria, we are using a type alias here instead.
// If Authority ever moves out of Noria, it should become an associated type.
// LeaderPayload must be Serialize + DeserializeOwned + PartialEq
type LeaderPayload = ControllerDescriptor;

pub type VolumeId = String;
pub type WorkerId = String;

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

/// Initial registration request body, sent from workers to controllers.
/// ///
/// (used for the `/worker_rx/register` route)
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WorkerDescriptor {
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

#[async_trait]
#[enum_dispatch]
pub trait AuthorityControl: Send + Sync {
    /// Initializes the authority. This performs any initialization that the authority client
    /// needs to perform with the backend. This should be performed before any other
    /// calls are made to AuthorityControl functions.
    /// TODO(justin): Existing authorities should guarentee authority usage adheres to this.
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

    /// Non-blocking call to retrieve the leader.
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

    /// Register a worker with their descriptor. Returns a unique identifier that represents this worker if successful.
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
    async fn update_controller_state<F, P, E>(&self, f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send;
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
