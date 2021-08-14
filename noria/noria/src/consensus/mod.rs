//! Trait for interacting with an conensus system (Zookeeper, Consul, etcd) to determine
//! which Noria worker acts as the controller, which Noria workers exist, detecting failed
//! workers which necessitate changes, and storing cluster wide global state.

/// TODO: As of now, Authority does not handle all the things described above.
/// There are comments below showing example functions and documentation that are
/// a draft for where this trait will go.
use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod local;
mod zk;
use crate::ControllerDescriptor;

pub use self::local::{LocalAuthority, LocalAuthorityStore};
pub use self::zk::ZookeeperAuthority;

pub const CONTROLLER_KEY: &str = "/controller";
pub const STATE_KEY: &str = "/state";

// This should be an associated type on Authority but since Authority will only have one possible
// type inside of Noria, we are using a type alias here instead.
// If Authority ever moves out of Noria, it should become an associated type.
// LeaderPayload must be Serialize + DeserializeOwned + PartialEq
type LeaderPayload = ControllerDescriptor;

pub trait Authority: Send + Sync {
    /// Attempt to become leader with a specific payload. The payload should be something that can
    /// be deserialized to get the information on how to connect to the leader. If it is successful
    /// the this will return Some(payload), otherwise None and another instance has become leader.
    fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error>;

    /// Voluntarily give up leadership, allowing another node to become leader. It is extremely
    /// important that the node calling this function is actually the leader.
    fn surrender_leadership(&self) -> Result<(), Error>;

    /// Returns the payload for the current leader, blocking if there is not currently a leader.
    /// This method is intended for clients to determine the current leader.
    fn get_leader(&self) -> Result<LeaderPayload, Error>;
    /// Same as `get_leader` but return None if there is no leader instead of blocking.
    fn try_get_leader(&self) -> Result<Option<LeaderPayload>, Error>;

    /// Wait until a new leader has been elected, and then return the leader payload epoch or None
    /// if a new leader needs to be elected. This method enables a leader to watch to see if it has
    /// been overthrown.
    fn await_new_leader(&self) -> Result<Option<LeaderPayload>, Error>;

    /// Do a non-blocking read at the indicated key.
    fn try_read<P>(&self, path: &str) -> Result<Option<P>, Error>
    where
        P: DeserializeOwned;

    // Temporarily here to support arbitrary introspection into the authority. Will replace with
    // better functions later.
    fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    fn read_modify_write<F, P, E>(&self, path: &str, f: F) -> Result<Result<P, E>, Error>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned;

    // Currently all these APIs do not exist but are planned to.

    // type WorkerPayload;

    // Register a worker with a payload. Returns a unique identifier that represents this worker if successful.
    // Payload can be updated with update_worker_payload.
    //fn register_worker(&self, payload: WorkerPayload) -> Result<Option<ID>, Error>
    //where
    //    WorkerPayload: Serialize;

    // After registering as a worker, the authority will have an ID. This method returns that ID.
    //fn get_id(&self) -> Result<Option<ID>, Error>;

    // Unregister as a worker. This will surrender any leadership that this worker had.
    // (Replaces surrender_leadership)
    //fn unregister_worker(&self) -> Result<(), Error>;

    // Update worker payload.
    // Could be a read_modify_write but probably not needed.
    //fn update_worker_payload(&self, payload: WorkerPayload) -> Result<(), Error
    //where
    //    WorkerPayload: Serialize;

    // Workers run this function on a regular cadence to confirm current state. Returns a response
    // for next actions for this particular worker or if it should continue being a worker.
    //fn worker_heartbeat(&self) -> Result<AuthorityWorkerHeartbeatResponse, Error>;

    // Attempt to grab the lock to become leader. The status of this will be returned on the next
    // heartbeat. Will return an Err if this worker should not be attempting this.
    //fn lock_leader(&self) -> Result<(), Error>;

    // After all the steps of becoming a leader have completed, run this to announce leadership.
    // Will return an Err if this worker should not be attempting this. The status of this will be
    // returned on the next heartbeat
    //fn become_leader(&self, payload: LeaderPayload) -> Result<(), Error>

    // Run this instead of heartbeat if you are the leader to get updates about other workers and
    // to confirm this worker is still the leader.
    //fn leader_heartbeat(&self) -> Result<AuthorityLeaderHeartbeatResponse, Error>;
}

// Currently unimplemented draft types for the above Trait
//
// Newtype for the ID. This will generally be a UUID like string.
//struct ID(String)
//
// Whenever we heartbeat, we verify the current leadership state and depending on the result we should
// take different actions.
//enum AuthorityHeartbeatResponse<LeaderPayload> {
//    // Worker should remain a worker and the ID is of the leader.
//    Worker(LeaderPayload),
//    // There is currently no leader. This worker should wait for a new leader.
//    NoLeader,
//    // This particular worker should attempt to grab the leader lock.
//    ShouldGetLeaderLock,
//    // This worker has the leader lock and should start doing the work to become the leader.
//    HaveLeaderLock,
//    // This worker is has become the leader. Should instead call leader_heartbeat going forward.
//    Leader
//    // A new leader has been elected since last heartbeat, further heartbeats
//    // will return Worker(ID)
//    NewLeader(LeaderPayload)
//}

//enum WorkerUpdate<WorkerPayload> {
//     WorkerInsertOrUpdate(ID, WorkerPayload),
//     WorkerRemove(ID),
//}

//enum AuthorityLeaderHeartbeatResponse<WorkerPayload> {
//    // This contains some structure representing the workers that have changed.
//    WorkersUpdates(Vec<WorkerUpdates<WorkerPayload>>)
//    // No longer a leader and should go back to worker heartbeat.
//    LostLeader
//}
