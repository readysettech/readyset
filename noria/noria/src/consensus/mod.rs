//! Code for interacting with ZooKeeper to determine which Noria worker acts as the controller, and
//! for detecting failed controllers which necessitate a controller changeover.

use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod local;
mod zk;
pub use self::local::LocalAuthority;
pub use self::zk::ZookeeperAuthority;

pub const CONTROLLER_KEY: &str = "/controller";
pub const STATE_KEY: &str = "/state";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(i64);

pub enum ElectionResult {
    Won(Epoch),
    Lost { epoch: Epoch, payload: Vec<u8> },
}

pub trait Authority: Send + Sync {
    /// Attempt to become leader. Returns leader epoch or None if there already was a leader. The
    /// payload_data must be unique among all possible leaders to avoid confusion about which was
    /// elected.
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Option<Epoch>, Error>;

    /// Voluntarily give up leadership, allowing another node to become leader. It is extremely
    /// important that the node calling this function is actually the leader.
    fn surrender_leadership(&self) -> Result<(), Error>;

    /// Returns the epoch and payload data for the current leader, blocking if there is not
    /// currently as leader. This method is intended for clients to determine the current leader.
    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Error>;

    /// Same as `get_leader` but return None if there is no leader instead of blocking.
    fn try_get_leader(&self) -> Result<Option<(Epoch, Vec<u8>)>, Error>;

    /// Wait until it is no longer the epoch indicated in `current_epoch`, and then return the new
    /// epoch or None if a new epoch hasn't started yet. This method enables a leader to watch to
    /// see if it has been overthrown.
    fn await_new_epoch(&self, current_epoch: Epoch) -> Result<Option<(Epoch, Vec<u8>)>, Error>;

    /// Do a non-blocking read at the indicated key.
    fn try_read(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    fn read_modify_write<F, P, E>(&self, key: &str, f: F) -> Result<Result<P, E>, Error>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned;
}
