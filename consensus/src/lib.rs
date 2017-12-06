#![feature(allow_fail)]
#![feature(option_filter)]

#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate zookeeper;

use serde::Serialize;
use serde::de::DeserializeOwned;

mod zk;
mod local;
pub use zk::ZookeeperAuthority;
pub use local::LocalAuthority;

const CONTROLLER_KEY: &'static str = "/controller";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(i64);

pub trait Authority: Send + Sync {
    /// Attempt to become leader, blocking until this is possible. Returns leader epoch upon sucess.
    fn become_leader(&self, payload_data: Vec<u8>) -> Epoch;

    /// Returns the epoch and payload data for the current leader, blocking if there is not
    /// currently as leader. This method is intended for clients to determine the current leader.
    fn get_leader(&self) -> (Epoch, Vec<u8>);

    /// Wait until it is no longer the epoch indicated in `current_epoch`, and then return the new
    /// epoch. This method enables a leader to watch to see if it has been overthrown.
    fn await_new_epoch(&self, current_epoch: Epoch) -> Epoch;

    /// Do a non-blocking read at the indicated key.
    fn try_read(&self, key: &str) -> Option<Vec<u8>>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    fn read_modify_write<F, P, E>(&self, key: &str, f: F) -> Result<P, E>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned;
}
