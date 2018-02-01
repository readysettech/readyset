#![feature(allow_fail)]
#![feature(box_syntax)]
#![feature(option_filter)]

#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate zookeeper;

use serde::Serialize;
use serde::de::DeserializeOwned;

use std::error::Error;

mod zk;
mod local;
pub use zk::ZookeeperAuthority;
pub use local::LocalAuthority;

pub const CONTROLLER_KEY: &'static str = "/controller";
pub const STATE_KEY: &'static str = "/state";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(i64);

pub trait Authority: Send + Sync {
    /// Attempt to become leader, blocking until this is possible. Returns leader epoch upon
    /// sucess. The payload_data must be unique among all possible leaders to avoid confusion about
    /// which was elected.
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Epoch, Box<Error + Send + Sync>>;

    /// Returns the epoch and payload data for the current leader, blocking if there is not
    /// currently as leader. This method is intended for clients to determine the current leader.
    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Box<Error + Send + Sync>>;

    /// Wait until it is no longer the epoch indicated in `current_epoch`, and then return the new
    /// epoch. This method enables a leader to watch to see if it has been overthrown.
    fn await_new_epoch(&self, current_epoch: Epoch) -> Result<Epoch, Box<Error + Send + Sync>>;

    /// Do a non-blocking read at the indicated key.
    fn try_read(&self, key: &str) -> Result<Option<Vec<u8>>, Box<Error + Send + Sync>>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    fn read_modify_write<F, P, E>(
        &self,
        key: &str,
        f: F,
    ) -> Result<Result<P, E>, Box<Error + Send + Sync>>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned;
}
