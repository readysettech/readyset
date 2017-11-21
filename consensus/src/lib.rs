extern crate zookeeper;

use std::process;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use zookeeper::{Acl, CreateMode, KeeperState, WatchedEvent, Watcher, ZkError, ZooKeeper};

const CONTROLLER_KEY: &'static str = "/controller";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch(i64);

struct EventWatcher;
impl Watcher for EventWatcher {
    fn handle(&self, e: WatchedEvent) {
        if e.keeper_state != KeeperState::SyncConnected {
            eprintln!("Lost connection to ZooKeeper! Aborting");
            process::abort();
        }
    }
}

struct ChannelWatcher(Sender<()>);
impl ChannelWatcher {
    pub fn new() -> (Self, Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        (ChannelWatcher(tx), rx)
    }
}
impl Watcher for ChannelWatcher {
    fn handle(&self, _: WatchedEvent) {
        let _ = self.0.send(());
    }
}

pub struct Connection {
    zk: ZooKeeper,
}

impl Connection {
    /// Create a new instance.
    pub fn new(connect_string: &str) -> Self {
        let zk = ZooKeeper::connect(connect_string, Duration::from_secs(1), EventWatcher).unwrap();
        let _ = zk.create(
            "/",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        Self { zk }
    }

    /// Attempt to become the leader, returning the epoch of that this instance is leader for. If
    /// there is already another leader, return None.
    pub fn become_leader(&self, payload_data: Vec<u8>) -> Option<Epoch> {
        let create = self.zk.create(
            CONTROLLER_KEY,
            payload_data,
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        );
        if let Ok(path) = create {
            if let Ok(Some(stat)) = self.zk.exists(&path, false) {
                return Some(Epoch(stat.czxid));
            }
        }
        return None;
    }

    /// Get the current epoch of the current leader, or return None if there is no leader.
    pub fn get_epoch(&self) -> Option<Epoch> {
        self.zk
            .exists(CONTROLLER_KEY, false)
            .ok()
            .and_then(|stat| stat)
            .map(|stat| Epoch(stat.czxid))
    }

    /// Returns the epoch and payload data for the current leader, blocking if there is not
    /// currently as leader.
    pub fn get_leader(&self) -> (Epoch, Vec<u8>) {
        loop {
            if let Ok((data, stat)) = self.zk.get_data(CONTROLLER_KEY, false) {
                return (Epoch(stat.czxid), data);
            }

            let (watcher, rx) = ChannelWatcher::new();
            if !self.zk.exists_w(CONTROLLER_KEY, watcher).is_ok() {
                let _ = rx.recv();
            }
        }
    }

    /// Does a read of the node at the given path, blocking until it exists.
    pub fn read(&self, path: &str) -> Vec<u8> {
        loop {
            if let Ok((data, _)) = self.zk.get_data(path, false) {
                return data;
            }

            let (watcher, rx) = ChannelWatcher::new();
            if let Err(ZkError::NoNode) = self.zk.exists_w(path, watcher) {
                let _ = rx.recv();
            }
        }
    }

    /// Store the data at the indicated path.
    pub fn write(&self, path: &str, data: Vec<u8>) {
        self.zk.set_data(path, data, None).unwrap();
    }

    /// Wait until it is no longer the epoch indicated in `current_epoch`, and then return the new
    /// epoch.
    pub fn await_new_epoch(&self, current_epoch: Epoch) -> Epoch {
        loop {
            let (watcher, rx) = ChannelWatcher::new();
            match self.zk.exists_w(CONTROLLER_KEY, watcher) {
                Ok(ref stat) if stat.czxid > current_epoch.0 => return Epoch(stat.czxid),
                Ok(_) | Err(ZkError::NoNode) => {
                    let _ = rx.recv();
                }
                Err(e) => panic!("{}", e),
            }
        }
    }
}
