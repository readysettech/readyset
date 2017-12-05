use std::process;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use zookeeper::{Acl, CreateMode, KeeperState, WatchedEvent, Watcher, ZkError, ZooKeeper};

use Authority;
use Epoch;
use CONTROLLER_KEY;

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

pub struct ZookeeperAuthority {
    zk: ZooKeeper,
}

impl ZookeeperAuthority {
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
}

impl Authority for ZookeeperAuthority {
    fn become_leader(&self, payload_data: Vec<u8>) -> Option<Epoch> {
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

    fn get_leader(&self) -> (Epoch, Vec<u8>) {
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

    fn await_new_epoch(&self, current_epoch: Epoch) -> Epoch {
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

    fn try_read(&self, path: &str) -> Option<Vec<u8>> {
        self.zk.get_data(path, false).ok().map(|d| d.0)
    }

    fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<P, E>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned,
    {
        loop {
            match self.zk.get_data(path, false) {
                Ok((data, stat)) => {
                    let p = serde_json::from_slice(&data).unwrap();
                    let result = f(Some(p));
                    if result.is_err()
                        || self.zk
                            .set_data(
                                path,
                                serde_json::to_vec(result.as_ref().ok().unwrap()).unwrap(),
                                Some(stat.version),
                            )
                            .is_ok()
                    {
                        return result;
                    }
                }
                Err(ZkError::NoNode) => {
                    let result = f(None);
                    if result.is_err()
                        || self.zk
                            .create(
                                path,
                                serde_json::to_vec(result.as_ref().ok().unwrap()).unwrap(),
                                Acl::open_unsafe().clone(),
                                CreateMode::Persistent,
                            )
                            .is_ok()
                    {
                        return result;
                    }
                }
                Err(e) => panic!("{}", e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn it_works() {
        let authority = Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/concensus_it_works"));
        assert!(authority.try_read(CONTROLLER_KEY).is_none());
        assert_eq!(
            authority.read_modify_write("/a", |_: Option<u32>| -> Result<u32, u32> { Ok(12) }),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a"), Some("12".bytes().collect()));
        assert!(authority.become_leader(vec![15]).is_some());
        assert_eq!(authority.get_leader().1, vec![15]);
        assert_eq!(authority.become_leader(vec![20]), None);
        assert_eq!(authority.get_leader().1, vec![15]);
    }
}
