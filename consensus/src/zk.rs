use std::error::Error;
use std::process;
use std::time::Duration;
use std::thread::{self, Thread};

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

/// Watcher which unparks the thread that created it upon triggering.
struct UnparkWatcher(Thread);
impl UnparkWatcher {
    pub fn new() -> Self {
        UnparkWatcher(thread::current())
    }
}
impl Watcher for UnparkWatcher {
    fn handle(&self, _: WatchedEvent) {
        self.0.unpark();
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
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Epoch, Box<Error + Send + Sync>> {
        loop {
            match self.zk
                .create(
                    CONTROLLER_KEY,
                    payload_data.clone(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Ephemeral,
                )
                .and_then(|path| self.zk.get_data(&path, false))
            {
                Ok((ref current_data, ref stat)) if *current_data == payload_data => {
                    return Ok(Epoch(stat.czxid));
                }
                Ok(_) | Err(ZkError::NoNode) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(box e),
            }

            match self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new()) {
                Ok(_) => thread::park_timeout(Duration::from_secs(60)),
                Err(ZkError::NoNode) => {}
                Err(e) => return Err(box e),
            }
        }
    }

    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Box<Error + Send + Sync>> {
        loop {
            match self.zk.get_data(CONTROLLER_KEY, false) {
                Ok((data, stat)) => return Ok((Epoch(stat.czxid), data)),
                Err(ZkError::NoNode) => {}
                Err(e) => return Err(box e),
            };

            match self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new()) {
                Ok(_) => {}
                Err(ZkError::NoNode) => thread::park_timeout(Duration::from_secs(60)),
                Err(e) => return Err(box e),
            }
        }
    }

    fn await_new_epoch(&self, current_epoch: Epoch) -> Result<Epoch, Box<Error + Send + Sync>> {
        loop {
            match self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new()) {
                Ok(ref stat) if stat.czxid > current_epoch.0 => return Ok(Epoch(stat.czxid)),
                Ok(_) | Err(ZkError::NoNode) => thread::park_timeout(Duration::from_secs(60)),
                Err(e) => return Err(box e),
            }
        }
    }

    fn try_read(&self, path: &str) -> Result<Option<Vec<u8>>, Box<Error + Send + Sync>> {
        match self.zk.get_data(path, false) {
            Ok((data, _)) => Ok(Some(data)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => Err(box e),
        }
    }

    fn read_modify_write<F, P, E>(
        &self,
        path: &str,
        mut f: F,
    ) -> Result<Result<P, E>, Box<Error + Send + Sync>>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned,
    {
        loop {
            match self.zk.get_data(path, false) {
                Ok((data, stat)) => {
                    let p = serde_json::from_slice(&data)?;
                    let result = f(Some(p));
                    if result.is_err() {
                        return Ok(result);
                    }

                    match self.zk.set_data(
                        path,
                        serde_json::to_vec(result.as_ref().ok().unwrap())?,
                        Some(stat.version),
                    ) {
                        Err(ZkError::NoNode) | Err(ZkError::BadVersion) => continue,
                        Ok(_) => return Ok(result),
                        Err(e) => return Err(box e),
                    };
                }
                Err(ZkError::NoNode) => {
                    let result = f(None);
                    if result.is_err() {
                        return Ok(result);
                    }
                    match self.zk.create(
                        path,
                        serde_json::to_vec(result.as_ref().ok().unwrap())?,
                        Acl::open_unsafe().clone(),
                        CreateMode::Persistent,
                    ) {
                        Err(ZkError::NodeExists) => continue,
                        Ok(_) => return Ok(result),
                        Err(e) => return Err(box e),
                    }
                }
                Err(e) => return Err(box e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::sync::Arc;

    #[test]
    #[allow_fail]
    fn it_works() {
        let authority = Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/concensus_it_works"));
        assert!(authority.try_read(CONTROLLER_KEY).is_none());
        assert_eq!(
            authority.read_modify_write("/a", |_: Option<u32>| -> Result<u32, u32> { Ok(12) }),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a"), Some("12".bytes().collect()));
        authority.become_leader(vec![15]);
        assert_eq!(authority.get_leader().1, vec![15]);
        {
            let authority = authority.clone();
            thread::spawn(move || authority.become_leader(vec![20]));
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader().1, vec![15]);
    }
}
