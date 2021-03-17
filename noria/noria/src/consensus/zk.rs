use std::process;
use std::thread::{self, Thread};
use std::time::Duration;

use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use zookeeper::{Acl, CreateMode, KeeperState, Stat, WatchedEvent, Watcher, ZkError, ZooKeeper};

use super::Authority;
use super::Epoch;
use super::CONTROLLER_KEY;
use crate::{ReadySetError, ReadySetResult};

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

/// Coordinator that shares connection information between workers and clients using ZooKeeper.
pub struct ZookeeperAuthority {
    zk: ZooKeeper,
    log: slog::Logger,
}

impl ZookeeperAuthority {
    /// Create a new instance.
    pub fn new(connect_string: &str) -> ReadySetResult<Self> {
        let zk = ZooKeeper::connect(connect_string, Duration::from_secs(1), EventWatcher).map_err(
            |e| ReadySetError::ZookeeperConnectionFailed {
                connect_string: connect_string.into(),
                reason: e.to_string(),
            },
        )?;
        let _ = zk.create(
            "/",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        Ok(Self {
            zk,
            log: slog::Logger::root(slog::Discard, o!()),
        })
    }

    /// Enable logging
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }
}

impl Authority for ZookeeperAuthority {
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Option<Epoch>, Error> {
        let path = match self.zk.create(
            CONTROLLER_KEY,
            payload_data.clone(),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(path) => path,
            Err(ZkError::NodeExists) => return Ok(None),
            Err(e) => bail!(e),
        };

        let (ref current_data, ref stat) = self.zk.get_data(&path, false)?;
        if *current_data == payload_data {
            info!(self.log, "became leader at epoch {}", stat.czxid);
            Ok(Some(Epoch(stat.czxid)))
        } else {
            Ok(None)
        }
    }

    fn surrender_leadership(&self) -> Result<(), Error> {
        self.zk.delete(CONTROLLER_KEY, None)?;
        Ok(())
    }

    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Error> {
        loop {
            match self.zk.get_data(CONTROLLER_KEY, false) {
                Ok((data, stat)) => return Ok((Epoch(stat.czxid), data)),
                Err(ZkError::NoNode) => {}
                Err(e) => bail!(e),
            };

            match self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new()) {
                Ok(_) => {}
                Err(ZkError::NoNode) => {
                    warn!(
                        self.log,
                        "no controller present, waiting for one to appear..."
                    );
                    thread::park_timeout(Duration::from_secs(60))
                }
                Err(e) => bail!(e),
            }
        }
    }

    fn try_get_leader(&self) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        match self.zk.get_data(CONTROLLER_KEY, false) {
            Ok((data, stat)) => Ok(Some((Epoch(stat.czxid), data))),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    fn await_new_epoch(&self, current_epoch: Epoch) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        let is_new_epoch = |stat: &Stat| stat.czxid > current_epoch.0;

        loop {
            match self.zk.get_data(CONTROLLER_KEY, false) {
                Ok((_, ref stat)) if !is_new_epoch(stat) => {}
                Ok((data, stat)) => return Ok(Some((Epoch(stat.czxid), data))),
                Err(ZkError::NoNode) => return Ok(None),
                Err(e) => bail!(e),
            };

            match self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new()) {
                Ok(Some(ref stat)) if is_new_epoch(stat) => {}
                Ok(_) | Err(ZkError::NoNode) => thread::park_timeout(Duration::from_secs(60)),
                Err(e) => bail!(e),
            }
        }
    }

    fn try_read(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.zk.get_data(path, false) {
            Ok((data, _)) => Ok(Some(data)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
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
                        Err(e) => bail!(e),
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
                        Err(e) => bail!(e),
                    }
                }
                Err(e) => bail!(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn it_works() {
        let authority =
            Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/concensus_it_works").unwrap());
        assert!(authority.try_read(CONTROLLER_KEY).unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |_: Option<u32>| -> Result<u32, u32> { Ok(12) })
                .unwrap(),
            Ok(12)
        );
        assert_eq!(
            authority.try_read("/a").unwrap(),
            Some("12".bytes().collect())
        );
        authority.become_leader(vec![15]).unwrap();
        assert_eq!(authority.get_leader().unwrap().1, vec![15]);
        {
            let authority = authority.clone();
            thread::spawn(move || authority.become_leader(vec![20]).unwrap());
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader().unwrap().1, vec![15]);
    }
}
