// TODO: Replace how leader election is done and locking implementation of recipes from
// https://zookeeper.apache.org/doc/current/recipes.html

use std::collections::HashSet;
use std::process;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, Thread};
use std::time::Duration;

use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use zookeeper::{Acl, CreateMode, KeeperState, Stat, WatchedEvent, Watcher, ZkError, ZooKeeper};

use super::{
    Authority, AuthorityWorkerHeartbeatResponse, GetLeaderResult, LeaderPayload, WorkerDescriptor,
};
use super::{WorkerId, CONTROLLER_KEY, WORKER_PATH, WORKER_PREFIX};
use crate::errors::internal_err;
use crate::{ReadySetError, ReadySetResult};
use backoff::backoff::Backoff;
use backoff::exponential::ExponentialBackoff;
use backoff::SystemClock;

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

struct ZookeeperAuthorityInner {
    leader_create_epoch: Option<i64>,
}

/// Coordinator that shares connection information between workers and clients using ZooKeeper.
pub struct ZookeeperAuthority {
    zk: ZooKeeper,

    log: slog::Logger,

    // Inner which contains state is only needed for server so making an Option.
    inner: Option<RwLock<ZookeeperAuthorityInner>>,
}

fn path_to_worker_id(path: &str) -> WorkerId {
    // See `worker_id_to_prefix` for the type of path this is called on.
    #[allow(clippy::unwrap_used)]
    path[(path.rfind('-').unwrap() + 1)..].to_owned()
}

fn worker_id_to_path(id: &str) -> String {
    WORKER_PREFIX.to_owned() + id
}

impl ZookeeperAuthority {
    fn new_with_inner(
        connect_string: &str,
        inner: Option<RwLock<ZookeeperAuthorityInner>>,
    ) -> ReadySetResult<Self> {
        let zk_connect_op = || -> Result<ZooKeeper, backoff::Error<ZkError>> {
            match ZooKeeper::connect(connect_string, Duration::from_secs(1), EventWatcher) {
                // HACK(fran): Currently, the Zookeeper::connect method won't fail if Zookeeper
                // is not available.
                // To workaround that, we make a call to zk.exists just to try to reach Zookeeper.
                // The downside of this, is that the zk.exists call might take longer than the duration
                // specified in Zookeeper::connect.
                // For more information, see https://github.com/bonifaido/rust-zookeeper/issues/64.
                Ok(zk) => zk
                    .exists("/", false)
                    .map(|_| zk)
                    .map_err(backoff::Error::Transient),
                Err(
                    e
                    @
                    (ZkError::ConnectionLoss
                    | ZkError::SessionExpired
                    | ZkError::OperationTimeout),
                ) => Err(backoff::Error::Transient(e)),
                Err(e) => Err(backoff::Error::Permanent(e)),
            }
        };
        let mut backoff: ExponentialBackoff<SystemClock> = ExponentialBackoff {
            max_elapsed_time: None,
            ..Default::default()
        };
        backoff.reset();
        let zk = backoff::retry(backoff, zk_connect_op).map_err(|e| {
            ReadySetError::ZookeeperConnectionFailed {
                connect_string: connect_string.into(),
                reason: e.to_string(),
            }
        })?;
        let _ = zk.create(
            "/",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        Ok(Self {
            zk,
            log: slog::Logger::root(slog::Discard, o!()),
            inner,
        })
    }

    /// Create a new instance.
    pub fn new(connect_string: &str) -> ReadySetResult<Self> {
        let inner = Some(RwLock::new(ZookeeperAuthorityInner {
            leader_create_epoch: None,
        }));
        Self::new_with_inner(connect_string, inner)
    }

    /// Enable logging
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    fn read_inner(&self) -> Result<RwLockReadGuard<'_, ZookeeperAuthorityInner>, Error> {
        if let Some(inner_mutex) = &self.inner {
            match inner_mutex.read() {
                Ok(inner) => Ok(inner),
                Err(e) => bail!(internal_err(format!("rwlock is poisoned: '{}'", e))),
            }
        } else {
            bail!(internal_err(
                "attempting to read inner on readonly zk authority"
            ))
        }
    }

    fn write_inner(&self) -> Result<RwLockWriteGuard<'_, ZookeeperAuthorityInner>, Error> {
        if let Some(inner_mutex) = &self.inner {
            match inner_mutex.write() {
                Ok(inner) => Ok(inner),
                Err(e) => bail!(internal_err(format!("rwlock is poisoned: '{}'", e))),
            }
        } else {
            bail!(internal_err(
                "attempting to mutate inner on readonly zk authority"
            ))
        }
    }

    fn update_leader_create_epoch(
        &self,
        new_leader_create_epoch: Option<i64>,
    ) -> Result<(), Error> {
        let mut inner = self.write_inner()?;
        inner.leader_create_epoch = new_leader_create_epoch;
        Ok(())
    }
}

impl Authority for ZookeeperAuthority {
    fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error> {
        let path = match self.zk.create(
            CONTROLLER_KEY,
            serde_json::to_vec(&payload)?,
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(path) => path,
            Err(ZkError::NodeExists) => return Ok(None),
            Err(e) => bail!(e),
        };

        let (ref current_data, ref stat) = self.zk.get_data(&path, false)?;
        let current_payload = serde_json::from_slice::<LeaderPayload>(current_data)?;
        if current_payload == payload {
            info!(self.log, "became leader at epoch {}", stat.czxid);
            self.update_leader_create_epoch(Some(stat.czxid))?;
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    fn surrender_leadership(&self) -> Result<(), Error> {
        self.zk.delete(CONTROLLER_KEY, None)?;
        Ok(())
    }

    fn get_leader(&self) -> Result<LeaderPayload, Error> {
        loop {
            match self.zk.get_data(CONTROLLER_KEY, false) {
                Ok((data, stat)) => {
                    self.update_leader_create_epoch(Some(stat.czxid))?;
                    let payload = serde_json::from_slice(&data)?;
                    return Ok(payload);
                }
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

    fn try_get_leader(&self) -> Result<GetLeaderResult, Error> {
        let inner = self.read_inner()?;
        let current_epoch = inner.leader_create_epoch;
        let is_new_epoch = |stat: &Stat| {
            if let Some(epoch) = current_epoch {
                stat.czxid > epoch
            } else {
                true
            }
        };

        Ok(match self.zk.get_data(CONTROLLER_KEY, false) {
            Ok((_, ref stat)) if !is_new_epoch(stat) => GetLeaderResult::Unchanged,
            Ok((data, stat)) => {
                self.update_leader_create_epoch(Some(stat.czxid))?;
                let payload = serde_json::from_slice(&data)?;
                GetLeaderResult::NewLeader(payload)
            }
            Err(ZkError::NoNode) => GetLeaderResult::NoLeader,
            Err(e) => bail!(e),
        })
    }

    fn can_watch(&self) -> bool {
        true
    }

    fn watch_leader(&self) -> Result<(), Error> {
        self.zk.exists_w(CONTROLLER_KEY, UnparkWatcher::new())?;
        Ok(())
    }

    fn await_new_leader(&self) -> Result<Option<LeaderPayload>, Error> {
        let inner = self.read_inner()?;
        let current_epoch = inner.leader_create_epoch;
        let is_new_epoch = |stat: &Stat| {
            if let Some(epoch) = current_epoch {
                stat.czxid > epoch
            } else {
                true
            }
        };

        loop {
            match self.zk.get_data(CONTROLLER_KEY, false) {
                Ok((_, ref stat)) if !is_new_epoch(stat) => {}
                Ok((data, stat)) => {
                    self.update_leader_create_epoch(Some(stat.czxid))?;
                    let payload = serde_json::from_slice(&data)?;
                    return Ok(Some(payload));
                }
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

    fn try_read<P: DeserializeOwned>(&self, path: &str) -> Result<Option<P>, Error> {
        match self.zk.get_data(path, false) {
            Ok((data, _)) => Ok(Some(serde_json::from_slice(&data)?)),
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
                    if let Ok(r) = &result {
                        match self
                            .zk
                            .set_data(path, serde_json::to_vec(r)?, Some(stat.version))
                        {
                            Err(ZkError::NoNode) | Err(ZkError::BadVersion) => continue,
                            Ok(_) => (),
                            Err(e) => bail!(e),
                        }
                    }
                    return Ok(result);
                }
                Err(ZkError::NoNode) => {
                    let result = f(None);
                    if let Ok(r) = &result {
                        match self.zk.create(
                            path,
                            serde_json::to_vec(r)?,
                            Acl::open_unsafe().clone(),
                            CreateMode::Persistent,
                        ) {
                            Err(ZkError::NodeExists) => continue,
                            Ok(_) => (),
                            Err(e) => bail!(e),
                        }
                    }
                    return Ok(result);
                }
                Err(e) => bail!(e),
            }
        }
    }

    fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.zk.get_data(path, false) {
            Ok((data, _)) => Ok(Some(data)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    fn register_worker(&self, payload: WorkerDescriptor) -> Result<Option<WorkerId>, Error>
    where
        WorkerDescriptor: Serialize,
    {
        // Attempt to create the base path in case we are the first worker.
        let _ = self.zk.create(
            WORKER_PATH,
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );

        let path = match self.zk.create(
            WORKER_PREFIX,
            serde_json::to_vec(&payload)?,
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential,
        ) {
            Ok(path) => path,
            Err(ZkError::NodeExists) => return Ok(None),
            Err(e) => bail!(e),
        };
        let worker_id = path_to_worker_id(&path);
        Ok(Some(worker_id))
    }

    fn worker_heartbeat(&self, id: WorkerId) -> Result<AuthorityWorkerHeartbeatResponse, Error> {
        let path = worker_id_to_path(&id);
        Ok(match self.zk.exists(&path, false) {
            Ok(Some(_)) => AuthorityWorkerHeartbeatResponse::Alive,
            _ => AuthorityWorkerHeartbeatResponse::Failed,
        })
    }

    fn get_workers(&self) -> Result<HashSet<WorkerId>, Error> {
        let children = match self.zk.get_children(WORKER_PATH, false) {
            Ok(v) => v,
            Err(e) => bail!(e),
        };
        Ok(children
            .into_iter()
            .map(|path| path_to_worker_id(&path))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Url;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn it_works() {
        let authority =
            Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/consensus_it_works").unwrap());
        assert!(authority.try_read::<u32>("/a").unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |_: Option<u32>| -> Result<u32, u32> { Ok(12) })
                .unwrap(),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a").unwrap(), Some(12));

        let payload = LeaderPayload {
            controller_uri: url::Url::parse("http://127.0.0.1:2181").unwrap(),
            nonce: 1,
        };
        let expected_leader_payload = payload.clone();
        assert_eq!(
            authority.become_leader(payload.clone()).unwrap(),
            Some(payload)
        );
        assert_eq!(&authority.get_leader().unwrap(), &expected_leader_payload);
        {
            let authority = authority.clone();
            let payload = LeaderPayload {
                controller_uri: url::Url::parse("http://127.0.0.1:2182").unwrap(),
                nonce: 2,
            };
            thread::spawn(move || authority.become_leader(payload).unwrap());
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(&authority.get_leader().unwrap(), &expected_leader_payload);
    }

    #[test]
    #[ignore]
    fn retrieve_workers() {
        let authority =
            Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/retrieve_workers").unwrap());

        let worker = WorkerDescriptor {
            worker_uri: Url::parse("http://127.0.0.1").unwrap(),
            reader_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234),
            region: None,
            reader_only: false,
            volume_id: None,
        };

        let workers = authority.get_workers().unwrap();
        assert!(workers.is_empty());

        let worker_id = authority.register_worker(worker.clone()).unwrap().unwrap();
        let workers = authority.get_workers().unwrap();
        assert_eq!(workers.len(), 1);
        assert!(workers.contains(&worker_id));
        assert_eq!(
            authority.worker_heartbeat(worker_id).unwrap(),
            AuthorityWorkerHeartbeatResponse::Alive
        );

        let worker_id = authority.register_worker(worker).unwrap().unwrap();
        let workers = authority.get_workers().unwrap();
        assert_eq!(workers.len(), 2);
        assert!(workers.contains(&worker_id));

        // Kill the session, this should remove the keys from the worker set.
        drop(authority);
        let authority =
            Arc::new(ZookeeperAuthority::new("127.0.0.1:2181/retrieve_workers").unwrap());
        let workers = authority.get_workers().unwrap();
        assert_eq!(
            authority.worker_heartbeat(worker_id).unwrap(),
            AuthorityWorkerHeartbeatResponse::Failed
        );
        assert_eq!(workers.len(), 0);
    }
}
