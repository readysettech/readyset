// TODO: Replace how leader election is done and locking implementation of recipes from
// https://zookeeper.apache.org/doc/current/recipes.html

use std::process;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use anyhow::{bail, Error};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn};
use zookeeper_async::{
    Acl, CreateMode, KeeperState, Stat, WatchedEvent, Watcher, ZkError, ZooKeeper,
};

use super::{
    AdapterId, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult, LeaderPayload,
    WorkerDescriptor, WorkerId,
};
use crate::errors::internal_err;
use crate::{ReadySetError, ReadySetResult};
use backoff::backoff::Backoff;
use backoff::exponential::ExponentialBackoff;
use backoff::SystemClock;

pub const CONTROLLER_KEY: &str = "/controller";
pub const STATE_KEY: &str = "/state";
pub const WORKER_PATH: &str = "/workers";
pub const WORKER_PREFIX: &str = "/workers/guid-";
const BACKOFF_MAX_TIME: Duration = Duration::from_secs(10);

struct EventWatcher;
impl Watcher for EventWatcher {
    fn handle(&self, e: WatchedEvent) {
        if e.keeper_state != KeeperState::SyncConnected {
            error!("Lost connection to ZooKeeper! Aborting");
            process::abort();
        }
    }
}

struct ZookeeperAuthorityInner {
    leader_create_epoch: Option<i64>,
    worker_id: Option<WorkerId>,
}

/// Coordinator that shares connection information between workers and clients using ZooKeeper.
pub struct ZookeeperAuthority {
    zk: ZooKeeper,

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
    async fn new_with_inner(
        connect_string: &str,
        inner: Option<RwLock<ZookeeperAuthorityInner>>,
    ) -> ReadySetResult<Self> {
        let mut backoff: ExponentialBackoff<SystemClock> = ExponentialBackoff {
            max_elapsed_time: Some(BACKOFF_MAX_TIME),
            ..Default::default()
        };
        backoff.reset();
        let zk = backoff::future::retry(backoff, || {
            async {
                match ZooKeeper::connect(connect_string, Duration::from_secs(1), EventWatcher).await
                {
                    // HACK(fran): Currently, the Zookeeper::connect method won't fail if Zookeeper
                    // is not available.
                    // To workaround that, we make a call to zk.exists just to try to reach Zookeeper.
                    // The downside of this, is that the zk.exists call might take longer than the duration
                    // specified in Zookeeper::connect.
                    // For more information, see https://github.com/bonifaido/rust-zookeeper/issues/64.
                    Ok(zk) => zk
                        .exists("/", false)
                        .await
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
            }
        })
        .await
        .map_err(|e| ReadySetError::ZookeeperConnectionFailed {
            connect_string: connect_string.into(),
            reason: e.to_string(),
        })?;
        let _ = zk
            .create(
                "/",
                vec![],
                Acl::open_unsafe().clone(),
                CreateMode::Persistent,
            )
            .await;
        Ok(Self { zk, inner })
    }

    /// Create a new instance.
    pub async fn new(connect_string: &str) -> ReadySetResult<Self> {
        let inner = Some(RwLock::new(ZookeeperAuthorityInner {
            leader_create_epoch: None,
            worker_id: None,
        }));
        Self::new_with_inner(connect_string, inner).await
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

#[async_trait]
impl AuthorityControl for ZookeeperAuthority {
    async fn init(&self) -> Result<(), Error> {
        // Attempt to create the base path in case we are the first worker.
        let _ = self.zk.create(
            WORKER_PATH,
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        Ok(())
    }

    async fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error> {
        let path = match self
            .zk
            .create(
                CONTROLLER_KEY,
                serde_json::to_vec(&payload)?,
                Acl::open_unsafe().clone(),
                CreateMode::Ephemeral,
            )
            .await
        {
            Ok(path) => path,
            Err(ZkError::NodeExists) => return Ok(None),
            Err(e) => bail!(e),
        };

        let (ref current_data, ref stat) = self.zk.get_data(&path, false).await?;
        let current_payload = serde_json::from_slice::<LeaderPayload>(current_data)?;
        if current_payload == payload {
            info!(epoch = %stat.czxid, "became leader");
            self.update_leader_create_epoch(Some(stat.czxid))?;
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    async fn surrender_leadership(&self) -> Result<(), Error> {
        self.zk.delete(CONTROLLER_KEY, None).await?;
        Ok(())
    }

    async fn get_leader(&self) -> Result<LeaderPayload, Error> {
        loop {
            match self.zk.get_data(CONTROLLER_KEY, false).await {
                Ok((data, stat)) => {
                    self.update_leader_create_epoch(Some(stat.czxid))?;
                    let payload = serde_json::from_slice(&data)?;
                    return Ok(payload);
                }
                Err(ZkError::NoNode) => {}
                Err(e) => bail!(e),
            };

            let notify = Arc::new(Notify::new());
            let wait = notify.clone();

            match self
                .zk
                .exists_w(CONTROLLER_KEY, move |_| notify.notify_waiters())
                .await
            {
                Ok(_) => {}
                Err(ZkError::NoNode) => {
                    warn!("no controller present, waiting for one to appear...");
                    wait.notified().await;
                }
                Err(e) => bail!(e),
            }
        }
    }

    async fn try_get_leader(&self) -> Result<GetLeaderResult, Error> {
        let current_epoch = {
            let inner = self.read_inner()?;
            inner.leader_create_epoch
        };
        let is_new_epoch = |stat: &Stat| {
            if let Some(epoch) = current_epoch {
                stat.czxid > epoch
            } else {
                true
            }
        };

        Ok(match self.zk.get_data(CONTROLLER_KEY, false).await {
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

    async fn watch_leader(&self) -> Result<(), Error> {
        let notify = Arc::new(Notify::new());
        let wait = notify.clone();

        self.zk
            .exists_w(CONTROLLER_KEY, move |_| notify.notify_waiters())
            .await?;
        wait.notified().await;
        Ok(())
    }

    async fn watch_workers(&self) -> Result<(), Error> {
        let notify = Arc::new(Notify::new());
        let wait = notify.clone();

        self.zk
            .get_children_w(WORKER_PATH, move |_| notify.notify_waiters())
            .await?;

        wait.notified().await;
        Ok(())
    }

    async fn try_read<P: DeserializeOwned>(&self, path: &str) -> Result<Option<P>, Error> {
        match self.zk.get_data(path, false).await {
            Ok((data, _)) => Ok(Some(serde_json::from_slice(&data)?)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    async fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        loop {
            match self.zk.get_data(path, false).await {
                Ok((data, stat)) => {
                    let p = serde_json::from_slice(&data)?;
                    let result = f(Some(p));
                    if let Ok(r) = result {
                        let as_vec = serde_json::to_vec(&r)?;
                        match self.zk.set_data(path, as_vec, Some(stat.version)).await {
                            Err(ZkError::NoNode) | Err(ZkError::BadVersion) => continue,
                            Ok(_) => return Ok(Ok(r)),
                            Err(e) => bail!(e),
                        }
                    }
                }
                Err(ZkError::NoNode) => {
                    let result = f(None);
                    if let Ok(r) = result {
                        let as_vec = serde_json::to_vec(&r)?;
                        match self
                            .zk
                            .create(
                                path,
                                as_vec,
                                Acl::open_unsafe().clone(),
                                CreateMode::Persistent,
                            )
                            .await
                        {
                            Err(ZkError::NodeExists) => continue,
                            Ok(_) => return Ok(Ok(r)),
                            Err(e) => bail!(e),
                        }
                    }
                }
                Err(e) => bail!(e),
            }
        }
    }

    async fn update_controller_state<F, P, E>(&self, f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        self.read_modify_write(STATE_KEY, f).await
    }

    async fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.zk.get_data(path, false).await {
            Ok((data, _)) => Ok(Some(data)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    async fn register_worker(&self, payload: WorkerDescriptor) -> Result<Option<WorkerId>, Error>
    where
        WorkerDescriptor: Serialize,
    {
        // Attempt to create the base path in case we are the first worker.
        let _ = self
            .zk
            .create(
                WORKER_PATH,
                Vec::new(),
                Acl::open_unsafe().clone(),
                CreateMode::Persistent,
            )
            .await;

        let path = match self
            .zk
            .create(
                WORKER_PREFIX,
                serde_json::to_vec(&payload)?,
                Acl::open_unsafe().clone(),
                CreateMode::EphemeralSequential,
            )
            .await
        {
            Ok(path) => path,
            Err(ZkError::NodeExists) => return Ok(None),
            Err(e) => bail!(e),
        };
        let worker_id = path_to_worker_id(&path);
        let mut inner = self.write_inner()?;
        inner.worker_id = Some(worker_id.clone());
        Ok(Some(worker_id))
    }

    async fn worker_heartbeat(
        &self,
        id: WorkerId,
    ) -> Result<AuthorityWorkerHeartbeatResponse, Error> {
        let path = worker_id_to_path(&id);
        Ok(match self.zk.exists(&path, false).await {
            Ok(Some(_)) => AuthorityWorkerHeartbeatResponse::Alive,
            _ => AuthorityWorkerHeartbeatResponse::Failed,
        })
    }

    async fn get_workers(&self) -> Result<HashSet<WorkerId>, Error> {
        let children = match self.zk.get_children(WORKER_PATH, false).await {
            Ok(v) => v,
            Err(ZkError::NoNode) => Vec::new(),
            Err(e) => bail!(e),
        };
        Ok(children
            .into_iter()
            .map(|path| path_to_worker_id(&path))
            .collect())
    }

    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> Result<HashMap<WorkerId, WorkerDescriptor>, Error> {
        let mut worker_descriptors: HashMap<WorkerId, WorkerDescriptor> = HashMap::new();

        for w in worker_ids {
            if let Ok((data, _)) = self.zk.get_data(&worker_id_to_path(&w), false).await {
                worker_descriptors.insert(w, serde_json::from_slice::<WorkerDescriptor>(&data)?);
            }
        }

        Ok(worker_descriptors)
    }

    async fn register_adapter(&self, _: SocketAddr) -> Result<Option<AdapterId>, Error> {
        todo!();
    }

    async fn get_adapters(&self) -> Result<HashSet<SocketAddr>, Error> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Url;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    #[tokio::test]
    #[ignore]
    async fn it_works() {
        let authority = Arc::new(
            ZookeeperAuthority::new("127.0.0.1:2181/consensus_it_works")
                .await
                .unwrap(),
        );
        assert!(authority.try_read::<u32>("/a").await.unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |_: Option<u32>| -> Result<u32, u32> { Ok(12) })
                .await
                .unwrap(),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a").await.unwrap(), Some(12));

        let payload = LeaderPayload {
            controller_uri: url::Url::parse("http://127.0.0.1:2181").unwrap(),
            nonce: 1,
        };
        let expected_leader_payload = payload.clone();
        assert_eq!(
            authority.become_leader(payload.clone()).await.unwrap(),
            Some(payload)
        );
        assert_eq!(
            &authority.get_leader().await.unwrap(),
            &expected_leader_payload
        );
        {
            let authority = authority.clone();
            let payload = LeaderPayload {
                controller_uri: url::Url::parse("http://127.0.0.1:2182").unwrap(),
                nonce: 2,
            };
            authority.become_leader(payload).await.unwrap();
        }

        assert_eq!(
            &authority.get_leader().await.unwrap(),
            &expected_leader_payload
        );
    }

    #[tokio::test]
    #[ignore]
    async fn retrieve_workers() {
        let authority = Arc::new(
            ZookeeperAuthority::new("127.0.0.1:2181/retrieve_workers")
                .await
                .unwrap(),
        );

        let worker = WorkerDescriptor {
            worker_uri: Url::parse("http://127.0.0.1").unwrap(),
            reader_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234),
            region: None,
            reader_only: false,
            volume_id: None,
        };

        let workers = authority.get_workers().await.unwrap();
        assert!(workers.is_empty());

        let worker_id = authority
            .register_worker(worker.clone())
            .await
            .unwrap()
            .unwrap();
        let workers = authority.get_workers().await.unwrap();
        assert_eq!(workers.len(), 1);
        assert!(workers.contains(&worker_id));
        assert_eq!(
            authority.worker_heartbeat(worker_id.clone()).await.unwrap(),
            AuthorityWorkerHeartbeatResponse::Alive
        );
        assert_eq!(
            worker,
            authority
                .worker_data(vec![worker_id.clone()])
                .await
                .unwrap()[&worker_id]
        );

        let worker_id = authority.register_worker(worker).await.unwrap().unwrap();
        let workers = authority.get_workers().await.unwrap();
        assert_eq!(workers.len(), 2);
        assert!(workers.contains(&worker_id));

        // Kill the session, this should remove the keys from the worker set.
        drop(authority);
        let authority = Arc::new(
            ZookeeperAuthority::new("127.0.0.1:2181/retrieve_workers")
                .await
                .unwrap(),
        );
        let workers = authority.get_workers().await.unwrap();
        assert_eq!(
            authority.worker_heartbeat(worker_id).await.unwrap(),
            AuthorityWorkerHeartbeatResponse::Failed
        );
        assert_eq!(workers.len(), 0);
    }
}
