//! [`StandaloneAuthority`] is intended to be used within a single (standalone) process, and state
//! can't be shared between different processes, but within a single process it acts as any other
//! authority. As such each process has a singleton "Store" that keeps handles for all deployments
//! used by the process.
//!
//! Since RockDB can't be opened by multiple processes with the settings we use, it also guarantees
//! state won't be accidentally shared.
//!
//! The [`StandaloneAuthority`] will persist the controller state and any other key to a RocksDB
//! database on disk, while keeping leadership and workers data ephemeral.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::lazy::SyncOnceCell;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
use noria_errors::internal;
use parking_lot::{Mutex, RwLock};
use rocksdb::DB;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{
    AdapterId, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult, LeaderPayload,
    WorkerDescriptor, WorkerId,
};

/// Path to the controller state.
const STATE_KEY: &str = "state";

/// How often to check if a leader was elected
const LEADER_UPDATE_PERIOD: Duration = Duration::from_secs(1);

type SharedStateHandle = Arc<SharedState>;
/// A shared state store, that maps between a deployment name to its state
type SharedStore = Arc<Mutex<HashMap<String, SharedStateHandle>>>;
/// A singleton shared state store for the process, since all readyset workers inside a given
/// process are supposed to have access to the same shared state, it is implemented as a static.
static SHARED_STORE: SyncOnceCell<SharedStore> = SyncOnceCell::new();

struct SharedState {
    db: RwLock<DB>,
    leader: RwLock<Option<LeaderPayload>>,
    workers: RwLock<HashMap<WorkerId, WorkerDescriptor>>,
}

pub struct StandaloneAuthority {
    state: SharedStateHandle,
    last_leader: RwLock<Option<LeaderPayload>>,
    // To keep track of workers that were registered using this handle, so we can remove them when
    // dropped ("failed")
    own_workers: RwLock<HashSet<WorkerId>>,
}

impl StandaloneAuthority {
    pub fn new(path: &str, deployment: &str) -> Result<Self, Error> {
        let mut db_store = SHARED_STORE.get_or_init(Default::default).lock();
        // Open or create a database at path, with the name ${deployment}.auth. This will ensure
        // that the database has the same prefix as base tables, so convenient to remove, while
        // avoiding accidental name collision.
        let state = match db_store.entry(deployment.to_string()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let mut path: PathBuf = path.into();
                if !path.is_dir() {
                    std::fs::create_dir_all(&path)?;
                }
                path.push(deployment);
                path.set_extension("auth");

                let mut options = rocksdb::Options::default();
                options.create_if_missing(true);
                options.set_compression_type(rocksdb::DBCompressionType::Lz4);
                let db = DB::open(&options, path)?;

                let new_state = SharedState {
                    db: RwLock::new(db),
                    leader: Default::default(),
                    workers: Default::default(),
                };

                e.insert(Arc::new(new_state)).clone()
            }
        };

        Ok(StandaloneAuthority {
            state,
            last_leader: Default::default(),
            own_workers: Default::default(),
        })
    }
}

impl Drop for StandaloneAuthority {
    fn drop(&mut self) {
        let own_workers = self.own_workers.read();
        let mut global_workers = self.state.workers.write();
        own_workers.iter().for_each(|id| {
            global_workers.remove(id);
        });
    }
}

#[async_trait]
impl AuthorityControl for StandaloneAuthority {
    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error> {
        // Leadership info is not persisted outside the process, it is kept in a shared state
        match &mut *self.state.leader.write() {
            Some(_) => Ok(None),
            empty_state => {
                *empty_state = Some(payload.clone());
                Ok(Some(payload))
            }
        }
    }

    async fn surrender_leadership(&self) -> Result<(), Error> {
        self.state.leader.write().take();
        Ok(())
    }

    async fn get_leader(&self) -> Result<LeaderPayload, Error> {
        loop {
            if let Some(leader) = &*self.state.leader.read() {
                return Ok(leader.clone());
            }
            tokio::time::sleep(LEADER_UPDATE_PERIOD).await;
        }
    }

    async fn try_get_leader(&self) -> Result<GetLeaderResult, Error> {
        let last_leader = self.last_leader.read().clone();

        match &*self.state.leader.read() {
            Some(leader) if last_leader.as_ref() == Some(leader) => Ok(GetLeaderResult::Unchanged),
            Some(leader) => {
                *self.last_leader.write() = Some(leader.clone());
                Ok(GetLeaderResult::NewLeader(leader.clone()))
            }
            None => Ok(GetLeaderResult::NoLeader),
        }
    }

    fn can_watch(&self) -> bool {
        false
    }

    async fn watch_leader(&self) -> Result<(), Error> {
        internal!("StandaloneAuthority does not support `watch_leader`.");
    }

    async fn watch_workers(&self) -> Result<(), Error> {
        internal!("StandaloneAuthority does not support `watch_workers`.");
    }

    /// Do a non-blocking read at the indicated key.
    async fn try_read<P>(&self, path: &str) -> Result<Option<P>, Error>
    where
        P: DeserializeOwned,
    {
        Ok(self
            .try_read_raw(path)
            .await?
            .map(|v| rmp_serde::from_slice(&v))
            .transpose()?)
    }

    async fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.state.db.read().get_pinned(path)?.map(|v| v.to_vec()))
    }

    async fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        let db = self.state.db.write();
        let current_val = db
            .get_pinned(path)?
            .map(|v| rmp_serde::from_slice(&v))
            .transpose()?;

        let res = f(current_val);

        if let Ok(updated_val) = &res {
            let new_val = rmp_serde::to_vec(&updated_val)?;
            db.put(path, new_val)?;
        }

        Ok(res)
    }

    /// Register a worker with their descriptor. Returns a unique identifier that represents this
    /// worker if successful.
    async fn register_worker(&self, payload: WorkerDescriptor) -> Result<Option<WorkerId>, Error>
    where
        WorkerDescriptor: Serialize,
    {
        let worker_id = uuid::Uuid::new_v4().to_string();
        self.state
            .workers
            .write()
            .insert(worker_id.clone(), payload);
        self.own_workers.write().insert(worker_id.clone());
        Ok(Some(worker_id))
    }

    /// Workers run this function on a regular cadence to confirm current state. Returns a response
    /// for next actions for this particular worker or if it should continue being a worker.
    async fn worker_heartbeat(
        &self,
        id: WorkerId,
    ) -> Result<AuthorityWorkerHeartbeatResponse, Error> {
        if self.state.workers.read().contains_key(&id) {
            Ok(AuthorityWorkerHeartbeatResponse::Alive)
        } else {
            Ok(AuthorityWorkerHeartbeatResponse::Failed)
        }
    }

    /// Retrieves the current set of workers from the authority.
    async fn get_workers(&self) -> Result<HashSet<WorkerId>, Error> {
        Ok(self.state.workers.read().keys().cloned().collect())
    }

    /// Retrieves the worker data for a set of workers.
    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> Result<HashMap<WorkerId, WorkerDescriptor>, Error> {
        let workers = self.state.workers.read();

        Ok(worker_ids
            .into_iter()
            .filter_map(|id| workers.get(&id).map(|p| (id, p.clone())))
            .collect())
    }

    async fn update_controller_state<F, U, P: 'static, E>(
        &self,
        f: F,
        _u: U,
    ) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        U: Send,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        self.read_modify_write(STATE_KEY, f).await
    }

    async fn register_adapter(&self, _: SocketAddr) -> Result<Option<AdapterId>, Error> {
        internal!("StandaloneAuthority does not support `register_adapter`.");
    }

    /// Retrieves the current set of adapter endpoints from the authority.
    async fn get_adapters(&self) -> Result<HashSet<SocketAddr>, Error> {
        internal!("StandaloneAuthority does not support `get_adapters`.");
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use reqwest::Url;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let dir = tempdir().unwrap();

        let authority =
            Arc::new(StandaloneAuthority::new(dir.path().to_str().unwrap(), "it_works").unwrap());
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
    async fn retrieve_workers() {
        let dir = tempdir().unwrap();

        let authority = Arc::new(
            StandaloneAuthority::new(dir.path().to_str().unwrap(), "retrieve_workers").unwrap(),
        );

        let worker = WorkerDescriptor {
            worker_uri: Url::parse("http://127.0.0.1").unwrap(),
            reader_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234),
            domain_scheduling_config: Default::default(),
            leader_eligible: true,
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

        let authority2 = Arc::new(
            StandaloneAuthority::new(dir.path().to_str().unwrap(), "retrieve_workers").unwrap(),
        );

        let workers = authority2.get_workers().await.unwrap();
        assert_eq!(
            authority.worker_heartbeat(worker_id.clone()).await.unwrap(),
            AuthorityWorkerHeartbeatResponse::Alive
        );
        assert_eq!(workers.len(), 2);

        // Kill the session, this should remove the keys from the worker set.
        drop(authority);

        let workers = authority2.get_workers().await.unwrap();
        assert_eq!(
            authority2.worker_heartbeat(worker_id).await.unwrap(),
            AuthorityWorkerHeartbeatResponse::Failed
        );
        assert_eq!(workers.len(), 0);
    }
}
