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
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Error;
use async_trait::async_trait;
#[cfg(feature = "failure_injection")]
use failpoint_macros::set_failpoint;
use parking_lot::{Mutex, RwLock};
#[cfg(feature = "failure_injection")]
use readyset_errors::ReadySetError;
use readyset_errors::{internal, internal_err, set_failpoint_return_err, ReadySetResult};
use replication_offset::ReplicationOffset;
use rocksdb::DB;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{
    AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult, LeaderPayload,
    WorkerDescriptor, WorkerId,
};
#[cfg(feature = "failure_injection")]
use crate::failpoints;

/// Path to the controller state.
const STATE_KEY: &str = "state";
/// Path to the schema replication offset
const SCHEMA_REPLICATION_OFFSET_KEY: &str = "state";

/// How often to check if a leader was elected
const LEADER_UPDATE_PERIOD: Duration = Duration::from_secs(1);

type SharedStateHandle = Arc<SharedState>;
/// A shared state store, that maps between a deployment name to its state
type SharedStore = Arc<Mutex<HashMap<String, SharedStateHandle>>>;
/// A singleton shared state store for the process, since all readyset workers inside a given
/// process are supposed to have access to the same shared state, it is implemented as a static.
static SHARED_STORE: OnceLock<SharedStore> = OnceLock::new();

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
    async fn init(&self) -> ReadySetResult<()> {
        Ok(())
    }

    async fn become_leader(&self, payload: LeaderPayload) -> ReadySetResult<Option<LeaderPayload>> {
        // Leadership info is not persisted outside the process, it is kept in a shared state
        match &mut *self.state.leader.write() {
            Some(_) => Ok(None),
            empty_state => {
                *empty_state = Some(payload.clone());
                Ok(Some(payload))
            }
        }
    }

    async fn surrender_leadership(&self) -> ReadySetResult<()> {
        self.state.leader.write().take();
        Ok(())
    }

    async fn get_leader(&self) -> ReadySetResult<LeaderPayload> {
        loop {
            if let Some(leader) = &*self.state.leader.read() {
                return Ok(leader.clone());
            }
            tokio::time::sleep(LEADER_UPDATE_PERIOD).await;
        }
    }

    async fn try_get_leader(&self) -> ReadySetResult<GetLeaderResult> {
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

    async fn watch_leader(&self) -> ReadySetResult<()> {
        internal!("StandaloneAuthority does not support `watch_leader`.");
    }

    async fn watch_workers(&self) -> ReadySetResult<()> {
        internal!("StandaloneAuthority does not support `watch_workers`.");
    }

    /// Do a non-blocking read at the indicated key.
    async fn try_read<P>(&self, path: &str) -> ReadySetResult<Option<P>>
    where
        P: DeserializeOwned,
    {
        Ok(self
            .try_read_raw(path)
            .await?
            .map(|v| rmp_serde::from_slice(&v))
            .transpose()?)
    }

    async fn try_read_raw(&self, path: &str) -> ReadySetResult<Option<Vec<u8>>> {
        Ok(self
            .state
            .db
            .read()
            .get_pinned(path)
            .map_err(|e| internal_err!("RocksDB error: {e}"))?
            .map(|v| v.to_vec()))
    }

    async fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> ReadySetResult<Result<P, E>>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        let db = self.state.db.write();
        let current_val = db
            .get_pinned(path)
            .map_err(|e| internal_err!("RocksDB error: {e}"))?
            .map(|v| rmp_serde::from_slice(&v))
            .transpose()?;

        let res = f(current_val);

        if let Ok(updated_val) = &res {
            let new_val = rmp_serde::to_vec(&updated_val)?;
            db.put(path, new_val)
                .map_err(|e| internal_err!("RocksDB error: {e}"))?;
        }

        Ok(res)
    }

    /// Register a worker with their descriptor. Returns a unique identifier that represents this
    /// worker if successful.
    async fn register_worker(&self, payload: WorkerDescriptor) -> ReadySetResult<Option<WorkerId>>
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
    ) -> ReadySetResult<AuthorityWorkerHeartbeatResponse> {
        if self.state.workers.read().contains_key(&id) {
            Ok(AuthorityWorkerHeartbeatResponse::Alive)
        } else {
            Ok(AuthorityWorkerHeartbeatResponse::Failed)
        }
    }

    /// Retrieves the current set of workers from the authority.
    async fn get_workers(&self) -> ReadySetResult<HashSet<WorkerId>> {
        Ok(self.state.workers.read().keys().cloned().collect())
    }

    /// Retrieves the worker data for a set of workers.
    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> ReadySetResult<HashMap<WorkerId, WorkerDescriptor>> {
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
    ) -> ReadySetResult<Result<P, E>>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        U: Send,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        set_failpoint_return_err!(failpoints::LOAD_CONTROLLER_STATE);
        self.read_modify_write(STATE_KEY, f).await
    }

    async fn overwrite_controller_state<S>(&self, state: S) -> ReadySetResult<()>
    where
        S: Send + Serialize + 'static,
    {
        let db = self.state.db.write();
        db.put(STATE_KEY, rmp_serde::to_vec(&state)?)
            .map_err(|e| internal_err!("RocksDB error: {e}"))
    }

    async fn set_schema_replication_offset<R>(
        &self,
        offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        let db = self.state.db.write();
        db.put(SCHEMA_REPLICATION_OFFSET_KEY, rmp_serde::to_vec(&offset)?)
            .map_err(|e| internal_err!("RocksDB error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use readyset_data::Dialect;
    use reqwest::Url;
    use tempfile::tempdir;

    use super::*;
    use crate::consensus::CacheDDLRequest;

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
            controller_uri: url::Url::parse("http://127.0.0.1:8500").unwrap(),
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

    #[tokio::test]
    async fn overwrite_controller_state() {
        let dir = tempdir().unwrap();
        let authority =
            StandaloneAuthority::new(dir.path().to_str().unwrap(), "retrieve_workers").unwrap();

        authority
            .become_leader(LeaderPayload {
                controller_uri: url::Url::parse("http://127.0.0.1:8500").unwrap(),
                nonce: 1,
            })
            .await
            .unwrap();

        async fn incr_state(authority: &StandaloneAuthority) -> u32 {
            authority
                .update_controller_state(
                    |n: Option<u32>| -> Result<u32, ()> {
                        match n {
                            None => Ok(0),
                            Some(mut n) => Ok({
                                n += 1;
                                n
                            }),
                        }
                    },
                    |_| {},
                )
                .await
                .unwrap()
                .unwrap()
        }

        for _ in 0..5 {
            incr_state(&authority).await;
        }

        authority.overwrite_controller_state(1).await.unwrap();
        assert_eq!(incr_state(&authority).await, 2);
    }

    #[tokio::test]
    async fn create_cache_statements() {
        let dir = tempdir().unwrap();
        let authority = Arc::new(
            StandaloneAuthority::new(dir.path().to_str().unwrap(), "create_cache_statements")
                .unwrap(),
        );

        assert!(authority.cache_ddl_requests().await.unwrap().is_empty());

        const STMTS: &[&str] = &["a", "b", "c", "d", "e", "f"];
        let mut futs = STMTS
            .iter()
            .map(|stmt| {
                let authority = authority.clone();
                let ddl_req = CacheDDLRequest {
                    unparsed_stmt: stmt.to_string(),
                    schema_search_path: vec![],
                    dialect: Dialect::DEFAULT_POSTGRESQL,
                };
                tokio::spawn(async move { authority.add_cache_ddl_request(ddl_req).await.unwrap() })
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = futs.next().await {
            res.unwrap();
        }

        let mut stmts = authority
            .cache_ddl_requests()
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.unparsed_stmt)
            .collect::<Vec<_>>();
        stmts.sort();
        assert_eq!(stmts, STMTS);
    }
}
