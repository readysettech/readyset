//! LocalAuthority is replicating the abstraction over the different authority
//! systems (Zookeeper, Consul, etcd) but for a single process and in memory
//! instead of requiring a server.
//!
//! As such, it is maintaining a separation between the Store and Authority. In
//! a process, only exactly one store should ever exist with multiple
//! Authorities linked to it.
//!
//! The LocalAuthority stores effectively a "cache" of what it was able to get
//! from LocalAuthorityStore when it last checked in.
//!
//! The LocalAuthority supports ephemeral keys, instead of tying these keys to
//! an active session similar to Zookeeper, the authority will drop ephemeral
//! keys it created when it is dropped.
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{
    AdapterId, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult, LeaderPayload,
    WorkerDescriptor, WorkerId,
};
use noria_errors::internal_err;

pub const CONTROLLER_KEY: &str = "/controller";
pub const STATE_KEY: &str = "/state";
pub const WORKER_PATH: &str = "/workers";
/// The format used by yazi for compression and decompression of controller state.
const COMPRESSION_FORMAT: yazi::Format = yazi::Format::Zlib;
/// The compression level used for compression of controller state.
const COMPRESSION_LEVEL: yazi::CompressionLevel = yazi::CompressionLevel::Default;

struct LocalAuthorityStoreInner {
    keys: BTreeMap<String, Vec<u8>>,
    leader_epoch: u64,
    next_worker_id: u64,
}

/// LocalAuthorityStore represents a global consensus system but in a single
/// process. As such, only one Store should exist per process if this is being
/// used.
pub struct LocalAuthorityStore {
    // This must be a Mutex as this represents a single global consensus and as
    // such should block everything when being looked at for any reason. As well,
    // Condvar is dependent on Mutex.
    inner: Mutex<LocalAuthorityStoreInner>,
    cv: Condvar,
}

/// LocalAuthorityInner is the cache of information received from the store.
struct LocalAuthorityInner {
    known_leader_epoch: Option<u64>,
    /// Set of keys that should be removed when the authority is dropped.
    ephemeral_keys: HashSet<String>,
}

pub struct LocalAuthority {
    store: Arc<LocalAuthorityStore>,
    inner: RwLock<LocalAuthorityInner>,
}

impl LocalAuthorityStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LocalAuthorityStoreInner {
                keys: BTreeMap::default(),
                leader_epoch: 0,
                next_worker_id: 0,
            }),
            cv: Condvar::new(),
        }
    }

    fn inner_lock(&self) -> Result<MutexGuard<'_, LocalAuthorityStoreInner>, Error> {
        match self.inner.lock() {
            Ok(inner) => Ok(inner),
            Err(e) => bail!(internal_err(format!("mutex is poisoned: '{}'", e))),
        }
    }

    fn inner_wait<'a>(
        &self,
        inner_guard: MutexGuard<'a, LocalAuthorityStoreInner>,
    ) -> Result<MutexGuard<'a, LocalAuthorityStoreInner>, Error> {
        match self.cv.wait(inner_guard) {
            Ok(inner) => Ok(inner),
            Err(e) => bail!(internal_err(format!("mutex is poisoned: '{}'", e))),
        }
    }

    fn inner_notify_all(&self) {
        self.cv.notify_all()
    }
}

impl Default for LocalAuthorityStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalAuthority {
    pub fn new() -> Self {
        // Allowing creation of a store inline if one is not given as cloning an
        // authority reuses the store.
        let store = Arc::new(LocalAuthorityStore::new());
        Self::new_with_store(store)
    }

    pub fn new_with_store(store: Arc<LocalAuthorityStore>) -> Self {
        Self {
            store,
            inner: RwLock::new(LocalAuthorityInner {
                known_leader_epoch: None,
                ephemeral_keys: HashSet::new(),
            }),
        }
    }

    fn inner_read(&self) -> Result<RwLockReadGuard<'_, LocalAuthorityInner>, Error> {
        match self.inner.read() {
            Ok(inner) => Ok(inner),
            Err(e) => bail!(internal_err(format!("rwlock is poisoned: '{}'", e))),
        }
    }

    fn inner_write(&self) -> Result<RwLockWriteGuard<'_, LocalAuthorityInner>, Error> {
        match self.inner.write() {
            Ok(inner) => Ok(inner),
            Err(e) => bail!(internal_err(format!("rwlock is poisoned: '{}'", e))),
        }
    }
}

impl Default for LocalAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for LocalAuthority {
    fn drop(&mut self) {
        self.delete_ephemeral();
    }
}

// A custom clone is implemented for Authority since each Authority should have its own "connection"
// to the store. This makes sure the local information from the Authority is not copied around.
// In addition, each Authority should eventually get a unique ID and we will be implicitly
// creating and passing one in when that happens.
impl Clone for LocalAuthority {
    fn clone(&self) -> Self {
        Self::new_with_store(Arc::clone(&self.store))
    }
}

impl LocalAuthority {
    /// Writes a key ephemerally, returns an error if the key already exists.
    /// Requires a mutex guard already be held as callers will likely have
    /// checked properties of the inner store.
    fn write_ephemeral(
        &self,
        mut store_inner: MutexGuard<'_, LocalAuthorityStoreInner>,
        key: &str,
        val: Vec<u8>,
    ) -> Result<(), Error> {
        if let Entry::Vacant(e) = store_inner.keys.entry(key.to_owned()) {
            let mut inner = self.inner_write()?;
            inner.ephemeral_keys.insert(key.to_owned());
            e.insert(val);
            Ok(())
        } else {
            Err(anyhow!("Key already exists"))
        }
    }

    /// This helper function deletes all ephemeral keys. It should be used
    /// for local testing when simulating session failure.
    pub fn delete_ephemeral(&self) {
        // If either mutex is poisoned we don't do anything.
        if let Ok(mut store_inner) = self.store.inner_lock() {
            if let Ok(inner) = self.inner_read() {
                for k in &inner.ephemeral_keys {
                    store_inner.keys.remove(k);
                }
            }
        }
    }
}

#[async_trait]
impl AuthorityControl for LocalAuthority {
    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error> {
        let mut store_inner = self.store.inner_lock()?;

        if !store_inner.keys.contains_key(CONTROLLER_KEY) {
            store_inner
                .keys
                .insert(CONTROLLER_KEY.to_owned(), serde_json::to_vec(&payload)?);

            store_inner.leader_epoch += 1;

            let mut inner = self.inner_write()?;
            inner.known_leader_epoch = Some(store_inner.leader_epoch);

            self.store.inner_notify_all();
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    async fn surrender_leadership(&self) -> Result<(), Error> {
        let mut store_inner = self.store.inner_lock()?;

        assert!(store_inner.keys.remove(CONTROLLER_KEY).is_some());
        store_inner.leader_epoch += 1;

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);

        self.store.inner_notify_all();
        Ok(())
    }

    async fn get_leader(&self) -> Result<LeaderPayload, Error> {
        let mut store_inner = self.store.inner_lock()?;
        while !store_inner.keys.contains_key(CONTROLLER_KEY) {
            store_inner = self.store.inner_wait(store_inner)?;
        }

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);

        match store_inner.keys.get(CONTROLLER_KEY) {
            Some(data) => match serde_json::from_slice(data) {
                Ok(payload) => Ok(payload),
                Err(e) => bail!(internal_err(format!(
                    "failed to deserialize leader payload '{}'",
                    e
                ))),
            },
            None => {
                bail!(internal_err("no keys found when looking for leader"))
            }
        }
    }

    async fn try_get_leader(&self) -> Result<GetLeaderResult, Error> {
        let is_same_epoch = |leader_epoch: u64| -> Result<bool, Error> {
            let inner = self.inner_read()?;
            if let Some(epoch) = inner.known_leader_epoch {
                Ok(leader_epoch == epoch)
            } else {
                Ok(false)
            }
        };

        let store_inner = self.store.inner_lock()?;
        if is_same_epoch(store_inner.leader_epoch)? {
            return Ok(GetLeaderResult::Unchanged);
        }

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);
        if let Some(r) = store_inner
            .keys
            .get(CONTROLLER_KEY)
            .and_then(|data| serde_json::from_slice(data).ok())
        {
            inner.known_leader_epoch = Some(store_inner.leader_epoch);
            Ok(GetLeaderResult::NewLeader(r))
        } else {
            Ok(GetLeaderResult::NoLeader)
        }
    }

    fn can_watch(&self) -> bool {
        false
    }

    async fn watch_leader(&self) -> Result<(), Error> {
        unreachable!("LocalAuthority does not support `watch_leader`.");
    }

    async fn watch_workers(&self) -> Result<(), Error> {
        unreachable!("LocalAuthority does not support `watch_workers`.");
    }

    async fn try_read<P>(&self, path: &str) -> Result<Option<P>, Error>
    where
        P: DeserializeOwned,
    {
        let store_inner = self.store.inner_lock()?;
        Ok(store_inner
            .keys
            .get(path)
            .and_then(|data| serde_json::from_slice(data).ok()))
    }

    async fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        let mut store_inner = self.store.inner_lock()?;

        let r = f(store_inner
            .keys
            .get(path)
            .and_then(|data| serde_json::from_slice(data).ok()));
        if let Ok(ref p) = r {
            store_inner
                .keys
                .insert(path.to_owned(), serde_json::to_vec(&p)?);
        }
        Ok(r)
    }

    // The controller state, unlike other keys, is serialized using MessagePack and compressed.
    async fn update_controller_state<F, P, E>(&self, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send,
    {
        let mut store_inner = self.store.inner_lock()?;

        let r = f(store_inner.keys.get(STATE_KEY).and_then(|data| {
            let compr = yazi::decompress(data, COMPRESSION_FORMAT).ok();
            compr.and_then(|(data, _)| rmp_serde::from_slice(&data).ok())
        }));

        if let Ok(ref p) = r {
            let val = rmp_serde::to_vec(&p)?;
            let compressed = yazi::compress(&val, COMPRESSION_FORMAT, COMPRESSION_LEVEL).unwrap();
            store_inner.keys.insert(STATE_KEY.to_owned(), compressed);
        }
        Ok(r)
    }

    async fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        let store_inner = self.store.inner_lock()?;
        Ok(store_inner.keys.get(path).cloned())
    }

    async fn register_worker(&self, payload: WorkerDescriptor) -> Result<Option<WorkerId>, Error>
    where
        WorkerDescriptor: Serialize,
    {
        let mut store_inner = self.store.inner_lock()?;
        let next_id = store_inner.next_worker_id;
        store_inner.next_worker_id += 1;

        let path = WORKER_PATH.to_string() + "/" + &next_id.to_string();

        if !store_inner.keys.contains_key(&path) {
            self.write_ephemeral(store_inner, &path, serde_json::to_vec(&payload)?)?;
            return Ok(Some(next_id.to_string()));
        }

        Err(anyhow!("Error registering worker"))
    }

    async fn worker_heartbeat(
        &self,
        id: WorkerId,
    ) -> Result<AuthorityWorkerHeartbeatResponse, Error> {
        let store_inner = self.store.inner_lock()?;
        let path = WORKER_PATH.to_string() + "/" + &id;
        Ok(if store_inner.keys.contains_key(&path) {
            AuthorityWorkerHeartbeatResponse::Alive
        } else {
            AuthorityWorkerHeartbeatResponse::Failed
        })
    }

    async fn get_workers(&self) -> Result<HashSet<WorkerId>, Error> {
        let store_inner = self.store.inner_lock()?;
        let worker_prefix = WORKER_PATH.to_string();

        Ok(store_inner
            .keys
            .range(worker_prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&worker_prefix))
            .map(|(k, _)| {
                // The worker path is always in the format: /workers/<id>
                #[allow(clippy::unwrap_used)]
                k[(k.rfind('/').unwrap() + 1)..].to_owned()
            })
            .collect())
    }

    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> Result<HashMap<WorkerId, WorkerDescriptor>, Error> {
        let store_inner = self.store.inner_lock()?;
        let worker_prefix = WORKER_PATH.to_string();

        Ok(worker_ids
            .into_iter()
            .filter_map(|id| {
                store_inner
                    .keys
                    .get(&(worker_prefix.clone() + "/" + &id))
                    .to_owned()
                    .map(|data| (id, serde_json::from_slice(data).unwrap()))
            })
            .collect())
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
    use std::thread;
    use std::time::Duration;

    #[tokio::test]
    async fn it_works() {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(LocalAuthority::new_with_store(authority_store));
        assert!(authority.try_read::<u32>("/a").await.unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |arg: Option<u32>| -> Result<u32, u32> {
                    assert!(arg.is_none());
                    Ok(12)
                })
                .await
                .unwrap(),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a").await.unwrap(), Some(12));

        let payload = LeaderPayload {
            controller_uri: url::Url::parse("http://a").unwrap(),
            nonce: 1,
        };
        let leader_payload = payload.clone();
        assert_eq!(
            authority.become_leader(payload.clone()).await.unwrap(),
            Some(payload)
        );
        assert_eq!(authority.get_leader().await.unwrap(), leader_payload);
        {
            let authority = authority.clone();
            let payload = LeaderPayload {
                controller_uri: url::Url::parse("http://b").unwrap(),
                nonce: 2,
            };
            let _ = authority.become_leader(payload).await;
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader().await.unwrap(), leader_payload);
    }

    #[tokio::test]
    async fn retrieve_workers() {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(LocalAuthority::new_with_store(authority_store));

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
    }

    #[tokio::test]
    async fn test_register_deregister() {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(LocalAuthority::new_with_store(authority_store.clone()));

        let worker = WorkerDescriptor {
            worker_uri: Url::parse("http://127.0.0.1").unwrap(),
            reader_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234),
            region: None,
            reader_only: false,
            volume_id: None,
        };
        authority
            .register_worker(worker.clone())
            .await
            .unwrap()
            .unwrap();
        let workers = authority.get_workers().await.unwrap();
        assert_eq!(workers.len(), 1);
        drop(authority);
        let authority = Arc::new(LocalAuthority::new_with_store(authority_store));
        let workers = authority.get_workers().await.unwrap();
        assert_eq!(workers.len(), 0);
    }
}
