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
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::CONTROLLER_KEY;
use super::{Authority, LeaderPayload};
use crate::errors::internal_err;

struct LocalAuthorityStoreInner {
    keys: BTreeMap<String, Vec<u8>>,
    leader_epoch: u64,
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

// A custom clone is implemented for Authority since each Authority should have its own "connection"
// to the store. This makes sure the local information from the Authority is not copied around.
// In addition, each Authority should eventually get a unique ID and we will be implicitly
// creating and passing one in when that happens.
impl Clone for LocalAuthority {
    fn clone(&self) -> Self {
        Self::new_with_store(Arc::clone(&self.store))
    }
}

impl Authority for LocalAuthority {
    fn become_leader(&self, payload: LeaderPayload) -> Result<Option<LeaderPayload>, Error> {
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

    fn surrender_leadership(&self) -> Result<(), Error> {
        let mut store_inner = self.store.inner_lock()?;

        assert!(store_inner.keys.remove(CONTROLLER_KEY).is_some());
        store_inner.leader_epoch += 1;

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);

        self.store.inner_notify_all();
        Ok(())
    }

    fn get_leader(&self) -> Result<LeaderPayload, Error> {
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

    fn try_get_leader(&self) -> Result<Option<LeaderPayload>, Error> {
        let store_inner = self.store.inner_lock()?;

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);

        Ok(store_inner
            .keys
            .get(CONTROLLER_KEY)
            .and_then(|data| serde_json::from_slice(data).ok()))
    }

    fn await_new_leader(&self) -> Result<Option<LeaderPayload>, Error> {
        let is_same_epoch = |leader_epoch: u64| -> Result<bool, Error> {
            let inner = self.inner_read()?;
            if let Some(epoch) = inner.known_leader_epoch {
                Ok(leader_epoch == epoch)
            } else {
                Ok(false)
            }
        };
        let mut store_inner = self.store.inner_lock()?;
        while is_same_epoch(store_inner.leader_epoch)?
            && store_inner.keys.contains_key(CONTROLLER_KEY)
        {
            store_inner = self.store.inner_wait(store_inner)?;
        }

        let mut inner = self.inner_write()?;
        inner.known_leader_epoch = Some(store_inner.leader_epoch);

        Ok(store_inner
            .keys
            .get(CONTROLLER_KEY)
            .and_then(|data| serde_json::from_slice(data).ok()))
    }

    fn try_read<P>(&self, path: &str) -> Result<Option<P>, Error>
    where
        P: DeserializeOwned,
    {
        let store_inner = self.store.inner_lock()?;
        Ok(store_inner
            .keys
            .get(path)
            .and_then(|data| serde_json::from_slice(data).ok()))
    }

    fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned,
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

    fn try_read_raw(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        let store_inner = self.store.inner_lock()?;
        Ok(store_inner.keys.get(path).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn it_works() {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(LocalAuthority::new_with_store(authority_store));
        assert!(authority.try_read::<u32>("/a").unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |arg: Option<u32>| -> Result<u32, u32> {
                    assert!(arg.is_none());
                    Ok(12)
                })
                .unwrap(),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a").unwrap(), Some(12));

        let payload = LeaderPayload {
            controller_uri: url::Url::parse("http://a").unwrap(),
            nonce: 1,
        };
        let leader_payload = payload.clone();
        assert_eq!(
            authority.become_leader(payload.clone()).unwrap(),
            Some(payload)
        );
        assert_eq!(authority.get_leader().unwrap(), leader_payload);
        {
            let authority = authority.clone();
            let payload = LeaderPayload {
                controller_uri: url::Url::parse("http://b").unwrap(),
                nonce: 2,
            };
            thread::spawn(move || authority.become_leader(payload));
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader().unwrap(), leader_payload);
    }
}
