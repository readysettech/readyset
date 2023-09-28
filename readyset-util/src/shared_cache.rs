//! Data structures providing a shared cache with thread-local memoization

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use futures::Future;
use tokio::sync::RwLock;

/// A shared cache providing a map from keys of type K to values of type V
///
/// This is implemented internally via a [`HashMap`] from `K` to `V`, which can be configured using
/// the hasher `S`.
///
/// New thread-local versions of this shared cache can be created using [`new_local`],
/// [`into_local`], etc. These local versions allow memoized lookup of cached values for keys
#[derive(Debug)]
pub struct SharedCache<K, V, S = RandomState> {
    inner: Arc<RwLock<HashMap<K, V, S>>>,
}

impl<K, V, S> Clone for SharedCache<K, V, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S> Default for SharedCache<K, V, S>
where
    S: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> SharedCache<K, V, S> {
    /// Creates an empty [`SharedCache`] which will use the given hash builder to hash
    /// keys.
    pub fn with_hasher(hash_builder: S) -> Self {
        SharedCache {
            inner: Arc::new(RwLock::new(HashMap::with_hasher(hash_builder))),
        }
    }

    /// Creates an empty [`SharedCache`] with at least the specified capacity, using
    /// hasher to hash the keys.
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        SharedCache {
            inner: Arc::new(RwLock::new(HashMap::with_capacity_and_hasher(
                capacity,
                hash_builder,
            ))),
        }
    }

    /// Creates a new local version of the given [`SharedCache`] which will use the given hash
    /// builder to hash keys.
    pub fn new_local_with_hasher(&self, hash_builder: S) -> LocalCache<K, V, S> {
        LocalCache {
            shared: self.clone(),
            local: HashMap::with_hasher(hash_builder),
        }
    }

    /// Convert this [`SharedCache`] into a new local version which will use the given hash builder
    /// to hash keys.
    pub fn into_local_with_hasher(self, hash_builder: S) -> LocalCache<K, V, S> {
        LocalCache {
            shared: self,
            local: HashMap::with_hasher(hash_builder),
        }
    }
}

impl<K, V, S> SharedCache<K, V, S>
where
    S: Default,
{
    /// Create a new empty [`SharedCache`]
    pub fn new() -> Self {
        SharedCache {
            inner: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    /// Create a new [`SharedCache`] with the given capacity
    pub fn with_capacity(capacity: usize) -> Self {
        SharedCache {
            inner: Arc::new(RwLock::new(HashMap::with_capacity_and_hasher(
                capacity,
                Default::default(),
            ))),
        }
    }

    /// Creates a new local version of the given [`SharedCache`].
    pub fn new_local(&self) -> LocalCache<K, V, S>
    where
        S: Default,
    {
        LocalCache {
            shared: self.clone(),
            local: Default::default(),
        }
    }

    /// Convert this [`SharedCache`] into a new local version
    pub fn into_local(self) -> LocalCache<K, V, S>
    where
        S: Default,
    {
        LocalCache {
            shared: self,
            local: Default::default(),
        }
    }

    /// Creates a new local version of the given [`SharedCache`] with at least the given capacity.
    pub fn new_local_with_capacity(&self, capacity: usize) -> LocalCache<K, V, S>
    where
        S: Default,
    {
        LocalCache {
            shared: self.clone(),
            local: HashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    /// Convert this [`SharedCache`] into a new local version with at least the given capacity.
    pub fn into_local_with_capacity(self, capacity: usize) -> LocalCache<K, V, S>
    where
        S: Default,
    {
        LocalCache {
            shared: self,
            local: HashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }
}

/// Mode to use when inserting missing values in the cache as part of
/// [`LocalCache::get_mut_or_try_insert_with`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertMode {
    /// Multiple tasks may try to call the function to obtain a new value at once, and whichever
    /// finishes first wins.
    Shared,
    /// Only one task per key may try to call the function to obtain a new value at once
    Locked,
}

/// A thread-local version of a [`SharedCache`].
///
/// This data structure allows looking up values which are present in the associated
/// [`SharedCache`], after which subsequent lookups of the same key will not use synchronization.
#[derive(Debug, Clone)]
pub struct LocalCache<K, V, S = RandomState> {
    shared: SharedCache<K, V, S>,
    local: HashMap<K, V, S>,
}

impl<K, V, S> LocalCache<K, V, S>
where
    K: Hash + Eq,
    V: Clone,
    S: BuildHasher,
{
    /// Obtain an exclusive reference to the value associated with the given key.
    ///
    /// The first time this method is called per-[`LocalCache`], it will use synchronization to
    /// access the key in the shared cache. After that, subsequent lookups will be local
    ///
    /// Note that any mutations to the value will only be reflected in this thread's local version.
    pub async fn get_mut<'a, Q>(&'a mut self, key: &Q) -> Option<&'a mut V>
    where
        Q: ?Sized + Hash + Eq + ToOwned<Owned = K>,
        K: Borrow<Q>,
    {
        if !self.local.contains_key(key) {
            let shared = self.shared.inner.read().await;
            let v = shared.get(key)?;
            self.local.insert(key.to_owned(), v.clone());
        }

        self.local.get_mut(key)
    }

    /// Insert a new key-value pair into the shared and local versions of this cache.
    pub async fn insert(&mut self, key: K, val: V)
    where
        K: Clone,
    {
        self.local.insert(key.clone(), val.clone());
        self.shared.inner.write().await.insert(key, val);
    }

    /// Remove a key from the shared and local versions of this cache
    pub async fn remove<Q>(&mut self, key: &Q)
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.local.remove(key);
        self.shared.inner.write().await.remove(key);
    }

    /// Remove the entry associated with the given value from the shared and local versions of this
    /// cache. O(size).
    pub async fn remove_val<U>(&mut self, val: &U)
    where
        V: PartialEq<U>,
    {
        self.local.retain(|_, v| v != val);
        self.shared.inner.write().await.retain(|_, v| v != val);
    }

    /// Completely clear the shared and local versions of this cache
    pub async fn clear(&mut self) {
        self.local.clear();
        self.shared.inner.write().await.clear();
    }

    /// Return the *key* associated with the given value within either the local, or the shared
    /// versions of this cache. O(size), and performs no memoization
    pub async fn key_for_val<'a, U>(&'a self, val: &U) -> Option<K>
    where
        V: PartialEq<U>,
        K: Clone,
    {
        if let Some((k, _)) = self.local.iter().find(|(_, v)| *v == val) {
            return Some(k.clone());
        }

        self.shared
            .inner
            .read()
            .await
            .iter()
            .find(|(_, v)| *v == val)
            .map(|(k, _)| k.clone())
    }

    /// Look up the value associated with the given key, and if it is not present, insert the value
    /// returned by the given future into the shared and local versions of the cache.
    ///
    /// `mode` controls the behavior if multiple simultaneous tasks call this method on different
    /// local versions of the same cache. See the documentation for [`InsertMode`] for the supported
    /// options.
    pub async fn get_mut_or_try_insert_with<'a, Q, F, E>(
        &'a mut self,
        key: &Q,
        mode: InsertMode,
        f: F,
    ) -> Result<&'a mut V, E>
    where
        Q: ?Sized + Hash + Eq + ToOwned<Owned = K>,
        K: Borrow<Q>,
        F: Future<Output = Result<V, E>>,
    {
        if !self.local.contains_key(key) {
            let shared = self.shared.inner.read().await;
            if let Some(v) = shared.get(key) {
                self.local.insert(key.to_owned(), v.clone());
            }
        }

        if !self.local.contains_key(key) {
            match mode {
                InsertMode::Shared => {
                    let val = f.await?;
                    let val = self
                        .shared
                        .inner
                        .write()
                        .await
                        .entry(key.to_owned())
                        .or_insert(val)
                        .clone();
                    self.local.insert(key.to_owned(), val);
                }
                InsertMode::Locked => {
                    let mut shared = self.shared.inner.write().await;
                    let val = f.await?;
                    let val = shared.entry(key.to_owned()).or_insert(val).clone();
                    self.local.insert(key.to_owned(), val);
                }
            }
        }

        Ok(self.local.get_mut(key).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn get_mut_or_try_insert_with_locked() {
        let futures_run = Arc::new(AtomicU8::new(0));
        let shared: SharedCache<String, i32> = SharedCache::new();
        let mut tasks = (0..1)
            .map(|_| {
                let mut local = shared.new_local();
                let futures_run = futures_run.clone();
                tokio::spawn(async move {
                    local
                        .get_mut_or_try_insert_with("a", InsertMode::Locked, async move {
                            futures_run.fetch_add(1, Ordering::Relaxed);
                            Result::<i32, String>::Ok(1)
                        })
                        .await
                        .unwrap();
                })
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = tasks.next().await {
            res.unwrap();
        }

        assert_eq!(futures_run.load(Ordering::Relaxed), 1);
        assert_eq!(*shared.new_local().get_mut("a").await.unwrap(), 1);
    }
}
