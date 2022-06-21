use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self};
use std::hash::{BuildHasher, Hash};
use std::iter::FromIterator;

use left_right::ReadGuard;
use noria::internal::IndexType;

use crate::inner::Inner;
use crate::values::Values;
use crate::{Error, Result};

mod read_ref;
pub use read_ref::{MapReadRef, ReadGuardIter};

pub use crate::inner::Miss;

/// A handle that may be used to read from the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible until the writer calls
/// [`publish`](crate::WriteHandle::publish). In other words, all operations performed on a
/// `ReadHandle` will *only* see writes to the map that preceeded the last call to `publish`.
pub struct ReadHandle<K, V, I, M = (), T = (), S = RandomState>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    pub(crate) handle: left_right::ReadHandle<Inner<K, V, M, T, S, I>>,
}

impl<K, V, I, M, T, S> fmt::Debug for ReadHandle<K, V, I, M, T, S>
where
    K: Ord + Clone + fmt::Debug,
    S: BuildHasher,
    M: fmt::Debug,
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle")
            .field("handle", &self.handle)
            .finish()
    }
}

impl<K, V, I, M, T, S> Clone for ReadHandle<K, V, I, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl<K, V, I, M, T, S> ReadHandle<K, V, I, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    pub(crate) fn new(handle: left_right::ReadHandle<Inner<K, V, M, T, S, I>>) -> Self {
        Self { handle }
    }
}

impl<K, V, I, M, T, S> ReadHandle<K, V, I, M, T, S>
where
    K: Ord + Clone + Hash,
    V: Eq + Hash,
    S: BuildHasher,
    M: Clone,
    T: Clone,
{
    fn enter_inner(&self) -> Result<ReadGuard<'_, Inner<K, V, M, T, S, I>>> {
        self.handle.enter().ok_or(Error::Destroyed)
    }

    /// Take out a guarded live reference to the read side of the map.
    ///
    /// This lets you perform more complex read operations on the map.
    ///
    /// While the reference lives, changes to the map cannot be published.
    ///
    /// If no publish has happened, or the map has been destroyed, this function returns `None`.
    ///
    /// See [`MapReadRef`].
    pub fn enter(&self) -> Result<MapReadRef<'_, K, V, I, M, T, S>> {
        let guard = self.enter_inner()?;
        if !guard.ready {
            return Err(Error::NotPublished);
        }
        Ok(MapReadRef { guard })
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.enter().map_or(0, |x| x.len())
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.enter().map_or(true, |x| x.is_empty())
    }

    /// Get the current meta value.
    pub fn meta(&self) -> Result<ReadGuard<'_, M>> {
        Ok(ReadGuard::map(self.enter_inner()?, |inner| &inner.meta))
    }

    /// Internal version of `get_and`
    fn get_raw<Q: ?Sized>(&self, key: &Q) -> Result<Option<ReadGuard<'_, Values<V>>>>
    where
        K: Borrow<Q>,
        Q: Ord + Hash,
    {
        let MapReadRef { guard } = self.enter()?;
        Ok(ReadGuard::try_map(guard, |inner| {
            let v = inner.data.get(key);
            if let Some(v) = v {
                inner.eviction_strategy.on_read(v.eviction_meta());
            }
            v
        }))
    }

    /// Returns a guarded reference to the values corresponding to the key.
    ///
    /// While the guard lives, changes to the map cannot be published.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// published by the writer. If no publish has happened, or the map has been destroyed, this
    /// function returns an [`Error`].
    #[inline]
    pub fn get<'rh, Q: ?Sized>(&'rh self, key: &'_ Q) -> Result<Option<ReadGuard<'rh, Values<V>>>>
    where
        K: Borrow<Q>,
        Q: Ord + Hash,
    {
        // call `borrow` here to monomorphize `get_raw` fewer times
        self.get_raw(key.borrow())
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, there are no guarantees to which element
    /// is returned.
    ///
    /// While the guard lives, changes to the map cannot be published.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or the map has been destroyed, this
    /// function returns an [`Error`].
    #[inline]
    pub fn first<'rh, Q: ?Sized>(&'rh self, key: &'_ Q) -> Result<Option<ReadGuard<'rh, V>>>
    where
        K: Borrow<Q>,
        Q: Ord + Clone + Hash,
    {
        let vs = if let Some(vs) = self.get_raw(key.borrow())? {
            vs
        } else {
            return Ok(None);
        };
        Ok(ReadGuard::try_map(vs, |x| x.first()))
    }

    /// Returns a guarded reference to the values corresponding to the key along with the map
    /// meta.
    ///
    /// While the guard lives, changes to the map cannot be published.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or the map has been destroyed, this
    /// function returns an [`Error`].
    ///
    /// If no values exist for the given key, `Ok(None, _)` is returned.
    pub fn meta_get<Q: ?Sized>(&self, key: &Q) -> Result<(Option<ReadGuard<'_, Values<V>>>, M)>
    where
        K: Borrow<Q>,
        Q: Ord + Clone + Hash,
    {
        let MapReadRef { guard } = self.enter()?;
        let meta = guard.meta.clone();
        let res = ReadGuard::try_map(guard, |inner| inner.data.get(key));
        Ok((res, meta))
    }

    /// Returns true if the [`WriteHandle`](crate::WriteHandle) has been dropped.
    pub fn was_dropped(&self) -> bool {
        self.handle.was_dropped()
    }

    /// Returns true if the map contains any values for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + Hash + ?Sized,
    {
        self.enter().map_or(false, |x| x.contains_key(key))
    }

    /// Read all values in the map, and transform them into a new collection.
    pub fn map_into<Map, Collector, Target>(&self, mut f: Map) -> Collector
    where
        Map: FnMut(&K, &Values<V>) -> Target,
        Collector: FromIterator<Target>,
    {
        self.enter()
            .iter()
            .flatten()
            .map(|(k, v)| f(k, v))
            .collect()
    }

    /// Returns the timestamp associated with the last write.
    ///
    /// Note that as this function does not return a read guard, the map may be mutated after
    /// reading the timestamp.
    ///
    /// If a guarded reference cannot be acquired to read the timestamp, an [`Error`] is returned.
    pub fn timestamp(&self) -> Result<T> {
        let MapReadRef { guard } = self.enter()?;
        Ok(guard.timestamp.clone())
    }

    /// Returns the index type of the underlying map, or None if no writes have been performed yet
    pub fn index_type(&self) -> Option<IndexType> {
        Some(self.handle.enter()?.data.index_type())
    }
}
