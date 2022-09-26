use std::borrow::Borrow;
use std::collections::btree_map;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::RangeBounds;

use left_right::ReadGuard;

use crate::inner::{Inner, Miss};
use crate::values::Values;
use crate::EvictionStrategy;
// To make [`WriteHandle`] and friends work.
#[cfg(doc)]
use crate::WriteHandle;

/// A live reference into the read half of an evmap.
///
/// As long as this lives, changes to the map being read cannot be published. If a writer attempts
/// to call [`WriteHandle::publish`], that call will block until this is dropped.
///
/// Since the map remains immutable while this lives, the methods on this type all give you
/// unguarded references to types contained in the map.
pub struct MapReadRef<'rh, K, V, I, M = (), T = (), S = RandomState>
where
    K: Ord + Clone,
    V: Eq + Hash,
    T: Clone,
{
    pub(super) guard: ReadGuard<'rh, Inner<K, V, M, T, S, I>>,
}

impl<'rh, K, V, I, M, T, S> fmt::Debug for MapReadRef<'rh, K, V, I, M, T, S>
where
    K: Ord + Clone,
    V: Eq + Hash,
    S: BuildHasher,
    K: fmt::Debug,
    M: fmt::Debug,
    V: fmt::Debug,
    T: fmt::Debug + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapReadRef")
            .field("guard", &self.guard)
            .finish()
    }
}

impl<'rh, K, V, I, M, T, S> MapReadRef<'rh, K, V, I, M, T, S>
where
    K: Ord + Clone + Hash,
    V: Eq + Hash,
    S: BuildHasher,
    T: Clone,
{
    /// Iterate over all key + valuesets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn iter(&self) -> ReadGuardIter<'_, K, V> {
        ReadGuardIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Constructs a double-ended iterator over a sub-range of key + valueset elements in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    ///
    /// # Panics
    ///
    /// Panics if the underlying map is not a [`BTreeMap`](readyset::internal::IndexType::BTreeMap).
    pub fn range<R, Q>(&self, range: &R) -> Result<RangeIter<'_, K, V>, Miss<K>>
    where
        R: RangeBounds<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        K: Borrow<Q>,
    {
        self.guard.data.range(range).map(|iter| RangeIter {
            iter,
            eviction_strategy: &self.guard.eviction_strategy,
        })
    }

    /// Iterate over all keys in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn keys(&self) -> KeysIter<'_, K, V> {
        KeysIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Iterate over all value sets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn values(&self) -> ValuesIter<'_, K, V> {
        ValuesIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.guard.data.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.guard.data.is_empty()
    }

    /// Get the current meta value.
    pub fn meta(&self) -> &M {
        &self.guard.meta
    }

    /// Returns a reference to the values corresponding to the key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// published by the writer. If no publish has happened, or the map has been destroyed, this
    /// function returns `None`.
    pub fn get<'a, Q>(&'a self, key: &'_ Q) -> Option<&'a Values<V>>
    where
        K: Borrow<Q> + Ord + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        self.guard.data.get(key).map(|v| {
            self.guard.eviction_strategy.on_read(v.eviction_meta());
            v
        })
    }

    /// Returns a guarded reference to the smallest value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, there are no guarantees to which element
    /// is returned.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// published by the writer. If no publish has happened, or the map has been destroyed, this
    /// function returns `None`.
    pub fn first<'a, Q: ?Sized>(&'a self, key: &'_ Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Ord + Hash,
    {
        self.guard.data.get(key).and_then(|values| values.first())
    }

    /// Returns true if the map contains any values for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Ord + Clone,
        Q: ?Sized + Hash + Ord,
    {
        self.guard.data.contains_key(key)
    }

    /// Returns true if the map contains the specified range of keys.
    pub fn contains_range<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<K>,
    {
        self.guard.data.contains_range(range)
    }

    /// Returns true if the map contains at least part of the specified range of keys.
    pub fn overlaps_range<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<K>,
    {
        self.guard.data.overlaps_range(range)
    }
}

impl<'rh, K, Q, V, M, T, S, I> std::ops::Index<&'_ Q> for MapReadRef<'rh, K, V, I, M, T, S>
where
    K: Ord + Clone + Borrow<Q> + Hash,
    V: Eq + Hash + Default,
    Q: Ord + ?Sized + Hash + ToOwned<Owned = K>,
    S: BuildHasher,
    T: Clone,
{
    type Output = Values<V>;
    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<'rg, 'rh, K, V, M, T, S, I> IntoIterator for &'rg MapReadRef<'rh, K, V, I, M, T, S>
where
    K: Ord + Clone + Hash,
    V: Eq + Hash,
    S: BuildHasher,
    T: Clone,
{
    type Item = (&'rg K, &'rg Values<V>);
    type IntoIter = ReadGuardIter<'rg, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An [`Iterator`] over keys and values in the evmap.
pub struct ReadGuardIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V>,
}

impl<'rg, K, V> fmt::Debug for ReadGuardIter<'rg, K, V>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadGuardIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V> Iterator for ReadGuardIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = (&'rg K, &'rg Values<V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// An [`Iterator`] over keys.
pub struct KeysIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V>,
}

impl<'rg, K, V> fmt::Debug for KeysIter<'rg, K, V>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("KeysIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V> Iterator for KeysIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = &'rg K;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

/// An [`Iterator`] over a range of keys.
pub struct RangeIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: btree_map::Range<'rg, K, Values<V>>,
    eviction_strategy: &'rg EvictionStrategy,
}

impl<'rg, K, V> fmt::Debug for RangeIter<'rg, K, V>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RangeIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V> Iterator for RangeIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = (&'rg K, &'rg Values<V>);
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next()?;
        self.eviction_strategy.on_read(next.1.eviction_meta());
        Some(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// An [`Iterator`] over value sets.
pub struct ValuesIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V>,
}

impl<'rg, K, V> fmt::Debug for ValuesIter<'rg, K, V>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ValuesIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V> Iterator for ValuesIter<'rg, K, V>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = &'rg Values<V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}
