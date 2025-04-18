use std::borrow::Borrow;
use std::collections::btree_map;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};

use left_right::ReadGuard;
use readyset_util::ranges::RangeBounds;

use crate::inner::{Inner, Miss};
use crate::values::Values;
use crate::EvictionStrategy;
use crate::InsertionOrder;

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
pub struct MapReadRef<'rh, K, V, I, M = (), S = RandomState>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    pub(super) guard: ReadGuard<'rh, Inner<K, V, M, S, I>>,
}

impl<K, V, I, M, S> fmt::Debug for MapReadRef<'_, K, V, I, M, S>
where
    K: Ord + Clone,
    V: Eq + Hash,
    S: BuildHasher,
    K: fmt::Debug,
    M: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapReadRef")
            .field("guard", &self.guard)
            .finish()
    }
}

impl<K, V, I, M, S> MapReadRef<'_, K, V, I, M, S>
where
    K: Ord + Clone + Hash,
    V: Eq + Hash,
    I: InsertionOrder<V>,
    S: BuildHasher,
{
    /// Iterate over all key + valuesets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn iter(&self) -> ReadGuardIter<'_, K, V, I> {
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
    /// Panics if the underlying map is not a
    /// [`BTreeMap`](readyset_client::internal::IndexType::BTreeMap).
    pub fn range<R, Q>(&self, range: &R) -> Result<RangeIter<'_, K, V, I>, Miss<K>>
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
    pub fn keys(&self) -> KeysIter<'_, K, V, I> {
        KeysIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Iterate over all value sets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn values(&self) -> ValuesIter<'_, K, V, I> {
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
    pub fn get<'a, Q>(&'a self, key: &'_ Q) -> Option<&'a Values<V, I>>
    where
        K: Borrow<Q> + Ord + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        self.guard.data.get(key).inspect(|v| {
            self.guard.eviction_strategy.on_read(v.eviction_meta());
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
    pub fn first<'a, Q>(&'a self, key: &'_ Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        V: Ord,
        Q: Ord + Hash + ?Sized,
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

impl<K, Q, V, M, S, I> std::ops::Index<&'_ Q> for MapReadRef<'_, K, V, I, M, S>
where
    K: Ord + Clone + Borrow<Q> + Hash,
    V: Eq + Hash + Default,
    I: InsertionOrder<V>,
    Q: Ord + ?Sized + Hash + ToOwned<Owned = K>,
    S: BuildHasher,
{
    type Output = Values<V, I>;

    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<'rg, K, V, M, S, I> IntoIterator for &'rg MapReadRef<'_, K, V, I, M, S>
where
    K: Ord + Clone + Hash,
    V: Eq + Hash,
    I: InsertionOrder<V>,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg Values<V, I>);
    type IntoIter = ReadGuardIter<'rg, K, V, I>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An [`Iterator`] over keys and values in the evmap.
pub struct ReadGuardIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V, I>,
}

impl<K, V, I> fmt::Debug for ReadGuardIter<'_, K, V, I>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadGuardIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, I> Iterator for ReadGuardIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = (&'rg K, &'rg Values<V, I>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// An [`Iterator`] over keys.
pub struct KeysIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V, I>,
}

impl<K, V, I> fmt::Debug for KeysIter<'_, K, V, I>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("KeysIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, I> Iterator for KeysIter<'rg, K, V, I>
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
pub struct RangeIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: btree_map::Range<'rg, K, Values<V, I>>,
    eviction_strategy: &'rg EvictionStrategy,
}

impl<K, V, I> fmt::Debug for RangeIter<'_, K, V, I>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RangeIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, I> Iterator for RangeIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
    I: InsertionOrder<V>,
{
    type Item = (&'rg K, &'rg Values<V, I>);

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
pub struct ValuesIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    iter: crate::inner::Iter<'rg, K, V, I>,
}

impl<K, V, I> fmt::Debug for ValuesIter<'_, K, V, I>
where
    K: Ord + Clone + fmt::Debug,
    V: Eq + Hash,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ValuesIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, I> Iterator for ValuesIter<'rg, K, V, I>
where
    K: Ord + Clone,
    V: Eq + Hash,
{
    type Item = &'rg Values<V, I>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}
