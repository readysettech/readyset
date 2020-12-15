use crate::values::ValuesInner;
use crate::{inner::Inner, values::Values, Aliased};
use left_right::ReadGuard;
use std::collections::hash_map::RandomState;
use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::{borrow::Borrow, ops::RangeBounds};

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
pub struct MapReadRef<'rh, K, V, M = (), S = RandomState>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    pub(super) guard: ReadGuard<'rh, Inner<K, V, M, S>>,
}

impl<'rh, K, V, M, S> fmt::Debug for MapReadRef<'rh, K, V, M, S>
where
    K: Ord,
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

impl<'rh, K, V, M, S> MapReadRef<'rh, K, V, M, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    /// Iterate over all key + valuesets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn iter(&self) -> ReadGuardIter<'_, K, V, S> {
        ReadGuardIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Constructs a double-ended iterator over a sub-range of key + valueset elements in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn range<Q, R>(&self, range: R) -> RangeIter<'_, K, V, S>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        RangeIter {
            iter: self.guard.data.range(range),
        }
    }

    /// Iterate over all keys in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn keys(&self) -> KeysIter<'_, K, V, S> {
        KeysIter {
            iter: self.guard.data.iter(),
        }
    }

    /// Iterate over all value sets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// publish changes will block waiting on this reader to finish.
    pub fn values(&self) -> ValuesIter<'_, K, V, S> {
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
    pub fn get<'a, Q: ?Sized>(&'a self, key: &'_ Q) -> Option<&'a Values<V, S>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.guard.data.get(key).map(AsRef::as_ref)
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
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
    pub fn get_one<'a, Q: ?Sized>(&'a self, key: &'_ Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.guard
            .data
            .get(key)
            .and_then(|values| values.as_ref().get_one())
    }

    /// Returns true if the map contains any values for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.guard.data.contains_key(key)
    }

    /// Returns true if the map contains the specified value for the specified key.
    ///
    /// The key and value may be any borrowed form of the map's respective types, but `Hash` and
    /// `Eq` on the borrowed form *must* match.
    pub fn contains_value<Q: ?Sized, W: ?Sized>(&self, key: &Q, value: &W) -> bool
    where
        K: Borrow<Q>,
        Aliased<V, crate::aliasing::NoDrop>: Borrow<W>,
        Q: Ord,
        W: Hash + Eq,
    {
        self.guard
            .data
            .get(key)
            .map_or(false, |values| values.as_ref().contains(value))
    }
}

impl<'rh, K, Q, V, M, S> std::ops::Index<&'_ Q> for MapReadRef<'rh, K, V, M, S>
where
    K: Ord + Borrow<Q>,
    V: Eq + Hash,
    Q: Ord + ?Sized,
    S: BuildHasher,
{
    type Output = Values<V, S>;
    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<'rg, 'rh, K, V, M, S> IntoIterator for &'rg MapReadRef<'rh, K, V, M, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg Values<V, S>);
    type IntoIter = ReadGuardIter<'rg, K, V, S>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An [`Iterator`] over keys and values in the evmap.
pub struct ReadGuardIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    iter: <&'rg BTreeMap<K, ValuesInner<V, S, crate::aliasing::NoDrop>> as IntoIterator>::IntoIter,
}

impl<'rg, K, V, S> fmt::Debug for ReadGuardIter<'rg, K, V, S>
where
    K: Ord + fmt::Debug,
    V: Eq + Hash,
    S: BuildHasher,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadGuardIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, S> Iterator for ReadGuardIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg Values<V, S>);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k, v.as_ref()))
    }
}

/// An [`Iterator`] over keys.
pub struct KeysIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    iter: <&'rg BTreeMap<K, ValuesInner<V, S, crate::aliasing::NoDrop>> as IntoIterator>::IntoIter,
}

impl<'rg, K, V, S> fmt::Debug for KeysIter<'rg, K, V, S>
where
    K: Ord + fmt::Debug,
    V: Eq + Hash,
    S: BuildHasher,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("KeysIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, S> Iterator for KeysIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    type Item = &'rg K;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

/// An [`Iterator`] over a range of keys.
pub struct RangeIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    iter: btree_map::Range<'rg, K, ValuesInner<V, S, crate::aliasing::NoDrop>>,
}

impl<'rg, K, V, S> fmt::Debug for RangeIter<'rg, K, V, S>
where
    K: Ord + fmt::Debug,
    V: Eq + Hash,
    S: BuildHasher,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RangeIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, S> Iterator for RangeIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg Values<V, S>);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k, v.as_ref()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// An [`Iterator`] over value sets.
pub struct ValuesIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    iter: <&'rg BTreeMap<K, ValuesInner<V, S, crate::aliasing::NoDrop>> as IntoIterator>::IntoIter,
}

impl<'rg, K, V, S> fmt::Debug for ValuesIter<'rg, K, V, S>
where
    K: Ord + fmt::Debug,
    V: Eq + Hash,
    S: BuildHasher,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ValuesIter").field(&self.iter).finish()
    }
}

impl<'rg, K, V, S> Iterator for ValuesIter<'rg, K, V, S>
where
    K: Ord,
    V: Eq + Hash,
    S: BuildHasher,
{
    type Item = &'rg Values<V, S>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v.as_ref())
    }
}
