#![feature(btree_drain_filter, bound_map)]

use std::borrow::Borrow;
pub use std::collections::btree_map::{Iter, Keys, Range, Values, ValuesMut};
use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::ops::{Bound, RangeBounds};

use merging_interval_tree::IntervalTreeSet;

/// A [`BTreeMap`] that knows what ranges of keys it has
///
/// [`PartialMap`] wraps a [`BTreeMap`], extending it to support knowledge not just about keys and
/// values, but also the *ranges* of keys that it contains. This allows us to differentiate, for
/// example, between having the keys `3` and `8`, and having *all* keys in the interval `[3, 8]` -
/// knowing as such that keys 4, 5, 6, and 7 *definitely* don't exist. This is reflected primarily
/// in the return value of [`range`](PartialMap::range), which returns an [`Err`](std::Result::Err)
/// containing the missing intervals in the case that the query covers any keys that are known to be
/// absent. [`get`](PartialMap::get) and [`get_mut`](PartialMap::get_mut) also return the
/// [`default`](std::Default::default) value in the case of a lookup into a key that is covered by a
/// range we have.
///
/// # Examples
///
/// We behave just like a [`BTreeMap`] normally:
///
/// ```rust
/// use partial_map::PartialMap;
///
/// let mut map = PartialMap::new();
/// map.insert(1, 2);
/// assert_eq!(map.get(&1), Some(&2));
/// ```
///
/// Ranges can be marked as present with [`insert_range`](PartialMap::insert_range):
///
/// ```rust
/// use partial_map::PartialMap;
///
/// let mut map: PartialMap<i32, Vec<i32>> = PartialMap::new();
/// assert_eq!(map.get(&1), None);
///
/// map.insert_range(0..=10);
/// assert_eq!(map.get(&1), Some(&vec![]));
/// ````
#[derive(PartialEq, Eq, Clone)]
pub struct PartialMap<K, V> {
    map: BTreeMap<K, V>,
    // NOTE: duplicating the key here is unfortunate - in the future post profiling we may want to
    // use some pinning to make these self-referential pointers to the keys in the map
    interval_tree: IntervalTreeSet<K>,
    /// A constant empty value, used so we can return a reference to it in `get` and have the
    /// lifetimes all match up
    empty_value: V,
}

impl<K, V> fmt::Debug for PartialMap<K, V>
where
    K: fmt::Debug + Ord,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialMap")
            .field("map", &self.map)
            .field("interval_tree", &self.interval_tree)
            .finish()
    }
}

impl<K, V> Default for PartialMap<K, V>
where
    V: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> PartialMap<K, V>
where
    V: Default,
{
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
            interval_tree: IntervalTreeSet::default(),
            empty_value: V::default(),
        }
    }
}

impl<K, V> PartialMap<K, V> {
    /// Returns the number of keys in this map.
    ///
    /// Note that this does *not* consider ranges, since for certain keys (strings, etc.) that are
    /// noncontiguous that's undecidable
    pub fn num_keys(&self) -> usize {
        self.map.len()
    }

    /// Returns true if this map contains no keys or ranges
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.interval_tree.is_empty()
    }

    /// Returns an iterator over the keys in this map
    ///
    /// Note that this does *not* consider ranges, since iteration is not well-defined for certain
    /// types of keys (floating points, strings, etc.)
    pub fn keys(&self) -> Keys<K, V> {
        self.map.keys()
    }

    /// Returns an iterator over the key-value pairs in the map
    ///
    /// Note that this does *not* consider ranges, since iteration is not well-defined for certain
    /// types of keys (floating points, strings, etc.)
    pub fn iter(&self) -> Iter<K, V> {
        self.map.iter()
    }

    /// Remove all entries and ranges in this map
    pub fn clear(&mut self) {
        self.interval_tree.clear();
        self.map.clear();
    }
}

impl<K, V> PartialMap<K, V>
where
    K: Ord + Clone,
    V: Default,
{
    /// Returns a mutable reference to the value at `key`.
    ///
    /// If that key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), the [`default`](std::Default::default) value will be inserted and a
    /// mutable reference to that will be returned.
    pub fn get_mut<'a, Q>(&'a mut self, key: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + ToOwned<Owned = K>,
    {
        match self.map.entry(key.to_owned()) {
            btree_map::Entry::Vacant(entry) => {
                if self.interval_tree.contains_point(key) {
                    Some(entry.insert(V::default()))
                } else {
                    None
                }
            }
            btree_map::Entry::Occupied(entry) => Some(entry.into_mut()),
        }
    }
}

impl<K, V> PartialMap<K, V>
where
    K: Ord,
{
    /// Returns a reference to the value at `key`.
    ///
    /// If that key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), a reference to the [`default`](std::Default::default) value will be
    /// returned.
    ///
    /// The above behavior (returrning the Default in the case of a miss) is perhaps more intuitive
    /// if you think of the specific *use case* of a PartialMap in ReadySet - specifically, as a map
    /// from keys to all the rows matching that key. In that instance, if we *know* we have all the
    /// rows whose keys are in a range, then it makes sense to return an empty vec (the Default
    /// value, in the case of this method) from a lookup on a key in that range, since that means
    /// that we *know* there are no rows with that key.
    pub fn get<'a, Q>(&'a self, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.map.get(key) {
            None if self.interval_tree.contains_point(key) => Some(&self.empty_value),
            res => res,
        }
    }

    /// Returns the key-value pair corresponding to the supplied key.
    ///
    /// If that key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), a reference to the [`default`](std::Default::default) value will be
    /// returned.
    pub fn get_key_value<'a>(&'a self, key: &'a K) -> Option<(&'a K, &'a V)> {
        match self.map.get_key_value(key) {
            None if self.interval_tree.contains_point(key) => Some((key, &self.empty_value)),
            res => res,
        }
    }

    /// Returns true if the map contains an entry for the given key, or contains a range covering
    /// the given key
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // TODO(aspen): Is it faster to just check the interval tree?
        self.map.contains_key(key) || self.interval_tree.contains_point(key)
    }

    /// Returns true if the map contains the entirety of the given range
    pub fn contains_range<R, Q>(&self, range: &R) -> bool
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.interval_tree.covers_interval(range)
    }

    /// Returns true if the map contains at least part of the given range
    pub fn overlaps_range<R, Q>(&self, range: &R) -> bool
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.interval_tree
            .get_interval_overlaps(range)
            .next()
            .is_some()
    }

    /// Insert `value` at `key`, returning the value that used to be there if any.
    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Clone,
    {
        self.interval_tree.insert_point(key.clone());
        self.map.insert(key, value)
    }

    /// Mark a range of keys as filled in this map.
    ///
    /// All keys covered by `range` will be considered to be set to the
    /// [`default`](std::Default::default) value.
    pub fn insert_range<R>(&mut self, range: R)
    where
        K: Clone,
        R: RangeBounds<K>,
    {
        self.interval_tree.insert_interval(range);
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    ///
    /// If the key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), the entry will contain the [`default`](std::Default::default) value.
    ///
    /// The [`Entry`] returned by this method supports (an incomplete subset of) the API exposed by
    /// [`std::collections::btree_map::Entry`]. If you come across this and need more of that API
    /// supported, there's no reason not to add it.
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        match self.map.entry(key) {
            btree_map::Entry::Vacant(inner) => {
                if self.interval_tree.contains_point(inner.key()) {
                    Entry::Occupied(OccupiedEntry::Default { inner })
                } else {
                    Entry::Vacant(VacantEntry {
                        tree: &mut self.interval_tree,
                        inner,
                    })
                }
            }
            btree_map::Entry::Occupied(inner) => Entry::Occupied(OccupiedEntry::Present { inner }),
        }
    }

    /// Return either an iterator over all the keys and values in this map corresponding to the keys
    /// in `range`, or an [`Err`](std::Result::Err) containing a list of sub-ranges of `range` that
    /// were missing.
    #[allow(clippy::type_complexity)]
    pub fn range<'a, R, Q>(
        &'a self,
        range: &R,
    ) -> Result<Range<'a, K, V>, Vec<(Bound<K>, Bound<K>)>>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        let diff = self
            .interval_tree
            .get_interval_difference(range)
            .map(|(lower, upper)| (lower.map(ToOwned::to_owned), upper.map(ToOwned::to_owned)))
            .collect::<Vec<_>>();

        if diff.is_empty() {
            Ok(self.map.range((range.start_bound(), range.end_bound())))
        } else {
            Err(diff)
        }
    }

    /// Returns an iterator over the values in this map
    ///
    /// Note that this does *not* consider ranges, since iteration is not well-defined for certain
    /// types of keys (floating points, strings, etc.)
    pub fn values(&self) -> Values<K, V> {
        self.map.values()
    }

    /// Returns an iterator over mutable references to the values in this map
    ///
    /// Note that this does *not* consider ranges, since iteration is not well-defined for certain
    /// types of keys (floating points, strings, etc.)
    pub fn values_mut(&mut self) -> ValuesMut<K, V> {
        self.map.values_mut()
    }

    /// Remove the value at the given `key` from this map, returning it if it was present
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Clone,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        self.interval_tree.remove_point(key);
        self.map.remove(key)
    }

    /// Remove the entry at the given `key` from this map, returning a tuple of the key and the
    /// value if it was present
    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)>
    where
        K: Clone,
    {
        self.interval_tree.remove_point(key);
        self.map.remove_entry(key)
    }

    /// Remove the values corresponding to the keys covered by `range`, and return them.
    pub fn remove_range<'a, B, R>(&'a mut self, range: R) -> impl Iterator<Item = (K, V)> + 'a
    where
        K: Borrow<B> + Clone,
        B: Ord + ?Sized + ToOwned<Owned = K>,
        R: RangeBounds<B> + 'a,
    {
        self.interval_tree.remove_interval(&range);
        // NOTE: it is deeply unfortunate that rust's BTreeMap doesn't have a drain(range) function
        // the way Vec does. This is forcing us into an O(n) operation where we could have an
        // O(log(n)) one.
        self.map
            .drain_filter(move |k, _| range.contains(k.borrow()))
    }

    /// Clone the interval tree from the given partial map into our interval tree/
    ///
    /// WARNING: misusing this function can result in keys in self that don't exist in intervals.
    /// It's not unsafe, since it can't cause undefined behavior, but it's definitely easy to
    /// misuse!
    pub fn clone_intervals_from<V2>(&mut self, other: &PartialMap<K, V2>)
    where
        K: Clone,
    {
        self.interval_tree.clone_from(&other.interval_tree)
    }
}

impl<K, V> Extend<(K, V)> for PartialMap<K, V>
where
    K: Ord + Clone,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        self.map.extend(
            iter.into_iter()
                .inspect(|(k, _)| self.interval_tree.insert_point(k.clone())),
        )
    }
}

pub struct VacantEntry<'a, K, V>
where
    K: Ord,
{
    tree: &'a mut IntervalTreeSet<K>,
    inner: btree_map::VacantEntry<'a, K, V>,
}

impl<'a, K, V> VacantEntry<'a, K, V>
where
    K: Ord + Clone,
{
    pub fn insert(self, value: V) -> &'a mut V {
        self.tree.insert_point(self.inner.key().clone());
        self.inner.insert(value)
    }
}

pub enum OccupiedEntry<'a, K, V>
where
    K: Ord,
{
    Default {
        inner: btree_map::VacantEntry<'a, K, V>,
    },
    Present {
        inner: btree_map::OccupiedEntry<'a, K, V>,
    },
}

impl<'a, K, V> OccupiedEntry<'a, K, V>
where
    K: Ord,
    V: Default,
{
    pub fn into_mut(self) -> &'a mut V {
        match self {
            OccupiedEntry::Default { inner } => inner.insert(Default::default()),
            OccupiedEntry::Present { inner } => inner.into_mut(),
        }
    }
}

pub enum Entry<'a, K, V>
where
    K: Ord,
{
    Vacant(VacantEntry<'a, K, V>),
    Occupied(OccupiedEntry<'a, K, V>),
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Ord + Clone,
    V: Default,
{
    pub fn or_default(self) -> &'a mut V {
        match self {
            Self::Occupied(entry) => entry.into_mut(),
            Self::Vacant(entry) => entry.insert(V::default()),
        }
    }
}

/// A trait that is required to keep the inner values in the correct order
pub trait InsertionOrder<V> {
    /// Return the position of the element in the list of elements, or the position where it can be
    /// inserted in order
    fn get_insertion_order(&self, values: &[V], elem: &V) -> std::result::Result<usize, usize>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_filled() {
        let mut map: PartialMap<i32, ()> = PartialMap::new();
        map.insert_range(1..);
        assert!(matches!(map.entry(2), Entry::Occupied(_)));
    }
}
