pub use std::collections::btree_map::{Keys, Values};
use std::collections::{btree_map, BTreeMap};
use std::ops::{Bound, RangeBounds};

use unbounded_interval_tree::IntervalTree;

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
/// use noria_dataflow::state::PartialMap;
///
/// let mut map = PartialMap::new();
/// map.insert(1, 2);
/// assert_eq!(map.get(&1), Some(&2));
/// ```
///
/// Ranges can be marked as present with [`insert_range`](PartialMap::insert_range):
///
/// ```rust
/// use noria_dataflow::state::PartialMap;
///
/// let mut map: PartialMap<i32, Vec<i32>> = PartialMap::new();
/// assert_eq!(map.get(&1), None);
///
/// map.insert_range(0..=10);
/// assert_eq!(map.get(&1), Some(&vec![]));
/// ````
#[derive(PartialEq)]
pub struct PartialMap<K, V>
where
    K: Ord + Clone,
{
    map: BTreeMap<K, V>,
    // NOTE: duplicating the key here is unfortunate - in the future post profiling we may want to
    // use some pinning to make these self-referential pointers to the keys in the map
    interval_tree: IntervalTree<K>,
    /// A constant empty value, used so we can return a reference to it in `get` and have the
    /// lifetimes all match up
    empty_value: V,
}

impl<K, V> Default for PartialMap<K, V>
where
    K: Ord + Clone,
    V: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> PartialMap<K, V>
where
    K: Ord + Clone,
    V: Default,
{
    pub fn new() -> Self {
        Self {
            map: BTreeMap::default(),
            interval_tree: IntervalTree::default(),
            empty_value: V::default(),
        }
    }

    /// Returns a mutable reference to the value at `key`.
    ///
    /// If that key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), the [`default`](std::Default::default) value will be inserted and a
    /// mutable reference to that will be returned.
    pub fn get_mut<'a>(&'a mut self, key: &K) -> Option<&'a mut V> {
        if self.interval_tree.contains_point(key) {
            Some(self.map.entry(key.clone()).or_insert_with(|| V::default()))
        } else {
            self.map.get_mut(key)
        }
    }
}

impl<K, V> PartialMap<K, V>
where
    K: Ord + Clone,
{
    /// Returns a reference to the value at `key`.
    ///
    /// If that key is covered by a range of keys we know we have (due to calls to
    /// [`insert_range`]), a reference to the [`default`](std::Default::default) value will be
    /// returned.
    ///
    /// The above behavior (returrning the Default in the case of a miss) is perhaps more intuitive
    /// if you think of the specific *use case* of a PartialMap in Noria - specifically, as a map
    /// from keys to all the rows matching that key. In that instance, if we *know* we have all the
    /// rows whose keys are in a range, then it makes sense to return an empty vec (the Default
    /// value, in the case of this method) from a lookup on a key in that range, since that means
    /// that we *know* there are no rows with that key.
    pub fn get<'a>(&'a self, key: &K) -> Option<&'a V> {
        match self.map.get(key) {
            None if self.interval_tree.contains_point(key) => Some(&self.empty_value),
            res => res,
        }
    }

    /// Insert `value` at `key`, returning the value that used to be there if any.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.interval_tree.insert_point(key.clone());
        self.map.insert(key, value)
    }

    /// Mark a range of keys as filled in this map.
    ///
    /// All keys covered by `range` will be considered to be set to the
    /// [`default`](std::Default::default) value.
    pub fn insert_range<R>(&mut self, range: R)
    where
        R: RangeBounds<K> + Clone,
    {
        self.interval_tree.insert(range);
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
    pub fn range<'a, R>(
        &'a self,
        range: &R,
    ) -> Result<btree_map::Range<'a, K, V>, Vec<(Bound<K>, Bound<K>)>>
    where
        R: RangeBounds<K> + Clone,
    {
        let diff = self.interval_tree.get_interval_difference(range);
        if diff.is_empty() {
            Ok(self.map.range((range.start_bound(), range.end_bound())))
        } else {
            Err(diff
                .into_iter()
                .map(|(lower, upper)| (lower.cloned(), upper.cloned()))
                .collect())
        }
    }

    /// Returns the number of keys in this map.
    ///
    /// Note that this does *not* consider ranges, since for certain keys (strings, etc.) that are
    /// noncontiguous that's undecidable
    pub fn len(&self) -> usize {
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

    /// Returns an iterator over the values in this map
    ///
    /// Note that this does *not* consider ranges, since iteration is not well-defined for certain
    /// types of keys (floating points, strings, etc.)
    pub fn values(&self) -> Values<K, V> {
        self.map.values()
    }

    /// Remove all entries and ranges in this map
    pub fn clear(&mut self) {
        self.interval_tree.clear();
        self.map.clear();
    }

    /// Remove the value at the given `key` from this map, returning it if it was present
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.interval_tree.remove_point(key);
        self.map.remove(key)
    }

    /// Remove the entry at the given `key` from this map, returning a tuple of the key and the
    /// value if it was present
    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        self.interval_tree.remove_point(key);
        self.map.remove_entry(key)
    }

    /// Remove the values corresponding to the keys covered by `range`, and return them.
    pub fn remove_range<'a, R>(&'a mut self, range: R) -> impl Iterator<Item = (K, V)> + 'a
    where
        R: RangeBounds<K> + 'a,
    {
        self.interval_tree.remove(&range);
        // NOTE: it is deeply unfortunate that rust's BTreeMap doesn't have a drain(range) function
        // the way Vec does. This is forcing us into an O(n) operation where we could have an
        // O(log(n)) one.
        self.map.drain_filter(move |k, _| range.contains(k))
    }
}

pub struct VacantEntry<'a, K, V>
where
    K: Ord + Clone,
{
    tree: &'a mut IntervalTree<K>,
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
    K: Ord + Clone,
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
    K: Ord + Clone,
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
    K: Ord + Clone,
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
