use std::fmt::{self, Debug};
use std::iter::{Enumerate, FromIterator};
use std::ops::{Index, IndexMut};
use std::{mem, slice};

use noria::internal::LocalNodeIndex;
use serde::{Deserialize, Serialize};

/// A map from [`LocalNodeIndex`] to `T`, implemented as a sparse vector for super-efficient and
/// cache-friendly indexing.
///
/// The contents of the map are stored directly in the vector, so for maximum efficiency
/// `size_of::<T>()` should generally be small - wrap it in a [`Box`] if `T` is large.
///
/// # Examples
///
/// ```rust
/// use noria_dataflow::NodeMap;
/// use noria::internal::LocalNodeIndex;
///
/// let node_1 = unsafe { LocalNodeIndex::make(1u32) };
///
/// let mut map = NodeMap::new();
/// map.insert(node_1, "node 1".to_owned());
///
/// assert_eq!(map.get(node_1), Some(&"node 1".to_owned()));
/// ```
#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct NodeMap<T> {
    /// The number of items in the Map.
    ///
    /// This will always be equivalent to the number of elements in `self.contents` that are `Some`.
    len: usize,

    /// The actual elements in the Map.
    ///
    /// Note that this is a *sparse array* for efficient (O(1)) and cache-friendly lookup. The
    /// indices in this vector are (numeric) [`LocalNodeIndex`]es, and the values are `Some(x)` if a
    /// value is present, or `None` if there is no value for that node.
    contents: Vec<Option<T>>,
}

impl<T> Default for NodeMap<T> {
    fn default() -> Self {
        NodeMap {
            len: 0,
            contents: Vec::default(),
        }
    }
}

/// A view into a single entry in a [`NodeMap`], which may either be vacant or occupied.
///
/// This enum is constructed from the [`entry`][] method on [`NodeMap`]
///
/// [`entry`]: NodeMap::entry
pub enum Entry<'a, V> {
    /// A vacant entry
    Vacant(VacantEntry<'a, V>),
    /// An occupied entry
    Occupied(OccupiedEntry<'a, V>),
}

/// A view into a vacant entry in a [`NodeMap`]. This is part of the [`Entry`] enum.
pub struct VacantEntry<'a, V> {
    map: &'a mut NodeMap<V>,
    index: LocalNodeIndex,
}

/// A view into an occupied entry in a [`NodeMap`]. This is part of the [`Entry`] enum.
pub struct OccupiedEntry<'a, V> {
    map: &'a mut NodeMap<V>,
    index: LocalNodeIndex,
}

impl<'a, V> Entry<'a, V> {
    /// Ensures a value is in the entry by inserting the default if empty, and returns a mutable
    /// reference to the value in the entry.
    pub fn or_insert(self, default: V) -> &'a mut V {
        self.or_insert_with(|| default)
    }

    /// Ensures a value is in the entry by inserting the default value if empty, and returns a
    /// mutable reference to the value in the entry.
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        self.or_insert_with(Default::default)
    }

    /// Ensures a value is in the entry by inserting the result of the default function if empty,
    /// and returns a mutable reference to the value in the entry.
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }
}

impl<'a, V> VacantEntry<'a, V> {
    /// Inserts a value into the entry, and returns a mutable reference to that value
    pub fn insert(self, value: V) -> &'a mut V {
        self.map.insert(self.index, value);
        self.map.get_mut(self.index).unwrap()
    }
}

impl<'a, V> OccupiedEntry<'a, V> {
    /// Convert this entry into a mutable reference to the underlying value
    pub fn into_mut(self) -> &'a mut V {
        self.map.get_mut(self.index).unwrap()
    }
}

impl<T> NodeMap<T> {
    /// Creates a new, empty node map
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a key-value pair into the node map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, addr: LocalNodeIndex, value: T) -> Option<T> {
        let i = addr.id();

        match self.contents.get_mut(i) {
            None => {
                let diff = i - self.contents.len();
                self.contents.reserve(diff + 1);
                for _ in 0..diff {
                    self.contents.push(None);
                }
                self.contents.push(Some(value));
                self.len += 1;
                None
            }
            Some(v) => {
                let old = mem::replace(v, Some(value));
                if old.is_none() {
                    self.len += 1;
                }
                old
            }
        }
    }

    /// Returns a reference to the value corresponding to the given local node index
    pub fn get(&self, addr: LocalNodeIndex) -> Option<&T> {
        self.contents.get(addr.id()).and_then(Option::as_ref)
    }

    /// Returns a mutable reference to the value corresponding to the given local node index
    pub fn get_mut(&mut self, addr: LocalNodeIndex) -> Option<&mut T> {
        self.contents.get_mut(addr.id()).and_then(Option::as_mut)
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key(&self, addr: LocalNodeIndex) -> bool {
        self.contents
            .get(addr.id())
            .map(Option::is_some)
            .unwrap_or(false)
    }

    /// Removes a key from the map, returning the value at the key if the key was previously in the
    /// map.
    pub fn remove(&mut self, addr: LocalNodeIndex) -> Option<T> {
        let i = addr.id();
        if i >= self.contents.len() {
            return None;
        }

        let ret = self.contents.get_mut(i).and_then(|e| e.take());
        if ret.is_some() {
            self.len -= 1;
        }
        ret
    }

    /// Construct an iterator visiting all key-value pairs in order.
    pub fn iter(&self) -> Iter<T> {
        Iter {
            inner: self.contents.iter().enumerate(),
        }
    }

    /// Construct an iterator visiting all key-value pairs in order, with mutable references to the
    /// values.
    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut {
            inner: self.contents.iter_mut().enumerate(),
        }
    }

    /// Construct an iterator over all keys in the map
    pub fn keys(&self) -> impl Iterator<Item = LocalNodeIndex> + '_ {
        (0..self.len).map(|i| (unsafe { LocalNodeIndex::make(i as u32) }))
    }

    /// Construct an iterator over all values in the map
    pub fn values(&self) -> impl Iterator<Item = &T> + '_ {
        self.contents.iter().filter_map(Option::as_ref)
    }

    /// Returns the number of entries in the map
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if this map has no entries
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation
    pub fn entry(&mut self, key: LocalNodeIndex) -> Entry<'_, T> {
        if self.contains_key(key) {
            Entry::Occupied(OccupiedEntry {
                map: self,
                index: key,
            })
        } else {
            Entry::Vacant(VacantEntry {
                map: self,
                index: key,
            })
        }
    }
}

impl<T> Debug for NodeMap<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T> Index<LocalNodeIndex> for NodeMap<T> {
    type Output = T;
    fn index(&self, index: LocalNodeIndex) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<T> IndexMut<LocalNodeIndex> for NodeMap<T> {
    fn index_mut(&mut self, index: LocalNodeIndex) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

impl<T> FromIterator<(LocalNodeIndex, T)> for NodeMap<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (LocalNodeIndex, T)>,
    {
        use std::collections::BTreeMap;

        // we've got to be a bit careful here, as the nodes may come in any order
        // we therefore sort them first
        let sorted: BTreeMap<_, _> = iter.into_iter().map(|(ni, v)| (ni.id(), v)).collect();

        let len = sorted.len();
        let end = match sorted.keys().last() {
            Some(k) => k + 1,
            // no entries -- fine
            None => return NodeMap::default(),
        };
        let mut contents = Vec::with_capacity(end);
        for (i, v) in sorted {
            for _ in contents.len()..i {
                contents.push(None);
            }
            contents.push(Some(v));
        }

        NodeMap { len, contents }
    }
}

/// An owning iterator over the entries in a [`NodeMap`].
///
/// This is constructed via the [`into_iter`][] method on [`NodeMap`] (provided by the
/// `IntoIterator` trait).
///
/// [`into_iter`]: IntoIterator::into_iter
pub struct IntoIter<T> {
    inner: Enumerate<<Vec<Option<T>> as IntoIterator>::IntoIter>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = (LocalNodeIndex, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .find_map(|(i, v)| v.map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v)))
    }
}

impl<T> IntoIterator for NodeMap<T> {
    type Item = (LocalNodeIndex, T);
    type IntoIter = IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.contents.into_iter().enumerate(),
        }
    }
}

/// An iterator over the entries in a [`NodeMap`].
///
/// This is constructed via the [`iter`][] method on [`NodeMap`].
///
/// [`iter`]: NodeMap::iter
pub struct Iter<'a, T: 'a> {
    inner: Enumerate<slice::Iter<'a, Option<T>>>,
}

impl<'a, T: 'a> Iterator for Iter<'a, T> {
    type Item = (LocalNodeIndex, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find_map(|(i, v)| {
            v.as_ref()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        })
    }
}

impl<'a, T: 'a> IntoIterator for &'a NodeMap<T> {
    type Item = (LocalNodeIndex, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A mutable iterator over the entries in a [`NodeMap`].
///
/// This is constructed via the [`iter_mut`][] method on [`NodeMap`].
///
/// [`iter_mut`]: NodeMap::iter_mut
pub struct IterMut<'a, T: 'a> {
    inner: Enumerate<slice::IterMut<'a, Option<T>>>,
}

impl<'a, T: 'a> Iterator for IterMut<'a, T> {
    type Item = (LocalNodeIndex, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find_map(|(i, v)| {
            v.as_mut()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        })
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut NodeMap<T> {
    type Item = (LocalNodeIndex, &'a mut T);
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
