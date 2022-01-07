use std::borrow::Borrow;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::{Bound, RangeBounds};

use itertools::Either;
use left_right::aliasing::DropBehavior;
use noria::internal::IndexType;
use partial_map::PartialMap;
use std::collections::{hash_map, HashMap};

use crate::values::ValuesInner;

/// Represents a miss when looking up a range.
///
/// Values in the vec are ranges of keys within the requested bound that are not present
#[derive(Debug, PartialEq, Eq)]
pub struct Miss<K>(pub Vec<(Bound<K>, Bound<K>)>);

impl<K: Clone> Miss<&K> {
    /// Map a `Miss<&K>` to a `Miss<K>` by cloning the key in the bounds of the missed ranges
    pub fn cloned(&self) -> Miss<K> {
        Miss(
            self.0
                .iter()
                .map(|(l, u)| (l.cloned(), u.cloned()))
                .collect(),
        )
    }
}

pub(crate) enum Data<K, V, S, D = crate::aliasing::NoDrop>
where
    K: Ord + Clone,
    S: BuildHasher,
    D: DropBehavior,
{
    BTreeMap {
        map: PartialMap<K, ValuesInner<V, S, D>>,
    },
    HashMap {
        map: HashMap<K, ValuesInner<V, S, D>, S>,
    },
}

impl<K, V, S> fmt::Debug for Data<K, V, S>
where
    K: Ord + Clone + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BTreeMap { map, .. } => f.debug_struct("BTreeMap").field("map", &map).finish(),
            Self::HashMap { map } => f.debug_struct("HashMap").field("map", &map).finish(),
        }
    }
}

macro_rules! with_map {
    ($data: expr, |$map: ident| $body: expr) => {
        match $data {
            Data::BTreeMap { map: $map, .. } => $body,
            Data::HashMap { map: $map } => $body,
        }
    };
}

pub(crate) type Iter<'a, K, V, S, D = crate::aliasing::NoDrop> = Either<
    partial_map::Iter<'a, K, ValuesInner<V, S, D>>,
    hash_map::Iter<'a, K, ValuesInner<V, S, D>>,
>;

impl<K, V, S, D> Data<K, V, S, D>
where
    K: Ord + Clone,
    S: BuildHasher,
    D: DropBehavior,
{
    pub(crate) fn with_index_type_and_hasher(index_type: IndexType, hash_builder: S) -> Self {
        match index_type {
            IndexType::HashMap => Self::HashMap {
                map: HashMap::with_hasher(hash_builder),
            },
            IndexType::BTreeMap => Self::BTreeMap {
                map: Default::default(),
            },
        }
    }

    pub(crate) fn index_type(&self) -> IndexType {
        match self {
            Self::BTreeMap { .. } => IndexType::BTreeMap,
            Self::HashMap { .. } => IndexType::HashMap,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        with_map!(self, |map| map.is_empty())
    }

    pub(crate) fn len(&self) -> usize {
        with_map!(self, |map| map.len())
    }

    /// Returns an empty version of the *same* type of index as this [`Data`]
    pub(crate) fn empty(&self) -> Self
    where
        S: Clone,
    {
        match self {
            Self::BTreeMap { .. } => Self::BTreeMap {
                map: Default::default(),
            },
            Self::HashMap { map } => {
                Self::with_index_type_and_hasher(IndexType::HashMap, (*map.hasher()).clone())
            }
        }
    }

    /// If both self and other are `BTreeMap`s, set self's interval tree to a clone of other's
    pub(crate) fn clone_intervals_from<D2>(&mut self, other: &Data<K, V, S, D2>)
    where
        D2: DropBehavior,
    {
        if let (Data::BTreeMap { ref mut map, .. }, Data::BTreeMap { map: other_map, .. }) =
            (self, other)
        {
            map.clone_intervals_from(other_map);
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, V, S, D> {
        match self {
            Self::BTreeMap { map, .. } => Either::Left(map.iter()),
            Self::HashMap { map } => Either::Right(map.iter()),
        }
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut ValuesInner<V, S, D>> {
        match self {
            Self::BTreeMap { map, .. } => Either::Left(map.values_mut()),
            Self::HashMap { map } => Either::Right(map.values_mut()),
        }
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        match self {
            Self::BTreeMap { map, .. } => Either::Left(map.keys()),
            Self::HashMap { map } => Either::Right(map.keys()),
        }
    }

    pub(crate) fn clear(&mut self) {
        match self {
            Self::BTreeMap { map, .. } => {
                map.clear();
            }
            Self::HashMap { map } => {
                map.clear();
            }
        }
    }

    pub(crate) fn range<R>(
        &'_ self,
        range: &R,
    ) -> Result<partial_map::Range<'_, K, ValuesInner<V, S, D>>, Miss<K>>
    where
        R: Clone + RangeBounds<K>,
    {
        match self {
            Self::BTreeMap { map, .. } => map.range(range).map_err(Miss),
            Self::HashMap { .. } => panic!("range called on a HashMap reader_map"),
        }
    }

    pub(crate) fn add_range<R>(&mut self, range: R)
    where
        R: RangeBounds<K> + Clone,
    {
        if let Self::BTreeMap { ref mut map, .. } = self {
            map.insert_range(range);
        }
    }

    pub(crate) fn remove_range<R>(&mut self, range: &R)
    where
        R: RangeBounds<K> + Clone,
    {
        match self {
            Self::BTreeMap { map, .. } => {
                // Returns an iterator, but we don't actually care about the elements (and dropping
                // the iterator still does all the removal)
                map.remove_range(range.clone()).for_each(|_| ());
            }
            Self::HashMap { .. } => panic!("remove_range called on a HashMap reader_map"),
        }
    }

    pub(crate) fn contains_range<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<K> + Clone,
    {
        match self {
            Self::BTreeMap { map, .. } => map.contains_range(range),
            Self::HashMap { .. } => panic!("contains_range called on a HashMap reader_map"),
        }
    }
}

impl<K, V, S, D> Data<K, V, S, D>
where
    K: Ord + Clone + Hash,
    S: BuildHasher,
    D: DropBehavior,
{
    pub(crate) fn get<Q>(&self, k: &Q) -> Option<&ValuesInner<V, S, D>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.get(k))
    }

    pub(crate) fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut ValuesInner<V, S, D>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        with_map!(self, |map| map.get_mut(k))
    }

    pub(crate) fn remove<Q>(&mut self, k: &Q) -> Option<ValuesInner<V, S, D>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        with_map!(self, |map| map.remove(k))
    }

    pub(crate) fn get_key_value<'a>(
        &'a self,
        k: &'a K,
    ) -> Option<(&'a K, &'a ValuesInner<V, S, D>)> {
        with_map!(self, |map| map.get_key_value(k))
    }

    pub(crate) fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.contains_key(k))
    }

    pub(crate) fn entry(&mut self, key: K) -> Entry<'_, K, V, S, D> {
        match self {
            Data::BTreeMap { map, .. } => match map.entry(key) {
                partial_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::BTreeMap(v)),
                partial_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::BTreeMap(o)),
            },
            Data::HashMap { map } => match map.entry(key) {
                hash_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::HashMap(v)),
                hash_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::HashMap(o)),
            },
        }
    }
}

impl<K, V, S, D> Extend<(K, ValuesInner<V, S, D>)> for Data<K, V, S, D>
where
    K: Ord + Clone + Eq + Hash,
    S: BuildHasher,
    D: DropBehavior,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (K, ValuesInner<V, S, D>)>,
    {
        match self {
            Self::BTreeMap { ref mut map, .. } => map.extend(iter),
            Self::HashMap { ref mut map } => map.extend(iter),
        }
    }
}

pub(crate) enum VacantEntry<'a, K, V, S, D: DropBehavior>
where
    K: Ord + Clone,
{
    HashMap(hash_map::VacantEntry<'a, K, ValuesInner<V, S, D>>),
    BTreeMap(partial_map::VacantEntry<'a, K, ValuesInner<V, S, D>>),
}

impl<'a, K: 'a, V: 'a, S, D: DropBehavior> VacantEntry<'a, K, V, S, D>
where
    K: Ord + Clone,
{
    pub(crate) fn insert(self, value: ValuesInner<V, S, D>) -> &'a mut ValuesInner<V, S, D> {
        match self {
            Self::HashMap(e) => e.insert(value),
            Self::BTreeMap(e) => e.insert(value),
        }
    }
}

pub(crate) enum OccupiedEntry<'a, K, V, S, D: DropBehavior>
where
    K: Ord + Clone,
{
    HashMap(hash_map::OccupiedEntry<'a, K, ValuesInner<V, S, D>>),
    BTreeMap(partial_map::OccupiedEntry<'a, K, ValuesInner<V, S, D>>),
}

impl<'a, K: 'a, V: 'a, S, D: DropBehavior> OccupiedEntry<'a, K, V, S, D>
where
    K: Ord + Clone,
{
    pub(crate) fn into_mut(self) -> &'a mut ValuesInner<V, S, D> {
        match self {
            Self::HashMap(e) => e.into_mut(),
            Self::BTreeMap(e) => e.into_mut(),
        }
    }
}

pub(crate) enum Entry<'a, K, V, S, D: DropBehavior>
where
    K: Ord + Clone,
{
    Vacant(VacantEntry<'a, K, V, S, D>),
    Occupied(OccupiedEntry<'a, K, V, S, D>),
}

impl<'a, K: 'a, V: 'a, S, D: DropBehavior> Entry<'a, K, V, S, D>
where
    K: Ord + Clone,
{
    pub(crate) fn or_insert_with<F>(self, default: F) -> &'a mut ValuesInner<V, S, D>
    where
        F: FnOnce() -> ValuesInner<V, S, D>,
    {
        match self {
            Entry::Vacant(e) => e.insert(default()),
            Entry::Occupied(e) => e.into_mut(),
        }
    }
}

pub(crate) struct Inner<K, V, M, T, S, D = crate::aliasing::NoDrop>
where
    K: Ord + Clone,
    S: BuildHasher,
    D: DropBehavior,
    T: Clone,
{
    pub(crate) data: Data<K, V, S, D>,
    pub(crate) meta: M,
    pub(crate) timestamp: T,
    pub(crate) ready: bool,
    pub(crate) hasher: S,
}

impl<K, V, M, T, S> fmt::Debug for Inner<K, V, M, T, S>
where
    K: Ord + Clone + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
    M: fmt::Debug,
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("data", &self.data)
            .field("meta", &self.meta)
            .field("ready", &self.ready)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl<K, V, M, T, S> Clone for Inner<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher + Clone,
    M: Clone,
    T: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: self.data.empty(),
            meta: self.meta.clone(),
            timestamp: self.timestamp.clone(),
            ready: self.ready,
            hasher: self.hasher.clone(),
        }
    }
}

impl<K, V, M, T, S> Inner<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher + Clone,
    T: Clone,
{
    pub(crate) fn with_index_type_and_hasher(
        index_type: IndexType,
        meta: M,
        timestamp: T,
        hasher: S,
    ) -> Self {
        Inner {
            data: Data::with_index_type_and_hasher(index_type, hasher.clone()),
            meta,
            timestamp,
            ready: false,
            hasher,
        }
    }
}
