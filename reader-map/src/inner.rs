use std::borrow::Borrow;
use std::collections::{hash_map, HashMap};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::{Bound, RangeBounds};

use iter_enum::{ExactSizeIterator, Iterator};
use itertools::Either;
use partial_map::PartialMap;
use readyset_client::internal::IndexType;

use crate::eviction::{EvictionMeta, EvictionStrategy};
use crate::values::Values;

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

/// Data contains the mapping from Keys to sets of Values.
#[derive(Clone, Iterator, ExactSizeIterator)]
pub(crate) enum Data<K, V, S> {
    /// Data is stored in a BTreeMap, both point and range lookups are possible
    BTreeMap(PartialMap<K, Values<V>>),
    /// Data is stored in a HashMap, only point lookups are possible
    HashMap(HashMap<K, Values<V>, S>),
}

impl<K, V, S> fmt::Debug for Data<K, V, S>
where
    K: Ord + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BTreeMap(map) => f.debug_struct("BTreeMap").field("map", &map).finish(),
            Self::HashMap(map) => f.debug_struct("HashMap").field("map", &map).finish(),
        }
    }
}

macro_rules! with_map {
    ($data: expr, |$map: ident| $body: expr) => {
        match $data {
            Data::BTreeMap($map) => $body,
            Data::HashMap($map) => $body,
        }
    };
}

pub(crate) type Iter<'a, K, V> =
    Either<partial_map::Iter<'a, K, Values<V>>, hash_map::Iter<'a, K, Values<V>>>;

impl<K, V, S> Data<K, V, S> {
    pub(crate) fn with_index_type_and_hasher(index_type: IndexType, hash_builder: S) -> Self {
        match index_type {
            IndexType::HashMap => Self::HashMap(HashMap::with_hasher(hash_builder)),
            IndexType::BTreeMap => Self::BTreeMap(Default::default()),
        }
    }

    pub(crate) fn index_type(&self) -> IndexType {
        match self {
            Self::BTreeMap(..) => IndexType::BTreeMap,
            Self::HashMap(..) => IndexType::HashMap,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        with_map!(self, |map| map.is_empty())
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Data::BTreeMap(map) => map.num_keys(),
            Data::HashMap(map) => map.len(),
        }
    }

    /// Returns an empty version of the *same* type of index as this [`Data`]
    pub(crate) fn empty(&self) -> Self
    where
        S: Clone,
    {
        match self {
            Self::BTreeMap(..) => Self::BTreeMap(Default::default()),
            Self::HashMap(map) => {
                Self::with_index_type_and_hasher(IndexType::HashMap, (*map.hasher()).clone())
            }
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        match self {
            Self::BTreeMap(map) => Either::Left(map.iter()),
            Self::HashMap(map) => Either::Right(map.iter()),
        }
    }

    pub(crate) fn clear(&mut self) {
        match self {
            Self::BTreeMap(map) => {
                map.clear();
            }
            Self::HashMap(map) => {
                map.clear();
            }
        }
    }

    pub(crate) fn range<R, Q>(
        &'_ self,
        range: &R,
    ) -> Result<partial_map::Range<'_, K, Values<V>>, Miss<K>>
    where
        R: RangeBounds<Q>,
        K: Borrow<Q> + Ord + Clone,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
    {
        match self {
            Self::BTreeMap(map, ..) => map.range(range).map_err(Miss),
            Self::HashMap(..) => panic!("range called on a HashMap reader_map"),
        }
    }

    pub(crate) fn add_range<R>(&mut self, range: R)
    where
        R: RangeBounds<K>,
        K: Ord + Clone,
    {
        if let Self::BTreeMap(ref mut map, ..) = self {
            map.insert_range(range);
        }
    }

    pub(crate) fn remove_range<R>(&mut self, range: R)
    where
        R: RangeBounds<K>,
        K: Ord + Clone,
    {
        match self {
            Self::BTreeMap(map, ..) => {
                // Returns an iterator, but we don't actually care about the elements (and dropping
                // the iterator still does all the removal)
                let _ = map.remove_range(range);
            }
            Self::HashMap(..) => panic!("remove_range called on a HashMap reader_map"),
        }
    }

    pub(crate) fn contains_range<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<K>,
        K: Ord,
    {
        match self {
            Self::BTreeMap(map, ..) => map.contains_range(range),
            Self::HashMap(..) => panic!("contains_range called on a HashMap reader_map"),
        }
    }

    pub(crate) fn overlaps_range<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<K>,
        K: Ord,
    {
        match self {
            Self::BTreeMap(map, ..) => map.overlaps_range(range),
            Self::HashMap(..) => panic!("contains_range called on a HashMap reader_map"),
        }
    }
}

impl<K, V, S> Data<K, V, S>
where
    K: Eq + Hash + Ord,
    S: BuildHasher,
{
    pub(crate) fn get<Q>(&self, k: &Q) -> Option<&Values<V>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.get(k))
    }

    pub(crate) fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut Values<V>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        with_map!(self, |map| map.get_mut(k))
    }

    pub(crate) fn remove<Q>(&mut self, k: &Q) -> Option<Values<V>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        with_map!(self, |map| map.remove(k))
    }

    pub(crate) fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.contains_key(k))
    }

    pub(crate) fn entry(&mut self, key: K) -> Entry<'_, K, V>
    where
        K: Clone,
    {
        match self {
            Data::BTreeMap(map) => match map.entry(key) {
                partial_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::BTreeMap(v)),
                partial_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::BTreeMap(o)),
            },
            Data::HashMap(map) => match map.entry(key) {
                hash_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::HashMap(v)),
                hash_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::HashMap(o)),
            },
        }
    }
}

pub(crate) enum VacantEntry<'a, K, V>
where
    K: Ord,
{
    HashMap(hash_map::VacantEntry<'a, K, Values<V>>),
    BTreeMap(partial_map::VacantEntry<'a, K, Values<V>>),
}

impl<'a, K: 'a, V: 'a> VacantEntry<'a, K, V>
where
    K: Ord + Clone,
{
    pub(crate) fn insert(self, value: Values<V>) -> &'a mut Values<V> {
        match self {
            Self::HashMap(e) => e.insert(value),
            Self::BTreeMap(e) => e.insert(value),
        }
    }
}

pub(crate) enum OccupiedEntry<'a, K, V>
where
    K: Ord,
{
    HashMap(hash_map::OccupiedEntry<'a, K, Values<V>>),
    BTreeMap(partial_map::OccupiedEntry<'a, K, Values<V>>),
}

impl<'a, K: 'a, V: 'a> OccupiedEntry<'a, K, V>
where
    K: Ord + Clone,
{
    pub(crate) fn into_mut(self) -> &'a mut Values<V> {
        match self {
            Self::HashMap(e) => e.into_mut(),
            Self::BTreeMap(e) => e.into_mut(),
        }
    }
}

pub(crate) enum Entry<'a, K, V>
where
    K: Ord,
{
    Vacant(VacantEntry<'a, K, V>),
    Occupied(OccupiedEntry<'a, K, V>),
}

impl<'a, K: 'a, V: 'a> Entry<'a, K, V>
where
    K: Ord + Clone,
{
    pub(crate) fn or_insert_with<F>(self, default: F) -> &'a mut Values<V>
    where
        F: FnOnce() -> Values<V>,
    {
        match self {
            Entry::Vacant(e) => e.insert(default()),
            Entry::Occupied(e) => e.into_mut(),
        }
    }
}

pub(crate) struct Inner<K, V, M, T, S, I> {
    pub(crate) data: Data<K, V, S>,
    pub(crate) meta: M,
    pub(crate) timestamp: T,
    pub(crate) ready: bool,
    pub(crate) hasher: S,
    pub(crate) eviction_strategy: EvictionStrategy,
    pub(crate) insertion_order: Option<I>,
}

impl<K, V, M, T, S, I> fmt::Debug for Inner<K, V, M, T, S, I>
where
    K: Ord + Clone + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
    M: fmt::Debug,
    T: fmt::Debug,
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

impl<K, V, M, T, S, I> Clone for Inner<K, V, M, T, S, I>
where
    K: Ord + Clone,
    S: BuildHasher + Clone,
    M: Clone,
    T: Clone,
    I: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: self.data.empty(),
            meta: self.meta.clone(),
            timestamp: self.timestamp.clone(),
            ready: self.ready,
            hasher: self.hasher.clone(),
            eviction_strategy: self.eviction_strategy.clone(),
            insertion_order: self.insertion_order.clone(),
        }
    }
}

impl<K, V, M, T, S, I> Inner<K, V, M, T, S, I>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    T: Clone,
{
    pub(crate) fn with_index_type_and_hasher(
        index_type: IndexType,
        meta: M,
        timestamp: T,
        hasher: S,
        eviction_strategy: EvictionStrategy,
        insertion_order: Option<I>,
    ) -> Self {
        Inner {
            data: Data::with_index_type_and_hasher(index_type, hasher.clone()),
            meta,
            timestamp,
            ready: false,
            hasher,
            eviction_strategy,
            insertion_order,
        }
    }

    pub(crate) fn data_entry(
        &mut self,
        key: K,
        eviction_meta: &mut Option<EvictionMeta>,
    ) -> &mut Values<V> {
        self.data.entry(key).or_insert_with(|| {
            if let Some(meta) = eviction_meta.take() {
                Values::new(meta)
            } else {
                let meta = self.eviction_strategy.new_meta();
                eviction_meta.replace(meta.clone());
                Values::new(meta)
            }
        })
    }
}
