use std::borrow::Borrow;
use std::fmt;
use std::hash::{BuildHasher, Hash};

use indexmap::map::{self, IndexMap};
use iter_enum::{ExactSizeIterator, Iterator};
use itertools::Either;
use metrics::{histogram, Histogram};
use partial_map::PartialMap;
use petgraph::graph::NodeIndex;
use readyset_client::internal::IndexType;
use readyset_util::ranges::{Bound, RangeBounds};

use crate::eviction::{EvictionMeta, EvictionStrategy};
use crate::recorded::{READER_MAP_LIFETIMES, READER_MAP_UPDATES};
use crate::values::{Metrics, Values};
use crate::InsertionOrder;

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

/// The mapping from Keys to sets of Values.
#[derive(Clone, Iterator, ExactSizeIterator)]
pub(crate) enum Data<K, V, I, S> {
    /// Data is stored in a BTreeMap; both point and range lookups are possible
    BTreeMap(PartialMap<K, Values<V, I>>),
    /// Data is stored in a IndexMap; only point lookups are possible
    HashMap(IndexMap<K, Values<V, I>, S>),
}

impl<K, V, I, S> fmt::Debug for Data<K, V, I, S>
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

pub(crate) type Iter<'a, K, V, I> =
    Either<partial_map::Iter<'a, K, Values<V, I>>, map::Iter<'a, K, Values<V, I>>>;

impl<K, V, I, S> Data<K, V, I, S>
where
    I: InsertionOrder<V>,
{
    pub(crate) fn with_index_type_and_hasher(index_type: IndexType, hash_builder: S) -> Self {
        match index_type {
            IndexType::HashMap => Self::HashMap(IndexMap::with_hasher(hash_builder)),
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

    pub(crate) fn iter(&self) -> Iter<'_, K, V, I> {
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
    ) -> Result<partial_map::Range<'_, K, Values<V, I>>, Miss<K>>
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

    pub(crate) fn add_full_range(&mut self)
    where
        K: Ord + Clone,
    {
        if let Self::BTreeMap(ref mut map, ..) = self {
            map.insert_full_range();
        }
    }

    pub(crate) fn remove_range<R, F>(&mut self, range: R, f: F)
    where
        R: RangeBounds<K>,
        K: Ord + Clone,
        F: Fn(&Metrics),
    {
        match self {
            Self::BTreeMap(map, ..) => {
                map.remove_range(range).for_each(|(_, v)| f(v.metrics()));
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

impl<K, V, I, S> Data<K, V, I, S>
where
    K: Eq + Hash + Ord,
    I: InsertionOrder<V>,
    S: BuildHasher,
{
    pub(crate) fn get<Q>(&self, k: &Q) -> Option<&Values<V, I>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.get(k))
    }

    pub(crate) fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut Values<V, I>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        with_map!(self, |map| map.get_mut(k))
    }

    pub(crate) fn remove<Q>(&mut self, k: &Q) -> Option<Values<V, I>>
    where
        K: Borrow<Q> + Clone,
        Q: ?Sized + Hash + Ord + ToOwned<Owned = K>,
    {
        match self {
            Data::BTreeMap(map) => map.remove(k),
            Data::HashMap(map) => map.swap_remove(k),
        }
    }

    pub(crate) fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        with_map!(self, |map| map.contains_key(k))
    }

    pub(crate) fn entry(&mut self, key: K) -> Entry<'_, K, V, I>
    where
        K: Clone,
    {
        match self {
            Data::BTreeMap(map) => match map.entry(key) {
                partial_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::BTreeMap(v)),
                partial_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::BTreeMap(o)),
            },
            Data::HashMap(map) => match map.entry(key) {
                map::Entry::Vacant(v) => Entry::Vacant(VacantEntry::HashMap(v)),
                map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry::HashMap(o)),
            },
        }
    }
}

impl<K, V, I, S> Data<K, V, I, S>
where
    I: InsertionOrder<V>,
{
    pub(crate) fn get_index_value(&self, i: usize) -> Option<&Values<V, I>> {
        match self {
            Self::BTreeMap(..) => panic!("get_index_value called on a BTreeMap reader_map"),
            Self::HashMap(map) => map.get_index(i).map(|(_k, v)| v),
        }
    }
}

pub(crate) enum VacantEntry<'a, K, V, I>
where
    K: Ord,
{
    HashMap(map::VacantEntry<'a, K, Values<V, I>>),
    BTreeMap(partial_map::VacantEntry<'a, K, Values<V, I>>),
}

impl<'a, K: 'a, V: 'a, I: 'a> VacantEntry<'a, K, V, I>
where
    K: Ord + Clone,
{
    pub(crate) fn insert(self, value: Values<V, I>) -> &'a mut Values<V, I> {
        match self {
            Self::HashMap(e) => e.insert(value),
            Self::BTreeMap(e) => e.insert(value),
        }
    }
}

pub(crate) enum OccupiedEntry<'a, K, V, I>
where
    K: Ord,
{
    HashMap(map::OccupiedEntry<'a, K, Values<V, I>>),
    BTreeMap(partial_map::OccupiedEntry<'a, K, Values<V, I>>),
}

impl<'a, K: 'a, V: 'a, I: 'a> OccupiedEntry<'a, K, V, I>
where
    K: Ord + Clone,
    I: InsertionOrder<V>,
{
    pub(crate) fn into_mut(self) -> &'a mut Values<V, I> {
        match self {
            Self::HashMap(e) => e.into_mut(),
            Self::BTreeMap(e) => e.into_mut(),
        }
    }
}

pub(crate) enum Entry<'a, K, V, I>
where
    K: Ord,
{
    Vacant(VacantEntry<'a, K, V, I>),
    Occupied(OccupiedEntry<'a, K, V, I>),
}

impl<'a, K: 'a, V: 'a, I: 'a> Entry<'a, K, V, I>
where
    K: Ord + Clone,
    I: InsertionOrder<V>,
{
    pub(crate) fn or_insert_with<F>(self, default: F) -> &'a mut Values<V, I>
    where
        F: FnOnce() -> Values<V, I>,
    {
        match self {
            Entry::Vacant(e) => e.insert(default()),
            Entry::Occupied(e) => e.into_mut(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WriteMetrics {
    // Captures the duration between updates at a given key (entry).
    entry_updated: Histogram,
    // Captures the lifetime of an entry, from the time it was first added
    // until it's eviction.
    lifetime_evict: Histogram,
}

impl WriteMetrics {
    fn new(node_index: Option<NodeIndex>) -> Self {
        let idx = match node_index {
            Some(idx) => idx.index().to_string(),
            None => "-1".to_string(),
        };
        let entry_updated = histogram!(READER_MAP_UPDATES, "node_idx" => idx.clone());
        let lifetime_evict = histogram!(READER_MAP_LIFETIMES, "node_idx" => idx);

        Self {
            entry_updated,
            lifetime_evict,
        }
    }

    pub(crate) fn record_updated(&self, values: &Metrics) {
        if let Some(duration) = values.last_update_interval() {
            self.entry_updated.record(duration.as_millis() as f64);
        }
    }

    pub(crate) fn record_evicted(&self, values: &Metrics) {
        if let Some(lifetime) = values.lifetime() {
            self.lifetime_evict.record(lifetime.as_millis() as f64);
        }
    }
}

pub(crate) struct Inner<K, V, M, T, S, I> {
    pub(crate) data: Data<K, V, I, S>,
    pub(crate) meta: M,
    pub(crate) timestamp: T,
    pub(crate) ready: bool,
    pub(crate) hasher: S,
    pub(crate) eviction_strategy: EvictionStrategy,
    pub(crate) order: I,
    pub(crate) metrics: WriteMetrics,
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
    I: InsertionOrder<V>,
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
            order: self.order.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, M, T, S, I> Inner<K, V, M, T, S, I>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    T: Clone,
    I: InsertionOrder<V>,
{
    pub(crate) fn with_index_type_and_hasher(
        index_type: IndexType,
        meta: M,
        timestamp: T,
        hasher: S,
        eviction_strategy: EvictionStrategy,
        order: I,
        node_index: Option<NodeIndex>,
    ) -> Self {
        Inner {
            data: Data::with_index_type_and_hasher(index_type, hasher.clone()),
            meta,
            timestamp,
            ready: false,
            hasher,
            eviction_strategy,
            order,
            metrics: WriteMetrics::new(node_index),
        }
    }

    pub(crate) fn data_entry(
        &mut self,
        key: K,
        eviction_meta: &mut Option<EvictionMeta>,
    ) -> &mut Values<V, I> {
        self.data.entry(key).or_insert_with(|| {
            if let Some(meta) = eviction_meta.take() {
                Values::new(meta, self.order.clone())
            } else {
                let meta = self.eviction_strategy.new_meta();
                eviction_meta.replace(meta.clone());
                Values::new(meta, self.order.clone())
            }
        })
    }
}
