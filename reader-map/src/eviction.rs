//! Implementations of various eviction strategies for the reader map.
//! All of the strategies are incorporated into a single enum, [`EvictionStrategy`],
//! using a single enum allows for a faster dispatch than using a dyn object,
//! for as long as the number of strategies is reasonable.
//!
//! The eviction has two components: the first one is the read component:
//! whenever a key is accessed the `on_read` method is called to update
//! the key metadata with a strategy specific method.
//!
//! The second component is `pick_keys_to_evict`, which is called whenever the
//! reader exceeds its memory quota. Once called the strategy will return an
//! iterator over the list of keys it proposes to evict.
//!
//! Currently two strategies are implemented:
//!
//! Random: simply sample an rng to evict the required number of keys
//! LRU: evicts the least recently used keys

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use itertools::Either;
use rand::seq::SliceRandom;

use crate::inner::Data;
use crate::values::Values;
use crate::InsertionOrder;

/// Handles the eviction of keys from the reader map
#[derive(Clone, Debug)]
pub enum EvictionStrategy {
    /// Evict keys at random
    Random(RandomEviction),
    /// Keeps track of how recently an entry was read, and evicts the ones that weren't in use
    /// recently
    LeastRecentlyUsed(LRUEviction),
}

impl Default for EvictionStrategy {
    fn default() -> Self {
        EvictionStrategy::Random(RandomEviction)
    }
}

/// Used to store strategy specific metadata for every key in the reader map
#[derive(Default, Clone, Debug)]
#[repr(transparent)]
pub struct EvictionMeta(Arc<AtomicU64>);

#[derive(Clone, Debug)]
pub struct RandomEviction;

/// Performs Least Recently Used eviction.
/// The structure keeps track of the total number of reads from the map in an atomic counter that is
/// incremented each time a read happens. This counter value is copied to the metadata of the key
/// that triggered the read. This way the metadata for the key that was read last always contains
/// the greatest counter value, and those values are monotonically increasing.
/// When performing an eviction we then simply evict the keys with the smallest counter value.
#[derive(Clone, Default, Debug)]
pub struct LRUEviction(Arc<AtomicU64>);

/// An iterator of sorts over [`EvictRangeGroup`] that groups together consecutive runs of evicted
/// keys in a BTreeMap map. Does not actually implement iterator as that would require a lending
/// iterator trait, which is not yet available (and the crate doesn't fit here well)
pub struct EvictRangeIter<K, I, F>
where
    F: FnMut(u64) -> bool,
    I: Iterator<Item = (u64, K)>,
{
    iter: I,
    group_by: F,
    next: Option<K>,
}

/// An iterator over a group of (K, Values) to be evicted from the map
pub struct EvictRangeGroup<'a, K, I, F>
where
    F: FnMut(u64) -> bool,
    I: Iterator<Item = (u64, K)>,
{
    inner: &'a mut EvictRangeIter<K, I, F>,
}

impl<K, I, F> EvictRangeIter<K, I, F>
where
    F: FnMut(u64) -> bool,
    I: Iterator<Item = (u64, K)>,
{
    /// Return the next consecutive range of keys to be evicted, the first and the last items in the
    /// group are the range bounds.
    pub fn next_range(&mut self) -> Option<EvictRangeGroup<'_, K, I, F>> {
        loop {
            // When out of elements to peek, we are done
            let next_key = self.iter.next()?;
            if (self.group_by)(next_key.0) {
                // If predicate is true, we reached the next range
                self.next = Some(next_key.1);
                break;
            }
        }

        Some(EvictRangeGroup { inner: self })
    }
}

impl<'a, K, I, F> Iterator for EvictRangeGroup<'a, K, I, F>
where
    F: FnMut(u64) -> bool,
    I: Iterator<Item = (u64, K)>,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.inner.next.take() {
            Some(v) => return Some(v),
            None => self.inner.iter.next()?,
        };

        (self.inner.group_by)(next.0).then_some(next.1)
    }
}

impl EvictionMeta {
    pub fn value(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

impl EvictionStrategy {
    /// Create an LRU eviction strategy
    pub fn new_lru() -> EvictionStrategy {
        EvictionStrategy::LeastRecentlyUsed(Default::default())
    }

    /// Create a random eviction strategy
    pub fn new_random() -> EvictionStrategy {
        EvictionStrategy::Random(RandomEviction)
    }

    /// Create new `EvictionMeta` for a newly added key
    pub(crate) fn new_meta(&self) -> EvictionMeta {
        match self {
            EvictionStrategy::Random(_) => Default::default(),
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.new_meta(),
        }
    }

    /// Update the metadata following a read event
    pub(crate) fn on_read(&self, meta: &EvictionMeta) {
        match self {
            EvictionStrategy::Random(_) => {}
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.on_read(meta),
        }
    }

    /// Return an iterator over the keys and values the strategy suggests to evict
    /// this cycle. Nothing is actually evicted following this call.
    pub(crate) fn pick_keys_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a Values<V, I>)>
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        match self {
            EvictionStrategy::Random(rand) => Either::Left(rand.pick_keys_to_evict(data, nkeys)),
            EvictionStrategy::LeastRecentlyUsed(lru) => {
                Either::Right(lru.pick_keys_to_evict(data, nkeys))
            }
        }
    }

    /// Returns a [`EvictRangeIter`] that iterates over groups of consecutive keys the strategy
    /// would suggest to evict. The first and last element of each group would form a range that
    /// should be evicted.
    pub(crate) fn pick_ranges_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> EvictRangeIter<
        (&'a K, &'a Values<V, I>),
        impl Iterator<Item = (u64, (&'a K, &'a Values<V, I>))>,
        impl FnMut(u64) -> bool,
    >
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        let mut lru_f = None;
        let mut rand_f = None;
        let iter = match self {
            EvictionStrategy::LeastRecentlyUsed(lru) => {
                let (iter, group_by) = lru.pick_ranges_to_evict(data, nkeys);
                lru_f = Some(group_by);
                Either::Left(iter)
            }
            EvictionStrategy::Random(rand) => {
                let (iter, group_by) = rand.pick_ranges_to_evict(data, nkeys);
                rand_f = Some(group_by);
                Either::Right(iter)
            }
        };

        EvictRangeIter {
            iter,
            group_by: move |val| {
                // This freak show is because we don't have an Either equivalent for Fn
                if let Some(f) = lru_f.as_mut() {
                    f(val)
                } else {
                    (rand_f.as_mut().unwrap())(val)
                }
            },
            next: None,
        }
    }
}

impl LRUEviction {
    fn new_meta(&self) -> EvictionMeta {
        EvictionMeta(AtomicU64::new(self.0.fetch_add(1, Relaxed)).into())
    }

    fn on_read(&self, meta: &EvictionMeta) {
        // For least recently used eviction strategy, we store the current value
        // of the shared counter in the meta, while incrementing its value.
        let current_counter = self.0.fetch_add(1, Relaxed);
        // Note: when storing the counter, we don't actually check if its value is
        // greater than the currently stored one, so it is possible for it to go
        // backwards, but this sort of accuracy is not our goal here, we prefer to
        // be (maybe) less accurate, but more performant.
        meta.0.store(current_counter, Relaxed);
    }

    fn pick_keys_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a Values<V, I>)>
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        // First we collect all the meta values into a single vector
        let mut ctrs = data
            .iter()
            .map(|(_, v)| v.eviction_meta().value())
            .collect::<Vec<_>>();

        let ctrs_save = ctrs.clone(); // Save the counters before sorting them to avoid atomic loads for the second time

        // We then find the value of the counter with the nkey'th value
        let cutoff = if nkeys >= ctrs.len() {
            u64::MAX
        } else {
            let (_, val, _) = ctrs.select_nth_unstable(nkeys);
            *val
        };

        // We return the iterator over the keys whose counter value is lower than that
        ctrs_save
            .into_iter()
            .zip(data.iter())
            .filter_map(move |(ctr, kv)| (ctr <= cutoff).then_some(kv))
    }

    fn pick_ranges_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> (
        impl Iterator<Item = (u64, (&'a K, &'a Values<V, I>))>,
        impl FnMut(u64) -> bool,
    )
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        // First we collect all the meta values into a single vector
        let mut ctrs = data
            .iter()
            .map(|(_, v)| v.eviction_meta().value())
            .collect::<Vec<_>>();

        let ctrs_save = ctrs.clone(); // Save the counters before sorting them to avoid atomic loads for the second time

        // We then find the value of the counter with the nkey'th value
        let cutoff = if nkeys >= ctrs.len() {
            u64::MAX
        } else {
            let (_, val, _) = ctrs.select_nth_unstable(nkeys);
            *val
        };

        (ctrs_save.into_iter().zip(data.iter()), move |ctr| {
            ctr <= cutoff
        })
    }
}

impl RandomEviction {
    /// Selects exactly nkeys keys to evict.
    fn pick_keys_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a Values<V, I>)>
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        // Allocate a random shuffling of indices corresponding to keys in data.
        let mut rng = rand::thread_rng();
        let mut indices = (0..nkeys).collect::<Vec<_>>();
        indices.shuffle(&mut rng);
        // Return an iterator yielding elements with idx < nkeys
        indices
            .into_iter()
            .zip(data.iter())
            .filter_map(move |(idx, entry)| (idx < nkeys).then_some(entry))
    }

    /// Selects exactly nkeys BTreeMap keys (not ranges) to evict.
    /// nkeys *must* be no more than data.len()
    fn pick_ranges_to_evict<'a, K, V, I, S>(
        &self,
        data: &'a Data<K, V, I, S>,
        nkeys: usize,
    ) -> (
        impl Iterator<Item = (u64, (&'a K, &'a Values<V, I>))>,
        impl FnMut(u64) -> bool,
    )
    where
        K: Ord + Clone,
        I: InsertionOrder<V>,
        S: std::hash::BuildHasher,
    {
        // Picking a random range to evict is kinda useless really, there is very little chance it
        // will be able to form proper ranges, unless ratio is high, but oh well, don't use random
        // for ranges I suppose.

        // Allocate a random shuffling of indices corresponding to keys in data.
        let mut rng = rand::thread_rng();
        let mut indices = (0..nkeys as u64).collect::<Vec<_>>();
        indices.shuffle(&mut rng);
        // Return an iterator over all elements and a cutoff function selecting all indices less
        // than nkeys
        (indices.into_iter().zip(data.iter()), move |v| {
            v < nkeys as u64
        })
    }
}
