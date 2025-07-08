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
//! The eviction strategy is approximate LRU.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use rand::Rng;

use crate::inner::Data;
use crate::values::Values;
use crate::InsertionOrder;

static CLOCK_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Handles the eviction of keys from the reader map
#[derive(Clone, Debug)]
pub enum EvictionStrategy {
    /// Keeps track of how recently an entry was read, and evicts the ones that weren't in use
    /// recently
    LeastRecentlyUsed(LRUEviction),
}

impl Default for EvictionStrategy {
    fn default() -> Self {
        EvictionStrategy::LeastRecentlyUsed(LRUEviction)
    }
}

/// Used to store strategy specific metadata for every key in the reader map
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct EvictionMeta(Arc<AtomicU64>);

/// Performs Least Recently Used eviction.
///
/// On read of a key, the current time in milliseconds is written to the metadata of the key
/// that triggered the read.  Access to the metadata is unsynchronized, so the timestamp may
/// may not be exactly accurate, though it should be fairly accurate.
///
/// When evicting, we sample some timestamps to establish an (approximately accurate) threshold
/// and then evict keys with earlier timestamps.
#[derive(Clone, Debug)]
pub struct LRUEviction;

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

impl<K, I, F> Iterator for EvictRangeGroup<'_, K, I, F>
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

impl Default for EvictionMeta {
    fn default() -> Self {
        EvictionMeta(AtomicU64::new(now()).into())
    }
}

impl EvictionStrategy {
    /// Create an LRU eviction strategy
    pub fn new_lru() -> EvictionStrategy {
        EvictionStrategy::LeastRecentlyUsed(LRUEviction)
    }

    /// Create new `EvictionMeta` for a newly added key
    pub(crate) fn new_meta(&self) -> EvictionMeta {
        match self {
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.new_meta(),
        }
    }

    /// Update the metadata following a read event
    pub(crate) fn on_read(&self, meta: &EvictionMeta) {
        match self {
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
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.pick_keys_to_evict(data, nkeys),
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
        let (iter, group_by) = match self {
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.pick_ranges_to_evict(data, nkeys),
        };

        EvictRangeIter {
            iter,
            group_by,
            next: None,
        }
    }
}

fn now() -> u64 {
    (Instant::now() - *CLOCK_START).as_millis() as _
}

impl LRUEviction {
    fn new_meta(&self) -> EvictionMeta {
        EvictionMeta(AtomicU64::new(now()).into())
    }

    fn on_read(&self, meta: &EvictionMeta) {
        meta.0.store(now(), Relaxed);
    }

    fn step_skip<K, V, I, S>(data: &Data<K, V, I, S>, nkeys: usize) -> (usize, usize)
    where
        I: InsertionOrder<V>,
    {
        // Instead of iterating over every value in the map, we choose a cutoff value for the
        // timestamp via a very approximate process.  Choose a sub-linear fraction to sample
        // and a random starting point.
        let step = data.len() / ((nkeys as f64).sqrt() as usize).max(1);
        let skip = rand::rng().random_range(0..step);
        (step, skip)
    }

    fn cutoff(meta: &mut [u64], nkeys: usize, step: usize) -> u64 {
        if meta.is_empty() {
            return u64::MAX; // select_nth_unstable panics
        }

        // Find the cutoff, scaling by the sampling factor.
        let (_, cutoff, _) = meta.select_nth_unstable((nkeys / step).min(meta.len() - 1));
        *cutoff
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
        let (step, skip) = Self::step_skip(data, nkeys);

        let mut meta = Vec::new();
        let mut i = skip;
        while i < data.len() {
            meta.push(data.get_index_value(i).unwrap().eviction_meta().value());
            i += step;
        }

        let cutoff = Self::cutoff(&mut meta, nkeys, step);

        // We might return more or fewer keys than requested due to approximation.  In testing,
        // errors of up to 50% were observed.  But this sampling is very cheap, and if we don't
        // evict enough, we'll just evict again soon.
        data.iter()
            .filter_map(move |(k, v)| (v.eviction_meta().value() <= cutoff).then_some((k, v)))
            .take(nkeys)
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
        let (step, skip) = Self::step_skip(data, nkeys);

        // This is less efficient than the HashMap version above because we have to iterate
        // over the entire B-tree even though we only want a fractional sample of it.
        let mut meta = data
            .iter()
            .skip(skip)
            .step_by(step)
            .map(|(_, v)| v.eviction_meta().value())
            .collect::<Vec<_>>();

        let cutoff = Self::cutoff(&mut meta, nkeys, step);

        (
            data.iter()
                .map(move |(k, v)| (v.eviction_meta().value(), (k, v))),
            move |ts| ts <= cutoff,
        )
    }
}
