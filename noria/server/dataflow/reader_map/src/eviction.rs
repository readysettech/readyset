//! Implementations of various eviction strategies for the reader map.
//! All of the strategies are incorprated into a single enum, [`EvictionStrategy`],
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
//! Currently three strategies are implemented:
//!
//! Random: simply sample an rng to evict the required number of keys
//! LRU: evicts the least recently used keys
//! Generational: like LRU but the count is inexact, and bucketed into
//! generations, generation is counted as one eviction cycle.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use itertools::Either;
use rand::Rng;

use crate::inner::Data;
use crate::values::ValuesInner;

/// Controls the maximum number of generations in Generational eviction.
/// The value of 100 ensures the granularity will be at least 1%.
const NUM_GENERATIONS: usize = 100;

/// Handles the eviction of keys from the reader map
#[derive(Clone, Debug)]
pub enum EvictionStrategy {
    /// Evict keys at random
    Random(RandomEviction),
    /// Keeps track of how recently an entry was read, and evicts the ones that weren't in use
    /// recently
    LeastRecentlyUsed(LRUEviction),
    /// Keeps track of how recently an entry was read with a generation accuracy, evicts the ones
    /// that are oldest
    Generational(GenerationalEviction),
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

/// Performs an approximate LRU eviction.
/// The structure keeps track of the total number of evictions that took place. We call that value
/// a `generation`. When a key is read, we copy the value of the current generation to its metadata.
/// This way each key tracks the value of the generation when it was last read.
/// When performing an eviction we sort the metadata into generation buckets, and compute the number
/// of keys to delete from each generation, where the oldest generations are evicted first.
#[derive(Clone, Default, Debug)]
pub struct GenerationalEviction(Arc<AtomicU64>);

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

    /// Create a generational eviction strategy
    pub fn new_generational() -> EvictionStrategy {
        EvictionStrategy::Generational(Default::default())
    }

    /// Create new `EvictionMeta` for a newly added key
    pub(crate) fn new_meta(&self) -> EvictionMeta {
        match self {
            EvictionStrategy::Random(_) => Default::default(),
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.new_meta(),
            EvictionStrategy::Generational(gen) => gen.new_meta(),
        }
    }

    /// Update the metadata following a read event
    pub(crate) fn on_read(&self, meta: &EvictionMeta) {
        match self {
            EvictionStrategy::Random(_) => {}
            EvictionStrategy::LeastRecentlyUsed(lru) => lru.on_read(meta),
            EvictionStrategy::Generational(gen) => gen.on_read(meta),
        }
    }

    /// Return an iterator over the keys and values the strategy suggests to evict
    /// this cycle. Nothing is actually evicted following this call.
    pub(crate) fn pick_keys_to_evict<'a, K, V, S>(
        &self,
        data: &'a Data<K, V, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a ValuesInner<V, S, crate::aliasing::NoDrop>)>
    where
        K: Ord + Clone,
        S: std::hash::BuildHasher,
    {
        match self {
            EvictionStrategy::Random(rand) => Either::Left(rand.pick_keys_to_evict(data, nkeys)),
            EvictionStrategy::LeastRecentlyUsed(lru) => {
                Either::Right(Either::Left(lru.pick_keys_to_evict(data, nkeys)))
            }
            EvictionStrategy::Generational(gen) => {
                Either::Right(Either::Right(gen.pick_keys_to_evict(data, nkeys)))
            }
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

    fn pick_keys_to_evict<'a, K, V, S>(
        &self,
        data: &'a Data<K, V, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a ValuesInner<V, S, crate::aliasing::NoDrop>)>
    where
        K: Ord + Clone,
        S: std::hash::BuildHasher,
    {
        // TODO(vlad): implement exact size iterator for data iterator
        // First we collect all the meta values into a single vector
        let mut ctrs = data
            .iter()
            .map(|(_, v)| v.as_ref().eviction_meta().value())
            .collect::<Vec<_>>();

        let ctrs_save = ctrs.clone(); // Save the counters before sorting them to avoid atomic loads for the second time

        // We then find the value of the counter with the nkey'th value
        let cutoff = {
            let (_, val, _) = ctrs.select_nth_unstable(nkeys);
            *val
        };

        // We return the iterator over the keys whos counter value is lower than that
        ctrs_save
            .into_iter()
            .zip(data.iter())
            .filter_map(move |(ctr, kv)| (ctr <= cutoff).then(|| kv))
    }
}

impl RandomEviction {
    fn pick_keys_to_evict<'a, K, V, S>(
        &self,
        data: &'a Data<K, V, S>,
        nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a ValuesInner<V, S, crate::aliasing::NoDrop>)>
    where
        K: Ord + Clone,
        S: std::hash::BuildHasher,
    {
        let ratio = nkeys as f64 / data.len() as f64;
        let mut rng = rand::thread_rng();
        // We return the iterator over the keys whos counter value is lower than that
        data.iter().filter(move |_| rng.gen_bool(ratio))
    }
}

impl GenerationalEviction {
    fn new_meta(&self) -> EvictionMeta {
        EvictionMeta(AtomicU64::new(self.0.load(Relaxed)).into())
    }

    fn on_read(&self, meta: &EvictionMeta) {
        // Generational simply assigns the generation counter to the metadata
        let current_counter = self.0.load(Relaxed);
        meta.0.store(current_counter, Relaxed);
    }

    fn pick_keys_to_evict<'a, K, V, S>(
        &self,
        data: &'a Data<K, V, S>,
        mut nkeys: usize,
    ) -> impl Iterator<Item = (&'a K, &'a ValuesInner<V, S, crate::aliasing::NoDrop>)>
    where
        K: Ord + Clone,
        S: std::hash::BuildHasher,
    {
        let current_gen = self.0.fetch_add(1, Relaxed);

        let mut buckets = [0usize; NUM_GENERATIONS];

        // Load the atomic values just once
        let ctrs = data
            .iter()
            .map(|(_, v)| v.as_ref().eviction_meta().value())
            .collect::<Vec<_>>();

        // We first count how many values are there to evict for each generation (up to
        // NUM_GENERATIONS generations back)
        ctrs.iter().for_each(|gen| {
            let bucket = ((current_gen - *gen) as usize).min(buckets.len() - 1);
            buckets[bucket] += 1;
        });

        // At this point at `bucket[0]` we have the count for keys in the current generation
        // `bucket[1]` for previous etc. We need to free a total of `nkeys`. We start from the
        // highest bucket, attempting to free an entire generation where possible. Finally if
        // we don't have sufficient keys from freeing entire generations, we will parially evict
        // another genration at random to get the required amount of keys

        // Everything before full_gen_to_evict, will be evicted fully
        // Everything *in* last_gen_to_evict, will be evicted randomly
        let mut last_bucket_to_evict = buckets.len();
        for cnt in buckets.iter().rev() {
            last_bucket_to_evict -= 1;
            if *cnt <= nkeys {
                nkeys -= *cnt;
            } else {
                break;
            }
        }

        // After the loop is finished, `nkeys` is the number of keys we have left
        // to evict the current bucket (`last_bucket_to_evict`). We will evict as
        // many keys from that bucket at random.
        let rand_ratio = nkeys as f64 / buckets[last_bucket_to_evict] as f64;
        let last_gen_to_evict = current_gen - last_bucket_to_evict as u64;

        let mut rng = rand::thread_rng();

        // We return the iterator over the keys to be evicted according to their generation
        ctrs.into_iter()
            .zip(data.iter())
            .filter_map(move |(gen, kv)| match gen {
                g if g < last_gen_to_evict => Some(kv),
                g if g == last_gen_to_evict => rng.gen_bool(rand_ratio).then(|| kv),
                _ => None,
            })
    }
}
