use std::ops::{Bound, RangeBounds};

use ahash::RandomState;
use dataflow_expression::PreInsertion;
use reader_map::EvictionQuantity;
use readyset_client::consistency::Timestamp;

use super::{key_to_single, Key};
use crate::prelude::*;

pub(super) enum Handle {
    Single(
        reader_map::handles::WriteHandle<
            DfValue,
            Box<[DfValue]>,
            PreInsertion,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
    Many(
        reader_map::handles::WriteHandle<
            Vec<DfValue>,
            Box<[DfValue]>,
            PreInsertion,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
}

impl Handle {
    pub fn base_value_size(&self) -> usize {
        match *self {
            Handle::Single(ref h) => h.base_value_size(),
            Handle::Many(ref h) => h.base_value_size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Handle::Single(ref h) => h.is_empty(),
            Handle::Many(ref h) => h.is_empty(),
        }
    }

    pub fn clear(&mut self, k: Key) {
        match *self {
            Handle::Single(ref mut h) => {
                h.clear(key_to_single(k).into_owned());
            }
            Handle::Many(ref mut h) => {
                h.clear(k.into_owned());
            }
        }
    }

    pub fn empty(&mut self, k: Key) {
        match *self {
            Handle::Single(ref mut h) => {
                h.remove_entry(key_to_single(k).into_owned());
            }
            Handle::Many(ref mut h) => {
                h.remove_entry(k.into_owned());
            }
        }
    }

    pub fn empty_range(&mut self, range: (Bound<Vec<DfValue>>, Bound<Vec<DfValue>>)) {
        match self {
            Handle::Single(h) => {
                h.remove_range((
                    range.0.map(|mut r| {
                        debug_assert_eq!(r.len(), 1);
                        r.pop().unwrap()
                    }),
                    range.1.map(|mut r| {
                        debug_assert_eq!(r.len(), 1);
                        r.pop().unwrap()
                    }),
                ));
            }
            Handle::Many(h) => {
                h.remove_range(range);
            }
        }
    }

    /// Evict keys that were selected by the assigned eviction strategy from the state, and return
    /// the number of bytes freed. The amount of keys evicted will be ceil(len() * ratio)
    pub fn evict(&mut self, keys_to_evict: EvictionQuantity) -> u64 {
        let base_value_size = self.base_value_size() as u64;
        match *self {
            Handle::Single(ref mut h) => h.evict_keys(keys_to_evict, |k, v| {
                // Each row's state is composed of: The key, the set of Values in the row (DfValues)
                // and the bytes required to hold the Row data structure.
                k.deep_size_of() + v.iter().map(|r| r.deep_size_of()).sum::<u64>() + base_value_size
            }),
            Handle::Many(ref mut h) => h.evict_keys(keys_to_evict, |k, v| {
                k.deep_size_of() + v.iter().map(|r| r.deep_size_of()).sum::<u64>() + base_value_size
            }),
        }
    }

    pub fn refresh(&mut self) {
        match *self {
            Handle::Single(ref mut h) => {
                h.publish();
            }
            Handle::Many(ref mut h) => {
                h.publish();
            }
        }
    }

    pub fn add<I>(&mut self, key: &[usize], cols: usize, rs: I) -> isize
    where
        I: IntoIterator<Item = Record>,
    {
        let mut memory_delta = 0isize;
        match *self {
            Handle::Single(ref mut h) => {
                assert_eq!(key.len(), 1);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert(r[key[0]].clone(), r.into_boxed_slice());
                        }
                        Record::Negative(r) => {
                            // TODO: reader_map will remove the empty vec for a key if we remove the
                            // last record. this means that future lookups will fail, and cause a
                            // replay, which will produce an empty result. this will work, but is
                            // somewhat inefficient.
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove_value(r[key[0]].clone(), r.into_boxed_slice());
                        }
                    }
                }
            }
            Handle::Many(ref mut h) => {
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    let key = key.iter().map(|&k| &r[k]).cloned().collect();
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert(key, r.into_boxed_slice());
                        }
                        Record::Negative(r) => {
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove_value(key, r.into_boxed_slice());
                        }
                    }
                }
            }
        }
        memory_delta
    }

    pub fn set_timestamp(&mut self, t: Timestamp) {
        match *self {
            Handle::Single(ref mut h) => h.set_timestamp(t),
            Handle::Many(ref mut h) => h.set_timestamp(t),
        }
    }

    pub fn insert_range<R>(&mut self, range: R)
    where
        R: RangeBounds<Vec<DfValue>>,
    {
        match self {
            Handle::Single(h) => {
                h.insert_range((
                    range.start_bound().map(|r| {
                        debug_assert_eq!(r.len(), 1);
                        &r[0]
                    }),
                    range.end_bound().map(|r| {
                        debug_assert_eq!(r.len(), 1);
                        &r[0]
                    }),
                ));
            }
            Handle::Many(h) => {
                h.insert_range(range);
            }
        }
    }

    pub fn read(&self) -> super::multir::Handle {
        match self {
            Handle::Single(h) => super::multir::Handle::Single((*h).clone()),
            Handle::Many(h) => super::multir::Handle::Many((*h).clone()),
        }
    }
}
