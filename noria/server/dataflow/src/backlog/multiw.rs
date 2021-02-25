use std::ops::RangeBounds;

use super::{key_to_double, key_to_single, Key};
use crate::prelude::*;
use ahash::RandomState;
use launchpad::intervals::{BoundAsRef, BoundFunctor};
use noria::consistency::Timestamp;

pub(super) enum Handle {
    Single(reader_map::handles::WriteHandle<DataType, Vec<DataType>, i64, Timestamp, RandomState>),
    Double(
        reader_map::handles::WriteHandle<
            (DataType, DataType),
            Vec<DataType>,
            i64,
            Timestamp,
            RandomState,
        >,
    ),
    Many(
        reader_map::handles::WriteHandle<Vec<DataType>, Vec<DataType>, i64, Timestamp, RandomState>,
    ),
}

impl Handle {
    pub fn is_empty(&self) -> bool {
        match *self {
            Handle::Single(ref h) => h.is_empty(),
            Handle::Double(ref h) => h.is_empty(),
            Handle::Many(ref h) => h.is_empty(),
        }
    }

    pub fn clear(&mut self, k: Key) {
        match *self {
            Handle::Single(ref mut h) => {
                h.clear(key_to_single(k).into_owned());
            }
            Handle::Double(ref mut h) => {
                h.clear(key_to_double(k).into_owned());
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
            Handle::Double(ref mut h) => {
                h.remove_entry(key_to_double(k).into_owned());
            }
            Handle::Many(ref mut h) => {
                h.remove_entry(k.into_owned());
            }
        }
    }

    pub fn empty_range<R>(&mut self, range: R)
    where
        R: RangeBounds<Vec<DataType>>,
    {
        match self {
            Handle::Single(h) => {
                h.remove_range((
                    range.start_bound().as_ref().map(|r| {
                        debug_assert_eq!(r.len(), 1);
                        &r[0]
                    }),
                    range.end_bound().as_ref().map(|r| {
                        debug_assert_eq!(r.len(), 1);
                        &r[0]
                    }),
                ));
            }
            Handle::Double(h) => {
                h.remove_range((
                    range.start_bound().as_ref().map(|r| {
                        debug_assert_eq!(r.len(), 2);
                        (r[0].clone(), r[1].clone())
                    }),
                    range.end_bound().as_ref().map(|r| {
                        debug_assert_eq!(r.len(), 2);
                        (r[0].clone(), r[1].clone())
                    }),
                ));
            }
            Handle::Many(h) => {
                h.remove_range(range);
            }
        }
    }

    /// Evict `n` randomly selected keys from state, and invoke `f` with each of the values that
    /// have been evicted
    pub fn empty_random_for_each(
        &mut self,
        rng: &mut impl rand::Rng,
        n: usize,
        mut f: impl FnMut(&reader_map::refs::Values<Vec<DataType>, RandomState>),
    ) {
        match *self {
            Handle::Single(ref mut h) => h.empty_random(rng, n).for_each(|r| f(r.1)),
            Handle::Double(ref mut h) => h.empty_random(rng, n).for_each(|r| f(r.1)),
            Handle::Many(ref mut h) => h.empty_random(rng, n).for_each(|r| f(r.1)),
        }
    }

    pub fn refresh(&mut self) {
        match *self {
            Handle::Single(ref mut h) => {
                h.publish();
            }
            Handle::Double(ref mut h) => {
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
                            h.insert(r[key[0]].clone(), r);
                        }
                        Record::Negative(r) => {
                            // TODO: reader_map will remove the empty vec for a key if we remove the
                            // last record. this means that future lookups will fail, and cause a
                            // replay, which will produce an empty result. this will work, but is
                            // somewhat inefficient.
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove_value(r[key[0]].clone(), r);
                        }
                    }
                }
            }
            Handle::Double(ref mut h) => {
                assert_eq!(key.len(), 2);
                for r in rs {
                    debug_assert!(r.len() >= cols);
                    match r {
                        Record::Positive(r) => {
                            memory_delta += r.deep_size_of() as isize;
                            h.insert((r[key[0]].clone(), r[key[1]].clone()), r);
                        }
                        Record::Negative(r) => {
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove_value((r[key[0]].clone(), r[key[1]].clone()), r);
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
                            h.insert(key, r);
                        }
                        Record::Negative(r) => {
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove_value(key, r);
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
            Handle::Double(ref mut h) => h.set_timestamp(t),
            Handle::Many(ref mut h) => h.set_timestamp(t),
        }
    }

    pub fn insert_range<R>(&mut self, range: R)
    where
        R: RangeBounds<Vec<DataType>>,
    {
        match self {
            Handle::Single(h) => {
                h.insert_range(
                    vec![],
                    (
                        range.start_bound().as_ref().map(|r| {
                            debug_assert_eq!(r.len(), 1);
                            &r[0]
                        }),
                        range.end_bound().as_ref().map(|r| {
                            debug_assert_eq!(r.len(), 1);
                            &r[0]
                        }),
                    ),
                );
            }
            Handle::Double(h) => {
                h.insert_range(
                    vec![],
                    (
                        range.start_bound().as_ref().map(|r| {
                            debug_assert_eq!(r.len(), 2);
                            (r[0].clone(), r[1].clone())
                        }),
                        range.end_bound().as_ref().map(|r| {
                            debug_assert_eq!(r.len(), 2);
                            (r[0].clone(), r[1].clone())
                        }),
                    ),
                );
            }
            Handle::Many(h) => {
                h.insert_range(vec![], range);
            }
        }
    }

    pub fn read(&self) -> super::multir::Handle {
        match self {
            Handle::Single(h) => super::multir::Handle::Single((*h).clone()),
            Handle::Double(h) => super::multir::Handle::Double((*h).clone()),
            Handle::Many(h) => super::multir::Handle::Many((*h).clone()),
        }
    }
}
