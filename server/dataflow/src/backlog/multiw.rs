use super::{key_to_double, key_to_single, Key};
use crate::prelude::*;
use ahash::RandomState;
use evmap;

pub(super) enum Handle {
    Single(evmap::WriteHandle<DataType, Vec<DataType>, i64, RandomState>),
    Double(evmap::WriteHandle<(DataType, DataType), Vec<DataType>, i64, RandomState>),
    Many(evmap::WriteHandle<Vec<DataType>, Vec<DataType>, i64, RandomState>),
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
                h.empty(key_to_single(k).into_owned());
            }
            Handle::Double(ref mut h) => {
                h.empty(key_to_double(k).into_owned());
            }
            Handle::Many(ref mut h) => {
                h.empty(k.into_owned());
            }
        }
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub fn empty_at_index(
        &mut self,
        index: usize,
    ) -> Option<&evmap::Values<Vec<DataType>, RandomState>> {
        match *self {
            Handle::Single(ref mut h) => h.empty_at_index(index).map(|r| r.1),
            Handle::Double(ref mut h) => h.empty_at_index(index).map(|r| r.1),
            Handle::Many(ref mut h) => h.empty_at_index(index).map(|r| r.1),
        }
    }

    pub fn refresh(&mut self) {
        match *self {
            Handle::Single(ref mut h) => {
                h.refresh();
            }
            Handle::Double(ref mut h) => {
                h.refresh();
            }
            Handle::Many(ref mut h) => {
                h.refresh();
            }
        }
    }

    pub fn meta_get_and<F, T>(&self, key: Key, then: F) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&evmap::Values<Vec<DataType>, RandomState>) -> T,
    {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                let map = h.read()?;
                let v = map.get(&key[0]).map(then);
                let m = *map.meta();
                Some((v, m))
            }
            Handle::Double(ref h) => {
                assert_eq!(key.len(), 2);
                // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
                // we're not guaranteed that they have the same memory layout
                // we *could* just clone DataType, but that would mean dealing with string refcounts
                // so instead, we play a trick where we memcopy onto the stack and then forget!
                //
                // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
                use std::mem;
                use std::ptr;
                unsafe {
                    let mut stack_key: (mem::MaybeUninit<DataType>, mem::MaybeUninit<DataType>) =
                        (mem::MaybeUninit::uninit(), mem::MaybeUninit::uninit());
                    ptr::copy_nonoverlapping(
                        &key[0] as *const DataType,
                        stack_key.0.as_mut_ptr(),
                        1,
                    );
                    ptr::copy_nonoverlapping(
                        &key[1] as *const DataType,
                        stack_key.1.as_mut_ptr(),
                        1,
                    );
                    let stack_key = mem::transmute::<_, &(DataType, DataType)>(&stack_key);
                    let map = h.read()?;
                    let v = map.get(&stack_key).map(then);
                    let m = *map.meta();
                    Some((v, m))
                }
            }
            Handle::Many(ref h) => {
                let map = h.read()?;
                let v = map.get(&key[..]).map(then);
                let m = *map.meta();
                Some((v, m))
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
                            // TODO: evmap will remove the empty vec for a key if we remove the
                            // last record. this means that future lookups will fail, and cause a
                            // replay, which will produce an empty result. this will work, but is
                            // somewhat inefficient.
                            memory_delta -= r.deep_size_of() as isize;
                            h.remove(r[key[0]].clone(), r);
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
                            h.remove((r[key[0]].clone(), r[key[1]].clone()), r);
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
                            h.remove(key, r);
                        }
                    }
                }
            }
        }
        memory_delta
    }
}
