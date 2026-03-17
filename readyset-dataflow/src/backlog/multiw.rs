use std::collections::HashMap;

use ahash::RandomState;
use dataflow_expression::PreInsertion;
use reader_map::{BatchEntry, BatchSegment, EvictionQuantity};
use readyset_data::Bound;
use readyset_util::ranges::RangeBounds;
use readyset_util::SizeOf;

use super::{key_to_single, Key};
use crate::prelude::*;

pub(super) enum Handle {
    Single(
        reader_map::handles::WriteHandle<DfValue, Box<[DfValue]>, PreInsertion, i64, RandomState>,
    ),
    Many(
        reader_map::handles::WriteHandle<
            Vec<DfValue>,
            Box<[DfValue]>,
            PreInsertion,
            i64,
            RandomState,
        >,
    ),
}

impl Handle {
    pub fn base_value_size(&self) -> usize {
        match self {
            Handle::Single(h) => h.base_value_size(),
            Handle::Many(h) => h.base_value_size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Handle::Single(h) => h.is_empty(),
            Handle::Many(h) => h.is_empty(),
        }
    }

    pub fn clear(&mut self, k: Key) {
        match self {
            Handle::Single(h) => {
                h.clear(key_to_single(k).into_owned());
            }
            Handle::Many(h) => {
                h.clear(k.into_owned());
            }
        }
    }

    pub fn empty(&mut self, k: Key) {
        match self {
            Handle::Single(h) => {
                h.remove_entry(key_to_single(k).into_owned());
            }
            Handle::Many(h) => {
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

    /// Evict keys that were selected by the assigned eviction strategy from the state. The amount
    /// of keys evicted will be ceil(len() * ratio).
    ///
    /// Returns the number of bytes evicted and the keys that were evicted.
    #[allow(clippy::type_complexity)]
    pub fn evict(
        &mut self,
        keys_to_evict: EvictionQuantity,
    ) -> (
        usize,
        Box<dyn Iterator<Item = (Vec<DfValue>, Option<Vec<DfValue>>)>>,
    ) {
        let base_value_size = self.base_value_size();
        match self {
            Handle::Single(ref mut h) => {
                let (bytes, keys) = h.evict_keys(keys_to_evict, |k, v| {
                    // Each row's state is composed of: The key, the set of Values in the row
                    // (DfValues) and the bytes required to hold the Row data
                    // structure.
                    k.deep_size_of()
                        + v.iter().map(|r| r.deep_size_of()).sum::<usize>()
                        + base_value_size
                });
                let keys = keys.map(|(x, y)| (vec![x], y.map(|y| vec![y])));
                (bytes, Box::new(keys))
            }
            Handle::Many(ref mut h) => h.evict_keys(keys_to_evict, |k, v| {
                k.deep_size_of()
                    + v.iter().map(|r| r.deep_size_of()).sum::<usize>()
                    + base_value_size
            }),
        }
    }

    pub fn publish(&mut self) {
        match self {
            Handle::Single(ref mut h) => {
                h.publish();
            }
            Handle::Many(ref mut h) => {
                h.publish();
            }
        }
    }

    pub fn add<I>(&mut self, key_columns: &[usize], cols: usize, records: I) -> isize
    where
        I: IntoIterator<Item = Record>,
    {
        let mut memory_delta = 0isize;
        match self {
            Handle::Single(ref mut h) => {
                assert_eq!(key_columns.len(), 1);
                let key_col = key_columns[0];
                let entries =
                    collect_batch(records, cols, &mut memory_delta, |r| r[key_col].clone());
                h.batch(entries);
            }
            Handle::Many(ref mut h) => {
                let entries = collect_batch(records, cols, &mut memory_delta, |r| {
                    key_columns.iter().map(|&k| r[k].clone()).collect()
                });
                h.batch(entries);
            }
        }
        memory_delta
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

/// Collect records into batch entries keyed by `extract_key`. Consecutive adds/removes for the
/// same key are grouped into a single `BatchSegment`.
fn collect_batch<K, F>(
    records: impl IntoIterator<Item = Record>,
    cols: usize,
    memory_delta: &mut isize,
    extract_key: F,
) -> Vec<BatchEntry<K, Box<[DfValue]>>>
where
    K: Eq + std::hash::Hash + Clone,
    F: Fn(&[DfValue]) -> K,
{
    // Map from key to index in `entries`.
    let mut key_index: HashMap<K, usize> = HashMap::new();
    let mut entries: Vec<BatchEntry<K, Box<[DfValue]>>> = Vec::new();

    for record in records {
        debug_assert!(record.len() >= cols);
        let (is_add, row) = match record {
            Record::Positive(r) => {
                *memory_delta += r.deep_size_of() as isize;
                (true, r)
            }
            Record::Negative(r) => {
                *memory_delta -= r.deep_size_of() as isize;
                (false, r)
            }
        };

        let key = extract_key(&row);
        let value = row.into_boxed_slice();

        let idx = *key_index.entry(key.clone()).or_insert_with(|| {
            let idx = entries.len();
            entries.push(BatchEntry::new(key));
            idx
        });
        let entry = &mut entries[idx];

        // Append to the last segment if it's the same type, otherwise start a new one.
        match (is_add, entry.segments.last_mut()) {
            (true, Some(BatchSegment::Adds(recs))) | (false, Some(BatchSegment::Removes(recs))) => {
                recs.push(value)
            }
            _ => entry.segments.push(if is_add {
                BatchSegment::Adds(vec![value])
            } else {
                BatchSegment::Removes(vec![value])
            }),
        }
    }

    entries
}
