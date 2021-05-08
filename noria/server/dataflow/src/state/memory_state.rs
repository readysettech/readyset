use std::collections::HashMap;
use std::rc::Rc;

use noria::KeyComparison;
use rand::{self, Rng};

use crate::prelude::*;
use crate::state::single_state::SingleState;
use common::SizeOf;

use super::RangeLookupResult;

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl SizeOf for MemoryState {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size
    }

    fn is_empty(&self) -> bool {
        self.state[0].is_empty()
    }
}

impl State for MemoryState {
    fn add_key(&mut self, index: &Index, partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(&index.columns) {
            // already keyed by this key; just adding tags
            (i, true)
        } else {
            // will eventually be assigned
            (self.state.len(), false)
        };

        if let Some(ref p) = partial {
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        if exists {
            return;
        }

        self.state.push(SingleState::new(index, partial.is_some()));

        if !self.state.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();

            if !old.is_empty() {
                assert!(!old[0].partial());
                for rs in old[0].values() {
                    for r in rs {
                        new.insert_row(Row::from(r.0.clone()));
                    }
                }
            }
        }
    }

    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(SingleState::partial)
    }

    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        _replication_offset: Option<usize>,
    ) {
        if self.is_partial() {
            records.retain(|r| {
                // we need to check that we're not erroneously filling any holes
                // there are two cases here:
                //
                //  - if the incoming record is a partial replay (i.e., partial.is_some()), then we
                //    *know* that we are the target of the replay, and therefore we *know* that the
                //    materialization must already have marked the given key as "not a hole".
                //  - if the incoming record is a normal message (i.e., partial.is_none()), then we
                //    need to be careful. since this materialization is partial, it may be that we
                //    haven't yet replayed this `r`'s key, in which case we shouldn't forward that
                //    record! if all of our indices have holes for this record, there's no need for us
                //    to forward it. it would just be wasted work.
                //
                //    XXX: we could potentially save come computation here in joins by not forcing
                //    `right` to backfill the lookup key only to then throw the record away
                match *r {
                    Record::Positive(ref r) => self.insert(r.clone(), partial_tag),
                    Record::Negative(ref r) => self.remove(r),
                }
            });
        } else {
            for r in records.iter() {
                match *r {
                    Record::Positive(ref r) => {
                        let hit = self.insert(r.clone(), None);
                        debug_assert!(hit);
                    }
                    Record::Negative(ref r) => {
                        let hit = self.remove(r);
                        debug_assert!(hit);
                    }
                }
            }
        }
    }

    fn rows(&self) -> usize {
        self.state.iter().map(SingleState::rows).sum()
    }

    fn mark_filled(&mut self, key: KeyComparison, tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        self.state[index].mark_filled(key);
    }

    fn mark_hole(&mut self, key: &KeyComparison, tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        let freed_bytes = self.state[index].mark_hole(key);
        self.mem_size = self.mem_size.checked_sub(freed_bytes).unwrap();
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = self
            .state_for(columns)
            .expect("lookup on non-indexed column set");
        let ret = self.state[index].lookup(key);
        if ret.is_some() {
            return ret;
        }
        // We missed in the index we tried to look up on, but we might have the rows
        // in an overlapping index (e.g. if we missed looking up [0, 1], [0] or [1]
        // might have the rows).
        // This happens when partial indices overlap between a parent and its children,
        // resulting in upqueries along an overlapping replay path.
        // FIXME(eta): a lot of this could be computed at index addition time.
        for state in self.state.iter() {
            // must be strictly less than, because otherwise it's either the same index, or
            // we'd have to magic up datatypes out of thin air
            if state.key().len() < columns.len() {
                // For each column in `columns`, find the corresponding column in `state.key()`,
                // if there is one, and return (its position in state.key(), its value from `key`).
                // FIXME(eta): this seems accidentally quadratic.
                let mut positions = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, col_idx)| {
                        state
                            .key()
                            .iter()
                            .position(|x| x == col_idx)
                            .map(|pos| (pos, key.get(i).expect("bogus key passed to lookup")))
                    })
                    .collect::<Vec<_>>();
                if positions.len() == state.key().len() {
                    // the index only contains columns from `columns` (and none other).
                    // make a new lookup key
                    positions.sort_unstable_by_key(|(idx, _)| *idx);
                    let kt = KeyType::from(positions.into_iter().map(|(_, val)| val));
                    if let LookupResult::Some(mut ret) = state.lookup(&kt) {
                        // filter the rows in this index to ones which actually match the key
                        // FIXME(eta): again, probably O(terrible)
                        ret.retain(|row| {
                            columns
                                .iter()
                                .enumerate()
                                .all(|(key_idx, &col_idx)| row.get(col_idx) == key.get(key_idx))
                        });
                        return LookupResult::Some(ret);
                    }
                }
            }
        }
        // oh well, that was a lot of CPU cycles and we didn't even have the records :(
        LookupResult::Missing
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        debug_assert!(
            !self.state.is_empty(),
            "lookup_range on uninitialized index"
        );
        let index = self
            .state_for(columns)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup_range(key)
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        #[allow(clippy::ptr_arg)]
        fn fix(rs: &Rows) -> impl Iterator<Item = Vec<DataType>> + '_ {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        self.state[0].values().flat_map(fix).collect()
    }

    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }

    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<(&[usize], u64)> {
        // we may be told to evict from a tag that add_key hasn't been called for yet
        // this can happen if an upstream domain issues an eviction for a replay path that we have
        // been told about, but that has not yet been finalized.
        self.by_tag.get(&tag).cloned().map(move |index| {
            let bytes = self.state[index].evict_keys(keys);
            self.mem_size = self.mem_size.saturating_sub(bytes);
            (self.state[index].key(), bytes)
        })
    }

    fn clear(&mut self) {
        for state in &mut self.state {
            state.clear();
        }
        self.mem_size = 0;
    }
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        let r = Rc::new(r);

        if let Some(tag) = partial_tag {
            let i = match self.by_tag.get(&tag) {
                Some(i) => *i,
                None => {
                    // got tagged insert for unknown tag. this will happen if a node on an old
                    // replay path is now materialized. must return true to avoid any records
                    // (which are destined for a downstream materialization) from being pruned.
                    return true;
                }
            };
            self.mem_size += r.deep_size_of();
            self.state[i].insert_row(Row::from(r))
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                hit_any |= self.state[i].insert_row(Row::from(r.clone()));
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Rc::strong_count(&row.0) == 1 {
                    self.mem_size = self.mem_size.checked_sub(row.deep_size_of()).unwrap();
                }
            }
        }

        hit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert<S: State>(state: &mut S, row: Vec<DataType>) {
        let record: Record = row.into();
        state.process_records(&mut record.into(), None, None);
    }

    #[test]
    fn memory_state_process_records() {
        let mut state = MemoryState::default();
        let records: Records = vec![
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ]
        .into();

        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        state.process_records(&mut Vec::from(&records[..3]).into(), None, None);
        state.process_records(&mut records[3].clone().into(), None, None);

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &KeyType::Single(&records[0][0])) {
            LookupResult::Some(RecordResult::Borrowed(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for record in &records[1..3] {
            match state.lookup(&[0], &KeyType::Single(&record[0])) {
                LookupResult::Some(RecordResult::Borrowed(rows)) => {
                    assert_eq!(&**rows.iter().next().unwrap(), &**record)
                }
                _ => unreachable!(),
            };
        }
    }

    #[test]
    fn memory_state_old_records_new_index() {
        let mut state = MemoryState::default();
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
        insert(&mut state, row.clone());
        state.add_key(&Index::new(IndexType::BTreeMap, vec![1]), None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(RecordResult::Borrowed(rows)) => {
                assert_eq!(&**rows.iter().next().unwrap(), &row)
            }
            _ => unreachable!(),
        };
    }

    mod lookup_range {
        use super::*;
        use std::ops::{Bound, RangeBounds};
        use vec1::vec1;

        mod partial {
            use launchpad::intervals::BoundFunctor;
            use vec1::Vec1;

            use super::*;

            fn setup() -> MemoryState {
                let mut state = MemoryState::default();
                let tag = Tag::new(1);
                state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), Some(vec![tag]));
                state.mark_filled(
                    KeyComparison::from_range(
                        &(vec1![DataType::from(0)]..vec1![DataType::from(10)]),
                    ),
                    tag,
                );
                state.process_records(
                    &mut (0..10)
                        .map(|n| Record::from(vec![n.into()]))
                        .collect::<Records>(),
                    None,
                    None,
                );
                state
            }

            #[test]
            fn missing() {
                let state = setup();
                let range = vec1![DataType::from(11)]..vec1![DataType::from(20)];
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&range)),
                    RangeLookupResult::Missing(vec![(
                        range.start_bound().map(Vec1::as_vec).cloned(),
                        range.end_bound().map(Vec1::as_vec).cloned()
                    )])
                );
            }
        }

        mod full {
            use super::*;

            fn setup() -> MemoryState {
                let mut state = MemoryState::default();
                state.add_key(&Index::new(IndexType::BTreeMap, vec![0]), None);
                state.process_records(
                    &mut (0..10)
                        .map(|n| Record::from(vec![n.into()]))
                        .collect::<Records>(),
                    None,
                    None,
                );
                state
            }

            #[test]
            fn missing() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(vec1![DataType::from(11)]..vec1![DataType::from(20)]))
                    ),
                    RangeLookupResult::Some(vec![].into())
                );
            }

            #[test]
            fn inclusive_exclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(vec1![DataType::from(3)]..vec1![DataType::from(7)]))
                    ),
                    RangeLookupResult::Some(
                        (3..7).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn inclusive_inclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(vec1![DataType::from(3)]..=vec1![DataType::from(7)]))
                    ),
                    RangeLookupResult::Some(
                        (3..=7).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn exclusive_exclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(
                            Bound::Excluded(vec1![DataType::from(3)]),
                            Bound::Excluded(vec1![DataType::from(7)])
                        ))
                    ),
                    RangeLookupResult::Some(
                        (3..7)
                            .skip(1)
                            .map(|n| vec![n.into()])
                            .collect::<Vec<_>>()
                            .into()
                    )
                );
            }

            #[test]
            fn exclusive_inclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(
                            Bound::Excluded(vec1![DataType::from(3)]),
                            Bound::Included(vec1![DataType::from(7)])
                        ))
                    ),
                    RangeLookupResult::Some(
                        (3..=7)
                            .skip(1)
                            .map(|n| vec![n.into()])
                            .collect::<Vec<_>>()
                            .into()
                    )
                );
            }

            #[test]
            fn inclusive_unbounded() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&(vec1![DataType::from(3)]..))),
                    RangeLookupResult::Some(
                        (3..10).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_inclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&(..=vec1![DataType::from(3)]))),
                    RangeLookupResult::Some(
                        (0..=3).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_exclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&(..vec1![DataType::from(3)]))),
                    RangeLookupResult::Some(
                        (0..3).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }
        }
    }
}
