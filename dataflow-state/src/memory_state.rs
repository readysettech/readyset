use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use common::{IndexType, Record, Records, SizeOf, Tag};
use rand::{self, Rng};
use readyset_client::internal::Index;
use readyset_client::{KeyComparison, KeyCount};
use readyset_data::DfValue;
use readyset_errors::ReadySetResult;
use replication_offset::ReplicationOffset;
use tracing::trace;

use crate::keyed_state::KeyedState;
use crate::single_state::SingleState;
use crate::{
    AllRecords, EvictBytesResult, EvictKeysResult, EvictRandomResult, LookupResult, PointKey,
    RangeKey, RangeLookupResult, RecordResult, Row, Rows, State,
};

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    weak_indices: HashMap<Vec<usize>, KeyedState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
    /// The latest replication offset that has been written to the base table backed by this
    /// [`MemoryState`], it is only used when [`LocalAuthority`] is the ReadySet authority.
    replication_offset: Option<ReplicationOffset>,
    /// If this state is fully materialized, has it received a complete full replay yet?
    pub(crate) replay_done: bool,
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

fn base_row_bytes_from_comparison(keys: &KeyComparison) -> u64 {
    if let KeyComparison::Equal(keys) = keys {
        base_row_bytes(keys)
    } else {
        // TODO(ENG-726): Properly calculate memory utilized by ranges.
        0
    }
}

fn base_row_bytes(keys: &[DfValue]) -> u64 {
    keys.iter().map(SizeOf::deep_size_of).sum::<u64>() + std::mem::size_of::<Row>() as u64
}

impl State for MemoryState {
    fn add_key(&mut self, index: Index, partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(&index.columns, index.index_type) {
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

    fn replay_done(&self) -> bool {
        self.replay_done
    }

    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        if self.is_partial() {
            records.retain(|r| {
                // we need to check that we're not erroneously filling any holes
                // there are two cases here:
                //
                //  - if the incoming record is a partial replay (i.e., partial_tag.is_some()), then
                //    we *know* that we are the target of the replay, and therefore we *know* that
                //    the materialization must already have marked the given key as "not a hole".
                //  - if the incoming record is a normal message (i.e., partial_tag.is_none()), then
                //    we need to be careful. since this materialization is partial, it may be that
                //    we haven't yet replayed this `r`'s key, in which case we shouldn't forward
                //    that record! if all of our indices have holes for this record, there's no need
                //    for us to forward it. it would just be wasted work.
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

        if let Some(replication_offset) = replication_offset {
            self.replication_offset = Some(replication_offset);
        }

        Ok(())
    }

    fn key_count(&self) -> KeyCount {
        let count = self.state.iter().map(SingleState::key_count).sum();
        KeyCount::ExactKeyCount(count)
    }

    fn row_count(&self) -> usize {
        self.state.iter().map(SingleState::row_count).sum()
    }

    fn mark_filled(&mut self, key: KeyComparison, tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        self.mem_size += base_row_bytes_from_comparison(&key);
        self.state[index].mark_filled(key);
    }

    fn mark_hole(&mut self, key: &KeyComparison, tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        let freed_bytes = self.state[index].mark_hole(key);
        self.mem_size = self
            .mem_size
            .saturating_sub(freed_bytes + base_row_bytes_from_comparison(key));
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &PointKey) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = self
            .state_for(columns, IndexType::HashMap)
            .or_else(|| self.state_for(columns, IndexType::BTreeMap))
            .expect("lookup on non-indexed column set");
        let ret = self.state[index].lookup(key);
        if ret.is_some() {
            return ret;
        }

        trace!(?columns, ?key, "lookup missed; trying other indexes");

        // We missed in the index we tried to look up on, but we might have the rows
        // in an overlapping index (e.g. if we missed looking up [0, 1], [0] or [1]
        // might have the rows).
        // This happens when partial indices overlap between a parent and its children,
        // resulting in upqueries along an overlapping replay path.
        // FIXME(eta): a lot of this could be computed at index addition time.
        for state in self.state.iter() {
            // Try other index types with the same columns
            if state.columns() == columns && state.index_type() != self.state[index].index_type() {
                let res = state.lookup(key);
                if res.is_some() {
                    return res;
                }
            }

            // We might have another index with the same columns in another order
            if state.columns() != columns && state.columns().len() == columns.len() {
                // Make a map from column index -> the position in the index we're trying to do a
                // lookup into
                //
                // eg if we're mapping [3, 4] to [4, 3]
                // we get {4 => 0, 3 => 1}
                let col_positions = state
                    .columns()
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(idx, col)| (col, idx))
                    .collect::<BTreeMap<_, _>>();
                if columns.iter().all(|c| col_positions.contains_key(c)) {
                    let mut shuffled_key = vec![DfValue::None; key.len()];
                    for (key_pos, col) in columns.iter().enumerate() {
                        let val = key
                            .get(key_pos)
                            .expect("Columns and key must have the same len");
                        let pos = col_positions[col];
                        shuffled_key[pos] = val.clone();
                    }
                    let key = PointKey::from(shuffled_key);
                    let res = state.lookup(&key);
                    if res.is_some() {
                        return res;
                    }
                }
            }

            // otherwise the length must be strictly less than, because otherwise it's either the
            // same index, or we'd have to magic up datatypes out of thin air
            if state.columns().len() < columns.len() {
                // For each column in `columns`, find the corresponding column in `state.key()`,
                // if there is one, and return (its position in state.key(), its value from `key`).
                // FIXME(eta): this seems accidentally quadratic.
                let mut positions = columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, col_idx)| {
                        state
                            .columns()
                            .iter()
                            .position(|x| x == col_idx)
                            .map(|pos| (pos, key.get(i).expect("bogus key passed to lookup")))
                    })
                    .collect::<Vec<_>>();
                if positions.len() == state.columns().len() {
                    // the index only contains columns from `columns` (and none other).
                    // make a new lookup key
                    positions.sort_unstable_by_key(|(idx, _)| *idx);
                    let kt = PointKey::from(positions.into_iter().map(|(_, val)| val.clone()));
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
            .state_for(columns, IndexType::BTreeMap)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup_range(key)
    }

    fn all_records(&self) -> AllRecords {
        #[allow(clippy::ptr_arg)]
        fn fix(rs: &Rows) -> impl Iterator<Item = Vec<DfValue>> + '_ {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        AllRecords::Owned(self.state[0].values().flat_map(fix).collect())
    }

    /// Evicts `bytes` by evicting random keys from the state. The key are first evicted from the
    /// strongly referenced `state`, then they are removed from the weakly referenced
    /// `weak_indices`.
    fn evict_bytes(&mut self, bytes: usize) -> Option<EvictBytesResult> {
        let mut rng = rand::thread_rng();
        let state_index = rng.gen_range(0, self.state.len());
        let mut bytes_freed = 0u64;
        let mut keys_evicted = Vec::new();

        while bytes_freed < bytes as u64 {
            let evicted = self.state[state_index].evict_random(&mut rng);

            if evicted.is_none() {
                // There are no more keys in this state.
                break;
            }

            let (keys, rows) = evicted?;
            rows.iter()
                .for_each(|row| bytes_freed += self.handle_evicted_row(row));
            bytes_freed += base_row_bytes(&keys);
            keys_evicted.push(keys);
        }

        if bytes_freed == 0 {
            return None;
        }

        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        return Some(EvictBytesResult {
            index: self.state[state_index].index(),
            keys_evicted,
            bytes_freed,
        });
    }

    /// Evicts the given `keys` for the state associated with the target of the given `tag`
    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<EvictKeysResult> {
        // we may be told to evict from a tag that add_key hasn't been called for yet
        // this can happen if an upstream domain issues an eviction for a replay path that we have
        // been told about, but that has not yet been finalized.
        self.by_tag.get(&tag).cloned().map(move |state_index| {
            let rows_evicted = self.state[state_index].evict_keys(keys);
            let mut bytes_freed = 0;

            rows_evicted
                .iter()
                .for_each(|row| bytes_freed += self.handle_evicted_row(row));

            let key_bytes = keys.iter().map(base_row_bytes_from_comparison).sum::<u64>();

            self.mem_size = self.mem_size.saturating_sub(bytes_freed + key_bytes);

            EvictKeysResult {
                index: self.state[state_index].index(),
                bytes_freed,
            }
        })
    }

    /// Randomly evict a single key from the state associated with the target of the given `tag`
    fn evict_random<R: rand::Rng>(&mut self, tag: Tag, rng: &mut R) -> Option<EvictRandomResult> {
        self.by_tag.get(&tag).cloned().and_then(move |state_index| {
            self.state[state_index]
                .evict_random(rng)
                .map(|(key, rows)| {
                    let mut bytes_freed = 0;
                    rows.iter()
                        .for_each(|row| bytes_freed += self.handle_evicted_row(row));
                    let key_bytes = key.deep_size_of();
                    bytes_freed += key_bytes;
                    self.mem_size = self.mem_size.saturating_sub(bytes_freed);

                    EvictRandomResult {
                        index: self.state[state_index].index(),
                        key_evicted: key,
                        bytes_freed,
                    }
                })
        })
    }

    fn clear(&mut self) {
        for state in &mut self.state {
            state.clear();
        }
        self.mem_size = 0;
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        self.replication_offset.as_ref()
    }

    fn add_weak_key(&mut self, index: Index) {
        let weak_index = KeyedState::from(&index);
        self.weak_indices.insert(index.columns, weak_index);
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>> {
        self.weak_indices[columns].lookup(key).map(From::from)
    }

    fn tear_down(self) -> ReadySetResult<()> {
        Ok(())
    }
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols` and with the given
    /// `index_type`, or None if no such index exists.
    fn state_for(&self, cols: &[usize], index_type: IndexType) -> Option<usize> {
        self.state
            .iter()
            .position(|s| s.columns() == cols && s.index_type() == index_type)
    }

    fn insert(&mut self, r: Vec<DfValue>, partial_tag: Option<Tag>) -> bool {
        let r = Row::from(r);

        let hit = if let Some(tag) = partial_tag {
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
            // SAFETY: row remains inside the same state
            self.state[i].insert_row(unsafe { r.clone() })
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                // SAFETY: row remains inside the same state
                hit_any |= self.state[i].insert_row(unsafe { r.clone() });
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            hit_any
        };

        if hit {
            for (key, weak_index) in self.weak_indices.iter_mut() {
                // SAFETY: row remains inside the same state
                weak_index.insert(key, unsafe { r.clone() }, false);
            }
        }

        hit
    }

    fn remove(&mut self, r: &[DfValue]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Rc::strong_count(&row.0) == 1 {
                    self.mem_size = self.mem_size.saturating_sub(row.deep_size_of());
                }
            }
        }

        if hit {
            for (key, weak_index) in self.weak_indices.iter_mut() {
                weak_index.remove(key, r, None);
            }
        }

        hit
    }

    /// Removes a `Row` that was evicted from `self::state` from `self::weak_indices`, and returns
    /// the number of bytes freed if the last reference to the `Row` was dropped.
    fn handle_evicted_row(&mut self, row: &Row) -> u64 {
        // TODO(ENG-3062): Possibly consider duplicate rows when removing from
        // self.weak_indices
        for (key, weak_index) in self.weak_indices.iter_mut() {
            weak_index.remove(key, row, None);
        }

        // Only count strong references after we removed a row from `weak_indices`
        // otherwise if it is there, it will never have a reference count of 1
        if Rc::strong_count(&row.0) == 1 {
            row.deep_size_of()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::ops::Bound;

    use vec1::vec1;

    use super::*;

    fn insert<S: State>(state: &mut S, row: Vec<DfValue>) {
        let record: Record = row.into();
        state
            .process_records(&mut record.into(), None, None)
            .unwrap();
    }

    #[test]
    fn memory_state_key_count_vs_row_count() {
        let mut state = MemoryState::default();
        state.add_key(Index::hash_map(vec![0]), None);
        insert(&mut state, vec![1.into(), 10.into()]);
        insert(&mut state, vec![1.into(), 20.into()]);
        insert(&mut state, vec![2.into(), 30.into()]);

        assert_eq!(KeyCount::ExactKeyCount(2), state.key_count());
        assert_eq!(3, state.row_count());
    }

    #[test]
    fn memory_state_process_records() {
        let mut state = MemoryState::default();
        let records: Records = vec![
            (vec![1.into(), "A".try_into().unwrap()], true),
            (vec![2.into(), "B".try_into().unwrap()], true),
            (vec![3.into(), "C".try_into().unwrap()], true),
            (vec![1.into(), "A".try_into().unwrap()], false),
        ]
        .into();

        state.add_key(Index::hash_map(vec![0]), None);
        state
            .process_records(&mut Vec::from(&records[..3]).into(), None, None)
            .unwrap();
        state
            .process_records(&mut records[3].clone().into(), None, None)
            .unwrap();

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &PointKey::Single(records[0][0].clone())) {
            LookupResult::Some(RecordResult::Borrowed(rows)) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for record in &records[1..3] {
            match state.lookup(&[0], &PointKey::Single(record[0].clone())) {
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
        let row: Vec<DfValue> = vec![10.into(), "Cat".try_into().unwrap()];
        state.add_key(Index::hash_map(vec![0]), None);
        insert(&mut state, row.clone());
        state.add_key(Index::hash_map(vec![1]), None);

        match state.lookup(&[1], &PointKey::Single(row[1].clone())) {
            LookupResult::Some(RecordResult::Borrowed(rows)) => {
                assert_eq!(&**rows.iter().next().unwrap(), &row)
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn multiple_indices_on_same_columns() {
        let mut state = MemoryState::default();
        state.add_key(Index::hash_map(vec![0]), None);
        state.add_key(Index::btree_map(vec![0]), None);
        insert(&mut state, vec![1.into()]);
        insert(&mut state, vec![2.into()]);
        insert(&mut state, vec![3.into()]);
        insert(&mut state, vec![4.into()]);

        assert_eq!(
            state
                .lookup(&[0], &PointKey::Single(1.into()))
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            state
                .lookup_range(
                    &[0],
                    &RangeKey::Single((Bound::Unbounded, Bound::Included(3.into())))
                )
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn point_lookup_only_btree() {
        let mut state = MemoryState::default();
        state.add_key(Index::btree_map(vec![0]), Some(vec![Tag::new(1)]));
        state.mark_filled(KeyComparison::from_range(&(..)), Tag::new(1));
        state.insert(vec![DfValue::from(1), DfValue::from(2)], Some(Tag::new(1)));

        let res = state.lookup(&[0], &PointKey::Single(DfValue::from(1)));
        assert!(res.is_some());
        assert_eq!(
            res.unwrap(),
            RecordResult::Owned(vec![vec![1.into(), 2.into()]])
        );
    }

    #[test]
    fn shuffled_columns() {
        let mut state = MemoryState::default();
        state.add_key(Index::hash_map(vec![2, 3]), Some(vec![Tag::new(0)]));
        state.add_key(Index::hash_map(vec![3, 2]), Some(vec![Tag::new(1)]));

        state.mark_filled(KeyComparison::Equal(vec1![1.into(), 1.into()]), Tag::new(0));
        state.insert(
            vec![1.into(), 1.into(), 1.into(), 1.into()],
            Some(Tag::new(0)),
        );

        let res = state.lookup(&[3, 2], &PointKey::Double((1.into(), 1.into())));
        assert!(res.is_some());
        let rows = res.unwrap();
        assert_eq!(
            rows,
            RecordResult::Owned(vec![vec![1.into(), 1.into(), 1.into(), 1.into()]])
        );
    }

    #[test]
    fn empty_column_set() {
        let mut state = MemoryState::default();
        state.add_key(Index::hash_map(vec![]), None);

        let mut rows = vec![
            vec![1.into()],
            vec![2.into()],
            vec![3.into()],
            vec![4.into()],
            vec![5.into()],
            vec![6.into()],
        ];

        state
            .process_records(&mut rows.clone().into(), None, None)
            .unwrap();
        let mut res = state
            .lookup(&[], &PointKey::Empty)
            .unwrap()
            .into_iter()
            .map(|v| v.into_owned())
            .collect::<Vec<_>>();
        res.sort();
        assert_eq!(res, rows);

        state
            .process_records(
                &mut vec![(vec![DfValue::from(6)], false)].into(),
                None,
                None,
            )
            .unwrap();
        rows.pop();
        let mut res = state
            .lookup(&[], &PointKey::Empty)
            .unwrap()
            .into_iter()
            .map(|v| v.into_owned())
            .collect::<Vec<_>>();
        res.sort();
        assert_eq!(res, rows);
    }

    mod lookup_range {
        use std::ops::{Bound, RangeBounds};

        use vec1::vec1;

        use super::*;

        mod partial {
            use vec1::Vec1;

            use super::*;

            fn setup() -> MemoryState {
                let mut state = MemoryState::default();
                let tag = Tag::new(1);
                state.add_key(Index::new(IndexType::BTreeMap, vec![0]), Some(vec![tag]));
                state.mark_filled(
                    KeyComparison::from_range(&(vec1![DfValue::from(0)]..vec1![DfValue::from(10)])),
                    tag,
                );
                state
                    .process_records(
                        &mut (0..10)
                            .map(|n| Record::from(vec![n.into()]))
                            .collect::<Records>(),
                        None,
                        None,
                    )
                    .unwrap();
                state
            }

            #[test]
            fn missing() {
                let state = setup();
                let range = vec1![DfValue::from(11)]..vec1![DfValue::from(20)];
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&range)),
                    RangeLookupResult::Missing(vec![(
                        range.start_bound().map(Vec1::as_vec).cloned(),
                        range.end_bound().map(Vec1::as_vec).cloned()
                    )])
                );
            }

            #[test]
            fn evict_random() {
                let mut state = MemoryState::default();

                let tag_1 = Tag::new(1);
                let tag_2 = Tag::new(2);

                // Initialize MemoryState with two indices
                state.add_key(Index::new(IndexType::HashMap, vec![0]), Some(vec![tag_1]));
                state.add_key(Index::new(IndexType::HashMap, vec![1]), Some(vec![tag_2]));

                // Mark two holes filled for Tag 1, and one for Tag 2
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(0)]), tag_1);
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(1)]), tag_1);
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(0)]), tag_2);

                // Evict a random key for Tag 1
                let mut rng = rand::thread_rng();
                state.evict_random(tag_1, &mut rng);
                let lookup_1 = state.lookup(&[0], &PointKey::Single(DfValue::from(0)));
                let lookup_2 = state.lookup(&[0], &PointKey::Single(DfValue::from(1)));
                let lookup_3 = state.lookup(&[1], &PointKey::Single(DfValue::from(0)));

                // Assert exactly one of the first two lookups missed and the third did not
                assert!(lookup_1.is_missing() ^ lookup_2.is_missing());
                assert!(!lookup_3.is_missing());
            }
        }

        mod full {
            use super::*;

            fn setup() -> MemoryState {
                let mut state = MemoryState::default();
                state.add_key(Index::new(IndexType::BTreeMap, vec![0]), None);
                state
                    .process_records(
                        &mut (0..10)
                            .map(|n| Record::from(vec![n.into()]))
                            .collect::<Records>(),
                        None,
                        None,
                    )
                    .unwrap();
                state
            }

            #[test]
            fn missing() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&(vec1![DfValue::from(11)]..vec1![DfValue::from(20)]))
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
                        &RangeKey::from(&(vec1![DfValue::from(3)]..vec1![DfValue::from(7)]))
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
                        &RangeKey::from(&(vec1![DfValue::from(3)]..=vec1![DfValue::from(7)]))
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
                            Bound::Excluded(vec1![DfValue::from(3)]),
                            Bound::Excluded(vec1![DfValue::from(7)])
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
                            Bound::Excluded(vec1![DfValue::from(3)]),
                            Bound::Included(vec1![DfValue::from(7)])
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
                    state.lookup_range(&[0], &RangeKey::from(&(vec1![DfValue::from(3)]..))),
                    RangeLookupResult::Some(
                        (3..10).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_inclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&(..=vec1![DfValue::from(3)]))),
                    RangeLookupResult::Some(
                        (0..=3).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_exclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&(..vec1![DfValue::from(3)]))),
                    RangeLookupResult::Some(
                        (0..3).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }
        }
    }

    mod weak_indices {
        use super::*;

        fn setup() -> MemoryState {
            let mut state = MemoryState::default();

            state.add_key(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
            state.add_weak_key(Index::hash_map(vec![1]));

            state
        }

        #[test]
        fn insert_lookup() {
            let mut state = setup();
            let mut records: Records = vec![
                (vec![1.into(), "A".into()], true),
                (vec![1.into(), "B".into()], true),
                (vec![2.into(), "A".into()], true),
            ]
            .into();
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            state.mark_filled(KeyComparison::Equal(vec1![2.into()]), Tag::new(0));
            state
                .process_records(&mut records, Some(Tag::new(0)), None)
                .unwrap();

            assert_eq!(records.len(), 3);

            let result = state.lookup_weak(&[1], &PointKey::Single(DfValue::from("A")));
            let mut rows: Vec<_> = result.unwrap().into_iter().collect();
            rows.sort();
            assert_eq!(
                rows,
                vec![vec![1.into(), "A".into()], vec![2.into(), "A".into()]]
            );
        }

        #[test]
        fn insert_delete_lookup() {
            let mut state = setup();
            let mut records: Records = vec![
                (vec![1.into(), "A".into()], true),
                (vec![1.into(), "B".into()], true),
                (vec![2.into(), "A".into()], true),
            ]
            .into();
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            state.mark_filled(KeyComparison::Equal(vec1![2.into()]), Tag::new(0));
            state
                .process_records(&mut records, Some(Tag::new(0)), None)
                .unwrap();
            assert_eq!(records.len(), 3);

            let mut delete_records: Records = vec![(vec![2.into(), "A".into()], false)].into();
            state
                .process_records(&mut delete_records, Some(Tag::new(0)), None)
                .unwrap();
            assert_eq!(delete_records.len(), 1);

            let result = state.lookup_weak(&[1], &PointKey::Single(DfValue::from("A")));
            assert_eq!(
                result,
                Some(RecordResult::Owned(vec![vec![1.into(), "A".into()],]))
            );
        }

        #[test]
        fn insert_evict_lookup() {
            let mut state = setup();
            let mut records: Records = vec![
                (vec![1.into(), "A".into()], true),
                (vec![1.into(), "B".into()], true),
                (vec![2.into(), "A".into()], true),
            ]
            .into();
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            state.mark_filled(KeyComparison::Equal(vec1![2.into()]), Tag::new(0));
            state
                .process_records(&mut records, Some(Tag::new(0)), None)
                .unwrap();
            assert_eq!(records.len(), 3);

            state.evict_keys(Tag::new(0), &[KeyComparison::Equal(vec1![2.into()])]);

            let result = state.lookup_weak(&[1], &PointKey::Single(DfValue::from("A")));
            assert_eq!(
                result,
                Some(RecordResult::Owned(vec![vec![1.into(), "A".into()],]))
            );
        }
    }
}
