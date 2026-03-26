use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::{IndexType, Record, Records, Tag};
use hashbag::HashBag;
use rand::{self, Rng};
use readyset_client::debug::info::KeyCount;
use readyset_client::internal::Index;
use readyset_client::KeyComparison;
use readyset_data::DfValue;
use readyset_errors::ReadySetResult;
use readyset_util::SizeOf;
use replication_offset::ReplicationOffset;
use tracing::trace;

use crate::keyed_state::KeyedState;
use crate::single_state::SingleState;
use crate::{
    AllRecords, EvictBytesResult, EvictKeysResult, EvictRandomResult, LookupResult,
    PersistencePoint, PointKey, RangeKey, RangeLookupResult, RecordResult, Row, Rows, State,
};

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    weak_indices: HashMap<Vec<usize>, KeyedState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: usize,
    /// The latest replication offset that has been written to the base table backed by this
    /// [`MemoryState`], it is only used when [`LocalAuthority`] is the ReadySet authority.
    replication_offset: Option<ReplicationOffset>,
    /// If this state is fully materialized, has it received a complete full replay yet?
    pub(crate) replay_done: bool,
}

impl SizeOf for MemoryState {
    fn deep_size_of(&self) -> usize {
        self.mem_size
    }

    fn size_is_empty(&self) -> bool {
        self.state.iter().all(|s| s.is_empty())
    }
}

fn base_row_bytes_from_comparison(keys: &KeyComparison) -> usize {
    if let KeyComparison::Equal(keys) = keys {
        base_row_bytes(keys)
    } else {
        // TODO(ENG-726): Properly calculate memory utilized by ranges.
        0
    }
}

fn base_row_bytes(keys: &[DfValue]) -> usize {
    keys.iter().map(SizeOf::deep_size_of).sum::<usize>() + std::mem::size_of::<Row>()
}

impl State for MemoryState {
    fn add_index(&mut self, index: Index, partial: Option<Vec<Tag>>) {
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
                        new.insert_row(Row::from(r.data.as_ref().clone()));
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
        let num_records = records.len();

        // OPTIMIZATION: For large batches, create rows in parallel
        let use_parallel_creation = num_records > 1000;
        let is_partial = self.is_partial();
        let insert_tag = if is_partial { partial_tag } else { None };

        if use_parallel_creation {
            // Parallel path: batch-create all positive rows, then insert/remove as needed.
            let mut positive_data = Vec::with_capacity(num_records);

            if is_partial {
                for r in records.iter() {
                    if let Record::Positive(ref r) = *r {
                        positive_data.push(r.clone());
                    }
                }
            } else {
                for r in records.iter() {
                    match *r {
                        Record::Positive(ref r) => {
                            positive_data.push(r.clone());
                        }
                        Record::Negative(ref r) => {
                            // Apply negatives immediately to avoid a later pass and extra indexing.
                            let hit = self.remove(r);
                            debug_assert!(hit);
                        }
                    }
                }
            }

            let mut rows_iter = if !positive_data.is_empty() {
                Row::batch_new_parallel(positive_data).into_iter()
            } else {
                Vec::new().into_iter()
            };

            if is_partial {
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
                let mut results = Vec::with_capacity(num_records);

                for r in records.iter() {
                    let keep = match *r {
                        Record::Positive(_) => {
                            let row = rows_iter
                                .next()
                                .expect("positive row count should match record count");
                            self.insert_row(row, insert_tag)
                        }
                        Record::Negative(ref r) => self.remove(r),
                    };
                    results.push(keep);
                }

                let mut result_idx = 0;
                records.retain(|_| {
                    let keep = results[result_idx];
                    result_idx += 1;
                    keep
                });
            } else {
                for row in rows_iter {
                    let hit = self.insert_row(row, None);
                    debug_assert!(hit);
                }
            }
        } else if is_partial {
            // Sequential path for partial state
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
                    Record::Positive(ref r) => self.insert(r.clone(), insert_tag),
                    Record::Negative(ref r) => self.remove(r),
                }
            });
        } else {
            // Sequential path for small batches
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
            // We can only perform lookups on states that are indexed on a subset of the columns in
            // the key passed into this method
            if state.columns().iter().all(|col| columns.contains(col)) {
                // For each column in this state, find the corresponding column in the column list
                // passed to this method and then the value in the given key for that column.
                // FIXME(eta): this seems accidentally quadratic.
                let vals = state.columns().iter().map(|col| {
                    let i = columns.iter().position(|c| col == c).unwrap();
                    key.get(i).unwrap().clone()
                });
                let kt = PointKey::from(vals);
                if let LookupResult::Some(mut ret) = state.lookup(&kt) {
                    // Filter the rows in this index to ones which actually match the key. This
                    // is required if the desired key is a superset of the key we are looking up
                    // here. For example, if the columns passed into this method were [0, 1] and we
                    // do a lookup here on [0], we need to filter out the rows that don't match our
                    // key on column 1
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
    fn evict_bytes(&mut self, bytes: usize) -> Option<EvictBytesResult<'_>> {
        let mut rng = rand::rng();
        let state_index = rng.random_range(0..self.state.len());
        let mut bytes_freed = 0;
        let mut keys_evicted = Vec::new();

        while bytes_freed < bytes {
            let evicted = self.state[state_index].evict_random(&mut rng);

            if evicted.is_none() {
                // There are no more keys in this state.
                break;
            }

            let (keys, rows) = evicted?;
            rows.iter()
                .for_each(|row| bytes_freed += self.handle_evicted_row(row, state_index));
            bytes_freed += base_row_bytes(&keys);
            keys_evicted.push(keys);
        }

        if bytes_freed == 0 {
            return None;
        }

        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        Some(EvictBytesResult {
            index: self.state[state_index].index(),
            keys_evicted,
            bytes_freed,
        })
    }

    /// Evicts the given `keys` for the state associated with the target of the given `tag`
    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<EvictKeysResult<'_>> {
        // we may be told to evict from a tag that add_index hasn't been called for yet
        // this can happen if an upstream domain issues an eviction for a replay path that we have
        // been told about, but that has not yet been finalized.
        self.by_tag.get(&tag).cloned().map(move |state_index| {
            let (key_was_present, rows_evicted) = self.state[state_index].evict_keys(keys);
            let mut bytes_freed = 0;

            rows_evicted
                .iter()
                .for_each(|row| bytes_freed += self.handle_evicted_row(row, state_index));

            let key_bytes = keys
                .iter()
                .map(base_row_bytes_from_comparison)
                .sum::<usize>();

            // If we actually evicted a key, include this to the bytes freed.
            // This will cause downstream nodes to propagate the eviction.
            if key_was_present {
                bytes_freed += key_bytes;
            }
            self.mem_size = self.mem_size.saturating_sub(bytes_freed);

            EvictKeysResult {
                index: self.state[state_index].index(),
                bytes_freed,
            }
        })
    }

    /// Randomly evict a single key from the state associated with the target of the given `tag`
    fn evict_random<R: rand::Rng>(
        &mut self,
        tag: Tag,
        rng: &mut R,
    ) -> Option<EvictRandomResult<'_>> {
        self.by_tag.get(&tag).cloned().and_then(move |state_index| {
            self.state[state_index]
                .evict_random(rng)
                .map(|(key, rows)| {
                    let mut bytes_freed = 0;
                    rows.iter()
                        .for_each(|row| bytes_freed += self.handle_evicted_row(row, state_index));
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

    fn persisted_up_to(&self) -> ReadySetResult<PersistencePoint> {
        Ok(PersistencePoint::Persisted)
    }

    fn add_weak_index(&mut self, index: Index) {
        let mut weak_index = KeyedState::from(&index);
        // Track which rows we've already inserted to deduplicate across strict indexes. The
        // previous approach checked `weak_index.lookup(&key)` using the *weak index key*, which
        // incorrectly skipped distinct rows that happened to share the same weak key value as a
        // row from an earlier strict index. Using a HashSet<Row> checks full row identity instead.
        // Row cloning is cheap (Arc refcount bump) and hashing is O(1) (cached hash).
        let mut seen = HashSet::<Row>::new();

        for strict_index in self.state.iter() {
            for (row, duplicate_count) in strict_index.values().flat_map(HashBag::set_iter) {
                if seen.insert(row.clone()) {
                    // First time seeing this row — insert all copies (duplicate_count accounts
                    // for identical rows within the same strict index bucket).
                    for _ in 0..duplicate_count {
                        weak_index.insert(&index.columns, row.clone(), false);
                    }
                }
            }
        }

        self.weak_indices.insert(index.columns, weak_index);
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>> {
        self.weak_indices[columns].lookup(key).map(From::from)
    }

    fn shut_down(&mut self) -> ReadySetResult<()> {
        Ok(())
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
        self.insert_row(r, partial_tag)
    }

    fn insert_row(&mut self, r: Row, partial_tag: Option<Tag>) -> bool {
        let (hit, target_index) = if let Some(tag) = partial_tag {
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
            (self.state[i].insert_row(r.clone()), Some(i))
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                hit_any |= self.state[i].insert_row(r.clone());
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            (hit_any, None)
        };

        if hit && !self.weak_indices.is_empty() {
            // For partial (tagged) inserts, skip the weak index insertion if the row already
            // exists in another strict index — that means a previous insert_row call already
            // added it to all weak indexes. Without this check, a row present in multiple
            // strict indexes' filled holes would be duplicated in the weak index.
            let dominated = target_index.is_some_and(|i| {
                self.state.iter().enumerate().any(|(j, s)| {
                    if j == i {
                        return false;
                    }
                    let key = PointKey::from(s.columns().iter().map(|&c| r[c].clone()));
                    match s.lookup(&key) {
                        LookupResult::Some(RecordResult::Borrowed(rows)) => {
                            // Must pass &Row (not &[DfValue]) so the hash matches Row::Hash,
                            // which hashes cached_hash rather than the raw data.
                            rows.contains(&r) > 0
                        }
                        _ => false,
                    }
                })
            });

            if !dominated {
                for (key, weak_index) in self.weak_indices.iter_mut() {
                    weak_index.insert(key, r.clone(), false);
                }
            }
        }

        hit
    }

    fn remove(&mut self, r: &[DfValue]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Arc::strong_count(&row.data) == 1 {
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
    fn handle_evicted_row(&mut self, row: &Row, evicted_index: usize) -> usize {
        if !self.weak_indices.is_empty() {
            // Mirror of the insert_row dedup logic: only remove the row from weak
            // indexes if it is NOT still present in another strict index.  Without
            // this, evicting a key from one strict index would silently lose the row
            // from the weak index even though it's still reachable via another strict
            // index's filled hole.
            let still_reachable = self.state.iter().enumerate().any(|(j, s)| {
                if j == evicted_index {
                    return false;
                }
                let key = PointKey::from(s.columns().iter().map(|&c| row[c].clone()));
                match s.lookup(&key) {
                    LookupResult::Some(RecordResult::Borrowed(rows)) => rows.contains(row) > 0,
                    _ => false,
                }
            });

            if !still_reachable {
                for (key, weak_index) in self.weak_indices.iter_mut() {
                    weak_index.remove(key, row, None);
                }
            }
        }

        // Only count strong references after we removed a row from `weak_indices`
        // otherwise if it is there, it will never have a reference count of 1
        if Arc::strong_count(&row.data) == 1 {
            row.deep_size_of()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use readyset_data::{Bound, IntoBoundedRange};
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
        state.add_index(Index::hash_map(vec![0]), None);
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
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ]
        .into();

        state.add_index(Index::hash_map(vec![0]), None);
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
        let row: Vec<DfValue> = vec![10.into(), "Cat".into()];
        state.add_index(Index::hash_map(vec![0]), None);
        insert(&mut state, row.clone());
        state.add_index(Index::hash_map(vec![1]), None);

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
        state.add_index(Index::hash_map(vec![0]), None);
        state.add_index(Index::btree_map(vec![0]), None);
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
                    &RangeKey::from(&vec1![DfValue::from(3)].range_to_inclusive())
                )
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn point_lookup_only_btree() {
        let mut state = MemoryState::default();
        state.add_index(Index::btree_map(vec![0]), Some(vec![Tag::new(1)]));
        state.mark_filled(
            KeyComparison::Range((
                Bound::Excluded(vec1![DfValue::MIN]),
                Bound::Excluded(vec1![DfValue::MAX]),
            )),
            Tag::new(1),
        );
        state.insert(vec![DfValue::from(1), DfValue::from(2)], Some(Tag::new(1)));

        let res = state.lookup(&[0], &PointKey::Single(DfValue::from(1)));
        assert!(res.is_some());
        assert_eq!(
            res.unwrap(),
            RecordResult::Owned(vec![vec![1.into(), 2.into()]])
        );
    }

    // Tests that lookups on keys with duplicate columns can perform lookups on states with
    // deduplicated set of columns
    #[test]
    fn duplicate_columns() {
        let mut state = MemoryState::default();
        state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
        state.add_index(Index::hash_map(vec![0, 0]), Some(vec![Tag::new(1)]));

        state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
        state.insert(
            vec![1.into(), 1.into(), 1.into(), 1.into()],
            Some(Tag::new(0)),
        );

        let res = state.lookup(&[0, 0], &PointKey::Double((1.into(), 1.into())));
        assert!(res.is_some());
        let rows = res.unwrap();
        assert_eq!(
            rows,
            RecordResult::Owned(vec![vec![1.into(), 1.into(), 1.into(), 1.into()]])
        );
    }

    // Tests that a lookup on a superset of columns in another index will still find the rows in
    // that index
    #[test]
    fn superset_lookup() {
        let mut state = MemoryState::default();
        state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
        state.add_index(Index::hash_map(vec![0, 1]), Some(vec![Tag::new(1)]));

        state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
        state.insert(
            vec![1.into(), 2.into(), 3.into(), 4.into()],
            Some(Tag::new(0)),
        );
        state.insert(
            vec![1.into(), 3.into(), 4.into(), 5.into()],
            Some(Tag::new(0)),
        );

        let res = state.lookup(&[0, 1], &PointKey::Double((1.into(), 2.into())));
        assert!(res.is_some());
        let rows = res.unwrap();
        assert_eq!(
            rows,
            RecordResult::Owned(vec![vec![1.into(), 2.into(), 3.into(), 4.into()]])
        );
    }

    // Tests that lookups on a subset of columns from another index do not return rows from that
    // index
    #[test]
    fn subset_lookup_miss() {
        let mut state = MemoryState::default();
        state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
        state.add_index(Index::hash_map(vec![0, 1]), Some(vec![Tag::new(1)]));

        state.mark_filled(KeyComparison::Equal(vec1![1.into(), 2.into()]), Tag::new(1));
        state.insert(
            vec![1.into(), 2.into(), 3.into(), 4.into()],
            Some(Tag::new(1)),
        );

        let res = state.lookup(&[0], &PointKey::Single(1.into()));
        assert!(res.is_missing());
    }

    #[test]
    fn shuffled_columns() {
        let mut state = MemoryState::default();
        state.add_index(Index::hash_map(vec![2, 3]), Some(vec![Tag::new(0)]));
        state.add_index(Index::hash_map(vec![3, 2]), Some(vec![Tag::new(1)]));

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
        state.add_index(Index::hash_map(vec![]), None);

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
        use vec1::vec1;

        use super::*;

        mod partial {
            use readyset_data::RangeBounds;
            use vec1::Vec1;

            use super::*;

            fn setup() -> MemoryState {
                let mut state = MemoryState::default();
                let tag = Tag::new(1);
                state.add_index(Index::new(IndexType::BTreeMap, vec![0]), Some(vec![tag]));
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
                state.add_index(Index::new(IndexType::HashMap, vec![0]), Some(vec![tag_1]));
                state.add_index(Index::new(IndexType::HashMap, vec![1]), Some(vec![tag_2]));

                // Mark two holes filled for Tag 1, and one for Tag 2
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(0)]), tag_1);
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(1)]), tag_1);
                state.mark_filled(KeyComparison::from(vec1![DfValue::from(0)]), tag_2);

                // Evict a random key for Tag 1
                let mut rng = rand::rng();
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
                state.add_index(Index::new(IndexType::BTreeMap, vec![0]), None);
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
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&vec1![DfValue::from(3)].range_from_inclusive())
                    ),
                    RangeLookupResult::Some(
                        (3..10).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_inclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(
                        &[0],
                        &RangeKey::from(&vec1![DfValue::from(3)].range_to_inclusive())
                    ),
                    RangeLookupResult::Some(
                        (0..=3).map(|n| vec![n.into()]).collect::<Vec<_>>().into()
                    )
                );
            }

            #[test]
            fn unbounded_exclusive() {
                let state = setup();
                assert_eq!(
                    state.lookup_range(&[0], &RangeKey::from(&vec1![DfValue::from(3)].range_to())),
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

            state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
            state.add_weak_index(Index::hash_map(vec![1]));

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

        /// Regression test for REA-3336: when multiple strict indexes have overlapping rows and
        /// distinct rows that share the same weak index key, the weak index must contain all
        /// unique rows without duplicates.
        #[test]
        fn multiple_strict_indexes_no_dropped_rows() {
            let mut state = MemoryState::default();

            // Two strict partial indexes on columns [0] and [1], weak index on column [2]
            state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
            state.add_index(Index::hash_map(vec![1]), Some(vec![Tag::new(1)]));

            // Fill hole [0] = 1 with rows that all have column [2] = 3
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            let mut records_0: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![1.into(), "b".into(), 3.into()], true),
                (vec![1.into(), "c".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_0, Some(Tag::new(0)), None)
                .unwrap();

            // Fill hole [1] = "a" with rows that also have column [2] = 3
            // Row [1, "a", 3] overlaps with the first strict index
            state.mark_filled(KeyComparison::Equal(vec1![DfValue::from("a")]), Tag::new(1));
            let mut records_1: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![2.into(), "a".into(), 3.into()], true),
                (vec![3.into(), "a".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_1, Some(Tag::new(1)), None)
                .unwrap();

            // Now add the weak index on column [2]. All 5 unique rows share weak key 3.
            state.add_weak_index(Index::hash_map(vec![2]));

            let result = state.lookup_weak(&[2], &PointKey::Single(3.into()));
            let mut rows: Vec<Vec<DfValue>> =
                result.unwrap().into_iter().map(|r| r.to_vec()).collect();
            rows.sort();

            let mut expected = vec![
                vec![1.into(), "a".into(), 3.into()],
                vec![1.into(), "b".into(), 3.into()],
                vec![1.into(), "c".into(), 3.into()],
                vec![2.into(), "a".into(), 3.into()],
                vec![3.into(), "a".into(), 3.into()],
            ];
            expected.sort();

            assert_eq!(rows, expected);
        }

        /// Regression test for REA-3336: when rows are inserted into multiple strict indexes
        /// via separate replays (with the weak index already existing), overlapping rows must
        /// not be duplicated in the weak index.
        #[test]
        fn insert_row_no_weak_duplicates() {
            let mut state = MemoryState::default();

            // Two strict partial indexes on columns [0] and [1], weak index on column [2].
            // Weak index is created BEFORE any holes are filled (the logictest scenario).
            state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
            state.add_index(Index::hash_map(vec![1]), Some(vec![Tag::new(1)]));
            state.add_weak_index(Index::hash_map(vec![2]));

            // Fill hole [0] = 1 — rows go into strict index [0] and weak index
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            let mut records_0: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![1.into(), "b".into(), 3.into()], true),
                (vec![1.into(), "c".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_0, Some(Tag::new(0)), None)
                .unwrap();

            // Fill hole [1] = "a" — rows go into strict index [1]. Row [1,"a",3] overlaps
            // with strict index [0] and must NOT be re-inserted into the weak index.
            state.mark_filled(KeyComparison::Equal(vec1![DfValue::from("a")]), Tag::new(1));
            let mut records_1: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![2.into(), "a".into(), 3.into()], true),
                (vec![3.into(), "a".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_1, Some(Tag::new(1)), None)
                .unwrap();

            let result = state.lookup_weak(&[2], &PointKey::Single(3.into()));
            let mut rows: Vec<Vec<DfValue>> =
                result.unwrap().into_iter().map(|r| r.to_vec()).collect();
            rows.sort();

            let mut expected = vec![
                vec![1.into(), "a".into(), 3.into()],
                vec![1.into(), "b".into(), 3.into()],
                vec![1.into(), "c".into(), 3.into()],
                vec![2.into(), "a".into(), 3.into()],
                vec![3.into(), "a".into(), 3.into()],
            ];
            expected.sort();

            assert_eq!(rows, expected);
        }

        /// Regression test: evicting a key from one strict index must not remove
        /// overlapping rows from the weak index if they're still reachable via
        /// another strict index.
        #[test]
        fn evict_key_preserves_overlapping_rows_in_weak_index() {
            let mut state = MemoryState::default();

            state.add_index(Index::hash_map(vec![0]), Some(vec![Tag::new(0)]));
            state.add_index(Index::hash_map(vec![1]), Some(vec![Tag::new(1)]));
            state.add_weak_index(Index::hash_map(vec![2]));

            // Fill hole [0] = 1
            state.mark_filled(KeyComparison::Equal(vec1![1.into()]), Tag::new(0));
            let mut records_0: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![1.into(), "b".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_0, Some(Tag::new(0)), None)
                .unwrap();

            // Fill hole [1] = "a" — row [1,"a",3] overlaps
            state.mark_filled(KeyComparison::Equal(vec1![DfValue::from("a")]), Tag::new(1));
            let mut records_1: Records = vec![
                (vec![1.into(), "a".into(), 3.into()], true),
                (vec![2.into(), "a".into(), 3.into()], true),
            ]
            .into();
            state
                .process_records(&mut records_1, Some(Tag::new(1)), None)
                .unwrap();

            // Weak index should have 3 unique rows
            let result = state.lookup_weak(&[2], &PointKey::Single(3.into()));
            assert_eq!(result.unwrap().len(), 3);

            // Evict hole [0] = 1 from strict index 0.
            // Row [1,"a",3] is still in strict index 1 → must remain in weak index.
            // Row [1,"b",3] is ONLY in strict index 0 → should be removed.
            state.evict_keys(Tag::new(0), &[KeyComparison::Equal(vec1![1.into()])]);

            let result = state.lookup_weak(&[2], &PointKey::Single(3.into()));
            let mut rows: Vec<Vec<DfValue>> =
                result.unwrap().into_iter().map(|r| r.to_vec()).collect();
            rows.sort();

            let mut expected = vec![
                vec![1.into(), "a".into(), 3.into()], // still in strict index 1
                vec![2.into(), "a".into(), 3.into()], // still in strict index 1
            ];
            expected.sort();

            assert_eq!(rows, expected);
        }
    }

    mod weak_index_proptest {
        use std::collections::BTreeMap;
        use std::ops::RangeInclusive;
        use std::time::Duration;

        use async_trait::async_trait;
        use hashbag::HashBag;
        use proptest::prelude::*;
        use proptest::sample;
        use proptest_stateful::{
            proptest_config_with_local_failure_persistence, ModelState, ProptestStatefulConfig,
        };
        use vec1::Vec1;

        use super::*;

        const NUM_COLS: usize = 3;
        const ELEM_RANGE: RangeInclusive<u8> = 0..=2;

        // Three elements is kind of arbitrary but seems like a good enough balance of small rows
        // that are more likely to have collisions but big enough to cover a good variety of edge
        // cases. (Keep in mind values range from 0..=2 so that gives us 3^3 = 27 possible rows.)
        type TestRow = [u8; NUM_COLS];

        fn test_row_to_record(row: TestRow) -> Record {
            let row = row.into_iter().map(|v| DfValue::Int(v as i64)).collect();
            Record::Positive(row)
        }

        #[derive(Clone, Debug)]
        enum Operation {
            InsertRow(TestRow),
            DeleteRow(TestRow),
            ReplayKey(Tag, Vec<u8>),
            CreateStrictIndex(Vec<usize>, Tag),
            CreateWeakIndex(Vec<usize>),
        }

        #[derive(Clone, Debug, Default)]
        struct IndexModelState {
            rows: Vec<TestRow>,
            filled_holes: BTreeMap<Vec<usize>, Vec<Vec<u8>>>,
            strict_indexes: BTreeMap<Tag, Vec<usize>>,
            weak_indexes: Vec<Vec<usize>>,
        }

        fn gen_test_row() -> impl Strategy<Value = TestRow> {
            [ELEM_RANGE; NUM_COLS]
        }

        prop_compose! {
            fn gen_insert_row()(row in gen_test_row()) -> Operation {
                Operation::InsertRow(row)
            }
        }

        prop_compose! {
            fn gen_delete_row(rows: Vec<TestRow>)(r in sample::select(rows)) -> Operation {
                Operation::DeleteRow(r)
            }
        }

        prop_compose! {
            fn gen_replay_key(tags: Vec<Tag>, strict_indexes: BTreeMap<Tag, Vec<usize>>)
                             (tag in sample::select(tags))
                             (key in vec![ELEM_RANGE; strict_indexes[&tag].len()],
                              tag in Just(tag))
                             -> Operation {
                Operation::ReplayKey(tag, key)
            }
        }

        // Create a Vec<usize> to represent a key by generating a column index for each bit that's
        // set to 1 in `columns`. This is the simplest way to generate a key that I've found since
        // we can use a range from 1..=16 to avoid generating an empty key Vec, we don't need to
        // worry about generating duplicate column indexes.
        fn col_vec_from_int(key: usize) -> Vec<usize> {
            let mut v = Vec::with_capacity(NUM_COLS);
            for i in 0..NUM_COLS {
                if key & (1 << i) != 0 {
                    v.push(i);
                }
            }
            v
        }

        prop_compose! {
            fn gen_create_strict(tag: Tag)(columns in 1_usize..(1 << NUM_COLS)) -> Operation {
                let columns = col_vec_from_int(columns);
                Operation::CreateStrictIndex(columns, tag)
            }
        }

        prop_compose! {
            fn gen_create_weak()(columns in 1_usize..(1 << NUM_COLS)) -> Operation {
                let columns = col_vec_from_int(columns);
                Operation::CreateWeakIndex(columns)
            }
        }

        #[async_trait(?Send)]
        impl ModelState for IndexModelState {
            type Operation = Operation;
            type RunContext = MemoryState;

            fn op_generators<'a>(&self) -> Vec<impl Strategy<Value = Self::Operation> + use<'a>> {
                // We can always perform any op except DeleteRow (since that one requires we have
                // at least one row to delete), and ReplayKey (since we need to have created a tag
                // to replay to), so most of the ops just get unconditionally thrown into a vec
                // right from the beginning.

                let insert_row_strat = gen_insert_row().boxed();
                let create_strict_strat = gen_create_strict(self.next_tag()).boxed();
                let create_weak_strat = gen_create_weak().boxed();

                let mut ops = vec![insert_row_strat, create_strict_strat, create_weak_strat];

                if !self.rows.is_empty() {
                    let delete_row_strat = gen_delete_row(self.rows.clone()).boxed();
                    ops.push(delete_row_strat);
                }

                if !self.strict_indexes.is_empty() {
                    let replay_key_strat = gen_replay_key(
                        self.strict_indexes.keys().cloned().collect(),
                        self.strict_indexes.clone(),
                    )
                    .boxed();
                    ops.push(replay_key_strat);
                }

                ops
            }

            fn preconditions_met(&self, op: &Self::Operation) -> bool {
                match op {
                    Operation::InsertRow(_)
                    | Operation::CreateStrictIndex(_, _)
                    | Operation::CreateWeakIndex(_) => true,
                    Operation::ReplayKey(tag, key) => {
                        // Make sure we have a strict index with this tag, and then make sure that
                        // the hole hasn't already been filled, since we would never trigger a
                        // replay to a filled hole:
                        self.strict_indexes.get(tag).is_some_and(|index| {
                            !self
                                .filled_holes
                                .get(index)
                                .is_some_and(|filled| filled.contains(key))
                        })
                    }
                    Operation::DeleteRow(row) => self.rows.contains(row),
                }
            }

            fn next_state(&mut self, op: &Operation) {
                match op {
                    Operation::InsertRow(row) => {
                        self.rows.push(*row);
                    }
                    Operation::DeleteRow(row) => {
                        let row_index = self.rows.iter().position(|r| r == row).unwrap();
                        self.rows.remove(row_index);
                    }
                    Operation::ReplayKey(tag, key) => {
                        let index = self.strict_indexes[tag].clone();
                        self.filled_holes
                            .entry(index)
                            .or_default()
                            .push(key.clone());
                    }
                    Operation::CreateStrictIndex(columns, tag) => {
                        self.strict_indexes.insert(*tag, columns.clone());
                    }
                    Operation::CreateWeakIndex(columns) => {
                        self.weak_indexes.push(columns.clone());
                    }
                }
            }

            async fn init_test_run(&self) -> Self::RunContext {
                MemoryState::default()
            }

            async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
                match op {
                    Operation::InsertRow(row) => {
                        let row = row.iter().map(|v| DfValue::Int(*v as i64)).collect();
                        ctxt.insert(row, None);
                    }
                    Operation::DeleteRow(row) => {
                        let row: Vec<DfValue> =
                            row.iter().map(|v| DfValue::Int(*v as i64)).collect();
                        ctxt.remove(&row);
                    }
                    Operation::ReplayKey(tag, key) => {
                        let key_comparison = KeyComparison::Equal(
                            Vec1::try_from_vec(
                                key.iter().map(|v| DfValue::Int(*v as i64)).collect(),
                            )
                            .unwrap(),
                        );
                        ctxt.mark_filled(key_comparison, *tag);

                        let index = &self.strict_indexes[tag];
                        let row_matches_key = |row: &TestRow| -> bool {
                            index.iter().enumerate().all(|(i, col)| row[*col] == key[i])
                        };
                        let mut records: Records = self
                            .rows
                            .iter()
                            .copied()
                            .filter(row_matches_key)
                            .map(test_row_to_record)
                            .collect();

                        ctxt.process_records(&mut records, Some(*tag), None)
                            .unwrap();
                    }
                    Operation::CreateStrictIndex(columns, tag) => {
                        let index = Index::new(IndexType::HashMap, columns.clone());
                        ctxt.add_index(index, Some(vec![*tag]));
                    }
                    Operation::CreateWeakIndex(columns) => {
                        let index = Index::new(IndexType::HashMap, columns.clone());
                        ctxt.add_weak_index(index);
                    }
                }
            }

            async fn check_postconditions(&self, ctxt: &mut Self::RunContext) {
                let materialized_rows: HashBag<Vec<u8>> = self
                    .rows
                    .iter()
                    .filter(|r| {
                        self.filled_holes.iter().any(|(index, filled)| {
                            let row_key = index.iter().map(|col| r[*col]).collect();
                            filled.contains(&row_key)
                        })
                    })
                    .map(|r| r.to_vec())
                    .collect();

                for weak_index_key in self.weak_indexes.iter() {
                    let keyed_state = &ctxt.weak_indices[weak_index_key];
                    let weak_index_rows: HashBag<_> = keyed_state
                        .values()
                        .flatten()
                        .map(|row| {
                            (0..NUM_COLS)
                                .map(|i| (&row[i]).try_into().unwrap())
                                .collect::<Vec<u8>>()
                        })
                        .collect();

                    assert_eq!(materialized_rows, weak_index_rows);
                }
            }

            async fn clean_up_test_run(&self, _: &mut Self::RunContext) {}
        }

        impl IndexModelState {
            fn next_tag(&self) -> Tag {
                Tag::new(self.strict_indexes.len() as u32)
            }
        }

        #[test]
        fn run_cases() {
            let config = ProptestStatefulConfig {
                min_ops: 10,
                max_ops: 100,
                test_case_timeout: Duration::from_secs(5),
                proptest_config: proptest_config_with_local_failure_persistence!(),
            };

            proptest_stateful::test::<IndexModelState>(config);
        }
    }

    #[test]
    fn state_is_not_empty() {
        let mut state = MemoryState::default();

        let index = Index::hash_map(vec![0]);
        let tag = Tag::new(1);

        let index2 = Index::hash_map(vec![1]);
        let tag2 = Tag::new(2);

        state.add_index(index, Some(vec![tag]));
        state.add_index(index2, Some(vec![tag2]));

        // we must first fill a hole then insert a row.
        state.mark_filled(KeyComparison::Equal(vec1![100.into()]), tag2);
        state.insert(vec![1.into(), 100.into()], Some(tag2));

        assert_eq!(state.row_count(), 1);
        assert!(!state.size_is_empty());
    }

    #[test]
    fn evict_keys_bytes() {
        let mut state = MemoryState::default();

        let index = Index::hash_map(vec![0]);
        let tag = Tag::new(1);

        state.add_index(index, Some(vec![tag]));
        state.mark_filled(KeyComparison::Equal(vec1![1.into()]), tag);

        let result = state.evict_keys(tag, &[KeyComparison::Equal(vec1![2.into()])]);
        assert!(result.is_some());
        assert_eq!(result.unwrap().bytes_freed, 0);

        let result = state.evict_keys(tag, &[KeyComparison::Equal(vec1![1.into()])]);
        assert!(result.is_some());
        assert!(result.unwrap().bytes_freed > 0);
    }
}
