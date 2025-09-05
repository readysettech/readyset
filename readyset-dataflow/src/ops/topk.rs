/// Please note that the ordering for TopK and Pagination is reversed during MIR lowering.
/// This is why every single use of `cmp` is reversed here.
/// Why this MIR reversal was done is still unknown to me and this moment.
///
/// Ideally, that MIR reversal should be removed and all reversals here should be removed
/// as well.
use std::borrow::Cow;
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::convert::TryInto;

use dataflow_state::PointKey;
use itertools::Itertools;
use readyset_client::{internal, KeyComparison};
use readyset_data::{Bound, DfValue};
use readyset_errors::{internal, internal_err, ReadySetResult};
use readyset_sql::ast::{NullOrder, OrderType};
use readyset_util::Indices;
use serde::{Deserialize, Serialize};
use tracing::trace;
use vec1::Vec1;

use crate::node::AuxiliaryNodeState;
use crate::ops::utils::Order;
use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

/// Data structure used internally by the Auxiliary State to
/// track the state of the TopK operator per key/group.
/// The Auxiliary State is used instead of the main State, because
/// we want to keep a buffer zone of rows past the top k rows.
pub type TopKState = HashMap<Vec<DfValue>, Vec<Vec<DfValue>>>;

/// Data structure used internally by TopK to track rows within a group.
/// Holds a reference to the `order` of the TopK operator to allow for a
/// custom `Ord` implementation, which compares records in reverse order
/// to support maintaining a sorted `Vec` for efficient binary search and
/// insertion/removal.
#[derive(Debug)]
struct CurrentRecord<'topk, 'state> {
    row: Cow<'state, [DfValue]>,
    order: &'topk Order,
    // If the key wasn't in Top K (or buffer) then it's a new entry
    // and `original_index` will be `None`.
    original_index: Option<usize>,
}

impl Ord for CurrentRecord<'_, '_> {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.order, other.order);
        self.order
            .cmp(self.row.as_ref(), other.row.as_ref())
            .reverse()
    }
}

impl PartialOrd for CurrentRecord<'_, '_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CurrentRecord<'_, '_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<[DfValue]> for CurrentRecord<'_, '_> {
    fn partial_cmp(&self, other: &[DfValue]) -> Option<Ordering> {
        Some(self.order.cmp(self.row.as_ref(), other).reverse())
    }
}

impl PartialEq<[DfValue]> for CurrentRecord<'_, '_> {
    fn eq(&self, other: &[DfValue]) -> bool {
        self.partial_cmp(other)
            .iter()
            .any(|c| *c == Ordering::Equal)
    }
}

impl Eq for CurrentRecord<'_, '_> {}

/// TopK provides an operator that will produce the top k elements for each group.
///
/// Positives are generally fast to process, while negative records can trigger expensive backwards
/// queries. It is also worth noting that due the nature of Readyset, the results of this operator are
/// unordered.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopK {
    src: IndexPair,

    /// The index of this node.
    our_index: Option<IndexPair>,

    /// The list of column indices that we're grouping by.
    group_by: Vec<usize>,

    order: Order,
    k: usize,

    /// The number of rows to keep (buffer) past k.
    /// This is used to reduce the number of lookups that need to be
    /// made in case of deletions from top k rows.
    buffered: usize,
}

impl TopK {
    /// Construct a new TopK operator.
    ///
    /// # Arguments
    ///
    /// * `src` - this operator's ancestor
    /// * `order` - The list of columns to compute top k over
    /// * `group_by` - the columns that this operator is keyed on
    /// * `k` - the maximum number of results per group.
    pub fn new(
        src: NodeIndex,
        order: Vec<(usize, OrderType, NullOrder)>,
        group_by: Vec<usize>,
        k: usize,
    ) -> Self {
        TopK {
            src: src.into(),
            our_index: None,
            group_by,
            order: order.into(),
            k,
            buffered: k,
        }
    }

    /// Project the columns we are grouping by out of the given record
    fn project_group<'rec, R>(&self, rec: &'rec R) -> ReadySetResult<Vec<&'rec DfValue>>
    where
        R: Indices<'static, usize, Output = DfValue> + ?Sized,
    {
        rec.indices(self.group_by.clone())
            .map_err(|_| ReadySetError::InvalidRecordLength)
    }

    /// Compare two records by the query's `ORDER BY`
    /// (reversed, check the comment at the top of this file),
    /// breaking ties with full-row comparison to ensure a deterministic
    /// order, which is required by the binary search used in `post_group` and `on_input`.
    fn total_cmp(&self, a: &[DfValue], b: &[DfValue]) -> Ordering {
        self.order.cmp(a, b).reverse().then(a.cmp(b))
    }

    /// Called inside of on_input after processing an individual group of input records, to turn
    /// that group into a set of records in `out`.
    ///
    /// `current` is the final contents of the current group, where the elements are structs of
    /// `(row, original_index)`. Check the comment on [`CurrentRecord::original_index`]
    /// for more details.
    ///
    /// `current_group_key` contains the projected key of the group.
    ///
    /// `original_group_len` contains the length of the group before we started making updates to
    /// it.
    #[allow(clippy::too_many_arguments)]
    fn post_group<'topk, 'state>(
        &'topk self,
        out: &mut Vec<Record>,
        current: &mut Vec<CurrentRecord<'topk, 'state>>,
        current_group_key: &[DfValue],
        original_group_len: usize,
        state: &'state StateMap,
        nodes: &DomainNodes,
        buffered_state: &mut TopKState,
    ) -> ReadySetResult<Option<Lookup>> {
        if current_group_key.is_empty() {
            return Ok(None);
        }

        let mut lookup = None;

        if original_group_len >= self.k && current.len() < self.k {
            // Previously this group contained >= k records, but now it has fewer.
            //
            // To recover, we pull all rows from the parent, sort them, and take
            // top k (+ buffered).
            //
            // Optimization: we avoid generating redundant updates (e.g., pushing a Negative
            // then a Positive for the same row). We track whether a row was already pushed
            // using its `original_index`:
            //   - `Some(_)` → row was already pushed, so we can skip re-emitting
            //   - `None` → row has never been pushed, so it must be emitted now
            //
            // The push/no-push decision is simplified here, please refer to the loops
            // below and their comments.
            let IngredientLookupResult::Records(parent_records) = self.lookup(
                *self.src,
                &self.group_by,
                &PointKey::from(current_group_key.iter().cloned()),
                nodes,
                state,
                LookupMode::Strict,
            )?
            else {
                internal!("We shouldn't have been able to get this record if the parent would miss")
            };

            let mut old_current = current
                .drain(..)
                .map(|r| (r.row, r.original_index))
                .collect::<HashMap<_, _>>();

            let mut rs = parent_records.collect::<Result<Vec<_>, _>>()?;
            rs.sort_unstable_by(|a, b| self.total_cmp(a, b));

            current.extend(
                rs.into_iter()
                    .take(self.k + self.buffered)
                    .map(|row| CurrentRecord {
                        original_index: old_current.remove(&row).unwrap_or_default(),
                        row,
                        order: &self.order,
                    }),
            );

            lookup = Some(Lookup {
                on: *self.src,
                cols: self.group_by.clone(),
                key: current_group_key.to_vec().try_into().expect("Empty group"),
            })
        }

        let k = min(current.len(), self.k);
        let end = min(current.len(), self.k + self.buffered);

        // check top k records first
        for r in current[..k].iter() {
            match r.original_index {
                // new entry in top k rows; push positive
                None => out.push(Record::Positive(r.row.to_vec())),
                // was in the buffer zone, now in top k; push positive
                Some(i) if i >= self.k => out.push(Record::Positive(r.row.to_vec())),
                // was in top k, still in top k; do nothing
                _ => (),
            }
        }

        // now check the buffer zone
        for r in current[k..end].iter() {
            // was in top k, now is in buffer; push negative
            if matches!(r.original_index, Some(i) if i < k) {
                out.push(Record::Negative(r.row.to_vec()));
            }
        }

        // update the buffered (auxiliary) state of this group.
        let _ = buffered_state.insert(
            current_group_key.to_vec(),
            // Since we're done processing this group, we must clear the current
            // group to prepare for the next group, so we use drain instead of iter
            current
                .drain(..)
                .take(self.k + self.buffered)
                .map(|r| r.row.to_vec())
                .collect(),
        );

        Ok(lookup)
    }

    /// Helper method to check if a group key falls within a given range
    /// Mainly used for ranged evictions
    fn key_in_range(
        &self,
        group_key: &[DfValue],
        start: &Bound<Vec1<DfValue>>,
        end: &Bound<Vec1<DfValue>>,
    ) -> bool {
        let compare_with_bound = |key: &[DfValue], bound_vec: &Vec1<DfValue>| -> Ordering {
            for (key_val, bound_val) in key.iter().zip(bound_vec.iter()) {
                match key_val.cmp(bound_val) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            Ordering::Equal
        };

        let start_ok = match start {
            Bound::Included(start_key) => {
                compare_with_bound(group_key, start_key) >= Ordering::Equal
            }
            Bound::Excluded(start_key) => {
                compare_with_bound(group_key, start_key) > Ordering::Equal
            }
        };

        if !start_ok {
            return false;
        }

        match end {
            Bound::Included(end_key) => compare_with_bound(group_key, end_key) <= Ordering::Equal,
            Bound::Excluded(end_key) => compare_with_bound(group_key, end_key) < Ordering::Equal,
        }
    }
}

impl Ingredient for TopK {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    impl_replace_sibling!(src);

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        // who's our parent really?
        self.src.remap(remap);

        // who are we?
        self.our_index = Some(remap[&us]);
    }

    fn on_eviction(
        &mut self,
        _from: LocalNodeIndex,
        _tag: Tag,
        keys: &[KeyComparison],
        auxiliary_node_states: &mut AuxiliaryNodeStateMap,
    ) {
        let aux_state = match auxiliary_node_states.get_mut(*self.our_index.unwrap()) {
            Some(AuxiliaryNodeState::TopK(state)) => state,
            _ => panic!("topk operators got the wrong auxiliary node state"),
        };

        for key in keys {
            match key {
                KeyComparison::Equal(exact) => {
                    aux_state.remove(&exact.clone().into_vec());
                }
                KeyComparison::Range((start, end)) => {
                    // For range evictions, we need to find all keys that fall within the range
                    // and remove them. Since we're dealing with group keys, we need to check
                    // which group keys fall within the specified range.
                    let keys_to_remove: Vec<_> = aux_state
                        .keys()
                        .filter(|group_key| self.key_in_range(group_key, start, end))
                        .cloned()
                        .collect();

                    for key in keys_to_remove {
                        aux_state.remove(&key);
                    }
                }
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        mut rs: Records,
        replay: &ReplayContext,
        nodes: &DomainNodes,
        state: &StateMap,
        auxiliary_node_states: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        rs.sort_by(|a: &Record, b: &Record| {
            self.project_group(&***a)
                .unwrap_or_default()
                .cmp(&self.project_group(&***b).unwrap_or_default())
        });

        let us = self.our_index.unwrap();
        let db = state.get(*us).ok_or_else(|| {
            internal_err!("topk operators must have their own state materialized")
        })?;

        let buffered_state = match auxiliary_node_states
            .get_mut(*us)
            .ok_or_else(|| internal_err!("topk operators must have their own state materialized"))?
        {
            AuxiliaryNodeState::TopK(state) => state,
            _ => internal!("topk operator got the wrong auxiliary node state"),
        };

        let mut out = Vec::with_capacity(rs.len());
        // the lookup key of the group currently being processed
        let mut current_group_key = Vec::new();
        // the original length of the group currently being processed before we started doing
        // anything to it. We need to keep track of this so that we can lookup into our parent to
        // backfill a group if processing drops us below `k` records when we were originally at `k`
        // records (if we weren't originally at `k` records we don't need to do anything special).
        let mut original_group_len = 0;
        let mut missed = false;

        let mut misses = Vec::new();
        let mut lookups = Vec::new();
        // +1 so insertions after reaching capacity don't cause a reallocation
        let current_capacity = self.k + self.buffered + 1;
        let mut current: Vec<CurrentRecord> = Vec::with_capacity(current_capacity);

        // records are now chunked by group
        for r in &rs {
            // Does this record belong to the same group or did we start processing a new group?
            if current_group_key.iter().cmp(self.project_group(r.rec())?) != Ordering::Equal {
                // new group!

                // first, tidy up the old one
                if let Some(lookup) = self.post_group(
                    &mut out,
                    &mut current,
                    &current_group_key,
                    original_group_len,
                    state,
                    nodes,
                    buffered_state,
                )? {
                    if replay.is_partial() {
                        lookups.push(lookup)
                    }
                }

                // make ready for the new one
                current_group_key = self.project_group(r.rec())?.into_iter().cloned().collect();

                // We can’t check for misses against `buffered_state` (the aux state), even though
                // evictions affect both main and aux.
                //
                // Only the main state marks missing keys as "filled" when an upquery resolves a hole.
                // If we relied on the aux state instead, we’d never see those fills and could loop
                // forever reporting misses.
                missed = if let LookupResult::Some(r) =
                    db.lookup(&self.group_by, &PointKey::from(current_group_key.clone()))
                {
                    if replay.is_partial() {
                        lookups.push(Lookup {
                            on: *us,
                            cols: self.group_by.clone(),
                            key: current_group_key.clone().try_into().expect("Empty group"),
                        });
                    }

                    // verify that the states match and that the eviction affected both states
                    debug_assert_eq!(
                        r.into_iter()
                            .map(|r| r.to_vec())
                            .sorted_by(|a, b| self.order.cmp(a, b).reverse().then(a.cmp(b)))
                            .collect::<Vec<_>>(),
                        buffered_state
                            .get(&current_group_key)
                            .unwrap_or(&Vec::new())
                            .iter()
                            .take(self.k)
                            .cloned()
                            .collect::<Vec<_>>()
                    );

                    false
                } else {
                    true
                };

                current = match buffered_state.get(&current_group_key) {
                    Some(records) => {
                        original_group_len = records.len();

                        records
                            .iter()
                            .cloned()
                            .enumerate()
                            .map(|(i, r)| CurrentRecord {
                                row: Cow::Owned(r),
                                order: &self.order,
                                original_index: Some(i),
                            })
                            .collect()
                    }
                    None => {
                        // New group or was previously evicted. Either way, we need to start fresh
                        original_group_len = 0;
                        Vec::with_capacity(current_capacity)
                    }
                };
            }

            if missed {
                misses.push(
                    Miss::builder()
                        .on(*us)
                        .lookup_idx(self.group_by.clone())
                        .lookup_key(self.group_by.clone())
                        .replay(replay)
                        .record(r.row().clone())
                        .build(),
                );
                continue;
            }

            match r {
                Record::Positive(r) => {
                    // If we're at capacity we can't consider positive records worse than our
                    // worst element, because it's possible that there are *other* records
                    // *between* our worst element and that positive record which we wouldn't
                    // know about. If we drop below k records during processing and it turns out
                    // that this positive record would have been in the topk, we'll figure that
                    // out in post_group when we query our parent.
                    if current.len() >= (self.k + self.buffered) {
                        if let Some(worst) = current.last() {
                            if worst <= r.as_slice() {
                                trace!(row = ?r, "topk skipping positive worse than worst");
                                continue;
                            }
                        }
                    }

                    let record = CurrentRecord {
                        row: Cow::Borrowed(r),
                        order: &self.order,
                        // New entry to the topk
                        original_index: None,
                    };

                    match current.binary_search_by(|cr| self.total_cmp(&cr.row, r)) {
                        Ok(idx) | Err(idx) => {
                            // we already know that this record is within bounds and
                            // at least better than our worst element
                            current.insert(idx, record);
                            // Immediately enforce size bound to prevent unbounded growth
                            // and vec reallocation
                            if current.len() > self.k + self.buffered {
                                current.truncate(self.k + self.buffered);
                            }
                        }
                    };
                }
                Record::Negative(r) => {
                    let _ = current
                        .binary_search_by(|cr| self.total_cmp(&cr.row, r))
                        .map(|idx| {
                            // This record was already in the topk
                            if matches!(current[idx].original_index, Some(i) if i < self.k) {
                                out.push(Record::Negative(r.clone()));
                            }
                            // Shifting sucks, we can use a deletion marker, but that
                            // will make the group-posting logic more complex
                            current.remove(idx);
                        });
                }
            }
        }

        if let Some(lookup) = self.post_group(
            &mut out,
            &mut current,
            &current_group_key,
            original_group_len,
            state,
            nodes,
            buffered_state,
        )? {
            if replay.is_partial() {
                lookups.push(lookup)
            }
        }

        Ok(ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        })
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        HashMap::from([
            (
                this,
                LookupIndex::Strict(internal::Index::hash_map(self.group_by.clone())),
            ),
            (
                self.src.as_global(),
                LookupIndex::Strict(internal::Index::hash_map(self.group_by.clone())),
            ),
        ])
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        ColumnSource::exact_copy(self.src.as_global(), cols.into())
    }

    fn description(&self) -> String {
        format!(
            "TopK k={} γ[{}] o[{}]",
            self.k,
            self.group_by.iter().join(", "),
            self.order
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops;

    fn setup(reversed: bool) -> (ops::test::MockGraph, IndexPair) {
        let cmp_rows = if reversed {
            vec![(2, OrderType::OrderDescending, NullOrder::NullsLast)]
        } else {
            vec![(2, OrderType::OrderAscending, NullOrder::NullsFirst)]
        };

        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        g.set_op(
            "topk",
            &["x", "y", "z"],
            TopK::new(s.as_global(), cmp_rows, vec![1], 3),
            true,
        );
        (g, s)
    }

    #[test]
    fn it_keeps_topk() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r12: Vec<DfValue> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".into(), 15.into()];
        let r10b: Vec<DfValue> = vec![6.into(), "z".into(), 10.into()];
        let r10c: Vec<DfValue> = vec![7.into(), "z".into(), 10.into()];

        g.narrow_one_row(r12, true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r10b, true);
        g.narrow_one_row(r10c, true);
        assert_eq!(g.states[ni].row_count(), 3);

        g.narrow_one_row(r15, true);
        g.narrow_one_row(r10, true);
        assert_eq!(g.states[ni].row_count(), 3);
    }

    #[test]
    fn it_forwards() {
        let (mut g, _) = setup(false);

        let r12: Vec<DfValue> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".into(), 15.into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11].into());

        let a = g.narrow_one_row(r5, true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    fn it_queries_parent_on_deletes() {
        let (mut g, s) = setup(false);

        let r12: Vec<DfValue> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".into(), 15.into()];

        // fill the parent (but not with 15 since we'll delete it)
        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());

        // fill topk
        g.narrow_one_row(r12, true);
        g.narrow_one_row(r10.clone(), true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r15.clone(), true);

        // [5, z, 15]
        // [1, z, 12]
        // [3, z, 11]

        // check that removing 15 brings back 10
        let delta = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(delta.len(), 2); // one negative, one positive
        assert!(delta.iter().any(|r| r == &(r15.clone(), false).into()));
        assert!(
            delta.iter().any(|r| r == &(r10.clone(), true).into()),
            "a = {:?} does not contain ({:?}, true)",
            &delta,
            r10
        );
    }

    #[test]
    fn it_queries_parent_on_deletes_reversed() {
        let (mut g, s) = setup(true);

        let r12: Vec<DfValue> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".into(), 15.into()];

        // fill the parent (but not with 5 since we'll delete it)
        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r15.clone());

        // fill topk
        g.narrow_one_row(r12.clone(), true);
        g.narrow_one_row(r10, true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5.clone(), true);
        g.narrow_one_row(r15, true);

        // [4, z, 5]
        // [2, z, 10]
        // [3, z, 11]

        // check that removing 5 brings back 12
        let delta = g.narrow_one_row((r5.clone(), false), true);
        assert_eq!(delta.len(), 2); // one negative, one positive
        assert!(delta.iter().any(|r| r == &(r5.clone(), false).into()));
        assert!(
            delta.iter().any(|r| r == &(r12.clone(), true).into()),
            "a = {:?} does not contain ({:?}, true)",
            &delta,
            r12
        );
    }

    #[test]
    fn it_forwards_reversed() {
        use std::convert::TryFrom;

        let (mut g, _) = setup(true);

        let r12: Vec<DfValue> = vec![1.into(), "z".into(), DfValue::try_from(-12.123).unwrap()];
        let r10: Vec<DfValue> = vec![2.into(), "z".into(), DfValue::try_from(0.0431).unwrap()];
        let r11: Vec<DfValue> = vec![3.into(), "z".into(), DfValue::try_from(-0.082).unwrap()];
        let r5: Vec<DfValue> = vec![4.into(), "z".into(), DfValue::try_from(5.601).unwrap()];
        let r15: Vec<DfValue> = vec![5.into(), "z".into(), DfValue::try_from(-15.9).unwrap()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11].into());

        let a = g.narrow_one_row(r5, true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    fn it_suggests_indices() {
        let (g, _) = setup(false);
        let me = 2.into();
        let parent = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 2);
        assert_eq!(
            &idx[&me],
            &LookupIndex::Strict(readyset_client::internal::Index::hash_map(vec![1]))
        );
        assert_eq!(
            &idx[&parent],
            &LookupIndex::Strict(readyset_client::internal::Index::hash_map(vec![1]))
        );
    }

    #[test]
    fn it_resolves() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_parent_columns() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_handles_updates() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "z".into(), 10.into()];
        let r2: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r3: Vec<DfValue> = vec![3.into(), "z".into(), 10.into()];
        let r4: Vec<DfValue> = vec![4.into(), "z".into(), 5.into()];
        let r4a: Vec<DfValue> = vec![4.into(), "z".into(), 10.into()];
        let r4b: Vec<DfValue> = vec![4.into(), "z".into(), 11.into()];

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3, true);

        // a positive for a row not in the Top-K should not change the Top-K and shouldn't emit
        // anything
        let emit = g.narrow_one_row(r4.clone(), true);
        assert_eq!(g.states[ni].row_count(), 3);
        assert_eq!(emit, Vec::<Record>::new().into());

        // should now have 3 rows in Top-K
        // [1, z, 10]
        // [2, z, 10]
        // [3, z, 10]

        let emit = g.narrow_one(
            vec![Record::Negative(r4), Record::Positive(r4a.clone())],
            true,
        );
        // nothing should have been emitted, as [4, z, 10] doesn't enter Top-K
        assert_eq!(emit, Vec::<Record>::new().into());

        let emit = g.narrow_one(vec![Record::Negative(r4a), Record::Positive(r4b)], true);

        // now [4, z, 11] is in, BUT we still only keep 3 elements
        // and have to remove one of the existing ones
        assert_eq!(g.states[ni].row_count(), 3);
        assert_eq!(emit.len(), 2); // 1 pos, 1 neg
        assert!(emit.iter().any(|r| !r.is_positive() && r[2] == 10.into()));
        assert!(emit.iter().any(|r| r.is_positive() && r[2] == 11.into()));
    }

    #[test]
    fn multiple_groups() {
        let (mut g, _) = setup(true);
        let ni = g.node().local_addr();

        let ra1: Vec<DfValue> = vec![1.into(), "a".into(), 1.into()];
        let ra2: Vec<DfValue> = vec![2.into(), "a".into(), 2.into()];
        let ra3: Vec<DfValue> = vec![3.into(), "a".into(), 3.into()];
        let ra4: Vec<DfValue> = vec![4.into(), "a".into(), 4.into()];
        let ra5: Vec<DfValue> = vec![5.into(), "a".into(), 5.into()];

        let rb1: Vec<DfValue> = vec![1.into(), "b".into(), 1.into()];
        let rb2: Vec<DfValue> = vec![2.into(), "b".into(), 2.into()];
        let rb3: Vec<DfValue> = vec![3.into(), "b".into(), 3.into()];
        let rb4: Vec<DfValue> = vec![4.into(), "b".into(), 4.into()];
        let rb5: Vec<DfValue> = vec![5.into(), "b".into(), 5.into()];

        g.narrow_one_row(ra3, true);
        g.narrow_one_row(ra4.clone(), true);
        g.narrow_one_row(ra5.clone(), true);

        g.narrow_one_row(rb3, true);
        g.narrow_one_row(rb4.clone(), true);
        g.narrow_one_row(rb5.clone(), true);

        assert_eq!(g.states[ni].row_count(), 6);

        let mut emit = g.narrow_one(
            vec![
                (ra1.clone(), true),
                (rb1.clone(), true),
                (ra2.clone(), true),
                (rb2.clone(), true),
            ],
            true,
        );
        assert_eq!(g.states[ni].row_count(), 6);
        emit.sort();
        assert_eq!(
            emit,
            vec![
                (ra1, true),
                (rb1, true),
                (ra2, true),
                (rb2, true),
                (ra4, false),
                (rb4, false),
                (ra5, false),
                (rb5, false),
            ]
            .into()
        )
    }

    #[test]
    fn update_shifting_out() {
        let (mut g, s) = setup(false);
        let ra1: Vec<DfValue> = vec![1.into(), "a".into(), 1.into()];
        let ra2: Vec<DfValue> = vec![2.into(), "a".into(), 2.into()];
        let ra3: Vec<DfValue> = vec![3.into(), "a".into(), 3.into()];
        let ra4: Vec<DfValue> = vec![4.into(), "a".into(), 4.into()];

        g.seed(s, ra1.clone());
        g.seed(s, ra2.clone());
        g.seed(s, ra4.clone());

        g.narrow_one_row(ra1.clone(), true);
        g.narrow_one_row(ra2, true);
        g.narrow_one_row(ra3.clone(), true);
        g.narrow_one_row(ra4, true);

        let ra0: Vec<DfValue> = vec![3.into(), "a".into(), 0.into()];

        let emit = g.narrow_one(vec![(ra3.clone(), false), (ra0, true)], true);
        assert_eq!(emit, vec![(ra3, false), (ra1, true)].into());
    }
}
