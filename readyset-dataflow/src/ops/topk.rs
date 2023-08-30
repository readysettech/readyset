use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::convert::TryInto;
use std::mem;
use std::num::NonZeroUsize;

use dataflow_state::PointKey;
use itertools::Itertools;
use nom_sql::OrderType;
use readyset_client::internal;
use readyset_errors::{internal, internal_err, invariant, ReadySetResult};
use readyset_util::Indices;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::ops::utils::Order;
use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

/// Data structure used internally to topk to track rows currently within a group. Contains a
/// reference to the `order` of the topk itself to allow for a custom Ord implementation, which
/// compares records in reverse order for efficient determination of the minimum record via
/// insertion into a [`BinaryHeap`]
#[derive(Debug)]
struct CurrentRecord<'topk, 'state> {
    row: Cow<'state, [DfValue]>,
    order: &'topk Order,
    is_new: bool,
}

impl<'topk, 'state> Ord for CurrentRecord<'topk, 'state> {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.order, other.order);
        self.order
            .cmp(self.row.as_ref(), other.row.as_ref())
            .reverse()
    }
}

impl<'topk, 'state> PartialOrd for CurrentRecord<'topk, 'state> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'topk, 'state> PartialOrd<[DfValue]> for CurrentRecord<'topk, 'state> {
    fn partial_cmp(&self, other: &[DfValue]) -> Option<Ordering> {
        Some(self.order.cmp(self.row.as_ref(), other))
    }
}

impl<'topk, 'state> PartialEq for CurrentRecord<'topk, 'state> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'topk, 'state> PartialEq<[DfValue]> for CurrentRecord<'topk, 'state> {
    fn eq(&self, other: &[DfValue]) -> bool {
        self.partial_cmp(other)
            .iter()
            .any(|c| *c == Ordering::Equal)
    }
}

impl<'topk, 'state> Eq for CurrentRecord<'topk, 'state> {}

/// TopK provides an operator that will produce the top k elements for each group.
///
/// Positives are generally fast to process, while negative records can trigger expensive backwards
/// queries. It is also worth noting that due the nature of Soup, the results of this operator are
/// unordered.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopK {
    src: IndexPair,

    /// The index of this node
    our_index: Option<IndexPair>,

    /// The list of column indices that we're grouping by.
    group_by: Vec<usize>,

    order: Order,
    k: usize,
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
        order: Vec<(usize, OrderType)>,
        group_by: Vec<usize>,
        k: usize,
    ) -> Self {
        TopK {
            src: src.into(),
            our_index: None,
            group_by,
            order: order.into(),
            k,
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

    /// Called inside of on_input after processing an individual group of input records, to turn
    /// that group into a set of records in `out`.
    ///
    /// `current` is the final contents of the current group, where the elements are tuples of
    /// `(row, whether the row has been newly added to the group)`.
    ///
    /// `current_group_key` contains the projected key of the group.
    ///
    /// `original_group_len` contains the length of the group before we started making updates to
    /// it.
    #[allow(clippy::too_many_arguments)]
    fn post_group<'topk, 'state>(
        &'topk self,
        out: &mut Vec<Record>,
        current: &mut BinaryHeap<CurrentRecord<'topk, 'state>>,
        current_group_key: &[DfValue],
        original_group_len: usize,
        state: &'state StateMap,
        nodes: &DomainNodes,
    ) -> ReadySetResult<Option<Lookup>> {
        let mut lookup = None;
        let group_start_index = current.len().saturating_sub(self.k);

        if original_group_len == self.k {
            if let Some(diff) = original_group_len
                .checked_sub(current.len())
                .and_then(NonZeroUsize::new)
            {
                // there used to be k things in the group, now there are fewer than k.
                match self.lookup(
                    *self.src,
                    &self.group_by,
                    &PointKey::from(current_group_key.iter().cloned()),
                    nodes,
                    state,
                    LookupMode::Strict,
                )? {
                    IngredientLookupResult::Miss => {
                        internal!(
                            "We shouldn't have been able to get this record if the parent would miss"
                        )
                    }
                    IngredientLookupResult::Records(rs) => {
                        let mut rs = rs.collect::<Result<Vec<_>, _>>()?;
                        rs.sort_unstable_by(|a, b| {
                            self.order.cmp(a.as_ref(), b.as_ref()).reverse()
                        });
                        current.extend(
                            rs.into_iter()
                                .map(|row| CurrentRecord {
                                    row,
                                    order: &self.order,
                                    is_new: true,
                                })
                                .skip(current.len())
                                .take(diff.get()),
                        );
                        lookup = Some(Lookup {
                            on: *self.src,
                            cols: self.group_by.clone(),
                            key: current_group_key.to_vec().try_into().expect("Empty group"),
                        })
                    }
                }
            }
        }

        let mut current = mem::take(current).into_sorted_vec();
        // TODO(aspen): it'd be nice to skip this reverse - we could maybe do that with minmaxheap
        // if they merge my addition of retain (https://github.com/tov/min-max-heap-rs/pull/19)
        current.reverse();

        // optimization: if we don't *have to* remove something, we don't
        for i in group_start_index..current.len() {
            if current[i].is_new {
                // we found an `is_new` in current
                // can we replace it with a !is_new with the same order value?
                let replace = current[0..group_start_index].iter().position(
                    |CurrentRecord {
                         row: ref r, is_new, ..
                     }| {
                        !is_new && self.order.cmp(r, &current[i].row) == Ordering::Equal
                    },
                );
                if let Some(ri) = replace {
                    current.swap(i, ri);
                }
            }
        }

        for CurrentRecord { row, is_new, .. } in current.drain(group_start_index..) {
            if is_new {
                out.push(Record::Positive(row.into_owned()));
            }
        }

        if !current.is_empty() {
            for CurrentRecord { row, is_new, .. } in current.drain(..) {
                if !is_new {
                    // Was in k, now isn't
                    out.push(Record::Negative(row.clone().into()));
                }
            }
        }
        Ok(lookup)
    }
}

impl Ingredient for TopK {
    fn take(&mut self) -> NodeOperator {
        self.clone().into()
    }

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

    #[allow(clippy::cognitive_complexity)]
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        replay: &ReplayContext,
        nodes: &DomainNodes,
        state: &StateMap,
        _auxiliary_node_states: &mut AuxiliaryNodeStateMap,
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
        let mut rs: Vec<_> = rs.into();
        rs.sort_by(|a: &Record, b: &Record| {
            self.project_group(&***a)
                .unwrap_or_default()
                .cmp(&self.project_group(&***b).unwrap_or_default())
        });

        let us = self.our_index.unwrap();
        let db = state.get(*us).ok_or_else(|| {
            internal_err!("topk operators must have their own state materialized")
        })?;

        let mut out = Vec::new();
        // the lookup key of the group currently being processed
        let mut current_group_key = Vec::new();
        // the original length of the group currently being processed before we started doing
        // anything to it. We need to keep track of this so that we can lookup into our parent to
        // backfill a group if processing drops us below `k` records when we were originally at `k`
        // records (if we weren't originally at `k` records we don't need to do anything special).
        let mut original_group_len = 0;
        let mut missed = false;
        // The current group being processed
        let mut current: BinaryHeap<CurrentRecord> = BinaryHeap::new();
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        // records are now chunked by group
        for r in &rs {
            if current_group_key.iter().cmp(self.project_group(r.rec())?) != Ordering::Equal {
                // new group!

                // first, tidy up the old one
                if !current_group_key.is_empty() {
                    if let Some(lookup) = self.post_group(
                        &mut out,
                        &mut current,
                        &current_group_key,
                        original_group_len,
                        state,
                        nodes,
                    )? {
                        if replay.is_partial() {
                            lookups.push(lookup)
                        }
                    }
                }
                invariant!(current.is_empty());

                // make ready for the new one
                // NOTE(aspen): Is this the most optimal way of doing this?
                current_group_key.clear();
                current_group_key.extend(self.project_group(r.rec())?.into_iter().cloned());

                // check out current state
                match db.lookup(
                    &self.group_by[..],
                    &PointKey::from(current_group_key.iter().cloned()),
                ) {
                    LookupResult::Some(local_records) => {
                        if replay.is_partial() {
                            lookups.push(Lookup {
                                on: *us,
                                cols: self.group_by.clone(),
                                key: current_group_key.clone().try_into().expect("Empty group"),
                            });
                        }

                        missed = false;
                        original_group_len = local_records.len();
                        current.extend(local_records.into_iter().map(|row| CurrentRecord {
                            row: row.clone(),
                            is_new: false,
                            order: &self.order,
                        }))
                    }
                    LookupResult::Missing => {
                        missed = true;
                    }
                }
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
            } else {
                match r {
                    Record::Positive(r) => {
                        // If we're at k records we can't consider positive records below our
                        // minimum element, because it's possible that there are *other* records
                        // *between* our minimum element and that positive record which we wouldn't
                        // know about. If we drop below k records during processing and it turns out
                        // that this positive record would have been in the topk, we'll figure that
                        // out in post_group when we query our parent.
                        if original_group_len >= self.k {
                            if let Some(min) = current.peek() {
                                if min > r.as_slice() {
                                    trace!(row = ?r, "topk skipping positive below minimum");
                                    continue;
                                }
                            }
                        }

                        current.push(CurrentRecord {
                            row: Cow::Owned(r.clone()),
                            is_new: true,
                            order: &self.order,
                        })
                    }
                    Record::Negative(r) => {
                        let mut found = false;
                        current.retain(|CurrentRecord { row, is_new, .. }| {
                            if found {
                                // we've already removed one copy of this row, don't need to do any
                                // more
                                return true;
                            }
                            if **row == *r {
                                found = true;
                                // is_new = we received a positive and a negative for the same value
                                // in one batch
                                // [note: topk-record-ordering]
                                // Note that since we sort records, and positive records compare
                                // less than negative records, we'll
                                // always get the positive first and the
                                // negative second
                                if !is_new {
                                    out.push(Record::Negative(r.clone()))
                                }
                                return false;
                            }

                            true
                        });
                    }
                }
            }
        }

        if !current_group_key.is_empty() {
            if let Some(lookup) = self.post_group(
                &mut out,
                &mut current,
                &current_group_key,
                original_group_len,
                state,
                nodes,
            )? {
                if replay.is_partial() {
                    lookups.push(lookup)
                }
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
        ColumnSource::exact_copy(self.src.as_global(), cols.try_into().unwrap())
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("TopK");
        }

        format!(
            "TopK k={} γ[{}] o[{}]",
            self.k,
            self.group_by.iter().join(", "),
            self.order
        )
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops;

    fn setup(reversed: bool) -> (ops::test::MockGraph, IndexPair) {
        let cmp_rows = if reversed {
            vec![(2, OrderType::OrderDescending)]
        } else {
            vec![(2, OrderType::OrderAscending)]
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

        let r12: Vec<DfValue> = vec![1.into(), "z".try_into().unwrap(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".try_into().unwrap(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".try_into().unwrap(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".try_into().unwrap(), 15.into()];
        let r10b: Vec<DfValue> = vec![6.into(), "z".try_into().unwrap(), 10.into()];
        let r10c: Vec<DfValue> = vec![7.into(), "z".try_into().unwrap(), 10.into()];

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

        let r12: Vec<DfValue> = vec![1.into(), "z".try_into().unwrap(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".try_into().unwrap(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".try_into().unwrap(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".try_into().unwrap(), 15.into()];

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

        let r12: Vec<DfValue> = vec![1.into(), "z".try_into().unwrap(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".try_into().unwrap(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".try_into().unwrap(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".try_into().unwrap(), 15.into()];

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

        let r12: Vec<DfValue> = vec![1.into(), "z".try_into().unwrap(), 12.into()];
        let r10: Vec<DfValue> = vec![2.into(), "z".try_into().unwrap(), 10.into()];
        let r11: Vec<DfValue> = vec![3.into(), "z".try_into().unwrap(), 11.into()];
        let r5: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 5.into()];
        let r15: Vec<DfValue> = vec![5.into(), "z".try_into().unwrap(), 15.into()];

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

        let r12: Vec<DfValue> = vec![
            1.into(),
            "z".try_into().unwrap(),
            DfValue::try_from(-12.123).unwrap(),
        ];
        let r10: Vec<DfValue> = vec![
            2.into(),
            "z".try_into().unwrap(),
            DfValue::try_from(0.0431).unwrap(),
        ];
        let r11: Vec<DfValue> = vec![
            3.into(),
            "z".try_into().unwrap(),
            DfValue::try_from(-0.082).unwrap(),
        ];
        let r5: Vec<DfValue> = vec![
            4.into(),
            "z".try_into().unwrap(),
            DfValue::try_from(5.601).unwrap(),
        ];
        let r15: Vec<DfValue> = vec![
            5.into(),
            "z".try_into().unwrap(),
            DfValue::try_from(-15.9).unwrap(),
        ];

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

        let r1: Vec<DfValue> = vec![1.into(), "z".try_into().unwrap(), 10.into()];
        let r2: Vec<DfValue> = vec![2.into(), "z".try_into().unwrap(), 10.into()];
        let r3: Vec<DfValue> = vec![3.into(), "z".try_into().unwrap(), 10.into()];
        let r4: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 5.into()];
        let r4a: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 10.into()];
        let r4b: Vec<DfValue> = vec![4.into(), "z".try_into().unwrap(), 11.into()];

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

        let ra1: Vec<DfValue> = vec![1.into(), "a".try_into().unwrap(), 1.into()];
        let ra2: Vec<DfValue> = vec![2.into(), "a".try_into().unwrap(), 2.into()];
        let ra3: Vec<DfValue> = vec![3.into(), "a".try_into().unwrap(), 3.into()];
        let ra4: Vec<DfValue> = vec![4.into(), "a".try_into().unwrap(), 4.into()];
        let ra5: Vec<DfValue> = vec![5.into(), "a".try_into().unwrap(), 5.into()];

        let rb1: Vec<DfValue> = vec![1.into(), "b".try_into().unwrap(), 1.into()];
        let rb2: Vec<DfValue> = vec![2.into(), "b".try_into().unwrap(), 2.into()];
        let rb3: Vec<DfValue> = vec![3.into(), "b".try_into().unwrap(), 3.into()];
        let rb4: Vec<DfValue> = vec![4.into(), "b".try_into().unwrap(), 4.into()];
        let rb5: Vec<DfValue> = vec![5.into(), "b".try_into().unwrap(), 5.into()];

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
        let ra1: Vec<DfValue> = vec![1.into(), "a".try_into().unwrap(), 1.into()];
        let ra2: Vec<DfValue> = vec![2.into(), "a".try_into().unwrap(), 2.into()];
        let ra3: Vec<DfValue> = vec![3.into(), "a".try_into().unwrap(), 3.into()];
        let ra4: Vec<DfValue> = vec![4.into(), "a".try_into().unwrap(), 4.into()];

        g.seed(s, ra1.clone());
        g.seed(s, ra2.clone());
        g.seed(s, ra4.clone());

        g.narrow_one_row(ra1.clone(), true);
        g.narrow_one_row(ra2, true);
        g.narrow_one_row(ra3.clone(), true);
        g.narrow_one_row(ra4, true);

        let ra0: Vec<DfValue> = vec![3.into(), "a".try_into().unwrap(), 0.into()];

        let emit = g.narrow_one(vec![(ra3.clone(), false), (ra0, true)], true);
        assert_eq!(emit, vec![(ra3, false), (ra1, true)].into());
    }
}
