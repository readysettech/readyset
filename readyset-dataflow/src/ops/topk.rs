/// Note: The ordering for TopK and Pagination is reversed during MIR lowering.
/// All comparisons in this module use `.reverse()` to compensate for that reversal.
///
/// TODO: Remove the MIR reversal and simplify the comparison logic here.
use std::borrow::Cow;
use std::cmp::{Ordering, min};
use std::collections::HashMap;
use std::convert::TryInto;

use dataflow_state::PointKey;
use itertools::Itertools;
use metrics::counter;
use readyset_client::metrics::recorded;
use readyset_client::{KeyComparison, internal};
use readyset_data::DfValue;
use readyset_errors::{ReadySetResult, internal, internal_err};
use readyset_sql::ast::{NullOrder, OrderType, Relation};
use readyset_util::Indices;
use serde::{Deserialize, Serialize};
use tracing::{error, trace};

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
/// Sorted and searched using [`TopK::total_cmp`], not the standard `Ord` trait.
#[derive(Debug)]
struct CurrentRecord<'state> {
    row: Cow<'state, [DfValue]>,
    /// `original_index` records the row's position in the persisted aux state
    /// at the start of the current batch. It encodes whether downstream has
    /// the row: positions `[0, k)` were the persisted top-k slice (downstream
    /// has them), positions `[k, k + buffered)` were buffer-only (downstream
    /// does not), and `None` means the row was newly added in this batch.
    /// Use the helpers below rather than comparing the index directly.
    original_index: Option<usize>,
}

impl<'state> CurrentRecord<'state> {
    /// True iff this row was in the persisted top-k slice at the start of
    /// this batch — i.e., downstream's main state currently holds it and
    /// any removal must emit a Negative.
    fn was_in_downstream_top_k(&self, k: usize) -> bool {
        matches!(self.original_index, Some(i) if i < k)
    }
}

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

    /// Name of the cache this operator serves, used to label the backfill counter
    /// ([`recorded::TOPK_BACKFILL_REQUESTS`]) so backfills can be attributed per cache.
    ///
    /// This is the `CREATE CACHE <name>` name (the `name` column in `SHOW CACHES`), set during
    /// MIR lowering via [`TopK::with_cache_name`]. It is carried on the operator because an
    /// operator can't borrow its own node out of `DomainNodes` while being processed. `None` only
    /// when never set (e.g. test constructors), in which case the counter reports
    /// `cache_name="unknown"`.
    #[serde(default)]
    cache_name: Option<Relation>,
}

impl TopK {
    /// Construct a new TopK operator with the default buffer size (`buffered = k`).
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
        Self::with_buffer_multiplier(src, order, group_by, k, None)
    }

    /// Construct a new TopK operator, optionally overriding the buffer size with a multiplier.
    ///
    /// `topk_buffer_multiplier` is the factor applied to `k` when sizing the backup buffer used
    /// to avoid backwards queries on negative records (`buffered = k * multiplier`). `None`
    /// preserves the legacy default of `buffered = k`. `Some(0)` disables buffering entirely.
    pub fn with_buffer_multiplier(
        src: NodeIndex,
        order: Vec<(usize, OrderType, NullOrder)>,
        group_by: Vec<usize>,
        k: usize,
        topk_buffer_multiplier: Option<usize>,
    ) -> Self {
        let buffered = match topk_buffer_multiplier {
            Some(m) => k.saturating_mul(m),
            None => k,
        };
        TopK {
            src: src.into(),
            our_index: None,
            group_by,
            order: order.into(),
            k,
            buffered,
            cache_name: None,
        }
    }

    /// Record the cache this operator serves (the `CREATE CACHE` name), so the backfill counter
    /// can be labeled with it. Set during MIR lowering, where the cache name is known.
    pub fn with_cache_name(mut self, cache_name: Relation) -> Self {
        self.cache_name = Some(cache_name);
        self
    }

    /// Total capacity per group (`k + buffered`), saturated to avoid overflow when `buffered`
    /// is at `usize::MAX`. Used as the `Vec::with_capacity` argument and as the comparison
    /// threshold for the trim path.
    #[inline]
    fn total_capacity(&self) -> usize {
        self.k.saturating_add(self.buffered)
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
    fn post_group<'state>(
        &self,
        out: &mut Vec<Record>,
        current: &mut Vec<CurrentRecord<'state>>,
        current_group_key: Option<&[DfValue]>,
        original_group_len: usize,
        state: &'state StateMap,
        nodes: &DomainNodes,
        buffered_state: &mut TopKState,
    ) -> ReadySetResult<Option<Lookup>> {
        let current_group_key = match current_group_key {
            Some(key) => key,
            None => return Ok(None),
        };

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
            let cache_name = self
                .cache_name
                .as_ref()
                .map(|n| n.display_unquoted().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            counter!(recorded::TOPK_BACKFILL_REQUESTS, "cache_name" => cache_name).increment(1);

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

            let mut old_current: Vec<_> = current
                .drain(..)
                .map(|r| (r.row, r.original_index))
                .collect();

            // Stream parent rows into a sorted Vec bounded at `k + buffered`
            // so groups with millions of rows do not materialize fully here.
            // We use a sorted Vec rather than a `BinaryHeap` because the
            // heap takes no custom comparator: each row would have to be
            // wrapped in a struct that captures `&self.order` to satisfy
            // `Ord`. For typical small k the asymptotic win of the heap
            // is not worth the per-row wrapper allocation and the extra
            // boilerplate.
            let cap = self.total_capacity();
            let mut rs: Vec<Cow<'_, [DfValue]>> = Vec::with_capacity(cap.saturating_add(1));
            for row in parent_records {
                let row = row?;
                // Once full, reject rows that don't beat the current worst
                // before paying for `binary_search_by` + `Vec::insert`.
                if rs.len() == cap
                    && rs
                        .last()
                        .is_some_and(|worst| !self.total_cmp(worst, &row).is_gt())
                {
                    continue;
                }
                let pos = rs
                    .binary_search_by(|existing| self.total_cmp(existing, &row))
                    .unwrap_or_else(|p| p);
                rs.insert(pos, row);
                if rs.len() > cap {
                    rs.pop();
                }
            }

            current.extend(rs.into_iter().map(|row| {
                // Find and remove the first matching entry from old_current
                // to correctly handle duplicate rows. Order of old_current
                // doesn't matter (it's a lookup bag), so use swap_remove
                // to avoid O(k) element shifting.
                let original_index = match old_current
                    .iter()
                    .position(|(old_row, _)| old_row.as_ref() == row.as_ref())
                {
                    Some(pos) => old_current.swap_remove(pos).1,
                    None => None,
                };
                CurrentRecord {
                    original_index,
                    row,
                }
            }));

            // Only construct the Lookup when the group key is non-empty, since
            // KeyComparison::Equal requires Vec1 (at least one element).
            // Empty group_by means a single global group — no key-based lookup needed.
            if let Ok(key) = current_group_key.to_vec().try_into() {
                lookup = Some(Lookup {
                    on: *self.src,
                    cols: self.group_by.clone(),
                    key,
                })
            } else {
                debug_assert!(
                    self.group_by.is_empty(),
                    "Failed to construct Lookup with non-empty group_by"
                );
            }
        }

        let top_end = min(current.len(), self.k);
        let buffer_end = min(current.len(), self.total_capacity());

        // Top-k slice: emit a Positive for any row downstream doesn't yet have.
        for r in current[..top_end].iter() {
            if !r.was_in_downstream_top_k(self.k) {
                out.push(Record::Positive(r.row.to_vec()));
            }
        }

        // Buffer slice: emit a Negative for any row downstream still has but
        // that has been demoted out of top-k.
        for r in current[top_end..buffer_end].iter() {
            if r.was_in_downstream_top_k(self.k) {
                out.push(Record::Negative(r.row.to_vec()));
            }
        }

        // Update the buffered (auxiliary) state of this group.
        debug_assert!(
            current.len() <= self.total_capacity(),
            "current exceeded capacity: {} > {}",
            current.len(),
            self.total_capacity()
        );
        buffered_state.insert(
            current_group_key.to_vec(),
            current.drain(..).map(|r| r.row.to_vec()).collect(),
        );

        Ok(lookup)
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
        let us = self
            .our_index
            .expect("TopK node index must be set after on_commit");
        let Some(AuxiliaryNodeState::TopK(aux_state)) = auxiliary_node_states.get_mut(*us) else {
            error!("TopK operator received wrong auxiliary node state during eviction");
            return;
        };

        for key in keys {
            match key {
                KeyComparison::Equal(exact) => {
                    aux_state.remove(exact.as_slice());
                }
                KeyComparison::Range(_) => {
                    // Range evictions don't normally reach here: TopK is
                    // hash-indexed and sits near leaves, so memory pressure
                    // produces point evictions for it. We handle Range
                    // defensively in case a future dataflow shape routes
                    // one through.
                    aux_state.retain(|group_key, _| !key.contains(group_key.iter()));
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

        // Sort records by group key so that records for the same group are contiguous.
        // Skip the sort when group_by is empty (single global group).
        if !self.group_by.is_empty() {
            rs.sort_by(|a: &Record, b: &Record| {
                self.group_by
                    .iter()
                    .map(|&col| &a.rec()[col])
                    .cmp(self.group_by.iter().map(|&col| &b.rec()[col]))
            });
        }

        let us = self
            .our_index
            .expect("TopK node index must be set after on_commit");
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
        let mut current_group_key: Option<Vec<DfValue>> = None;
        // the original length of the group currently being processed before we started doing
        // anything to it. We need to keep track of this so that we can lookup into our parent to
        // backfill a group if processing drops us below `k` records when we were originally at `k`
        // records (if we weren't originally at `k` records we don't need to do anything special).
        let mut original_group_len = 0;
        let mut missed = false;

        let mut misses = Vec::new();
        let mut lookups = Vec::new();
        // +1 so insertions after reaching capacity don't cause a reallocation
        let current_capacity = self.total_capacity().saturating_add(1);
        let mut current: Vec<CurrentRecord> = Vec::with_capacity(current_capacity);

        // records are now chunked by group
        for r in &rs {
            let projected_refs = self.project_group(r.rec())?;

            // Does this record belong to the same group or did we start processing a new group?
            // Compare using borrowed references to avoid allocating on every record.
            let is_same_group = current_group_key.as_deref().is_some_and(|key: &[DfValue]| {
                key.len() == projected_refs.len()
                    && key.iter().zip(projected_refs.iter()).all(|(a, b)| a == *b)
            });
            if !is_same_group {
                // new group!

                // first, tidy up the old one
                if let Some(lookup) = self.post_group(
                    &mut out,
                    &mut current,
                    current_group_key.as_deref(),
                    original_group_len,
                    state,
                    nodes,
                    buffered_state,
                )? && replay.is_partial()
                {
                    lookups.push(lookup)
                }

                // make ready for the new one — only clone into owned values on group change
                current_group_key.replace(projected_refs.into_iter().cloned().collect());

                // We can’t check for misses against `buffered_state` (the aux state), even though
                // evictions affect both main and aux.
                //
                // Only the main state marks missing keys as "filled" when an upquery resolves a hole.
                // If we relied on the aux state instead, we’d never see those fills and could loop
                // forever reporting misses.
                // Invariant: current_group_key was just set to Some above
                let group_key_ref = current_group_key
                    .as_ref()
                    .expect("current_group_key must be Some after assignment");
                missed = if let LookupResult::Some(r) =
                    db.lookup(&self.group_by, &PointKey::from(group_key_ref.clone()))
                {
                    if replay.is_partial() {
                        // Empty group_by means a single global group — no key-based
                        // lookup needed since Vec1 requires at least one element.
                        if let Ok(key) = group_key_ref.clone().try_into() {
                            lookups.push(Lookup {
                                on: *us,
                                cols: self.group_by.clone(),
                                key,
                            });
                        } else {
                            debug_assert!(
                                self.group_by.is_empty(),
                                "Failed to construct Lookup with non-empty group_by"
                            );
                        }
                    }

                    // Verify aux state agrees with main state. Debug-only by design:
                    // release-mode detection of divergence (e.g. via a partial-replay
                    // or restart-recovery race) is delegated to the upstream query
                    // sampler, which catches wrong results at the system level. Do
                    // not add a release-mode check here without coordinating with it.
                    debug_assert_eq!(
                        r.into_iter()
                            .map(|r| r.to_vec())
                            .sorted_by(|a, b| self.total_cmp(a, b))
                            .collect::<Vec<_>>(),
                        buffered_state
                            .get(group_key_ref)
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

                // Reuse `current`'s allocation across group transitions. Its
                // capacity was set at the top of `on_input` and stays valid
                // for every group; clear+extend avoids a malloc per group.
                current.clear();
                match buffered_state.get(group_key_ref) {
                    Some(records) => {
                        // The aux state for a group must hold at most k +
                        // buffered rows. The first k define downstream's
                        // top-k slice and the rest define the buffer; the
                        // `original_index < k` predicate (encoded by the
                        // CurrentRecord helpers) depends on this shape.
                        if records.len() > self.total_capacity() {
                            internal!(
                                "topk aux state for group has {} rows, exceeds k + buffered ({} + {})",
                                records.len(),
                                self.k,
                                self.buffered,
                            );
                        }

                        original_group_len = records.len();

                        current.extend(records.iter().cloned().enumerate().map(|(i, r)| {
                            CurrentRecord {
                                row: Cow::Owned(r),
                                original_index: Some(i),
                            }
                        }));
                    }
                    None => {
                        // New group or was previously evicted. Either way, we start fresh.
                        original_group_len = 0;
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
                continue;
            }

            match r {
                Record::Positive(r) => {
                    // If the group started this batch already at >= k, skip
                    // any positive that is not strictly better than our
                    // current worst. The buffer is bounded at k + buffered,
                    // so the parent may hold rows between our worst and this
                    // positive that we have never seen; admitting it would
                    // risk promoting it to top-k later (via deletes) ahead
                    // of those better rows.
                    //
                    // If deletes drain the group below k by end of batch,
                    // post_group backfills from the parent and recovers any
                    // skipped positive that does belong in top-k. An
                    // under-filled group (original_group_len < k) has no
                    // downstream-visible top-k to protect and no hidden
                    // parent rows in dataflow, so we admit everything.
                    if original_group_len >= self.k
                        && let Some(worst) = current.last()
                        && !self.total_cmp(&worst.row, r).is_gt()
                    {
                        trace!(row = ?r, "topk skipping positive worse than worst");
                        continue;
                    }

                    // `Err` is a non-duplicate insertion; `Ok` means a
                    // byte-identical row already exists in `current` (under
                    // `total_cmp`, equality requires full-row equality). In
                    // the `Ok` case we insert a duplicate intentionally; the
                    // negative handler below resolves it via the smallest
                    // `original_index` tiebreak when the row is later removed.
                    match current.binary_search_by(|cr| self.total_cmp(&cr.row, r)) {
                        Ok(idx) | Err(idx) => {
                            current.insert(
                                idx,
                                CurrentRecord {
                                    row: Cow::Borrowed(r),
                                    original_index: None, // New entry to top-k
                                },
                            );

                            // Enforce size bound. The popped row is worse than every
                            // survivor; it can only matter to a future top-k once they
                            // are all deleted, which triggers backfill in post_group.
                            // If the popped row was in the original top-k, emit a
                            // Negative since downstream has it.
                            if current.len() > self.total_capacity() {
                                let dropped =
                                    current.pop().expect("current is non-empty after insert");
                                if dropped.was_in_downstream_top_k(self.k) {
                                    out.push(Record::Negative(dropped.row.into_owned()));
                                }
                            }
                        }
                    };
                }
                Record::Negative(r) => {
                    match current.binary_search_by(|cr| self.total_cmp(&cr.row, r)) {
                        Ok(idx) => {
                            // binary_search found *an* equal element. When there are
                            // duplicates, prefer removing the one with the smallest
                            // original_index (most likely in top-k) so we correctly
                            // emit a Negative if it was materialized downstream.
                            let mut best = idx;
                            for i in (0..idx).rev() {
                                if self.total_cmp(&current[i].row, r) != Ordering::Equal {
                                    break;
                                }
                                if current[i].original_index < current[best].original_index {
                                    best = i;
                                }
                            }
                            for i in (idx + 1)..current.len() {
                                if self.total_cmp(&current[i].row, r) != Ordering::Equal {
                                    break;
                                }
                                if current[i].original_index < current[best].original_index {
                                    best = i;
                                }
                            }
                            if current[best].was_in_downstream_top_k(self.k) {
                                out.push(Record::Negative(r.clone()));
                            }
                            current.remove(best);
                        }
                        Err(_) => {
                            trace!(row = ?r, "topk negative for row not in buffer, ignoring");
                        }
                    }
                }
            }
        }

        if let Some(lookup) = self.post_group(
            &mut out,
            &mut current,
            current_group_key.as_deref(),
            original_group_len,
            state,
            nodes,
            buffered_state,
        )? && replay.is_partial()
        {
            lookups.push(lookup)
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
    fn with_buffer_multiplier_sets_buffered() {
        // Default (None) keeps the legacy behavior: buffered == k.
        let topk = TopK::new(NodeIndex::new(0), vec![], vec![], 10);
        assert_eq!(topk.k, 10);
        assert_eq!(topk.buffered, 10);

        // Some(1) is explicitly equivalent to the default.
        let topk = TopK::with_buffer_multiplier(NodeIndex::new(0), vec![], vec![], 10, Some(1));
        assert_eq!(topk.buffered, 10);

        // Some(3) scales the buffer: buffered = k * multiplier.
        let topk = TopK::with_buffer_multiplier(NodeIndex::new(0), vec![], vec![], 10, Some(3));
        assert_eq!(topk.buffered, 30);

        // Some(0) disables buffering past k.
        let topk = TopK::with_buffer_multiplier(NodeIndex::new(0), vec![], vec![], 10, Some(0));
        assert_eq!(topk.buffered, 0);
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

    /// Test that empty group_by with duplicate values handles deletes correctly.
    ///
    /// Exercises two bugs:
    /// 1. Empty group key (`group_by = []`) collided with the `Vec::new()` sentinel
    ///    used for "no group selected yet", causing `post_group` to skip processing.
    /// 2. HashMap in backfill collapsed duplicate rows (same value) into one entry,
    ///    causing spurious Positive emissions that inflated the row count.
    #[test]
    fn empty_group_by_with_duplicate_values() {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "val"]);

        // group_by = [] means all rows are in one group; k = 3
        g.set_op(
            "topk",
            &["x", "val"],
            TopK::new(
                s.as_global(),
                vec![(1, OrderType::OrderAscending, NullOrder::NullsFirst)],
                vec![],
                3,
            ),
            true,
        );

        let ni = g.node().local_addr();

        // 4 rows all with val = 1, distinguished by x
        let r1: Vec<DfValue> = vec![1.into(), 1.into()];
        let r2: Vec<DfValue> = vec![2.into(), 1.into()];
        let r3: Vec<DfValue> = vec![3.into(), 1.into()];
        let r4: Vec<DfValue> = vec![4.into(), 1.into()];

        // Seed the parent with all 4 rows (needed for backfill lookups)
        g.seed(s, r1.clone());
        g.seed(s, r2.clone());
        g.seed(s, r3.clone());
        g.seed(s, r4.clone());

        // Insert all 4 rows into topk
        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);
        g.narrow_one_row(r4, true);

        // With k=3, top-k state should have exactly 3 rows
        assert_eq!(g.states[ni].row_count(), 3);

        // Delete 2 rows. Negatives for r1 and r2.
        let emit = g.narrow_one(vec![(r1, false), (r2, false)], true);

        // Should have emitted negatives for r1 and r2 (they were in top-k)
        assert!(
            emit.iter().any(|r| !r.is_positive() && r[0] == 1.into()),
            "expected negative for r1, got: {:?}",
            emit
        );
        assert!(
            emit.iter().any(|r| !r.is_positive() && r[0] == 2.into()),
            "expected negative for r2, got: {:?}",
            emit
        );

        // After backfill from parent (which still has all 4 rows in seed state),
        // topk should maintain 3 rows in state
        assert_eq!(
            g.states[ni].row_count(),
            3,
            "state should have 3 rows after backfill"
        );
    }

    /// When the buffer is full and a new row ties on ORDER BY columns but
    /// beats the worst element under total_cmp, it must be inserted.
    #[test]
    fn skip_optimization_ordering_mismatch() {
        let (mut g, s) = setup(false);
        let ni = g.node().local_addr();

        // All rows have the same ORDER BY value (z=10), differ only in x.
        // total_cmp tiebreaks on full row, so x matters for position.
        // k=3, buffered=3, capacity=6.
        let r1: Vec<DfValue> = vec![1.into(), "z".into(), 10.into()];
        let r2: Vec<DfValue> = vec![2.into(), "z".into(), 10.into()];
        let r3: Vec<DfValue> = vec![3.into(), "z".into(), 10.into()];
        let r4: Vec<DfValue> = vec![4.into(), "z".into(), 10.into()];
        let r5: Vec<DfValue> = vec![5.into(), "z".into(), 10.into()];
        let r6: Vec<DfValue> = vec![6.into(), "z".into(), 10.into()];

        // Seed parent with all rows for backfill
        for r in [&r1, &r2, &r3, &r4, &r5, &r6] {
            g.seed(s, r.clone());
        }

        // Fill buffer to capacity (6 entries)
        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3.clone(), true);
        g.narrow_one_row(r4, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r6, true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Now insert a row with same ORDER BY value but x=0, which sorts
        // BEFORE all existing rows under total_cmp. It should enter the
        // buffer and displace the worst.
        let r0: Vec<DfValue> = vec![0.into(), "z".into(), 10.into()];
        g.seed(s, r0.clone());
        let emit = g.narrow_one_row(r0.clone(), true);

        // r0 sorts before all existing rows under total_cmp, so it should
        // enter the top-k and displace the worst element.
        assert!(
            emit.iter().any(|r| r.is_positive()),
            "r0 should have been inserted but was incorrectly skipped. emit={:?}",
            emit
        );
    }

    /// When a batch of positives displaces records that were in the original
    /// top-k, Negatives must be emitted for the displaced entries.
    #[test]
    fn truncation_drops_topk_without_negative() {
        let (mut g, s) = setup(false);
        let ni = g.node().local_addr();

        // k=3, buffered=3, capacity=6. Effective ordering is DESC.
        // Fill with scores 100-600; top-k (DESC) = [600, 500, 400], buffer = [300, 200, 100]
        let r1: Vec<DfValue> = vec![1.into(), "z".into(), 100.into()];
        let r2: Vec<DfValue> = vec![2.into(), "z".into(), 200.into()];
        let r3: Vec<DfValue> = vec![3.into(), "z".into(), 300.into()];
        let r4: Vec<DfValue> = vec![4.into(), "z".into(), 400.into()];
        let r5: Vec<DfValue> = vec![5.into(), "z".into(), 500.into()];
        let r6: Vec<DfValue> = vec![6.into(), "z".into(), 600.into()];

        for r in [&r1, &r2, &r3, &r4, &r5, &r6] {
            g.seed(s, r.clone());
        }

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3, true);
        g.narrow_one_row(r4, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r6, true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Send 7 positives in a SINGLE BATCH with scores 700-706 (all better
        // than existing in DESC). These should displace the old top-k entries
        // [600, 500, 400] and Negatives must be emitted for them.
        let new_rows: Vec<Record> = (700..=706)
            .enumerate()
            .map(|(i, score)| {
                let r: Vec<DfValue> = vec![(10 + i as i32).into(), "z".into(), score.into()];
                g.seed(s, r.clone());
                Record::Positive(r)
            })
            .collect();

        let emit = g.narrow_one(new_rows, true);

        let neg_count = emit.iter().filter(|r| !r.is_positive()).count();
        let pos_count = emit.iter().filter(|r| r.is_positive()).count();

        // 3 negatives for displaced top-k entries [400, 500, 600]
        // 3 positives for new top-k entries
        assert_eq!(
            neg_count, 3,
            "Should have 3 negatives for displaced top-k entries, \
             got {} negatives. Full emit: {:?}",
            neg_count, emit
        );
        assert_eq!(
            pos_count, 3,
            "Should have 3 positives for new top-k entries, got {}. Full emit: {:?}",
            pos_count, emit
        );

        assert_eq!(g.states[ni].row_count(), 3);
    }

    /// Duplicate positives (byte-identical rows re-sent) must not grow
    /// state beyond k.
    #[test]
    fn duplicate_positive_does_not_grow_state() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r1: Vec<DfValue> = vec![1.into(), "z".into(), 10.into()];
        let r2: Vec<DfValue> = vec![2.into(), "z".into(), 11.into()];
        let r3: Vec<DfValue> = vec![3.into(), "z".into(), 12.into()];
        let r4: Vec<DfValue> = vec![4.into(), "z".into(), 13.into()];

        // Fill top k (k=3)
        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);
        g.narrow_one_row(r4, true);
        assert_eq!(g.states[ni].row_count(), 3);

        // Re-send identical positives for rows already in top k.
        // State must stay at k=3.
        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3, true);
        assert_eq!(g.states[ni].row_count(), 3);
    }

    /// A worse-than-worst positive must not be admitted to the buffer when
    /// the parent has hidden rows: later deletes can promote it to top-k
    /// ahead of a better parent row, since `current.len() == k` post-delete
    /// does not trigger backfill.
    #[test]
    fn worse_than_worst_positive_promotes_stale_buffer_row() {
        let (mut g, s) = setup(false);
        let ni = g.node().local_addr();

        let r100: Vec<DfValue> = vec![1.into(), "z".into(), 100.into()];
        let r90: Vec<DfValue> = vec![2.into(), "z".into(), 90.into()];
        let r80: Vec<DfValue> = vec![3.into(), "z".into(), 80.into()];
        let r70: Vec<DfValue> = vec![4.into(), "z".into(), 70.into()];
        let r60: Vec<DfValue> = vec![5.into(), "z".into(), 60.into()];
        let r50: Vec<DfValue> = vec![6.into(), "z".into(), 50.into()];
        let r40: Vec<DfValue> = vec![7.into(), "z".into(), 40.into()];
        let r30: Vec<DfValue> = vec![8.into(), "z".into(), 30.into()];
        let r20: Vec<DfValue> = vec![9.into(), "z".into(), 20.into()];

        // Parent state at backfill time: r100/r90/r80/r70 have been
        // deleted and r20 inserted.
        g.seed(s, r60.clone());
        g.seed(s, r50.clone());
        g.seed(s, r40.clone());
        g.seed(s, r30.clone());
        g.seed(s, r20.clone());

        // Ascending arrival ensures r40 and r30 are popped during fill
        // and remain hidden from the operator.
        for r in [&r30, &r40, &r50, &r60, &r70, &r80, &r90, &r100] {
            g.narrow_one_row(r.clone(), true);
        }

        g.narrow_one(
            vec![
                Record::Negative(r100.clone()),
                Record::Positive(r20.clone()),
            ],
            true,
        );

        g.narrow_one_row((r90.clone(), false), true);
        g.narrow_one_row((r80.clone(), false), true);
        let final_emit = g.narrow_one_row((r70.clone(), false), true);

        assert!(
            final_emit.iter().any(|r| r == &(r40.clone(), true).into()),
            "expected +r40 in final emit; got {:?}",
            final_emit,
        );
        assert!(
            !final_emit.iter().any(|r| r == &(r20.clone(), true).into()),
            "r20 must not be promoted to top-k; got {:?}",
            final_emit,
        );

        assert_eq!(g.states[ni].row_count(), 3);
        let LookupResult::Some(rows) = g.states[ni].lookup(&[1], &PointKey::from(["z".into()]))
        else {
            panic!("expected materialized rows for group 'z'");
        };
        let vals: Vec<DfValue> = rows.into_iter().map(|r| r[2].clone()).collect();
        assert!(
            vals.contains(&60.into()) && vals.contains(&50.into()) && vals.contains(&40.into()),
            "expected state to contain {{60, 50, 40}}; got {:?}",
            vals,
        );
        assert!(
            !vals.contains(&20.into()),
            "state must not contain stale r20; got {:?}",
            vals,
        );
    }
}
