use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};

use dataflow_state::PointKey;
use itertools::Itertools;
use readyset_client::KeyComparison;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_util::intervals::into_bound_endpoint;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::debug;
use vec1::{vec1, Vec1};

use dataflow_expression::Expr;

use super::Side;
use crate::prelude::*;
use crate::processing::{
    ColumnMiss, ColumnRef, ColumnSource, IngredientLookupResult, LookupIndex, LookupMode,
};

/// Kind of join
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Left join between two views
    Left,
    /// Inner join between two views
    Inner,
}

/// Execution mode for joins.
#[derive(Debug)]
pub enum JoinExecutionMode {
    /// Regular Join, with upquery in one side and lookup using ON keys on other side.
    RegularLookup,
    /// Straddled with two upqueries. Execute a hash join once both sides arrive.
    StraddledHashJoin,
    /// Straddled with one upquery and state lookups on other side. This is done when the right side is fully materialized and we are guaranteed to not miss on lookup.
    StraddledRegularLookup,
}

/// Join rows between two nodes based on a (compound) equal join key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    left: IndexPair,
    right: IndexPair,

    /// Key columns in the left and right parents respectively
    on: Vec<(usize, usize)>,

    // Which columns to emit
    emit: Vec<(Side, usize)>,

    /// Buffered records from one half of a remapped upquery. The key is (column index,
    /// side).
    // We skip serde since we don't want the state of the node, just the configuration.
    #[serde(skip)]
    generated_column_buffer: HashMap<(Vec<usize>, Side), Records>,

    kind: JoinType,
    // Indicates if the right side of the join is fully materialized.
    // If true, its guaranteed that we cannot miss on lookup.
    rhs_full_mat: bool,

    /// Missing upqueries for the other side of the join. The key is (column index,
    /// side).
    // We skip serde since we don't want the state of the node, just the configuration.
    #[serde(skip)]
    pub missing_upqueries: HashMap<(Vec<KeyComparison>, Side), Vec<ColumnMiss>>,

    /// Optional filter expression evaluated on left rows for LEFT JOINs.
    /// When present and the filter evaluates to false for a left row,
    /// the join skips the right-side lookup and directly emits a NULL-extended row.
    /// This implements correct LEFT JOIN semantics for ON-clause predicates
    /// that reference only the left side of the join.
    left_filter: Option<Expr>,

    /// Whether `generate_row_move` is safe to use (no duplicate columns from the same side
    /// in `emit`). Precomputed at construction time; recomputed in `post_deserialize`.
    #[serde(skip)]
    emit_move_safe: bool,
}

impl Join {
    /// Create a new instance of Join
    ///
    /// `left` and `right` are the left and right parents respectively. `on` is a tuple specifying
    /// the join columns: (left_parent_column, right_parent_column) and `emit` dictates for each
    /// output colunm, which source and column should be used (true means left parent, and false
    /// means right parent).
    pub fn new(
        left: NodeIndex,
        right: NodeIndex,
        kind: JoinType,
        on: Vec<(usize, usize)>,
        emit: Vec<(Side, usize)>,
        rhs_full_mat: bool,
        left_filter: Option<Expr>,
    ) -> Self {
        debug!(
            "Join::new: left: {:?}, right: {:?}, kind: {:?}, on: {:?}, rhs_full_mat: {:?}",
            left, right, kind, on, rhs_full_mat
        );
        let mut join = Self {
            left: left.into(),
            right: right.into(),
            on,
            emit,
            generated_column_buffer: Default::default(),
            kind,
            rhs_full_mat,
            missing_upqueries: Default::default(),
            left_filter,
            emit_move_safe: false,
        };
        join.recompute_emit_metadata();
        join
    }

    pub fn node_is_lhs(&self, node: LocalNodeIndex) -> bool {
        node == *self.left
    }

    pub fn node_is_rhs(&self, node: LocalNodeIndex) -> bool {
        node == *self.right
    }

    /// Recompute all `#[serde(skip)]` emit-related metadata from `self.emit`.
    /// Called from `new()` and `post_deserialize()`.
    fn recompute_emit_metadata(&mut self) {
        self.emit_move_safe = {
            let mut left_cols = HashSet::new();
            let mut right_cols = HashSet::new();
            self.emit.iter().all(|&(side, col)| match side {
                Side::Left => left_cols.insert(col),
                Side::Right => right_cols.insert(col),
            })
        };
    }

    pub fn on_left(&self) -> Vec<usize> {
        self.on.iter().map(|(l, _)| *l).collect()
    }

    pub fn on_right(&self) -> Vec<usize> {
        self.on.iter().map(|(_, r)| *r).collect()
    }

    fn generate_row(&self, left: &[DfValue], right: &[DfValue]) -> Vec<DfValue> {
        self.emit
            .iter()
            .map(|&(side, col)| match side {
                Side::Left => left[col].clone(),
                Side::Right => right[col].clone(),
            })
            .collect()
    }

    /// Like [`generate_row`], but moves values from the owned side instead of cloning.
    /// The caller must not read from `owned` after this call.
    fn generate_row_move(
        &self,
        owned: &mut [DfValue],
        borrowed: &[DfValue],
        owned_is_left: bool,
    ) -> Vec<DfValue> {
        let mut result = Vec::with_capacity(self.emit.len());
        for &(side, col) in &self.emit {
            let from_owned =
                (owned_is_left && side == Side::Left) || (!owned_is_left && side == Side::Right);
            if from_owned {
                result.push(std::mem::take(&mut owned[col]));
            } else {
                result.push(borrowed[col].clone());
            }
        }
        result
    }

    /// Build a hash map from one of the sides of the join.
    fn build_join_hash_map<'a>(
        &'a self,
        records: &'a Records,
        key: &[usize],
    ) -> HashMap<Vec<&'a DfValue>, Vec<&'a Record>> {
        let mut hm = HashMap::new();
        for rec in records {
            let key: Vec<&DfValue> = key.iter().map(|idx| &rec[*idx]).collect();
            hm.entry(key)
                .and_modify(|entry: &mut Vec<&Record>| entry.push(rec))
                .or_insert(vec![rec]);
        }
        hm
    }

    /// Perform a hash join between two sets of records.
    fn hash_join(&self, left: Records, right: Records) -> ReadySetResult<Records> {
        let mut probe_keys = vec![];
        let mut build_keys = vec![];
        let mut ret: Vec<Record> = vec![];
        let probe_is_left = left.len() > right.len();
        for (left_key, right_key) in &self.on {
            match probe_is_left {
                true => {
                    probe_keys.push(*left_key);
                    build_keys.push(*right_key);
                }
                false => {
                    probe_keys.push(*right_key);
                    build_keys.push(*left_key);
                }
            }
        }
        let (probe_side, build_side) = match probe_is_left {
            true => (&left, &right),
            false => (&right, &left),
        };
        let hm = self.build_join_hash_map(build_side, &build_keys);

        let mut key: Vec<&DfValue> = vec![&DfValue::None; probe_keys.len()];

        if self.kind == JoinType::Left && !probe_is_left {
            // When probe is right and build is left for a LEFT JOIN,
            // we need to handle this differently: iterate left (build) side
            // and look up right (probe) side.
            // For correctness, iterate left records directly.
            let right_keys: Vec<usize> = self.on.iter().map(|(_, r)| *r).collect();
            let left_keys: Vec<usize> = self.on.iter().map(|(l, _)| *l).collect();
            let right_hm = self.build_join_hash_map(&right, &right_keys);

            let mut rkey: Vec<&DfValue> = vec![&DfValue::None; left_keys.len()];
            for left_rec in left.iter() {
                invariant!(
                    left_rec.is_positive(),
                    "replays should only include positive records"
                );
                if !self.left_filter_passes(left_rec.row())? {
                    ret.push(Record::Positive(self.generate_null(left_rec.row())));
                    continue;
                }
                for i in 0..left_keys.len() {
                    rkey[i] = &left_rec[left_keys[i]];
                }
                if let Some(right_recs) = right_hm.get(&rkey) {
                    for right_rec in right_recs {
                        ret.push(Record::Positive(
                            self.generate_row(left_rec.row(), right_rec.row()),
                        ));
                    }
                } else {
                    ret.push(Record::Positive(self.generate_null(left_rec.row())));
                }
            }
            return Ok(ret.into());
        }

        for prob_rec in probe_side {
            for i in 0..probe_keys.len() {
                key[i] = &prob_rec[probe_keys[i]];
            }

            // For LEFT JOINs where probe is left, check the filter
            let filter_passes = if self.kind == JoinType::Left && probe_is_left {
                self.left_filter_passes(prob_rec.row())?
            } else {
                true
            };

            if !filter_passes {
                // Left row fails filter — emit NULL-extended
                invariant!(
                    prob_rec.is_positive(),
                    "replays should only include positive records"
                );
                ret.push(Record::Positive(self.generate_null(prob_rec.row())));
                continue;
            }

            if let Some(build_recs) = hm.get(&key) {
                invariant!(
                    prob_rec.is_positive(),
                    "replays should only include positive records"
                );
                for build_rec in build_recs {
                    invariant!(
                        build_rec.is_positive(),
                        "replays should only include positive records"
                    );

                    match probe_is_left {
                        true => ret.push(Record::Positive(
                            self.generate_row(prob_rec.row(), build_rec.row()),
                        )),
                        false => ret.push(Record::Positive(
                            self.generate_row(build_rec.row(), prob_rec.row()),
                        )),
                    }
                }
            } else if self.kind == JoinType::Left && probe_is_left {
                // Left row with no right match — emit NULL-extended
                invariant!(
                    prob_rec.is_positive(),
                    "replays should only include positive records"
                );
                ret.push(Record::Positive(self.generate_null(prob_rec.row())));
            }
        }
        Ok(ret.into())
    }

    // TODO: make non-allocating
    fn generate_null(&self, left: &[DfValue]) -> Vec<DfValue> {
        self.emit
            .iter()
            .map(|&(side, col)| {
                if side == Side::Left {
                    left[col].clone()
                } else {
                    DfValue::None
                }
            })
            .collect()
    }

    /// Returns `true` if the left row passes the `left_filter`, or if no filter is set.
    ///
    /// # Errors
    /// Propagates any evaluation error from the filter expression.
    fn left_filter_passes(&self, row: &[DfValue]) -> ReadySetResult<bool> {
        match &self.left_filter {
            Some(filter) => Ok(filter.eval(row)?.is_truthy()),
            None => Ok(true),
        }
    }

    /// Given a column index, check if it comes from the left or the right side of the join.
    ///
    /// # Arguments
    ///
    /// * `col` - the column index to resolve.
    ///
    /// # Returns
    ///
    /// A tuple of (left column index, right column index).
    fn resolve_col(&self, col: usize) -> (Option<usize>, Option<usize>) {
        let (side, pcol) = self.emit[col];

        if let Some((on_l, on_r)) = self
            .on
            .iter()
            // if the column comes from the left and is in the join, find the corresponding right
            // column
            .find(|(l, _)| side == Side::Left && *l == pcol)
            // otherwise, if the column comes from the right and is in the join, find the
            // corresponding left column
            .or_else(|| {
                self.on
                    .iter()
                    .find(|(_, r)| side == Side::Right && *r == pcol)
            })
        {
            // Join column comes from both parents
            (Some(*on_l), Some(*on_r))
        } else if side == Side::Left {
            (Some(pcol), None)
        } else {
            (None, Some(pcol))
        }
    }

    /// Given a list of column indices, resolve them into left and right key column indices.
    fn resolve_cols(&self, cols: &[usize]) -> (SmallVec<[usize; 8]>, SmallVec<[usize; 8]>) {
        let mut left_cols = SmallVec::<[usize; 8]>::new();
        let mut right_cols = SmallVec::<[usize; 8]>::new();
        for col in cols {
            let (left_idx, right_idx) = self.resolve_col(*col);
            if let Some(li) = left_idx {
                left_cols.push(li);
            } else if let Some(ri) = right_idx {
                right_cols.push(ri);
            }
        }
        (left_cols, right_cols)
    }

    /// Given a replay and the node this replay is from, determine the JoinExecutionMode.
    /// If we can trace all the replay columns to a single side of the join, we can execute a regular join. (The
    /// predicates come from this side and all we need to do is to lookup the other side based on the ON keys).
    /// If we cannot trace all the replay columns to a single side of the join, we are executing a straddled join.
    /// If the right side is fully materialized, we trigger just a single upquery and we can lookup the right side
    /// using the ON keys + the predicates we are missing on. Otherwise (rhs not fully materialized), we expect two
    /// upquery responses. We will buffer the first response and execute a hash join once the second response arrives.
    ///
    /// # Parameters
    /// - replay: The replay context.
    /// - from: The node this replay is from.
    ///
    /// # Returns
    /// - The JoinExecutionMode.
    fn execution_type_for_replay(
        &self,
        replay: &ReplayContext<'_>,
        from: LocalNodeIndex,
    ) -> JoinExecutionMode {
        match self.trace_replay_column_source(replay, from) {
            Ok(_) => JoinExecutionMode::RegularLookup,
            Err(_) => {
                if self.rhs_full_mat {
                    JoinExecutionMode::StraddledRegularLookup
                } else {
                    JoinExecutionMode::StraddledHashJoin
                }
            }
        }
    }

    /// Translate the replay column index into the columns indexes in the parent table.
    /// If any of the columns are generated, or if we have multiple columns and they come
    /// from different sides of the join (Straddled Joins), return an error.
    fn trace_replay_column_source(
        &self,
        replay: &ReplayContext<'_>,
        from: LocalNodeIndex,
    ) -> Result<Option<Vec<usize>>, ()> {
        replay
            .cols()
            .map(|cols| {
                cols.iter()
                    .map(|&col| -> Result<usize, ()> {
                        match self.emit[col] {
                            (Side::Left, l) if from == *self.left => return Ok(l),
                            (Side::Right, r) if from == *self.right => return Ok(r),
                            (Side::Left, l) => {
                                if let Some(r) =
                                    self.on.iter().find_map(
                                        |(on_l, r)| {
                                            if *on_l == l {
                                                Some(r)
                                            } else {
                                                None
                                            }
                                        },
                                    )
                                {
                                    // since we didn't hit the case above, we know that the
                                    // message
                                    // *isn't* from left.
                                    return Ok(*r);
                                }
                            }
                            (Side::Right, r) => {
                                if let Some(l) =
                                    self.on.iter().find_map(
                                        |(l, on_r)| {
                                            if *on_r == r {
                                                Some(l)
                                            } else {
                                                None
                                            }
                                        },
                                    )
                                {
                                    // same
                                    return Ok(*l);
                                }
                            }
                        }
                        Err(())
                    })
                    .collect()
            })
            .transpose()
    }

    /// Add a missing upquery for the other side of the join.
    pub fn add_missing_upquery(
        &mut self,
        key_cols: Vec<KeyComparison>,
        side: Side,
        missed_keys: ColumnMiss,
    ) {
        self.missing_upqueries
            .entry((key_cols, side))
            .and_modify(|entry| {
                // Ensure all existing entries have the same column indices as the new one
                debug_assert!(entry
                    .iter()
                    .all(|miss| { miss.column_indices == missed_keys.column_indices }));
                entry.push(missed_keys.clone());
            })
            .or_insert_with(|| vec![missed_keys.clone()]);
    }

    /// Execute a regular lookup for a join. This happens when we have predicates only on one side of the join.
    fn execute_regular_lookup(
        &mut self,
        replay: &ReplayContext<'_>,
        from: LocalNodeIndex,
        rs: Records,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        let from_left = from == *self.left;

        let other = if from_left { *self.right } else { *self.left };

        let replay_key_cols = self.trace_replay_column_source(replay, from);

        // On clause columns from this side (ts) and other side (os)
        let (on_cols_ts, on_cols_os): (Vec<usize>, Vec<usize>) = if from_left {
            self.on.iter().copied().unzip()
        } else {
            let (on_cols_os, on_cols_ts) = self.on.iter().copied().unzip();
            (on_cols_ts, on_cols_os)
        };
        let replay_key_cols =
            replay_key_cols.expect("replay columns must map through emit for regular lookup");
        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());

        let grouped_records = rs.into_iter().chunk_by(|rec| {
            on_cols_ts
                .iter()
                .map(|i| rec[*i].clone())
                .collect::<Vec<_>>()
        });

        let is_replay = replay_key_cols.is_some();
        // Only do a lookup into a weak index if we're processing regular updates,
        // not if we're processing a replay, since regular updates should represent
        // all rows that won't hit holes downstream but replays need to have *all*
        // rows.
        // Also use Strict mode if the other side is a Constant node, since Constant
        // nodes are fully materialized and don't support weak indices.
        let from_is_constant = nodes[from].borrow().is_constant();
        let other_is_constant = nodes[other].borrow().is_constant();

        let lookup_mode = if is_replay || from_is_constant || other_is_constant {
            LookupMode::Strict
        } else {
            LookupMode::Weak
        };

        for (join_key, group) in grouped_records.into_iter() {
            // [note: null-join-keys]
            // The semantics of NULL in SQL are tri-state - while obviously `1 = 1`, it is *not* the
            // case that `null = null`. Usually this is irrelevant for lookups into state since it's
            // impossible to upquery for null keys (since IS and IS NOT can't be parameterized,
            // syntactically), but we *do* have to have an extra case here in the case of join
            // lookups - two NULL join keys should *not* match each other in the semantics of the
            // join, even though they *would* match normally due to the semantics of the DfValue
            // type.
            let nulls = join_key.iter().any(|v| v.is_none());

            // The difference between a left join and an inner join, is that for the former we must
            // emit rows with nulls even if we later get no match in the other side.

            let mut new_right_count = None;

            if self.kind == JoinType::Left && !from_left {
                let rc = self.lookup(
                    *self.right,
                    &self.on_right(),
                    &PointKey::from(join_key.iter().cloned()),
                    nodes,
                    state,
                    lookup_mode,
                )?;

                match rc {
                    IngredientLookupResult::Records(rc) => {
                        if replay_key_cols.is_some() && !nulls {
                            lookups.push(Lookup {
                                on: *self.right,
                                cols: self.on_right(),
                                key: join_key
                                    .clone()
                                    .try_into()
                                    .map_err(|_| internal_err!("Empty join key"))?,
                            });
                        }

                        let rc = rc.count();
                        new_right_count = Some(rc);
                    }
                    IngredientLookupResult::Miss => {
                        // We got something from right, but that row's join key is not in
                        // right's state. This can happen in two cases:
                        //
                        // 1. Two partial indices on right (e.g., on column `a` = join key and
                        //    column `b` = non-join key). A replay for b=4 brings (a=1,b=4),
                        //    but a=1 is a hole in the [a] index. We should skip this record
                        //    so the system can fill a=1 first, then retry b=4.
                        //
                        // 2. The replay is for a non-join-key column (e.g., test_int) while
                        //    the right side is only indexed on that column, not the join key.
                        //    The join-key lookup misses because the join-key index was never
                        //    filled by this replay. In this case, the NULL emission/retraction
                        //    count is irrelevant — the replay is filling a downstream hole
                        //    from scratch — so we proceed without the count. (REA-6339)
                        if let Some(ref rkc) = replay_key_cols {
                            // Compare as sets: rkc is ordered by replay.cols()
                            // (downstream index), while on_cols_ts is ordered by
                            // self.on declaration.  For compound keys these can
                            // differ, so a plain slice comparison would give a
                            // false negative.
                            let replay_is_join_key = rkc.len() == on_cols_ts.len()
                                && rkc.iter().all(|c| on_cols_ts.contains(c));
                            if replay_is_join_key {
                                // Case 1: replay key IS the join key, skip and retry later.
                                continue;
                            }
                            // Case 2: replay key differs from join key, proceed without
                            // the right-side count (disables NULL emission/retraction).
                            // (REA-6339)
                        } else {
                            // Non-replay update: right-side join-key index is a hole,
                            // skip this record (picked up on a future replay).
                            continue;
                        }
                    }
                }
            }

            let mut other_lookup = match nulls {
                true => IngredientLookupResult::empty(),
                false => self.lookup(
                    other,
                    &on_cols_os,
                    &PointKey::from(join_key.iter().cloned()),
                    nodes,
                    state,
                    lookup_mode,
                )?,
            };

            let other_records = match other_lookup.take() {
                IngredientLookupResult::Records(recs) => recs,
                IngredientLookupResult::Miss => {
                    misses.extend(group.map(|record| {
                        Miss::builder()
                            .on(other)
                            .lookup_idx(on_cols_os.clone())
                            .lookup_key(on_cols_ts.clone())
                            .replay(replay)
                            .replay_key_cols(replay_key_cols.as_deref())
                            .record(record.into_row())
                            .build()
                    }));
                    continue;
                }
            };

            if is_replay && !nulls {
                lookups.push(Lookup {
                    on: other,
                    cols: on_cols_os.clone(),
                    key: join_key
                        .try_into()
                        .map_err(|_| internal_err!("Empty join key"))?,
                });
            }

            let other_rows = other_records.collect::<Result<Vec<_>, _>>()?;

            let mut rc_diff = 0isize;
            for r in group {
                let (mut row, positive) = r.extract();

                rc_diff += if positive { 1 } else { -1 };

                // For LEFT JOINs from the left side, check the left_filter.
                // If it fails, this row should be NULL-extended regardless of matches.
                let left_filter_passes = if from_left && self.kind == JoinType::Left {
                    self.left_filter_passes(&row)?
                } else {
                    true
                };

                if !left_filter_passes {
                    // ON-clause LHS predicate failed - emit NULL-extended row
                    ret.push((self.generate_null(&row), positive).into());
                } else if other_rows.is_empty() {
                    if self.kind == JoinType::Left && from_left {
                        // left join, got a thing from left, no rows in right == NULL
                        ret.push((self.generate_null(&row), positive).into());
                    }
                } else {
                    let last_other = other_rows.len() - 1;
                    for (idx, other) in other_rows.iter().enumerate() {
                        // When processing from the right side, check if the left row
                        // passes the filter
                        if !from_left && !self.left_filter_passes(other)? {
                            continue;
                        }

                        if self.emit_move_safe && idx == last_other {
                            // Move values from the owned row on the last iteration
                            // to avoid cloning "this side" columns.
                            ret.push(
                                (self.generate_row_move(&mut row, other, from_left), positive)
                                    .into(),
                            );
                        } else if from_left {
                            ret.push((self.generate_row(&row, other), positive).into());
                        } else {
                            ret.push((self.generate_row(other, &row), positive).into());
                        }
                    }
                }
            }

            // For a left join with updates from the right side, we also have to emit/delete NULL
            // rows if row count changed to/from zero
            if let Some(new_rc) = new_right_count {
                let old_rc = new_rc as isize - rc_diff;
                if new_rc == 0 && old_rc != 0 {
                    for other in other_rows.iter() {
                        // Skip left rows where the left_filter currently fails — while the
                        // filter does not pass, these rows remain NULL-extended and should
                        // not be affected by right-side match count changes.
                        if !self.left_filter_passes(other)? {
                            continue;
                        }
                        ret.push((self.generate_null(other), true).into());
                    }
                } else if new_rc != 0 && old_rc == 0 {
                    for other in other_rows.iter() {
                        if !self.left_filter_passes(other)? {
                            continue;
                        }
                        ret.push((self.generate_null(other), false).into());
                    }
                }
            }
        }

        Ok(ProcessingResult {
            results: ret.into(),
            lookups,
            misses,
        })
    }

    /// Based on replay, check if we are ready to execute a hash join.
    /// For hash join, we originally trigger upquery for both sides of the join.
    /// One side will arrive and we will buffer it. Once the other side arrives, we can execute the hash join.
    fn execute_or_buffer_straddled_hash_join(
        &mut self,
        replay: &ReplayContext<'_>,
        from: LocalNodeIndex,
        rs: Records,
    ) -> ReadySetResult<ProcessingResult> {
        let cols = replay.cols().unwrap();
        let is_left = from == *self.left;
        if let Some(other) = self.generated_column_buffer.remove(&(
            cols.to_vec(),
            if is_left { Side::Right } else { Side::Left },
        )) {
            // we have both sides now
            let (left, right) = if is_left { (rs, other) } else { (other, rs) };
            let ret = self.hash_join(left, right)?;
            Ok(ProcessingResult {
                results: ret,
                ..Default::default()
            })
        } else {
            // store the records for when we get the other upquery response
            self.generated_column_buffer.insert(
                (
                    cols.to_vec(),
                    if is_left { Side::Left } else { Side::Right },
                ),
                rs,
            );
            Ok(Default::default())
        }
    }

    /// Given a node index where the replay is coming from, return the side of
    /// the join that the replay is coming from, the other side of the join as
    /// node index and a pair of vectors of ON clause columns from this side
    /// (ts) and other side (os).
    fn get_side_and_on_cols(
        &self,
        from: LocalNodeIndex,
    ) -> (Side, LocalNodeIndex, (Vec<usize>, Vec<usize>)) {
        if from == *self.left {
            (Side::Left, *self.right, self.on.iter().copied().unzip())
        } else {
            let (on_cols_os, on_cols_ts): (Vec<usize>, Vec<usize>) =
                self.on.iter().copied().unzip();
            (Side::Right, *self.left, (on_cols_ts, on_cols_os))
        }
    }

    /// Execute a straddled lookup. One side of the join has arrived. We need to figure out which records
    /// from the other side to lookup. See [the docs section on straddled joins][straddled-joins] for more
    /// information about how we handle the execution.
    ///
    /// [straddled-joins]: http://docs/dataflow/replay_paths.html#straddled-joins
    fn execute_straddled_regular_lookup(
        &mut self,
        replay: &ReplayContext<'_>,
        from: LocalNodeIndex,
        rs: Records,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }
        let mut lookups = Vec::with_capacity(rs.len());
        let mut results = Records::from(Vec::<Record>::with_capacity(rs.len() * 2));
        let mut os_key_buffer = Vec::with_capacity(8);
        let (side, os_node, (on_cols_idx_ts, on_cols_idx_os)) = self.get_side_and_on_cols(from);

        // 1. From replay.cols which columns come from this side of the join
        let (left_key_cols, right_key_cols) = self.resolve_cols(replay.cols().unwrap());

        // 2. Pre-group records by replay key and join key
        let grouped_records = Self::group_records_by_replay_key(
            &rs,
            &left_key_cols,
            &right_key_cols,
            &on_cols_idx_ts,
            side,
        );

        // 3. Process each replay key using pre-grouped records.
        // TODO: One optimization is to key group_records by replay key and join key, so we don't need to skip the ones
        //that don't match the current key.
        let replay_keys = replay.keys().unwrap();
        for key in replay_keys {
            let other_predicate = self
                .missing_upqueries
                .remove(&(vec![key.clone()], side))
                .ok_or_else(|| internal_err!("No missing keys found for key {:?}", key))?;

            let mut os_key_col_idx = SmallVec::<[usize; 8]>::from_slice(&on_cols_idx_os);

            // We have an invariant that the columns we are missing on are the same for all
            // entries in the missing_upqueries map. Safe to extract columns from first entry
            os_key_col_idx.extend_from_slice(&other_predicate[0].column_indices);

            // 4. For each record, we need to build the new lookup key, based on the record ON column + the original predicate from the other side of the join.
            // Process pre-grouped records directly to avoid extra memory allocation
            for ((group_replay_key, join_key), group_records) in &grouped_records {
                if !key.contains(group_replay_key) {
                    continue;
                }
                // Process each predicate key
                for missing_key in &other_predicate {
                    os_key_buffer.clear();
                    os_key_buffer.extend_from_slice(join_key);

                    // 5. Execute the lookup for the new key & generate the result set.
                    let other_side_records = self.lookup_other_side_records(
                        missing_key,
                        &mut os_key_buffer,
                        &mut lookups,
                        os_node,
                        &os_key_col_idx,
                        nodes,
                        state,
                        &on_cols_idx_os,
                    )?;

                    if self.kind == JoinType::Left && side == Side::Left {
                        // Records come from left side; check left_filter on each left row
                        for row in group_records {
                            if !self.left_filter_passes(row.row())? {
                                results.push((self.generate_null(row.row()), true).into());
                                continue;
                            }
                            if other_side_records.is_empty() {
                                results.push((self.generate_null(row.row()), true).into());
                            } else {
                                for other_row in &other_side_records {
                                    results.push(
                                        (self.generate_row(row.row(), other_row.row()), true)
                                            .into(),
                                    );
                                }
                            }
                        }
                    } else if self.kind == JoinType::Left && side == Side::Right {
                        // Records come from right side; other_side_records are left rows
                        for other_row in &other_side_records {
                            if !self.left_filter_passes(other_row.row())? {
                                // Left row fails filter — skip matched rows
                                continue;
                            }
                            for row in group_records {
                                results.push(
                                    (self.generate_row(other_row.row(), row.row()), true).into(),
                                );
                            }
                        }
                    } else {
                        for other_row in &other_side_records {
                            for row in group_records {
                                if side == Side::Left {
                                    results.push(
                                        (self.generate_row(row.row(), other_row.row()), true)
                                            .into(),
                                    );
                                } else {
                                    results.push(
                                        (self.generate_row(other_row.row(), row.row()), true)
                                            .into(),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(ProcessingResult {
            results,
            lookups,
            misses: Vec::new(),
        })
    }

    /// Group records by replay key and join key.
    ///
    /// # Arguments
    /// * `rs` - A reference to the records to group.
    /// * `left_key_cols` - The column indices from replay that come from the left side of the join.
    /// * `right_key_cols` - The column indices from replay that come from the right side of the join.
    /// * `on_cols_ts` - The column indices from the ON clause that come from the this side of the join.
    /// * `side` - The side of the join that the records are coming from.
    ///
    /// # Returns
    /// A map of (replay key, join key) to a reference list of records.
    fn group_records_by_replay_key<'a>(
        rs: &'a Records,
        left_key_cols: &[usize],
        right_key_cols: &[usize],
        on_cols_ts: &[usize],
        side: Side,
    ) -> HashMap<(Vec<DfValue>, Vec<DfValue>), Vec<&'a Record>> {
        let mut grouped_records: HashMap<(Vec<DfValue>, Vec<DfValue>), Vec<&'a Record>> =
            HashMap::new();

        for rec in rs {
            // Extract replay key columns
            let replay_key_values: Vec<DfValue> = if side == Side::Left {
                left_key_cols.iter().map(|i| rec[*i].clone()).collect()
            } else {
                right_key_cols.iter().map(|i| rec[*i].clone()).collect()
            };

            // Extract join key columns
            let join_key_values: Vec<DfValue> =
                on_cols_ts.iter().map(|i| rec[*i].clone()).collect();

            grouped_records
                .entry((replay_key_values, join_key_values))
                .or_default()
                .push(rec);
        }
        grouped_records
    }

    #[allow(clippy::too_many_arguments)]
    fn lookup_other_side_records(
        &self,
        missing_key: &ColumnMiss,
        os_key_buffer: &mut Vec<DfValue>,
        lookups: &mut Vec<Lookup>,
        os_node: LocalNodeIndex,
        os_key_cols: &[usize],
        nodes: &DomainNodes,
        state: &StateMap,
        on_cols_os: &[usize],
    ) -> ReadySetResult<Records> {
        match missing_key.missed_keys.first() {
            KeyComparison::Equal(_) => {
                // ICP: Since join keys are always equal, we can perform index condition pushdown and lookup on disk ON and predicate keys.
                os_key_buffer.extend(missing_key.missed_keys.iter().flat_map(|k| match k {
                    KeyComparison::Equal(values) => values.iter().cloned(),
                    KeyComparison::Range(_) => {
                        unreachable!("all missed keys should be equal")
                    }
                }));
                lookups.push(Lookup {
                    on: os_node,
                    cols: os_key_cols.to_vec(),
                    key: KeyComparison::from(Vec1::try_from(os_key_buffer.clone()).unwrap()),
                });
                self.lookup_key_from_cache(
                    os_key_buffer,
                    os_key_cols,
                    os_node,
                    nodes,
                    state,
                    LookupMode::Strict, // SJ will always be strict
                )
            }
            KeyComparison::Range(_) => {
                lookups.push(Lookup {
                    on: os_node,
                    cols: os_key_cols.to_vec(),
                    key: KeyComparison::from(Vec1::try_from(os_key_buffer.clone()).unwrap()),
                });
                let records = self.lookup_key_from_cache(
                    os_key_buffer,
                    on_cols_os,
                    os_node,
                    nodes,
                    state,
                    LookupMode::Strict, // SJ will always be strict
                )?;
                // Apply filter for range queries
                Ok(records
                    .iter()
                    .filter(|r| {
                        missing_key.missed_keys.iter().all(|k| {
                            let record_values: SmallVec<[DfValue; 4]> = missing_key
                                .column_indices
                                .iter()
                                .map(|i| r[*i].clone())
                                .collect();

                            match k {
                                KeyComparison::Equal(_) => {
                                    unreachable!("Equality not supported")
                                }
                                KeyComparison::Range(_) => k.contains(&record_values),
                            }
                        })
                    })
                    .cloned()
                    .collect::<Records>())
            }
        }
    }

    /// Lookup a key in the materialized state.
    fn lookup_key_from_cache(
        &self,
        os_key: &[DfValue],
        os_key_cols: &[usize],
        os_node: LocalNodeIndex,
        nodes: &DomainNodes,
        state: &StateMap,
        lookup_mode: LookupMode,
    ) -> ReadySetResult<Records> {
        let lookup_key = PointKey::from(os_key.to_vec());
        let lookup = self.lookup(os_node, os_key_cols, &lookup_key, nodes, state, lookup_mode)?;
        match lookup {
            IngredientLookupResult::Records(records) => records
                .map(|r| r.map(|row| Record::Positive(row.to_vec())))
                .collect::<Result<Records, ReadySetError>>(),
            IngredientLookupResult::Miss => {
                unreachable!("Straddled lookup should not miss");
            }
        }
    }
    /// Returns true if the right side of the join is fully materialized.
    pub fn is_rhs_full_mat(&self) -> bool {
        self.rhs_full_mat
    }

    /// Returns the side of the join that is more efficient to trigger an upquery in case of a straddled join.
    /// At the moment, we always trigger the upquery from the left side.
    /// Once we have some planner logic, based on stats, we can adjust side to dynamically
    /// choose which side to query first.
    pub fn side_to_trigger_upquery(&self) -> Side {
        Side::Left
    }
}

impl Ingredient for Join {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.left.as_global(), self.right.as_global()]
    }

    fn is_join(&self) -> bool {
        true
    }

    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        Some(Some(self.left.as_global()).into_iter().collect())
    }

    fn post_deserialize(&mut self) {
        self.recompute_emit_metadata();
    }

    impl_replace_sibling!(left, right);

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.left.remap(remap);
        self.right.remap(remap);
    }

    #[allow(clippy::cognitive_complexity)]
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        replay: &ReplayContext<'_>,
        nodes: &DomainNodes,
        state: &StateMap,
        _auxiliary_node_states: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        let join_execution_mode = self.execution_type_for_replay(replay, from);
        debug!(
            "on_input join_execution_mode: {:?} for from: {:?} for replay: {:?}, records: {:?}, rhs_full_mat: {:?}",
            join_execution_mode, from, replay, rs.len(), self.rhs_full_mat
        );
        match join_execution_mode {
            JoinExecutionMode::RegularLookup => {
                self.execute_regular_lookup(replay, from, rs, nodes, state)
            }
            JoinExecutionMode::StraddledHashJoin => {
                self.execute_or_buffer_straddled_hash_join(replay, from, rs)
            }
            JoinExecutionMode::StraddledRegularLookup => {
                self.execute_straddled_regular_lookup(replay, from, rs, nodes, state)
            }
        }
    }

    fn suggest_indexes(&self, _this: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        // Replays might have happened through our parents into keys *other* than the join key, and
        // we need to find those rows when looking up values to perform the join as part of forward
        // processing of normal writes - so we use a weak index here to avoid dropping writes in
        // that case.
        HashMap::from([
            (
                self.left.as_global(),
                LookupIndex::Weak(Index::hash_map(self.on_left())),
            ),
            (
                self.right.as_global(),
                LookupIndex::Weak(Index::hash_map(self.on_right())),
            ),
        ])
    }

    fn description(&self) -> String {
        let emit = self
            .emit
            .iter()
            .map(|&(side, col)| {
                let src = match side {
                    Side::Left => self.left,
                    Side::Right => self.right,
                };
                format!("{}:{}", src.as_global().index(), col)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let op = match self.kind {
            JoinType::Left => "⋉",
            JoinType::Inner => "⋈",
        };

        format!(
            "[{}] {}:({}) {} {}:({})",
            emit,
            self.left.as_global().index(),
            self.on_left().into_iter().map(|i| i.to_string()).join(", "),
            op,
            self.right.as_global().index(),
            self.on_right()
                .into_iter()
                .map(|i| i.to_string())
                .join(", ")
        )
    }

    /// Called for joins that are partial on columns that are sourced from both parents
    ///
    /// We receive an upquery with a set of (potentially multiple) keys, and need to split each of
    /// those into one upquery to each parent with the same total number of keys each.
    fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        // reminder: this function *only* gets called for column indices that are sourced from
        // both parents

        // First, which side does each column come from?
        let mut left_cols = vec![];
        let mut right_cols = vec![];
        let mut col_sides = vec![];
        for col in miss.column_indices {
            let (left_idx, right_idx) = self.resolve_col(col);
            if let Some(li) = left_idx {
                left_cols.push(li);
                col_sides.push(Side::Left);
            } else if let Some(ri) = right_idx {
                right_cols.push(ri);
                col_sides.push(Side::Right);
            } else {
                internal!("could not resolve col {} in join upquery", col);
            }
        }

        // Now, split each of the keys into an upquery to each of the left and the right
        let mut left_keys = Vec::with_capacity(miss.missed_keys.len());
        let mut right_keys = Vec::with_capacity(miss.missed_keys.len());
        for key in miss.missed_keys {
            let (left_key, right_key) = match key {
                KeyComparison::Equal(key) => {
                    let mut left_key = Vec::with_capacity(left_cols.len());
                    let mut right_key = Vec::with_capacity(right_cols.len());
                    for (value, side) in key.into_iter().zip(&col_sides) {
                        match side {
                            Side::Left => left_key.push(value),
                            Side::Right => right_key.push(value),
                        }
                    }

                    // If either of these is empty, that means the columns weren't actually
                    // straddling both parents - which is an invariant of this function!
                    (
                        left_key.try_into().map_err(|_| {
                            internal_err!("Join handle_upquery passed a non-straddled join key")
                        })?,
                        right_key.try_into().map_err(|_| {
                            internal_err!("Join handle_upquery passed a non-straddled join key")
                        })?,
                    )
                }
                KeyComparison::Range((lower, upper)) => {
                    let mut left_lower =
                        lower.as_ref().map(|_| Vec::with_capacity(left_cols.len()));
                    let mut right_lower =
                        lower.as_ref().map(|_| Vec::with_capacity(right_cols.len()));
                    let mut left_upper =
                        upper.as_ref().map(|_| Vec::with_capacity(left_cols.len()));
                    let mut right_upper =
                        upper.as_ref().map(|_| Vec::with_capacity(right_cols.len()));

                    if let Some(lower_endpoint) = into_bound_endpoint(lower.into()) {
                        for (value, side) in lower_endpoint.into_iter().zip(&col_sides) {
                            match side {
                                Side::Left => left_lower.inner_mut(),
                                Side::Right => right_lower.inner_mut(),
                            }
                            .push(value);
                        }
                    }

                    if let Some(upper_endpoint) = into_bound_endpoint(upper.into()) {
                        for (value, side) in upper_endpoint.into_iter().zip(&col_sides) {
                            match side {
                                Side::Left => left_upper.inner_mut(),
                                Side::Right => right_upper.inner_mut(),
                            }
                            .push(value);
                        }
                    }

                    #[allow(clippy::unwrap_used)]
                    // If any of these is empty, that means the columns weren't actually straddling
                    // both parents - which is an invariant of this function!
                    (
                        KeyComparison::Range((
                            left_lower.map(|k| k.try_into().unwrap()),
                            left_upper.map(|k| k.try_into().unwrap()),
                        )),
                        KeyComparison::Range((
                            right_lower.map(|k| k.try_into().unwrap()),
                            right_upper.map(|k| k.try_into().unwrap()),
                        )),
                    )
                }
            };

            left_keys.push(left_key);
            right_keys.push(right_key);
        }

        Ok(vec![
            ColumnMiss {
                node: *self.left,
                column_indices: left_cols,
                missed_keys: Vec1::try_from(left_keys).unwrap(),
            },
            ColumnMiss {
                node: *self.right,
                column_indices: right_cols,
                missed_keys: Vec1::try_from(right_keys).unwrap(),
            },
        ])
    }

    /// Based on a list of input column indices, get a list of column outputs required to fulfill the join
    ///
    /// # Arguments
    ///
    /// * `cols` - A list of column indices to get the column source for. Based on the join PREDICATES
    ///
    /// # Returns
    ///
    /// A `ColumnSource` object that represents the column source for the given column indices
    /// In case of a straddled join, we return the mapping from cols (input) tracing if they came
    /// from left or right, and also the ON columns so we can do a single lookup with PREDICATE + ON columns
    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        // NOTE: This function relies pretty heavily on the fact that upqueries for NULLs are not
        // possible. If they were possible, you could return incorrect results, because
        //   SELECT * FROM a LEFT JOIN b where b.col IS NULL;
        // means "get me all rows in a not in b" (i.e. a \ b), not "get me rows in b where col
        // is NULL" (which is what this function would do).

        // column indices in the left parent
        let mut left_cols = vec![];
        // column indices in the right parent
        let mut right_cols = vec![];
        for (left_idx, right_idx) in cols.iter().map(|&col| self.resolve_col(col)) {
            left_cols.push(left_idx);
            right_cols.push(right_idx);
        }
        if left_cols.iter().all(|x| x.is_some()) {
            // the left parent has all the columns in `cols`
            // we can just 1:1 index the left parent and do the joining bits on replay
            ColumnSource::exact_copy(
                self.left.as_global(),
                left_cols.into_iter().flatten().collect::<Vec<_>>(),
            )
        } else if right_cols.iter().all(|x| x.is_some()) {
            // same for right parent
            ColumnSource::exact_copy(
                self.right.as_global(),
                right_cols.into_iter().flatten().collect::<Vec<_>>(),
            )
        } else {
            let right_cols = right_cols
                .into_iter()
                .enumerate()
                // don't include columns in the right that also come from the left
                .filter_map(|(idx, col)| col.filter(|_| left_cols[idx].is_none()))
                .collect::<Vec<_>>();
            let left_cols = left_cols.into_iter().flatten().collect::<Vec<_>>();
            if self.is_rhs_full_mat() {
                let mut join_right_cols = self.on.iter().map(|(_l, r)| *r).collect::<Vec<_>>();
                join_right_cols.extend(right_cols.iter().copied());
                ColumnSource::GeneratedFromColumns(vec1![
                    ColumnRef {
                        node: self.left.as_global(),
                        columns: left_cols
                    },
                    ColumnRef {
                        node: self.right.as_global(),
                        columns: join_right_cols
                    }
                ])
            } else {
                ColumnSource::GeneratedFromColumns(vec1![
                    ColumnRef {
                        node: self.left.as_global(),
                        columns: left_cols
                    },
                    ColumnRef {
                        node: self.right.as_global(),
                        columns: right_cols
                    },
                ])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1"]);

        let j = Join::new(
            l.as_global(),
            r.as_global(),
            JoinType::Left,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            true,
            None,
        );

        g.set_op("join", &["j0", "j1", "j2"], j, false);
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (j, l, r) = setup();
        assert_eq!(
            j.node().description(),
            format!("[{l}:0, {l}:1, {r}:1] {l}:(0) ⋉ {r}:(0)")
        );
    }

    #[test]
    fn it_works() {
        let (mut j, l, r) = setup();
        let l_a1 = vec![1.into(), "a".into()];
        let l_b2 = vec![2.into(), "b".into()];
        let l_c3 = vec![3.into(), "c".into()];

        let r_x1 = vec![1.into(), "x".into()];
        let r_y1 = vec![1.into(), "y".into()];
        let r_z2 = vec![2.into(), "z".into()];
        let r_w3 = vec![3.into(), "w".into()];
        let r_v4 = vec![4.into(), "w".into()];

        let r_nop: Vec<Record> = vec![
            (vec![3.into(), "w".into()], false).into(),
            (vec![3.into(), "w".into()], true).into(),
        ];

        j.seed(r, r_x1.clone());
        j.seed(r, r_y1.clone());
        j.seed(r, r_z2.clone());

        j.one_row(r, r_x1, false);
        j.one_row(r, r_y1, false);
        j.one_row(r, r_z2, false);

        // forward c3 from left; should produce [c3 + None] since no records in right are 3
        let null = vec![(vec![3.into(), "c".into(), DfValue::None], true)].into();
        j.seed(l, l_c3.clone());
        let rs = j.one_row(l, l_c3.clone(), false);
        assert_eq!(rs, null);

        // doing it again should produce the same result
        j.seed(l, l_c3.clone());
        let rs = j.one_row(l, l_c3, false);
        assert_eq!(rs, null);

        // record from the right should revoke the nulls and replace them with full rows
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3.clone(), false);
        assert_eq!(
            rs,
            vec![
                (vec![3.into(), "c".into(), "w".into()], true),
                (vec![3.into(), "c".into(), "w".into()], true),
                (vec![3.into(), "c".into(), DfValue::None], false),
                (vec![3.into(), "c".into(), DfValue::None], false),
            ]
            .into()
        );

        // Negative followed by positive should not trigger nulls.
        // TODO: it shouldn't trigger any updates at all...
        let rs = j.one(r, r_nop, false);
        assert_eq!(
            rs,
            vec![
                (vec![3.into(), "c".into(), "w".into()], false),
                (vec![3.into(), "c".into(), "w".into()], false),
                (vec![3.into(), "c".into(), "w".into()], true),
                (vec![3.into(), "c".into(), "w".into()], true),
            ]
            .into()
        );

        // forward from left with single matching record on right
        j.seed(l, l_b2.clone());
        let rs = j.one_row(l, l_b2, false);
        assert_eq!(
            rs,
            vec![(vec![2.into(), "b".into(), "z".into()], true)].into()
        );

        // forward from left with two matching records on right
        j.seed(l, l_a1.clone());
        let rs = j.one_row(l, l_a1, false);
        assert_eq!(rs.len(), 2);
        assert!(rs.has_positive(&[1.into(), "a".into(), "x".into()][..]));
        assert!(rs.has_positive(&[1.into(), "a".into(), "y".into()][..]));

        // forward from right with two matching records on left (and one more on right)
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3, false);
        assert_eq!(
            rs,
            vec![
                (vec![3.into(), "c".into(), "w".into()], true),
                (vec![3.into(), "c".into(), "w".into()], true),
            ]
            .into()
        );

        // unmatched forward from right should have no effect
        j.seed(r, r_v4.clone());
        let rs = j.one_row(r, r_v4, false);
        assert_eq!(rs.len(), 0);
    }

    #[test]
    fn nulls_from_left() {
        let (mut j, l, r) = setup();

        let r_1x = vec![1.into(), "x".into()];
        j.seed(r, r_1x.clone());
        j.one_row(r, r_1x, false);

        let r_nullx = vec![DfValue::None, "y".into()];
        j.seed(r, r_nullx.clone());
        j.one_row(r, r_nullx, false);

        let l_nulla = vec![DfValue::None, "a".into()];

        j.seed(l, l_nulla.clone());
        let rs = j.one_row(l, l_nulla, false);
        assert_eq!(
            rs,
            vec![(vec![DfValue::None, "a".into(), DfValue::None], true)].into()
        );
    }

    #[test]
    fn nulls_from_right() {
        let (mut j, l, r) = setup();

        let l_nulla = vec![DfValue::None, "a".into()];
        j.seed(l, l_nulla.clone());
        j.one_row(l, l_nulla, false);

        let r_nullx = vec![DfValue::None, "y".into()];
        j.seed(r, r_nullx.clone());
        let rs = j.one_row(r, r_nullx, false);
        assert_eq!(rs, Records::default());
    }

    #[test]
    fn it_suggests_indices() {
        let me = 2.into();
        let (g, l, r) = setup();
        let expected = HashMap::from([
            (l.as_global(), LookupIndex::Weak(Index::hash_map(vec![0]))), // join column for left
            (r.as_global(), LookupIndex::Weak(Index::hash_map(vec![0]))), // join column for right
        ]);
        assert_eq!(g.node().suggest_indexes(me), expected);
    }

    #[test]
    fn parent_join_columns() {
        let (g, l, _) = setup();
        let res = g.node().parent_columns(0);
        assert_eq!(res, vec![(l.as_global(), Some(0))]);
    }

    mod handle_upquery {
        use readyset_data::{Bound, IntoBoundedRange};

        use super::*;

        #[test]
        fn compound_key() {
            let (j, l, r) = setup();
            let node = j.node().local_addr();
            let res = j
                .node_mut()
                .handle_upquery(ColumnMiss {
                    node,
                    column_indices: vec![0, 1, 2],
                    missed_keys: vec1![
                        vec1![DfValue::from(1), DfValue::from(2), DfValue::from(3)].into()
                    ],
                })
                .unwrap();

            let left_miss = res.iter().find(|miss| miss.node == *l).unwrap();
            let right_miss = res.iter().find(|miss| miss.node == *r).unwrap();

            assert_eq!(left_miss.column_indices, vec![0, 1]);
            assert_eq!(right_miss.column_indices, vec![1]);

            assert_eq!(
                left_miss.missed_keys,
                vec1![vec1![DfValue::from(1), DfValue::from(2)].into()]
            );
            assert_eq!(
                right_miss.missed_keys,
                vec1![vec1![DfValue::from(3)].into()]
            );
        }

        #[test]
        fn multiple_compound_keys() {
            let (j, l, r) = setup();
            let node = j.node().local_addr();
            let res = j
                .node_mut()
                .handle_upquery(ColumnMiss {
                    node,
                    column_indices: vec![0, 1, 2],
                    missed_keys: vec1![
                        vec1![DfValue::from(1), DfValue::from(2), DfValue::from(3)].into(),
                        vec1![DfValue::from(4), DfValue::from(5), DfValue::from(6)].into()
                    ],
                })
                .unwrap();

            let left_miss = res.iter().find(|miss| miss.node == *l).unwrap();
            let right_miss = res.iter().find(|miss| miss.node == *r).unwrap();

            assert_eq!(left_miss.column_indices, vec![0, 1]);
            assert_eq!(right_miss.column_indices, vec![1]);

            assert_eq!(
                left_miss.missed_keys,
                vec1![
                    vec1![DfValue::from(1), DfValue::from(2)].into(),
                    vec1![DfValue::from(4), DfValue::from(5)].into()
                ]
            );
            assert_eq!(
                right_miss.missed_keys,
                vec1![
                    vec1![DfValue::from(3)].into(),
                    vec1![DfValue::from(6)].into()
                ]
            );
        }

        #[test]
        fn range_key_double_ended() {
            let (j, l, r) = setup();
            let node = j.node().local_addr();
            let res = j
                .node_mut()
                .handle_upquery(ColumnMiss {
                    node,
                    column_indices: vec![0, 1, 2],
                    missed_keys: vec1![KeyComparison::Range((
                        Bound::Included(vec1![
                            DfValue::from(1),
                            DfValue::from(2),
                            DfValue::from(3)
                        ]),
                        Bound::Excluded(vec1![
                            DfValue::from(4),
                            DfValue::from(5),
                            DfValue::from(6)
                        ])
                    ))],
                })
                .unwrap();

            let left_miss = res.iter().find(|miss| miss.node == *l).unwrap();
            let right_miss = res.iter().find(|miss| miss.node == *r).unwrap();

            assert_eq!(left_miss.column_indices, vec![0, 1]);
            assert_eq!(right_miss.column_indices, vec![1]);

            assert_eq!(
                left_miss.missed_keys,
                vec1![KeyComparison::Range((
                    Bound::Included(vec1![DfValue::from(1), DfValue::from(2)]),
                    Bound::Excluded(vec1![DfValue::from(4), DfValue::from(5)])
                ))]
            );
            assert_eq!(
                right_miss.missed_keys,
                vec1![KeyComparison::Range((
                    Bound::Included(vec1![DfValue::from(3)]),
                    Bound::Excluded(vec1![DfValue::from(6)])
                ))]
            );
        }

        #[test]
        fn range_key_one_side_unbounded() {
            let (j, l, r) = setup();
            let node = j.node().local_addr();
            let res = j
                .node_mut()
                .handle_upquery(ColumnMiss {
                    node,
                    column_indices: vec![0, 1, 2],
                    missed_keys: vec1![KeyComparison::Range(
                        vec1![DfValue::from(1), DfValue::from(2), DfValue::from(3)]
                            .range_from_inclusive()
                    )],
                })
                .unwrap();

            let left_miss = res.iter().find(|miss| miss.node == *l).unwrap();
            let right_miss = res.iter().find(|miss| miss.node == *r).unwrap();

            assert_eq!(left_miss.column_indices, vec![0, 1]);
            assert_eq!(right_miss.column_indices, vec![1]);

            assert_eq!(
                left_miss.missed_keys,
                vec1![KeyComparison::Range(
                    vec1![DfValue::from(1), DfValue::from(2)].range_from_inclusive()
                )]
            );
            assert_eq!(
                right_miss.missed_keys,
                vec1![KeyComparison::Range(
                    vec1![DfValue::from(3)].range_from_inclusive()
                )]
            );
        }
    }

    /// REA-6339: When a LEFT JOIN replay is keyed on a non-join-key column
    /// (e.g. column 2 = Right.r1) and the right-side join-key lookup misses
    /// (because the partial index on the join key has a hole for that value),
    /// the record must NOT be dropped. The old code treated this as Case 1
    /// (replay-key == join-key) and issued `continue`, discarding the row.
    #[test]
    fn left_join_replay_non_join_key_miss_proceeds() {
        use std::collections::HashSet;

        use dataflow_state::MaterializedNodeState;
        use readyset_client::KeyComparison;

        // Standard left-join setup:
        //   left  [l0, l1]       right [r0, r1]
        //   join on (l0 = r0)
        //   emit  [L0, L1, R1]  →  output columns j0, j1, j2
        let (mut g, l, r) = setup();

        // ---- make right's join-key index PARTIAL ----
        // Replace right's state so that column-0 (the join key) is a partial
        // index with tag 0. Keys that haven't been explicitly filled are holes.
        let tag = Tag::new(0);
        let mut partial_state = MemoryState::default();
        partial_state.add_index(Index::hash_map(vec![0]), Some(vec![tag]));
        // Also need the weak index that suggest_indexes added, so the
        // non-replay weak-lookup path still works.
        partial_state.add_weak_index(Index::hash_map(vec![0]));
        g.states
            .insert(*r, MaterializedNodeState::Memory(partial_state));

        // ---- seed left (non-partial) with a row whose join key = 5 ----
        g.seed(l, vec![5.into(), "left_val".into()]);

        // ---- build the partial replay context ----
        // Replay key is emitted column 2, which maps to Right.r1.
        // trace_replay_column_source will return [1] (right col 1),
        // which differs from on_cols_ts = [0] (right col 0).
        let replay_key: KeyComparison = vec1!["rval".into()].into();
        let replay_keys = HashSet::from([replay_key]);
        let replay_ctx = ReplayContext::Partial {
            key_cols: &[2], // emitted column 2 = Right.r1
            keys: &replay_keys,
            requesting_shard: 0,
            requesting_replica: 0,
            tag,
            unishard: true,
        };

        // ---- send a right-side record during the replay ----
        // join-key = 5, r1 = "rval". The partial index on right col 0
        // does NOT have key=5 filled, so the self-lookup at line 482
        // returns Miss. With the fix (Case 2), we proceed anyway.
        let right_record: Record = vec![5.into(), "rval".into()].into();
        let res = g.input_raw(r, vec![right_record], replay_ctx, false);

        // The Join operator returns Regular (not ReplayPiece) — replay
        // buffering happens at a higher layer. Verify the rows are present.
        match res {
            RawProcessingResult::Regular(ref pr) => {
                assert!(
                    !pr.results.is_empty(),
                    "replay records should not be dropped when replay key != join key"
                );
                assert!(
                    pr.results
                        .has_positive(&[5.into(), "left_val".into(), "rval".into()][..]),
                    "expected joined row (5, left_val, rval), got: {:?}",
                    pr.results
                );
            }
            other => panic!("expected Regular processing result, got: {:?}", other),
        }
    }

    mod compound_keys {
        use super::*;

        pub(super) fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
            let mut g = ops::test::MockGraph::new();
            let l = g.add_base("left", &["l0", "l1", "l2"]);
            let r = g.add_base("right", &["r0", "r1", "r2"]);

            let j = Join::new(
                l.as_global(),
                r.as_global(),
                JoinType::Left,
                vec![(0, 0), (1, 1)],
                vec![
                    (Side::Left, 0),
                    (Side::Left, 1),
                    (Side::Left, 2),
                    (Side::Right, 2),
                ],
                true,
                None,
            );

            g.set_op("join", &["j0", "j1", "j2", "j3"], j, false);
            (g, l, r)
        }

        #[test]
        fn left_join_null() {
            let (mut j, l, r) = setup();

            // forward row from left; should produce [row + None] since no records in right match
            j.seed(l, vec![3.into(), 4.into(), "c".into()]);
            let rs = j.one_row(l, vec![3.into(), 4.into(), "c".into()], false);
            assert_eq!(
                rs,
                vec![(vec![3.into(), 4.into(), "c".into(), DfValue::None], true)].into()
            );

            // Both of the keys have to match to give us a match
            j.seed(r, vec![3.into(), 3.into(), "w".into()]);
            assert!(j
                .one_row(r, vec![3.into(), 3.into(), "w".into()], false)
                .is_empty());

            // Once we get a match, we should revoke the nulls and replace it with a full row
            j.seed(r, vec![3.into(), 4.into(), "w".into()]);
            let rs = j.one_row(r, vec![3.into(), 4.into(), "w".into()], false);
            assert_eq!(
                rs,
                vec![
                    (vec![3.into(), 4.into(), "c".into(), "w".into()], true),
                    (vec![3.into(), 4.into(), "c".into(), DfValue::None], false)
                ]
                .into()
            );
        }

        #[test]
        fn lookup_matching() {
            let (mut j, l, r) = setup();

            j.seed(r, vec![1.into(), 2.into(), "x".into()]);
            j.seed(r, vec![2.into(), 2.into(), "y".into()]);
            j.seed(r, vec![2.into(), 2.into(), "z".into()]);

            j.one_row(r, vec![1.into(), 2.into(), "x".into()], false);
            j.one_row(r, vec![2.into(), 2.into(), "y".into()], false);
            j.one_row(r, vec![2.into(), 2.into(), "z".into()], false);

            // forward from left with single matching record on right
            j.seed(l, vec![1.into(), 2.into(), "a".into()]);
            let rs = j.one_row(l, vec![1.into(), 2.into(), "a".into()], false);
            assert_eq!(
                rs,
                vec![(vec![1.into(), 2.into(), "a".into(), "x".into()], true)].into()
            );

            // forward from left with two matching records on right
            j.seed(l, vec![2.into(), 2.into(), "b".into()]);
            let mut rs: Vec<_> = j
                .one_row(l, vec![2.into(), 2.into(), "b".into()], false)
                .into();
            rs.sort();
            assert_eq!(
                rs,
                vec![
                    vec![2.into(), 2.into(), "b".into(), "y".into()].into(),
                    vec![2.into(), 2.into(), "b".into(), "z".into()].into()
                ]
            );
        }
    }

    /// Compound-key variant of left_join_replay_non_join_key_miss_proceeds.
    /// Replay key columns ARE the join key but listed in reversed order
    /// (downstream index chose [j1, j0] instead of [j0, j1]).  The
    /// set-equality check must still recognise this as Case 1 (skip).
    /// A plain slice comparison would give a false negative, incorrectly
    /// taking Case 2 (proceed without count).
    #[test]
    fn compound_join_key_reversed_order_is_case1() {
        use std::collections::HashSet;

        use dataflow_state::MaterializedNodeState;
        use readyset_client::KeyComparison;

        // Compound left-join setup:
        //   left  [l0, l1, l2]     right [r0, r1, r2]
        //   join ON (l0=r0, l1=r1)
        //   emit  [L0, L1, L2, R2]  →  output columns j0, j1, j2, j3
        let (mut g, l, r) = compound_keys::setup();

        // Make right's compound join-key index PARTIAL.
        let tag = Tag::new(0);
        let mut partial_state = MemoryState::default();
        partial_state.add_index(Index::hash_map(vec![0, 1]), Some(vec![tag]));
        partial_state.add_weak_index(Index::hash_map(vec![0, 1]));
        g.states
            .insert(*r, MaterializedNodeState::Memory(partial_state));

        // Seed left with a matching row.
        g.seed(l, vec![5.into(), 6.into(), "left_val".into()]);

        // Replay key_cols = [1, 0] (emitted columns j1, j0) — these map to
        // the join key columns but in REVERSED order relative to self.on
        // which declares [(0,0),(1,1)].
        //
        // trace_replay_column_source resolves:
        //   emit[1] = (Left,1) → right col 1 (via on)
        //   emit[0] = (Left,0) → right col 0 (via on)
        // So rkc = [1, 0], while on_cols_ts = [0, 1].
        // These represent the same set of columns → Case 1 (skip).
        let replay_key: KeyComparison = vec1![6.into(), 5.into()].into();
        let replay_keys = HashSet::from([replay_key]);
        let replay_ctx = ReplayContext::Partial {
            key_cols: &[1, 0], // reversed order
            keys: &replay_keys,
            requesting_shard: 0,
            requesting_replica: 0,
            tag,
            unishard: true,
        };

        // Send a right-side record whose compound join key (5,6) is a HOLE
        // in right's partial index.  Case 1 should `continue` (skip it).
        let right_record: Record = vec![5.into(), 6.into(), "rval".into()].into();
        let res = g.input_raw(r, vec![right_record], replay_ctx, false);

        match res {
            RawProcessingResult::Regular(ref pr) => {
                // Case 1 skips the record, so output must be empty.
                assert!(
                    pr.results.is_empty(),
                    "replay key == join key (reversed order) should be Case 1 (skip), \
                     but got results: {:?}",
                    pr.results
                );
            }
            other => panic!("expected Regular processing result, got: {:?}", other),
        }
    }

    /// Compound-key join where the replay is on a non-join-key column.
    /// Should take Case 2 (proceed without right-side count).
    #[test]
    fn compound_join_non_join_key_replay_proceeds() {
        use std::collections::HashSet;

        use dataflow_state::MaterializedNodeState;
        use readyset_client::KeyComparison;

        // Compound left-join setup:
        //   left  [l0, l1, l2]     right [r0, r1, r2]
        //   join ON (l0=r0, l1=r1)
        //   emit  [L0, L1, L2, R2]  →  output columns j0, j1, j2, j3
        let (mut g, l, r) = compound_keys::setup();

        // Make right's compound join-key index PARTIAL.
        let tag = Tag::new(0);
        let mut partial_state = MemoryState::default();
        partial_state.add_index(Index::hash_map(vec![0, 1]), Some(vec![tag]));
        partial_state.add_weak_index(Index::hash_map(vec![0, 1]));
        g.states
            .insert(*r, MaterializedNodeState::Memory(partial_state));

        // Seed left with a matching row.
        g.seed(l, vec![5.into(), 6.into(), "left_val".into()]);

        // Replay key_cols = [3] (emitted column j3 = Right.r2).
        // trace_replay_column_source resolves emit[3] = (Right,2) → right col 2.
        // rkc = [2], on_cols_ts = [0, 1] → different set → Case 2 (proceed).
        let replay_key: KeyComparison = vec1!["rval".into()].into();
        let replay_keys = HashSet::from([replay_key]);
        let replay_ctx = ReplayContext::Partial {
            key_cols: &[3], // emitted column 3 = Right.r2 (non-join-key)
            keys: &replay_keys,
            requesting_shard: 0,
            requesting_replica: 0,
            tag,
            unishard: true,
        };

        // Send a right-side record.  Compound join key (5,6) is a HOLE in
        // right's partial index, but replay key != join key → Case 2,
        // proceed and produce the joined row.
        let right_record: Record = vec![5.into(), 6.into(), "rval".into()].into();
        let res = g.input_raw(r, vec![right_record], replay_ctx, false);

        match res {
            RawProcessingResult::Regular(ref pr) => {
                assert!(
                    !pr.results.is_empty(),
                    "non-join-key replay on compound join should proceed (Case 2)"
                );
                assert!(
                    pr.results
                        .has_positive(&[5.into(), 6.into(), "left_val".into(), "rval".into()][..]),
                    "expected joined row, got: {:?}",
                    pr.results
                );
            }
            other => panic!("expected Regular processing result, got: {:?}", other),
        }
    }

    mod left_filter {
        use dataflow_expression::{BinaryOperator, Expr};
        use readyset_data::DfType;

        use super::*;

        /// Build a filter expression: left_column[col_idx] = literal_val
        fn eq_filter(col_idx: usize, literal_val: DfValue) -> Expr {
            Expr::Op {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column {
                    index: col_idx,
                    ty: DfType::Int,
                }),
                right: Box::new(Expr::Literal {
                    val: literal_val,
                    ty: DfType::Int,
                }),
                ty: DfType::Bool,
            }
        }

        /// Setup a LEFT JOIN with a left_filter that checks l0 == 1.
        /// Columns: left (l0, l1), right (r0, r1)
        /// Emit: [l0, l1, r1]
        /// Join ON: l0 = r0
        fn setup_with_filter() -> (ops::test::MockGraph, IndexPair, IndexPair) {
            let mut g = ops::test::MockGraph::new();
            let l = g.add_base("left", &["l0", "l1"]);
            let r = g.add_base("right", &["r0", "r1"]);

            // Filter: left column 0 must equal 1
            let filter = eq_filter(0, DfValue::from(1));

            let j = Join::new(
                l.as_global(),
                r.as_global(),
                JoinType::Left,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
                true,
                Some(filter),
            );

            g.set_op("join", &["j0", "j1", "j2"], j, false);
            (g, l, r)
        }

        #[test]
        fn left_row_passes_filter_normal_join_match() {
            let (mut j, l, r) = setup_with_filter();

            // Seed right side with a matching row (r0=1)
            let r_row = vec![1.into(), "x".into()];
            j.seed(r, r_row.clone());
            j.one_row(r, r_row, false);

            // Insert left row with l0=1 (passes filter l0==1), should match right
            let l_row = vec![1.into(), "a".into()];
            j.seed(l, l_row.clone());
            let rs = j.one_row(l, l_row, false);

            assert_eq!(
                rs,
                vec![(vec![1.into(), "a".into(), "x".into()], true)].into()
            );
        }

        #[test]
        fn left_row_fails_filter_null_extended() {
            let (mut j, l, r) = setup_with_filter();

            // Seed right side with a matching row (r0=2)
            let r_row = vec![2.into(), "x".into()];
            j.seed(r, r_row.clone());
            j.one_row(r, r_row, false);

            // Insert left row with l0=2 (fails filter l0==1).
            // Even though right has a matching row for key 2, it should be NULL-extended.
            let l_row = vec![2.into(), "b".into()];
            j.seed(l, l_row.clone());
            let rs = j.one_row(l, l_row, false);

            assert_eq!(
                rs,
                vec![(vec![2.into(), "b".into(), DfValue::None], true)].into()
            );
        }

        #[test]
        fn right_insert_left_passes_filter_matched() {
            let (mut j, l, r) = setup_with_filter();

            // Seed left side with a row that passes filter (l0=1)
            let l_row = vec![1.into(), "a".into()];
            j.seed(l, l_row.clone());
            j.one_row(l, l_row, false);

            // Now insert a matching right row
            let r_row = vec![1.into(), "x".into()];
            j.seed(r, r_row.clone());
            let rs = j.one_row(r, r_row, false);

            // Should emit the matched row plus revoke the null-extended row
            assert_eq!(
                rs,
                vec![
                    (vec![1.into(), "a".into(), "x".into()], true),
                    (vec![1.into(), "a".into(), DfValue::None], false),
                ]
                .into()
            );
        }

        #[test]
        fn right_insert_left_fails_filter_no_change() {
            let (mut j, l, r) = setup_with_filter();

            // Seed left side with a row that fails filter (l0=2)
            let l_row = vec![2.into(), "b".into()];
            j.seed(l, l_row.clone());
            j.one_row(l, l_row, false);

            // Insert a matching right row for key 2
            let r_row = vec![2.into(), "y".into()];
            j.seed(r, r_row.clone());
            let rs = j.one_row(r, r_row, false);

            // Left row fails filter, so no matched rows should be emitted
            assert_eq!(rs.len(), 0);
        }

        #[test]
        fn right_delete_left_passes_filter() {
            let (mut j, l, r) = setup_with_filter();

            // Seed right side with two rows for key 1
            j.seed(r, vec![1.into(), "x".into()]);
            j.one_row(r, vec![1.into(), "x".into()], false);
            j.seed(r, vec![1.into(), "y".into()]);
            j.one_row(r, vec![1.into(), "y".into()], false);

            // Seed left side (passes filter, l0=1)
            j.seed(l, vec![1.into(), "a".into()]);
            j.one_row(l, vec![1.into(), "a".into()], false);

            // Delete one right row — left row passes filter so matched row is revoked
            let rs = j.one_row(r, (vec![1.into(), "x".into()], false), false);
            assert!(rs.has_negative(&[1.into(), "a".into(), "x".into()][..]));
        }

        #[test]
        fn right_delete_left_fails_filter() {
            let (mut j, l, r) = setup_with_filter();

            // Seed right side with two rows for key 2
            j.seed(r, vec![2.into(), "x".into()]);
            j.one_row(r, vec![2.into(), "x".into()], false);
            j.seed(r, vec![2.into(), "y".into()]);
            j.one_row(r, vec![2.into(), "y".into()], false);

            // Seed left side (fails filter, l0=2)
            j.seed(l, vec![2.into(), "b".into()]);
            j.one_row(l, vec![2.into(), "b".into()], false);

            // Delete one right row — left row fails filter so no matched row
            // existed; the delete should not produce any output for this left row
            let rs = j.one_row(r, (vec![2.into(), "x".into()], false), false);
            assert_eq!(rs.len(), 0);
        }
    }

    #[test]
    fn post_deserialize_restores_emit_move_safe() {
        let l = NodeIndex::new(0);
        let r = NodeIndex::new(1);
        // Emit has no duplicate columns per side, so `emit_move_safe` should be true.
        let j = Join::new(
            l,
            r,
            JoinType::Left,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            true,
            None,
        );
        assert!(j.emit_move_safe, "new() should set emit_move_safe");

        let bytes = bincode::serialize(&j).unwrap();
        let mut round_tripped: Join = bincode::deserialize(&bytes).unwrap();
        assert!(
            !round_tripped.emit_move_safe,
            "deserialize should leave emit_move_safe at its Default (false)"
        );

        round_tripped.post_deserialize();
        assert!(
            round_tripped.emit_move_safe,
            "post_deserialize should recompute emit_move_safe"
        );
    }

    #[test]
    fn post_deserialize_leaves_emit_move_safe_false_when_duplicates() {
        let l = NodeIndex::new(0);
        let r = NodeIndex::new(1);
        // Emit contains (Left, 0) twice, so `emit_move_safe` must stay false.
        let mut j = Join::new(
            l,
            r,
            JoinType::Left,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Left, 0), (Side::Right, 1)],
            true,
            None,
        );
        assert!(!j.emit_move_safe);
        j.post_deserialize();
        assert!(!j.emit_move_safe);
    }
}
