use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};

use dataflow_state::PointKey;
use itertools::Itertools;
use readyset_client::KeyComparison;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_util::intervals::into_bound_endpoint;
use readyset_util::Indices;
use serde::{Deserialize, Serialize};
use vec1::{vec1, Vec1};

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

/// Join rows between two nodes based on a (compound) equal join key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    left: IndexPair,
    right: IndexPair,

    /// Key columns in the left and right parents respectively
    on: Vec<(usize, usize)>,

    // Which columns to emit
    emit: Vec<(Side, usize)>,

    // Which columns to emit when the left/right row is being modified in place. True means the
    // column is from the left parent, false means from the right
    in_place_left_emit: Vec<(Side, usize)>,
    in_place_right_emit: Vec<(Side, usize)>,

    /// Buffered records from one half of a remapped upquery. The key is (column index,
    /// side).
    // We skip serde since we don't want the state of the node, just the configuration.
    #[serde(skip)]
    generated_column_buffer: HashMap<(Vec<usize>, Side), Records>,

    kind: JoinType,
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
    ) -> Self {
        let (in_place_left_emit, in_place_right_emit) = {
            let compute_in_place_emit = |side| {
                let num_columns = emit
                    .iter()
                    .filter(|&&(from_side, _)| from_side == side)
                    .map(|&(_, c)| c + 1)
                    .max()
                    .unwrap_or(0);

                // Tracks how columns have moved. At any point during the iteration, column i in
                // the original row will be located at position remap[i].
                let mut remap: Vec<_> = (0..num_columns).collect();
                emit.iter()
                    .enumerate()
                    .map(|(i, &(from_side, c))| {
                        if from_side == side {
                            let remapped = remap[c];
                            let other = remap.iter().position(|&c| c == i);

                            remap[c] = i;
                            if let Some(other) = other {
                                remap[other] = remapped;
                            }

                            (from_side, remapped)
                        } else {
                            (from_side, c)
                        }
                    })
                    .collect::<Vec<_>>()
            };

            (
                compute_in_place_emit(Side::Left),
                compute_in_place_emit(Side::Right),
            )
        };

        Self {
            left: left.into(),
            right: right.into(),
            on,
            emit,
            in_place_left_emit,
            in_place_right_emit,
            generated_column_buffer: Default::default(),
            kind,
        }
    }

    fn on_left(&self) -> Vec<usize> {
        self.on.iter().map(|(l, _)| *l).collect()
    }

    fn on_right(&self) -> Vec<usize> {
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

    fn handle_replay_for_generated(
        &self,
        left: Records,
        right: Records,
    ) -> ReadySetResult<Records> {
        let mut from_key = vec![];
        let mut other_key = vec![];
        let mut ret: Vec<Record> = vec![];
        for (left_key, right_key) in &self.on {
            from_key.push(*left_key);
            other_key.push(*right_key);
        }
        for rec in left {
            let (rec, positive) = rec.extract();
            invariant!(positive, "replays should only include positive records");

            for other_rec in right
                .iter()
                .filter(|r| rec.indices(from_key.clone()) == r.indices(other_key.clone()))
            {
                ret.push(Record::Positive(self.generate_row(&rec, other_rec.row())))
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

    fn on_connected(&mut self, _g: &Graph) {}

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
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        let from_left = from == *self.left;

        let other = if from_left { *self.right } else { *self.left };

        let (from_key, other_key): (Vec<usize>, Vec<usize>) = if from_left {
            self.on.iter().copied().unzip()
        } else {
            let (other_key, from_key) = self.on.iter().copied().unzip();
            (from_key, other_key)
        };

        let orkc = replay.key();

        let replay_key_cols: Result<Option<Vec<usize>>, ()> =
            replay
                .key()
                .map(|cols| {
                    cols.iter()
                        .map(|&col| -> Result<usize, ()> {
                            match self.emit[col] {
                                (Side::Left, l) if from == *self.left => return Ok(l),
                                (Side::Right, r) if from == *self.right => return Ok(r),
                                (Side::Left, l) => {
                                    if let Some(r) = self.on.iter().find_map(|(on_l, r)| {
                                        if *on_l == l {
                                            Some(r)
                                        } else {
                                            None
                                        }
                                    }) {
                                        // since we didn't hit the case above, we know that the
                                        // message
                                        // *isn't* from left.
                                        return Ok(*r);
                                    }
                                }
                                (Side::Right, r) => {
                                    if let Some(l) = self.on.iter().find_map(|(l, on_r)| {
                                        if *on_r == r {
                                            Some(l)
                                        } else {
                                            None
                                        }
                                    }) {
                                        // same
                                        return Ok(*l);
                                    }
                                }
                            }
                            Err(())
                        })
                        .collect()
                })
                .transpose();

        let replay_key_cols = match replay_key_cols {
            Ok(v) => v,
            Err(_) => {
                // columns generated!
                let orkc = orkc.unwrap();
                let is_left = from == *self.left;
                return if let Some(other) = self.generated_column_buffer.remove(&(
                    orkc.to_vec(),
                    if is_left { Side::Right } else { Side::Left },
                )) {
                    // we have both sides now
                    let (left, right) = if is_left { (rs, other) } else { (other, rs) };
                    let ret = self.handle_replay_for_generated(left, right)?;
                    Ok(ProcessingResult {
                        results: ret,
                        ..Default::default()
                    })
                } else {
                    // store the records for when we get the other upquery response
                    self.generated_column_buffer.insert(
                        (
                            orkc.to_vec(),
                            if is_left { Side::Left } else { Side::Right },
                        ),
                        rs,
                    );
                    Ok(Default::default())
                };
            }
        };

        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());

        let grouped_records = rs
            .into_iter()
            .group_by(|rec| from_key.iter().map(|i| rec[*i].clone()).collect::<Vec<_>>());

        let is_replay = replay_key_cols.is_some();

        // Only do a lookup into a weak index if we're processing regular updates,
        // not if we're processing a replay, since regular updates should represent
        // all rows that won't hit holes downstream but replays need to have *all*
        // rows
        let lookup_mode = if is_replay {
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
                        // we got something from right, but that row's key is not in right??
                        //
                        // this *can* happen! imagine if you have two partial indices on right,
                        // one on column a and one on column b. imagine that a is the join key.
                        // we get a replay request for b = 4, which must then be replayed from
                        // right (since left doesn't have b). say right replays (a=1,b=4). we
                        // will hit this case, since a=1 is not in right. the correct thing to
                        // do here is to replay a=1 first, and *then* replay b=4 again
                        // (possibly several times over for each a).
                        continue;
                    }
                }
            }

            let mut other_lookup = match nulls {
                true => IngredientLookupResult::empty(),
                false => self.lookup(
                    other,
                    &other_key,
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
                            .lookup_idx(other_key.clone())
                            .lookup_key(from_key.clone())
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
                    cols: other_key.clone(),
                    key: join_key
                        .try_into()
                        .map_err(|_| internal_err!("Empty join key"))?,
                });
            }

            let other_rows = other_records.collect::<Result<Vec<_>, _>>()?;

            let mut rc_diff = 0isize;
            for r in group {
                let (row, positive) = r.extract();

                rc_diff += if positive { 1 } else { -1 };

                if other_rows.is_empty() {
                    if self.kind == JoinType::Left && from_left {
                        // left join, got a thing from left, no rows in right == NULL
                        ret.push((self.generate_null(&row), positive).into());
                    }
                } else {
                    for other in other_rows.iter() {
                        if from == *self.left {
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
                        ret.push((self.generate_null(other), true).into());
                    }
                } else if new_rc != 0 && old_rc == 0 {
                    for other in other_rows.iter() {
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

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from(match self.kind {
                JoinType::Left => "⋉",
                JoinType::Inner => "⋈",
            });
        }

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

                    if let Some(lower_endpoint) = into_bound_endpoint(lower) {
                        for (value, side) in lower_endpoint.into_iter().zip(&col_sides) {
                            into_bound_endpoint(match side {
                                Side::Left => left_lower.as_mut(),
                                Side::Right => right_lower.as_mut(),
                            })
                            .unwrap()
                            .push(value);
                        }
                    }

                    if let Some(upper_endpoint) = into_bound_endpoint(upper) {
                        for (value, side) in upper_endpoint.into_iter().zip(&col_sides) {
                            into_bound_endpoint(match side {
                                Side::Left => left_upper.as_mut(),
                                Side::Right => right_upper.as_mut(),
                            })
                            .unwrap()
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
        );

        g.set_op("join", &["j0", "j1", "j2"], j, false);
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (j, l, r) = setup();
        assert_eq!(
            j.node().description(true),
            format!("[{}:0, {}:1, {}:1] {}:(0) ⋉ {}:(0)", l, l, r, l, r)
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
        use std::ops::Bound;

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
                    missed_keys: vec1![KeyComparison::Range((
                        Bound::Included(vec1![
                            DfValue::from(1),
                            DfValue::from(2),
                            DfValue::from(3)
                        ]),
                        Bound::Unbounded
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
                    Bound::Unbounded
                ))]
            );
            assert_eq!(
                right_miss.missed_keys,
                vec1![KeyComparison::Range((
                    Bound::Included(vec1![DfValue::from(3)]),
                    Bound::Unbounded
                ))]
            );
        }
    }

    mod compound_keys {
        use super::*;

        fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
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
}
