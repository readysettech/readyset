use itertools::Itertools;
use launchpad::Indices;
use maplit::hashmap;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::mem;
use vec1::{vec1, Vec1};

use super::Side;
use crate::prelude::*;
use crate::processing::{ColumnMiss, ColumnRef, ColumnSource, SuggestedIndex};
use noria::errors::{internal_err, ReadySetResult};
use noria::{internal, KeyComparison};

/// Kind of join
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    /// Left join between two views
    Left,
    /// Inner join between two views
    Inner,
}

/// Where to source a join column
#[derive(Debug, Clone)]
pub enum JoinSource {
    /// Column in left parent
    L(usize),
    /// Column in right parent
    R(usize),
    /// Join column that occurs in both parents
    B(usize, usize),
}

/// Join provides a left outer join between two views.
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
    generated_column_buffer: HashMap<(Vec<usize>, Side), Records>,

    kind: JoinType,
}

enum Preprocessed {
    Left,
    Right,
    Neither,
}

impl Join {
    /// Create a new instance of Join
    ///
    /// `left` and `right` are the left and right parents respectively. `on` is a tuple specifying
    /// the join columns: (left_parent_column, right_parent_column) and `emit` dictates for each
    /// output colunm, which source and column should be used (true means left parent, and false
    /// means right parent).
    pub fn new(left: NodeIndex, right: NodeIndex, kind: JoinType, emit: Vec<JoinSource>) -> Self {
        let mut join_columns = Vec::new();
        let emit: Vec<_> = emit
            .into_iter()
            .map(|join_source| match join_source {
                JoinSource::L(c) => (Side::Left, c),
                JoinSource::R(c) => (Side::Right, c),
                JoinSource::B(lc, rc) => {
                    join_columns.push((lc, rc));
                    (Side::Left, lc)
                }
            })
            .collect();

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

                            // Columns can't appear multiple times in join output!
                            assert!((remapped >= i) || (emit[remapped].0 != side));

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
            on: join_columns,
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

    fn generate_row(
        &self,
        left: &[DataType],
        right: &[DataType],
        reusing: Preprocessed,
    ) -> Vec<DataType> {
        self.emit
            .iter()
            .enumerate()
            .map(|(i, &(side, col))| match side {
                Side::Left => {
                    if let Preprocessed::Left = reusing {
                        left[i].clone()
                    } else {
                        left[col].clone()
                    }
                }
                Side::Right => {
                    if let Preprocessed::Right = reusing {
                        right[i].clone()
                    } else {
                        right[col].clone()
                    }
                }
            })
            .collect()
    }

    fn regenerate_row(
        &self,
        mut reuse: Vec<DataType>,
        other: &[DataType],
        reusing_left: bool,
        other_prepreprocessed: bool,
    ) -> Vec<DataType> {
        let emit = if reusing_left {
            &self.in_place_left_emit
        } else {
            &self.in_place_right_emit
        };
        reuse.resize(emit.len(), DataType::None);
        for (i, &(side, c)) in emit.iter().enumerate() {
            let from_left = side == Side::Left;
            if (from_left == reusing_left) && i != c {
                reuse.swap(i, c);
            }
        }
        for (i, &(side, c)) in emit.iter().enumerate() {
            let from_left = side == Side::Left;
            if from_left != reusing_left {
                if other_prepreprocessed {
                    reuse[i] = other[i].clone();
                } else {
                    reuse[i] = other[c].clone();
                }
            }
        }
        reuse
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
                ret.push(Record::Positive(self.generate_row(
                    &rec,
                    other_rec.row(),
                    Preprocessed::Neither,
                )))
            }
        }
        Ok(ret.into())
    }

    // TODO: make non-allocating
    fn generate_null(&self, left: &[DataType]) -> Vec<DataType> {
        self.emit
            .iter()
            .map(|&(side, col)| {
                if side == Side::Left {
                    left[col].clone()
                } else {
                    DataType::None
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
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

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

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.left.remap(remap);
        self.right.remap(remap);
    }

    #[allow(clippy::cognitive_complexity)]
    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        replay_key_cols: Option<&[usize]>,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        let other = if from == *self.left {
            *self.right
        } else {
            *self.left
        };

        let mut from_key = vec![];
        let mut other_key = vec![];
        for (left_key, right_key) in &self.on {
            if from == *self.left {
                from_key.push(*left_key);
                other_key.push(*right_key);
            } else {
                from_key.push(*right_key);
                other_key.push(*left_key);
            }
        }

        let orkc = replay_key_cols;

        let replay_key_cols: Result<Option<Vec<usize>>, ()> =
            replay_key_cols
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
                                        // since we didn't hit the case above, we know that the message
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

        // First, we want to be smart about multiple added/removed rows with the same join key
        // value. For example, if we get a -, then a +, for the same key, we don't want to execute
        // two queries. We'll do this by sorting the batch by our join key.
        let mut rs: Vec<_> = rs.into();
        {
            rs.sort_by(|a: &Record, b: &Record| {
                a.indices(from_key.clone())
                    .unwrap_or_default()
                    .cmp(&b.indices(from_key.clone()).unwrap_or_default())
            });
        }

        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());
        let mut at = 0;
        while at != rs.len() {
            let mut old_right_count = None;
            let mut new_right_count = None;
            let prev_join_key = rs[at]
                .indices(from_key.clone())
                .map_err(|_| ReadySetError::InvalidRecordLength)?;

            if from == *self.right && self.kind == JoinType::Left {
                let rc = self
                    .lookup(
                        *self.right,
                        &self.on_right(),
                        &KeyType::from(prev_join_key.clone()),
                        nodes,
                        state,
                    )
                    .unwrap();

                if let Some(rc) = rc {
                    if replay_key_cols.is_some() {
                        lookups.push(Lookup {
                            on: *self.right,
                            cols: self.on_right(),
                            key: prev_join_key
                                .clone()
                                .into_iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .try_into()
                                .map_err(|_| internal_err("Empty join key"))?,
                        });
                    }

                    let rc = rc.count();
                    old_right_count = Some(rc);
                    new_right_count = Some(rc);
                } else {
                    // we got something from right, but that row's key is not in right??
                    //
                    // this *can* happen! imagine if you have two partial indices on right,
                    // one on column a and one on column b. imagine that a is the join key.
                    // we get a replay request for b = 4, which must then be replayed from
                    // right (since left doesn't have b). say right replays (a=1,b=4). we
                    // will hit this case, since a=1 is not in right. the correct thing to
                    // do here is to replay a=1 first, and *then* replay b=4 again
                    // (possibly several times over for each a).
                    at = rs[at..]
                        .iter()
                        .position(|r| {
                            r.indices(from_key.clone())
                                .into_iter()
                                .any(|k| k != prev_join_key)
                        })
                        .map(|p| at + p)
                        .unwrap_or_else(|| rs.len());
                    continue;
                }
            }

            // get rows from the other side
            let mut other_rows = self
                .lookup(
                    other,
                    &other_key,
                    &KeyType::from(prev_join_key.clone()),
                    nodes,
                    state,
                )
                .unwrap();

            if other_rows.is_none() {
                // we missed in the other side!
                let from = at;
                at = rs[at..]
                    .iter()
                    .position(|r| {
                        r.indices(from_key.clone())
                            .into_iter()
                            .any(|k| k != prev_join_key)
                    })
                    .map(|p| at + p)
                    .unwrap_or_else(|| rs.len());

                // NOTE: we're stealing data here!
                let mut records = (from..at)
                    .map(|i| {
                        mem::take(&mut *rs[i])
                            .try_into()
                            .map_err(|_| internal_err("empty record"))
                    })
                    .collect::<ReadySetResult<Vec<_>>>()?
                    .into_iter();

                misses.extend((from..at).map(|_| Miss {
                    on: other,
                    lookup_idx: other_key.clone(),
                    lookup_cols: from_key.clone(),
                    replay_cols: replay_key_cols.clone(),
                    record: records.next().unwrap(),
                }));
                continue;
            }

            if replay_key_cols.is_some() {
                lookups.push(Lookup {
                    on: other,
                    cols: other_key.clone(),
                    key: prev_join_key
                        .clone()
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .try_into()
                        .map_err(|_| internal_err("Empty join key"))?,
                });
            }

            let start = at;
            let mut make_null = None;
            if self.kind == JoinType::Left && from == *self.right {
                // If records are being received from the right, we need to find the number of
                // records that existed *before* this batch of records was processed so we know
                // whether or not to generate +/- NULL rows.
                if let Some(mut old_rc) = old_right_count {
                    while at != rs.len()
                        && rs[at]
                            .indices(from_key.clone())
                            .map_err(|_| ReadySetError::InvalidRecordLength)?
                            == prev_join_key
                    {
                        if rs[at].is_positive() {
                            old_rc -= 1
                        } else {
                            old_rc += 1
                        }
                        at += 1;
                    }

                    // emit null rows if necessary for left join
                    let new_rc = new_right_count.unwrap();
                    if new_rc == 0 && old_rc != 0 {
                        // all lefts for this key must emit + NULLs
                        make_null = Some(true);
                    } else if new_rc != 0 && old_rc == 0 {
                        // all lefts for this key must emit - NULLs
                        make_null = Some(false);
                    }
                } else {
                    // we got a right, but missed in right; clearly, a replay is needed
                    let start = at;
                    at = rs[at..]
                        .iter()
                        .position(|r| {
                            r.indices(from_key.clone())
                                .into_iter()
                                .any(|k| k != prev_join_key)
                        })
                        .map(|p| at + p)
                        .unwrap_or_else(|| rs.len());

                    // NOTE: we're stealing data here!
                    let mut records = (start..at)
                        .map(|i| {
                            mem::take(&mut *rs[i])
                                .try_into()
                                .map_err(|_| internal_err("empty record"))
                        })
                        .collect::<ReadySetResult<Vec<_>>>()?
                        .into_iter();

                    misses.extend((start..at).map(|_| Miss {
                        on: from,
                        lookup_idx: self.on_right(),
                        lookup_cols: from_key.clone(),
                        replay_cols: replay_key_cols.clone(),
                        record: records.next().unwrap(),
                    }));
                    continue;
                }
            }

            if start == at {
                // we didn't find the end above, so find it now
                at = rs[at..]
                    .iter()
                    .position(|r| {
                        r.indices(from_key.clone())
                            .into_iter()
                            .any(|k| k != prev_join_key)
                    })
                    .map(|p| at + p)
                    .unwrap_or_else(|| rs.len());
            }

            let mut other_rows_count = 0;
            for r in &mut rs[start..at] {
                // put something bogus in rs (which will be discarded anyway) so we can take r.
                let r = mem::replace(r, Record::Positive(Vec::new()));
                let (row, positive) = r.extract();

                if let Some(other_rows) = other_rows.take() {
                    // we have yet to iterate through other_rows
                    let mut other_rows = other_rows.peekable();
                    if other_rows.peek().is_none() {
                        if self.kind == JoinType::Left && from == *self.left {
                            // left join, got a thing from left, no rows in right == NULL
                            ret.push((self.generate_null(&row), positive).into());
                        }
                        continue;
                    }

                    // we're going to pull a little trick here so that the *last* time we use
                    // `row`, we re-use its memory instead of allocating a new Vec. we do this by
                    // (ab)using .peek() to terminate the loop one iteration early.
                    other_rows_count += 1;
                    let mut other = other_rows.next().unwrap()?;
                    while other_rows.peek().is_some() {
                        if let Some(false) = make_null {
                            // we need to generate a -NULL for all these lefts
                            ret.push((self.generate_null(&other), false).into());
                        }
                        if from == *self.left {
                            ret.push(
                                (
                                    self.generate_row(&row, &other, Preprocessed::Neither),
                                    positive,
                                )
                                    .into(),
                            );
                        } else {
                            ret.push(
                                (
                                    self.generate_row(&other, &row, Preprocessed::Neither),
                                    positive,
                                )
                                    .into(),
                            );
                        }
                        if let Some(true) = make_null {
                            // we need to generate a +NULL for all these lefts
                            ret.push((self.generate_null(&other), true).into());
                        }
                        other = other_rows.next().unwrap()?;
                        other_rows_count += 1;
                    }

                    if let Some(false) = make_null {
                        // we need to generate a -NULL for the last left too
                        ret.push((self.generate_null(&other), false).into());
                    }
                    ret.push(
                        (
                            self.regenerate_row(row, &other, from == *self.left, false),
                            positive,
                        )
                            .into(),
                    );
                    if let Some(true) = make_null {
                        // we need to generate a +NULL for the last left too
                        ret.push((self.generate_null(&other), true).into());
                    }
                } else if other_rows_count == 0 {
                    if self.kind == JoinType::Left && from == *self.left {
                        // left join, got a thing from left, no rows in right == NULL
                        ret.push((self.generate_null(&row), positive).into());
                    }
                } else {
                    // we no longer have access to `other_rows`
                    // *but* the values are all in ret[-other_rows_count:]!
                    let start = ret.len() - other_rows_count;
                    let end = ret.len();
                    // we again use the trick above where the last row we produce reuses `row`
                    for i in start..(end - 1) {
                        if from == *self.left {
                            let r = (
                                self.generate_row(&row, &ret[i], Preprocessed::Right),
                                positive,
                            )
                                .into();
                            ret.push(r);
                        } else {
                            let r = (
                                self.generate_row(&ret[i], &row, Preprocessed::Left),
                                positive,
                            )
                                .into();
                            ret.push(r);
                        }
                    }
                    let r = (
                        self.regenerate_row(row, &ret[end - 1], from == *self.left, true),
                        positive,
                    )
                        .into();
                    ret.push(r);
                }
            }
        }

        Ok(ProcessingResult {
            results: ret.into(),
            lookups,
            misses,
        })
    }

    fn suggest_indexes(&self, _this: NodeIndex) -> HashMap<NodeIndex, SuggestedIndex> {
        // Replays might have happened through our parents into keys *other* than the join key, and
        // we need to find those rows when looking up values to perform the join as part of forward
        // processing of normal writes - so we use a weak index here to avoid dropping writes in
        // that case.
        hashmap! {
            self.left.as_global() => SuggestedIndex::Weak(Index::hash_map(self.on_left())),
            self.right.as_global() => SuggestedIndex::Weak(Index::hash_map(self.on_right())),
        }
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

    fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        // reminder: this function *only* gets called for column indices that are sourced from
        // both parents

        let mut flipped_keys = std::iter::repeat(vec![])
            .take(miss.missed_columns.columns.len())
            .collect::<Vec<_>>();
        for i in 0..miss.missed_columns.columns.len() {
            for keycomparison in miss.missed_key.iter() {
                match keycomparison.equal() {
                    Some(lst) => {
                        flipped_keys[i].push(lst[i].clone());
                    }
                    None => {
                        // TODO(eta): make this work with range queries as well.
                        internal!("range queries aren't supported by Join's handle_upquery impl")
                    }
                }
            }
        }
        let mut keys = flipped_keys.into_iter();

        let mut left_cols = vec![];
        let mut left_keys = vec![];
        let mut right_cols = vec![];
        let mut right_keys = vec![];
        for col in miss.missed_columns.columns {
            let key = match keys.next() {
                Some(k) => k,
                None => {
                    internal!("malformed miss passed to Join's handle_upquery");
                }
            };
            let (left_idx, right_idx) = self.resolve_col(col);
            if let Some(li) = left_idx {
                left_cols.push(li);
                left_keys.push(KeyComparison::Equal(Vec1::try_from(key).unwrap()));
            } else if let Some(ri) = right_idx {
                right_cols.push(ri);
                right_keys.push(KeyComparison::Equal(Vec1::try_from(key).unwrap()));
            } else {
                internal!("could not resolve col {} in join upquery", col);
            }
        }
        Ok(vec![
            ColumnMiss {
                missed_columns: ColumnRef {
                    node: self.left.as_global(),
                    columns: Vec1::try_from(left_cols).unwrap(),
                },
                missed_key: Vec1::try_from(left_keys).unwrap(),
            },
            ColumnMiss {
                missed_columns: ColumnRef {
                    node: self.right.as_global(),
                    columns: Vec1::try_from(right_cols).unwrap(),
                },
                missed_key: Vec1::try_from(right_keys).unwrap(),
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
                left_cols
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
        } else if right_cols.iter().all(|x| x.is_some()) {
            // same for right parent
            ColumnSource::exact_copy(
                self.right.as_global(),
                right_cols
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
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
                    columns: left_cols.try_into().unwrap()
                },
                ColumnRef {
                    node: self.right.as_global(),
                    columns: right_cols.try_into().unwrap()
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

        use self::JoinSource::*;
        let j = Join::new(
            l.as_global(),
            r.as_global(),
            JoinType::Left,
            vec![B(0, 0), L(1), R(1)],
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
        let l_a1 = vec![1.into(), "a".try_into().unwrap()];
        let l_b2 = vec![2.into(), "b".try_into().unwrap()];
        let l_c3 = vec![3.into(), "c".try_into().unwrap()];

        let r_x1 = vec![1.into(), "x".try_into().unwrap()];
        let r_y1 = vec![1.into(), "y".try_into().unwrap()];
        let r_z2 = vec![2.into(), "z".try_into().unwrap()];
        let r_w3 = vec![3.into(), "w".try_into().unwrap()];
        let r_v4 = vec![4.into(), "w".try_into().unwrap()];

        let r_nop: Vec<Record> = vec![
            (vec![3.into(), "w".try_into().unwrap()], false).into(),
            (vec![3.into(), "w".try_into().unwrap()], true).into(),
        ];

        j.seed(r, r_x1.clone());
        j.seed(r, r_y1.clone());
        j.seed(r, r_z2.clone());

        j.one_row(r, r_x1, false);
        j.one_row(r, r_y1, false);
        j.one_row(r, r_z2, false);

        // forward c3 from left; should produce [c3 + None] since no records in right are 3
        let null = vec![(
            vec![3.into(), "c".try_into().unwrap(), DataType::None],
            true,
        )]
        .into();
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
                (
                    vec![3.into(), "c".try_into().unwrap(), DataType::None],
                    false
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), DataType::None],
                    false
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
            ]
            .into()
        );

        // Negative followed by positive should not trigger nulls.
        // TODO: it shouldn't trigger any updates at all...
        let rs = j.one(r, r_nop, false);
        assert_eq!(
            rs,
            vec![
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    false
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    false
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
            ]
            .into()
        );

        // forward from left with single matching record on right
        j.seed(l, l_b2.clone());
        let rs = j.one_row(l, l_b2, false);
        assert_eq!(
            rs,
            vec![(
                vec![2.into(), "b".try_into().unwrap(), "z".try_into().unwrap()],
                true
            )]
            .into()
        );

        // forward from left with two matching records on right
        j.seed(l, l_a1.clone());
        let rs = j.one_row(l, l_a1, false);
        assert_eq!(rs.len(), 2);
        assert!(rs.has_positive(&[1.into(), "a".try_into().unwrap(), "x".try_into().unwrap()][..]));
        assert!(rs.has_positive(&[1.into(), "a".try_into().unwrap(), "y".try_into().unwrap()][..]));

        // forward from right with two matching records on left (and one more on right)
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3, false);
        assert_eq!(
            rs,
            vec![
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
                (
                    vec![3.into(), "c".try_into().unwrap(), "w".try_into().unwrap()],
                    true
                ),
            ]
            .into()
        );

        // unmatched forward from right should have no effect
        j.seed(r, r_v4.clone());
        let rs = j.one_row(r, r_v4, false);
        assert_eq!(rs.len(), 0);
    }

    #[test]
    fn it_suggests_indices() {
        let me = 2.into();
        let (g, l, r) = setup();
        let expected = hashmap! {
            l.as_global() => SuggestedIndex::Weak(Index::hash_map(vec![0])), // join column for left
            r.as_global() => SuggestedIndex::Weak(Index::hash_map(vec![0])), // join column for right
        };
        assert_eq!(g.node().suggest_indexes(me), expected);
    }

    #[test]
    fn parent_join_columns() {
        let (g, l, _) = setup();
        let res = g.node().parent_columns(0);
        assert_eq!(res, vec![(l.as_global(), Some(0))]);
    }

    mod compound_keys {
        use super::*;

        fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
            let mut g = ops::test::MockGraph::new();
            let l = g.add_base("left", &["l0", "l1", "l2"]);
            let r = g.add_base("right", &["r0", "r1", "r2"]);

            use self::JoinSource::*;
            let j = Join::new(
                l.as_global(),
                r.as_global(),
                JoinType::Left,
                vec![B(0, 0), B(1, 1), L(2), R(2)],
            );

            g.set_op("join", &["j0", "j1", "j2", "j3"], j, false);
            (g, l, r)
        }

        #[test]
        fn left_join_null() {
            let (mut j, l, r) = setup();

            // forward row from left; should produce [row + None] since no records in right match
            j.seed(l, vec![3.into(), 4.into(), "c".try_into().unwrap()]);
            let rs = j.one_row(l, vec![3.into(), 4.into(), "c".try_into().unwrap()], false);
            assert_eq!(
                rs,
                vec![(
                    vec![3.into(), 4.into(), "c".try_into().unwrap(), DataType::None],
                    true
                )]
                .into()
            );

            // Both of the keys have to match to give us a match
            j.seed(r, vec![3.into(), 3.into(), "w".try_into().unwrap()]);
            assert!(j
                .one_row(r, vec![3.into(), 3.into(), "w".try_into().unwrap()], false)
                .is_empty());

            // Once we get a match, we should revoke the nulls and replace it with a full row
            j.seed(r, vec![3.into(), 4.into(), "w".try_into().unwrap()]);
            let rs = j.one_row(r, vec![3.into(), 4.into(), "w".try_into().unwrap()], false);
            assert_eq!(
                rs,
                vec![
                    (
                        vec![3.into(), 4.into(), "c".try_into().unwrap(), DataType::None],
                        false
                    ),
                    (
                        vec![
                            3.into(),
                            4.into(),
                            "c".try_into().unwrap(),
                            "w".try_into().unwrap()
                        ],
                        true
                    )
                ]
                .into()
            );
        }

        #[test]
        fn lookup_matching() {
            let (mut j, l, r) = setup();

            j.seed(r, vec![1.into(), 2.into(), "x".try_into().unwrap()]);
            j.seed(r, vec![2.into(), 2.into(), "y".try_into().unwrap()]);
            j.seed(r, vec![2.into(), 2.into(), "z".try_into().unwrap()]);

            j.one_row(r, vec![1.into(), 2.into(), "x".try_into().unwrap()], false);
            j.one_row(r, vec![2.into(), 2.into(), "y".try_into().unwrap()], false);
            j.one_row(r, vec![2.into(), 2.into(), "z".try_into().unwrap()], false);

            // forward from left with single matching record on right
            j.seed(l, vec![1.into(), 2.into(), "a".try_into().unwrap()]);
            let rs = j.one_row(l, vec![1.into(), 2.into(), "a".try_into().unwrap()], false);
            assert_eq!(
                rs,
                vec![(
                    vec![
                        1.into(),
                        2.into(),
                        "a".try_into().unwrap(),
                        "x".try_into().unwrap()
                    ],
                    true
                )]
                .into()
            );

            // forward from left with two matching records on right
            j.seed(l, vec![2.into(), 2.into(), "b".try_into().unwrap()]);
            let mut rs: Vec<_> = j
                .one_row(l, vec![2.into(), 2.into(), "b".try_into().unwrap()], false)
                .into();
            rs.sort();
            assert_eq!(
                rs,
                vec![
                    vec![
                        2.into(),
                        2.into(),
                        "b".try_into().unwrap(),
                        "y".try_into().unwrap()
                    ]
                    .into(),
                    vec![
                        2.into(),
                        2.into(),
                        "b".try_into().unwrap(),
                        "z".try_into().unwrap()
                    ]
                    .into()
                ]
            );
        }
    }
}
