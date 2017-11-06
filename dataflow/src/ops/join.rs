use std::collections::HashMap;
use std::collections::HashSet;

use prelude::*;

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

    // Key column in the left and right parents respectively
    on: (usize, usize),

    // Which columns to emit. True means the column is from the left parent, false means from the
    // right
    emit: Vec<(bool, usize)>,

    // Which columns to emit when the left/right row is being modified in place. True means the
    // column is from the left parent, false means from the right
    in_place_left_emit: Vec<(bool, usize)>,
    in_place_right_emit: Vec<(bool, usize)>,

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
        let emit: Vec<_> = emit.into_iter()
            .map(|join_source| match join_source {
                JoinSource::L(c) => (true, c),
                JoinSource::R(c) => (false, c),
                JoinSource::B(lc, rc) => {
                    join_columns.push((lc, rc));
                    (true, lc)
                }
            })
            .collect();

        assert_eq!(join_columns.len(), 1, "only supports single column joins");
        let on = *join_columns.iter().next().unwrap();

        let (in_place_left_emit, in_place_right_emit) = {
            let compute_in_place_emit = |left| {
                let num_columns = emit.iter()
                    .filter(|&&(from_left, _)| from_left == left)
                    .map(|&(_, c)| c + 1)
                    .max()
                    .unwrap_or(0);

                // Tracks how columns have moved. At any point during the iteration, column i in
                // the original row will be located at position remap[i].
                let mut remap: Vec<_> = (0..num_columns).collect();
                emit.iter()
                    .enumerate()
                    .map(|(i, &(from_left, c))| {
                        if from_left == left {
                            let remapped = remap[c];
                            let other = remap.iter().position(|&c| c == i);

                            // Columns can't appear multiple times in join output!
                            assert!((remapped >= i) || (emit[remapped].0 != left));

                            remap[c] = i;
                            if let Some(other) = other {
                                remap[other] = remapped;
                            }

                            (from_left, remapped)
                        } else {
                            (from_left, c)
                        }
                    })
                    .collect::<Vec<_>>()
            };

            (compute_in_place_emit(true), compute_in_place_emit(false))
        };

        Self {
            left: left.into(),
            right: right.into(),
            on: on,
            emit: emit,
            in_place_left_emit,
            in_place_right_emit,
            kind: kind,
        }
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
            .map(|(i, &(from_left, col))| if from_left {
                if let Preprocessed::Left = reusing {
                    left[i].clone()
                } else {
                    left[col].clone()
                }
            } else {
                if let Preprocessed::Right = reusing {
                    right[i].clone()
                } else {
                    right[col].clone()
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
        for (i, &(from_left, c)) in emit.iter().enumerate() {
            if (from_left == reusing_left) && i != c {
                reuse.swap(i, c);
            }
        }
        for (i, &(from_left, c)) in emit.iter().enumerate() {
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

    // TODO: make non-allocating
    fn generate_null(&self, left: &[DataType]) -> Vec<DataType> {
        self.emit
            .iter()
            .map(|&(from_left, col)| if from_left {
                left[col].clone()
            } else {
                DataType::None
            })
            .collect()
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
        match self.kind {
            JoinType::Left => Some(Some(self.left.as_global()).into_iter().collect()),
            JoinType::Inner => Some(
                vec![self.left.as_global(), self.right.as_global()]
                    .into_iter()
                    .collect(),
            ),
        }
    }

    fn on_connected(&mut self, _g: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.left.remap(remap);
        self.right.remap(remap);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_col: Option<usize>,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        let mut misses = Vec::new();

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        let (other, from_key, other_key) = if from == *self.left {
            (*self.right, self.on.0, self.on.1)
        } else {
            (*self.left, self.on.1, self.on.0)
        };

        let replay_key_col = replay_key_col.map(|col| {
            match self.emit[col] {
                (true, l) if from == *self.left => l,
                (false, r) if from == *self.right => r,
                (true, l) if l == self.on.0 => {
                    // since we didn't hit the case above, we know that the message
                    // *isn't* from left.
                    self.on.1
                }
                (false, r) if r == self.on.1 => {
                    // same
                    self.on.0
                }
                _ => {
                    // we're getting a partial replay, but the replay key doesn't exist
                    // in the parent we're getting the replay from?!
                    unreachable!()
                }
            }
        });

        // First, we want to be smart about multiple added/removed rows with the same join key
        // value. For example, if we get a -, then a +, for the same key, we don't want to execute
        // two queries. We'll do this by sorting the batch by our join key.
        let mut rs: Vec<_> = rs.into();
        {
            let cmp = |a: &Record, b: &Record| a[from_key].cmp(&b[from_key]);
            rs.sort_by(cmp);
        }

        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());
        let mut at = 0;
        while at != rs.len() {
            let mut old_right_count = None;
            let mut new_right_count = None;
            let prev_join_key = rs[at][from_key].clone();

            if from == *self.right && self.kind == JoinType::Left {
                let rc = self.lookup(
                    *self.right,
                    &[self.on.1],
                    &KeyType::Single(&prev_join_key),
                    nodes,
                    state,
                ).unwrap();

                if rc.is_none() {
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
                        .position(|r| r[from_key] != prev_join_key)
                        .map(|p| at + p)
                        .unwrap_or(rs.len());
                    continue;
                } else {
                    let rc = rc.unwrap().count();
                    old_right_count = Some(rc);
                    new_right_count = Some(rc);
                }
            }

            // get rows from the other side
            let mut other_rows = self.lookup(
                other,
                &[other_key],
                &KeyType::Single(&prev_join_key),
                nodes,
                state,
            ).unwrap();

            if other_rows.is_none() {
                let replay_key = replay_key_col.map(|col| vec![rs[at][col].clone()]);
                at = rs[at..]
                    .iter()
                    .position(|r| r[from_key] != prev_join_key)
                    .map(|p| at + p)
                    .unwrap_or(rs.len());
                misses.push(Miss {
                    node: other,
                    columns: vec![other_key],
                    replay_key,
                    key: vec![prev_join_key],
                });
                continue;
            }

            let start = at;
            let mut make_null = None;
            if self.kind == JoinType::Left && from == *self.right {
                // If records are being received from the right, we need to find the number of
                // records that existed *before* this batch of records was processed so we know
                // whether or not to generate +/- NULL rows.
                if let Some(mut old_rc) = old_right_count {
                    while at != rs.len() && rs[at][from_key] == prev_join_key {
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
                    misses.push(Miss {
                        node: from,
                        columns: vec![self.on.1],
                        replay_key: replay_key_col.map(|col| vec![rs[at][col].clone()]),
                        key: vec![rs[at][self.on.1].clone()],
                    });
                    continue;
                }
            }

            if start == at {
                // we didn't find the end above, so find it now
                at = rs[at..]
                    .iter()
                    .position(|r| r[from_key] != prev_join_key)
                    .map(|p| at + p)
                    .unwrap_or(rs.len());
            }

            let mut other_rows_count = 0;
            for ri in start..at {
                use std::mem;

                // put something bogus in rs (which will be discarded anyway) so we can take r.
                let r = mem::replace(&mut rs[ri], Record::DeleteRequest(Vec::new()));
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
                    let mut other = other_rows.next().unwrap();
                    while other_rows.peek().is_some() {
                        if let Some(false) = make_null {
                            // we need to generate a -NULL for all these lefts
                            ret.push((self.generate_null(other), false).into());
                        }
                        if from == *self.left {
                            ret.push(
                                (
                                    self.generate_row(&row, other, Preprocessed::Neither),
                                    positive,
                                ).into(),
                            );
                        } else {
                            ret.push(
                                (
                                    self.generate_row(other, &row, Preprocessed::Neither),
                                    positive,
                                ).into(),
                            );
                        }
                        if let Some(true) = make_null {
                            // we need to generate a +NULL for all these lefts
                            ret.push((self.generate_null(other), true).into());
                        }
                        other = other_rows.next().unwrap();
                        other_rows_count += 1;
                    }

                    if let Some(false) = make_null {
                        // we need to generate a -NULL for the last left too
                        ret.push((self.generate_null(other), false).into());
                    }
                    ret.push(
                        (
                            self.regenerate_row(row, other, from == *self.left, false),
                            positive,
                        ).into(),
                    );
                    if let Some(true) = make_null {
                        // we need to generate a +NULL for the last left too
                        ret.push((self.generate_null(other), true).into());
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
                            ).into();
                            ret.push(r);
                        } else {
                            let r = (
                                self.generate_row(&ret[i], &row, Preprocessed::Left),
                                positive,
                            ).into();
                            ret.push(r);
                        }
                    }
                    let r = (
                        self.regenerate_row(row, &ret[end - 1], from == *self.left, true),
                        positive,
                    ).into();
                    ret.push(r);
                }
            }
        }

        ProcessingResult {
            results: ret.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, _this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        vec![
            (self.left.as_global(), (vec![self.on.0], true)),
            (self.right.as_global(), (vec![self.on.1], true)),
        ].into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        let e = self.emit[col];
        if e.0 {
            Some(vec![(self.left.as_global(), e.1)])
        } else {
            Some(vec![(self.right.as_global(), e.1)])
        }
    }

    fn description(&self) -> String {
        let emit = self.emit
            .iter()
            .map(|&(from_left, col)| {
                let src = if from_left { self.left } else { self.right };
                format!("{}:{}", src.as_global().index(), col)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let op = match self.kind {
            JoinType::Left => "⋉",
            JoinType::Inner => "⋈",
        };

        format!(
            "[{}] {}:{} {} {}:{}",
            emit,
            self.left.as_global().index(),
            self.on.0,
            op,
            self.right.as_global().index(),
            self.on.1
        )
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeIndex, Option<usize>)> {
        let pcol = self.emit[col];
        if (pcol.0 && pcol.1 == self.on.0) || (!pcol.0 && pcol.1 == self.on.1) {
            // Join column comes from both parents
            vec![
                (self.left.as_global(), Some(self.on.0)),
                (self.right.as_global(), Some(self.on.1)),
            ]
        } else {
            vec![
                (
                    if pcol.0 { &self.left } else { &self.right }.as_global(),
                    Some(pcol.1),
                ),
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1"]);

        use JoinSource::*;
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
            j.node().description(),
            format!("[{}:0, {}:1, {}:1] {}:0 ⋉ {}:0", l, l, r, l, r)
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

        j.one_row(r, r_x1.clone(), false);
        j.one_row(r, r_y1.clone(), false);
        j.one_row(r, r_z2.clone(), false);

        // forward c3 from left; should produce [c3 + None] since no records in right are 3
        let null = vec![((vec![3.into(), "c".into(), DataType::None], true))].into();
        j.seed(l, l_c3.clone());
        let rs = j.one_row(l, l_c3.clone(), false);
        assert_eq!(rs, null);

        // doing it again should produce the same result
        j.seed(l, l_c3.clone());
        let rs = j.one_row(l, l_c3.clone(), false);
        assert_eq!(rs, null);

        // record from the right should revoke the nulls and replace them with full rows
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3.clone(), false);
        assert_eq!(
            rs,
            vec![
                (vec![3.into(), "c".into(), DataType::None], false),
                (vec![3.into(), "c".into(), "w".into()], true),
                (vec![3.into(), "c".into(), DataType::None], false),
                (vec![3.into(), "c".into(), "w".into()], true),
            ].into()
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
            ].into()
        );

        // forward from left with single matching record on right
        j.seed(l, l_b2.clone());
        let rs = j.one_row(l, l_b2.clone(), false);
        assert_eq!(
            rs,
            vec![((vec![2.into(), "b".into(), "z".into()], true))].into()
        );

        // forward from left with two matching records on right
        j.seed(l, l_a1.clone());
        let rs = j.one_row(l, l_a1.clone(), false);
        assert_eq!(
            rs,
            vec![
                ((vec![1.into(), "a".into(), "x".into()], true)),
                ((vec![1.into(), "a".into(), "y".into()], true)),
            ].into()
        );

        // forward from right with two matching records on left (and one more on right)
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3.clone(), false);
        assert_eq!(
            rs,
            vec![
                ((vec![3.into(), "c".into(), "w".into()], true)),
                ((vec![3.into(), "c".into(), "w".into()], true)),
            ].into()
        );

        // unmatched forward from right should have no effect
        j.seed(r, r_v4.clone());
        let rs = j.one_row(r, r_v4.clone(), false);
        assert_eq!(rs.len(), 0);
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let me = 2.into();
        let (g, l, r) = setup();
        let hm: HashMap<_, _> = vec![
            (l.as_global(), (vec![0], true)), /* join column for left */
            (r.as_global(), (vec![0], true)), /* join column for right */
        ].into_iter()
            .collect();
        assert_eq!(g.node().suggest_indexes(me), hm);
    }

    #[test]
    fn it_resolves() {
        let (g, l, r) = setup();
        assert_eq!(g.node().resolve(0), Some(vec![(l.as_global(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(l.as_global(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(r.as_global(), 1)]));
    }
}
