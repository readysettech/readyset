use std::collections::HashMap;
use std::collections::HashSet;

use flow::prelude::*;

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

    // Stores number of records on the right with each key before and after applying the new records
    // within `on_input()`. Used to avoid creating a new HashMap for every call to `on_input()`, but
    // is never used except for in that function.
    right_counts: HashMap<DataType, (usize, usize)>,

    kind: JoinType,
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
        let on = *join_columns.iter().next().unwrap();

        let (in_place_left_emit, in_place_right_emit) = {
            let compute_in_place_emit = |left| {
                let num_columns = emit.iter()
                    .filter(|&&(from_left, _)| from_left == left)
                    .map(|&(_, c)| c + 1)
                    .max()
                    .unwrap_or(0);

                /// Tracks how columns have moved. At any point during the iteration, column i in
                /// the original row will be located at position remap[i].
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
            right_counts: HashMap::new(),
            kind: kind,
        }
    }

    fn generate_row(&self, left: &Vec<DataType>, right: &Vec<DataType>) -> Vec<DataType> {
        self.emit
            .iter()
            .map(|&(from_left, col)| if from_left {
                left[col].clone()
            } else {
                right[col].clone()
            })
            .collect()
    }

    fn generate_row_from_left(
        &self,
        mut left: Vec<DataType>,
        right: &Vec<DataType>,
    ) -> Vec<DataType> {
        left.resize(self.in_place_left_emit.len(), DataType::None);
        for (i, &(from_left, c)) in self.in_place_left_emit.iter().enumerate() {
            if from_left && i != c {
                left.swap(i, c);
            }
        }
        for (i, &(from_left, c)) in self.in_place_left_emit.iter().enumerate() {
            if !from_left {
                left[i] = right[c].clone();
            }
        }
        left
    }

    fn generate_row_from_right(
        &self,
        left: &Vec<DataType>,
        mut right: Vec<DataType>,
    ) -> Vec<DataType> {
        right.resize(self.in_place_right_emit.len(), DataType::None);
        for (i, &(from_left, c)) in self.in_place_right_emit.iter().enumerate() {
            if !from_left && i != c{
                right.swap(i, c);
            }
        }
        for (i, &(from_left, c)) in self.in_place_right_emit.iter().enumerate() {
            if from_left {
                right[i] = left[c].clone();
            }
        }
        right
    }

    fn generate_null(&self, left: &Vec<DataType>) -> Vec<DataType> {
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

    fn should_materialize(&self) -> bool {
        false
    }

    fn is_join(&self) -> bool {
        true
    }

    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        match self.kind {
            JoinType::Left => Some(Some(self.left.as_global()).into_iter().collect()),
            JoinType::Inner => {
                Some(
                    vec![self.left.as_global(), self.right.as_global()]
                        .into_iter()
                        .collect(),
                )
            }
        }
    }

    fn will_query(&self, _: bool) -> bool {
        true
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
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        let mut misses = Vec::new();

        if from == *self.right && self.kind == JoinType::Left {
            // If records are being received from the right, then populate self.right_counts
            // with the number of records that existed for each key *before* this batch of
            // records was processed.
            self.right_counts.clear();
            for r in rs.iter() {
                let ref key = r.rec()[self.on.1];
                let adjust = |rc| if r.is_positive() { rc - 1 } else { rc + 1 };

                if let Some(&mut (ref mut rc, _)) = self.right_counts.get_mut(&key) {
                    *rc = adjust(*rc);
                    continue;
                }

                let rc = self.lookup(
                    *self.right,
                    &[self.on.0],
                    &KeyType::Single(&key),
                    nodes,
                    state,
                ).unwrap();
                if rc.is_none() {
                    // we got something from right, but that row's key is not in right??
                    unreachable!();
                }
                let rc = rc.unwrap().count();
                self.right_counts.insert(key.clone(), (adjust(rc), rc));
            }

            self.right_counts
                .retain(|_, &mut (before, after)| (before == 0) != (after == 0));
        }

        let (other, from_key, other_key) = if from == *self.left {
            (*self.right, self.on.0, self.on.1)
        } else {
            (*self.left, self.on.1, self.on.0)
        };

        // okay, so here's what's going on:
        // the record(s) we receive are all from one side of the join. we need to query the
        // other side(s) for records matching the incoming records on that side's join
        // fields.
        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());
        for rec in rs {
            let (row, positive) = rec.extract();

            let other_rows = self.lookup(
                other,
                &[other_key],
                &KeyType::Single(&row[from_key]),
                nodes,
                state,
            ).unwrap();
            if other_rows.is_none() {
                misses.push(Miss {
                    node: other,
                    key: vec![row[from_key].clone()],
                });
                continue;
            }
            let mut other_rows = other_rows.unwrap().peekable();

            if self.kind == JoinType::Left {
                // emit null rows if necessary for left join
                if from == *self.right {
                    let rc = if let Some(&mut (ref mut rc, _)) =
                        self.right_counts.get_mut(&row[self.on.0])
                    {
                        if positive {
                            *rc += 1;
                        } else {
                            *rc -= 1;
                        }
                        Some(*rc)
                    } else {
                        None
                    };

                    if let Some(rc) = rc {
                        if (positive && rc == 1) || (!positive && rc == 0) {
                            ret.extend(other_rows.flat_map(|r| -> Vec<Record> {
                                let foo: Records = vec![
                                    (self.generate_null(r), !positive),
                                    (self.generate_row(r, &row), positive),
                                ].into();
                                foo.into()
                            }));
                            continue;
                        }
                    }
                } else if other_rows.peek().is_none() {
                    ret.push((self.generate_null(&row), positive).into());
                }
            }

            if from == *self.right && other_rows.peek().is_some() {
                let mut r = other_rows.next().unwrap();
                while other_rows.peek().is_some() {
                    ret.push((self.generate_row(r, &row), positive).into());
                    r = other_rows.next().unwrap();
                }
                ret.push((self.generate_row_from_right(r, row), positive).into());
            } else if other_rows.peek().is_some() {
                let mut r = other_rows.next().unwrap();
                while other_rows.peek().is_some() {
                    ret.push((self.generate_row(&row, r), positive).into());
                    r = other_rows.next().unwrap();
                }
                ret.push((self.generate_row_from_left(row, r), positive).into());
            }
        }

        ProcessingResult {
            results: ret.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, _this: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        vec![
            (self.left.as_global(), vec![self.on.0]),
            (self.right.as_global(), vec![self.on.1]),
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
        if (pcol.0 && pcol.1 == self.on.0) || (pcol.0 && pcol.1 == self.on.1) {
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
            (l.as_global(), vec![0]), /* join column for left */
            (r.as_global(), vec![0]), /* join column for right */
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
