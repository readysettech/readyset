use std::collections::HashMap;
use std::collections::HashSet;

use std::sync::Arc;

use flow::prelude::*;

/// Kind of join
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone)]
pub struct Join {
    left: NodeAddress,
    right: NodeAddress,

    // Key column in the left and right parents respectively
    on: (usize, usize),

    // Which columns to emit. True means the column is from the left parent, false means from the
    // right
    emit: Vec<(bool, usize)>,

    // Stores number of records on the right with each key. Used to avoid creating a new HashMap for
    // every call to `on_input()`, but is empty except for in that function.
    right_counts: HashMap<DataType, usize>,

    kind: JoinType,
}

impl Join {
    /// Create a new instance of Join
    ///
    /// `left` and `right` are the left and right parents respectively. `on` is a tuple specifying
    /// the join columns: (left_parent_column, right_parent_column) and `emit` dictates for each
    /// output colunm, which source and column should be used (true means left parent, and false
    /// means right parent).
    pub fn new(left: NodeAddress,
               right: NodeAddress,
               kind: JoinType,
               emit: Vec<JoinSource>)
               -> Self {
        let mut join_columns = Vec::new();
        let emit = emit.into_iter()
            .map(|join_source| match join_source {
                     JoinSource::L(c) => (true, c),
                     JoinSource::R(c) => (false, c),
                     JoinSource::B(lc, rc) => {
                         join_columns.push((lc, rc));
                         (true, lc)
                     }
                 })
            .collect();

        assert_eq!(join_columns.len(), 1);
        let on = *join_columns.iter().next().unwrap();

        Self {
            left: left,
            right: right,
            on: on,
            emit: emit,
            right_counts: HashMap::new(),
            kind: kind,
        }
    }
    fn generate_row(&self, left: &Arc<Vec<DataType>>, right: &Arc<Vec<DataType>>) -> Vec<DataType> {
        self.emit
            .iter()
            .map(|&(from_left, col)| if from_left {
                     left[col].clone()
                 } else {
                     right[col].clone()
                 })
            .collect()
    }

    fn generate_null(&self, left: &Arc<Vec<DataType>>) -> Vec<DataType> {
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

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.left, self.right]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn is_join(&self) -> bool {
        true
    }

    fn must_replay_among(&self) -> Option<HashSet<NodeAddress>> {
        match self.kind {
            JoinType::Left => Some(Some(self.left).into_iter().collect()),
            JoinType::Inner => Some(vec![self.left, self.right].into_iter().collect()),
        }
    }

    fn will_query(&self, _: bool) -> bool {
        true
    }

    fn on_connected(&mut self, _g: &Graph) {}

    fn on_commit(&mut self, _: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        if let Some(left) = remap.get(&self.left) {
            self.left = left.clone();
        }

        if let Some(right) = remap.get(&self.right) {
            self.right = right.clone();
        }
    }

    fn on_input(&mut self,
                from: NodeAddress,
                rs: Records,
                _: &mut Tracer,
                nodes: &DomainNodes,
                state: &StateMap)
                -> ProcessingResult {
        let mut misses = Vec::new();

        if from == self.right && self.kind == JoinType::Left {
            // If records are being received from the right, then populate self.right_counts
            // with the number of records that existed for each key *before* this batch of
            // records was processed.
            self.right_counts.clear();
            for r in rs.iter() {
                let ref key = r.rec()[self.on.1];
                let adjust = |rc| if r.is_positive() { rc - 1 } else { rc + 1 };

                if let Some(rc) = self.right_counts.get_mut(&key) {
                    *rc = adjust(*rc);
                    continue;
                }

                let rc = self.lookup(self.right,
                                     &[self.on.0],
                                     &KeyType::Single(&key),
                                     nodes,
                                     state)
                    .unwrap();
                if rc.is_none() {
                    // we got something from right, but that row's key is not in right??
                    unreachable!();
                }
                self.right_counts
                    .insert(key.clone(), adjust(rc.unwrap().count()));
            }
        }

        let (other, from_key, other_key) = if from == self.left {
            (self.right, self.on.0, self.on.1)
        } else {
            (self.left, self.on.1, self.on.0)
        };

        // okay, so here's what's going on:
        // the record(s) we receive are all from one side of the join. we need to query the
        // other side(s) for records matching the incoming records on that side's join
        // fields.
        let mut ret: Vec<Record> = Vec::with_capacity(rs.len());
        for rec in rs.iter() {
            let (row, positive) = match *rec {
                Record::Positive(ref row) => (row, true),
                Record::Negative(ref row) => (row, false),
                _ => unreachable!(),
            };

            let other_rows = self.lookup(other,
                                         &[other_key],
                                         &KeyType::Single(&row[from_key]),
                                         nodes,
                                         state)
                .unwrap();
            if other_rows.is_none() {
                misses.push(Miss {
                                node: *other.as_local(),
                                key: vec![row[from_key].clone()],
                            });
                continue;
            }
            let mut other_rows = other_rows.unwrap().peekable();

            if self.kind == JoinType::Left {
                // emit null rows if necessary for left join
                if from == self.right {
                    let rc = {
                        let rc = self.right_counts.get_mut(&row[self.on.0]).unwrap();
                        if positive {
                            *rc += 1;
                        } else {
                            *rc -= 1;
                        }
                        *rc
                    };

                    if (positive && rc == 1) || (!positive && rc == 0) {
                        ret.extend(other_rows.flat_map(|r| -> Vec<Record> {
                            let foo: Records = vec![(self.generate_null(r), !positive),
                                                    (self.generate_row(r, &row), positive)]
                                    .into();
                            foo.into()
                        }));
                        continue;
                    }
                } else if other_rows.peek().is_none() {
                    ret.push((self.generate_null(&row), positive).into());
                }
            }

            if from == self.right {
                ret.extend(other_rows.map(|r| (self.generate_row(r, &row), positive).into()));
            } else {
                ret.extend(other_rows.map(|r| (self.generate_row(&row, r), positive).into()));
            }
        }

        ProcessingResult {
            results: ret.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, _this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        vec![(self.left, vec![self.on.0]), (self.right, vec![self.on.1])]
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        let e = self.emit[col];
        if e.0 {
            Some(vec![(self.left, e.1)])
        } else {
            Some(vec![(self.right, e.1)])
        }
    }

    fn description(&self) -> String {
        let emit = self.emit
            .iter()
            .map(|&(from_left, col)| {
                     let src = if from_left { self.left } else { self.right };
                     format!("{}:{}", src, col)
                 })
            .collect::<Vec<_>>()
            .join(", ");

        let op = match self.kind {
            JoinType::Left => "⋉",
            JoinType::Inner => "⋈",
        };

        format!("[{}] {}:{} {} {}:{}",
                emit,
                self.left,
                self.on.0,
                op,
                self.right,
                self.on.1)
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeAddress, Option<usize>)> {
        let pcol = self.emit[col];
        if (pcol.0 && pcol.1 == self.on.0) || (pcol.0 && pcol.1 == self.on.1) {
            // Join column comes from both parents
            vec![(self.left, Some(self.on.0)), (self.right, Some(self.on.1))]
        } else {
            vec![(if pcol.0 { self.left } else { self.right }, Some(pcol.1))]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, NodeAddress, NodeAddress) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1"]);

        use JoinSource::*;
        let j = Join::new(l, r, JoinType::Left, vec![B(0, 0), L(1), R(1)]);

        g.set_op("join", &["j0", "j1", "j2"], j, false);
        let (l, r) = (g.to_local(l), g.to_local(r));
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (j, l, r) = setup();
        assert_eq!(j.node().description(),
                   format!("[{}:0, {}:1, {}:1] {}:0 ⋉ {}:0", l, l, r, l, r));
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
        assert_eq!(rs,
                   vec![((vec![3.into(), "c".into(), DataType::None], false)),
                        ((vec![3.into(), "c".into(), "w".into()], true)),
                        ((vec![3.into(), "c".into(), DataType::None], false)),
                        ((vec![3.into(), "c".into(), "w".into()], true))]
                           .into());

        // forward from left with single matching record on right
        j.seed(l, l_b2.clone());
        let rs = j.one_row(l, l_b2.clone(), false);
        assert_eq!(rs,
                   vec![((vec![2.into(), "b".into(), "z".into()], true))].into());

        // forward from left with two matching records on right
        j.seed(l, l_a1.clone());
        let rs = j.one_row(l, l_a1.clone(), false);
        assert_eq!(rs,
                   vec![((vec![1.into(), "a".into(), "x".into()], true)),
                        ((vec![1.into(), "a".into(), "y".into()], true))]
                           .into());

        // forward from right with two matching records on left (and one more on right)
        j.seed(r, r_w3.clone());
        let rs = j.one_row(r, r_w3.clone(), false);
        assert_eq!(rs,
                   vec![((vec![3.into(), "c".into(), "w".into()], true)),
                        ((vec![3.into(), "c".into(), "w".into()], true))]
                           .into());

        // unmatched forward from right should have no effect
        j.seed(r, r_v4.clone());
        let rs = j.one_row(r, r_v4.clone(), false);
        assert_eq!(rs.len(), 0);
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let me = NodeAddress::mock_global(2.into());
        let (g, l, r) = setup();
        let hm: HashMap<_, _> = vec![(l, vec![0]), /* join column for left */
                                     (r, vec![0]) /* join column for right */]
                .into_iter()
                .collect();
        assert_eq!(g.node().suggest_indexes(me), hm);
    }

    #[test]
    fn it_resolves() {
        let (g, l, r) = setup();
        assert_eq!(g.node().resolve(0), Some(vec![(l, 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(l, 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(r, 1)]));
    }
}
