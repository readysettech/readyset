use ops;
use query;
use flow::NodeIndex;

use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

/// Supported kinds of extremum operators.
#[derive(Debug)]
pub enum Extremum {
    /// The minimum value that occurs in the `over` column in each group.
    MIN,
    /// The maximum value of the `over` column for all records of each group.
    MAX,
}

impl Extremum {
    /// Construct a new `ExtremumOperator` that performs this operation.
    ///
    /// The aggregation will be aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier. The `over` column should not be in the `group_by` array.
    pub fn over(self,
                src: NodeIndex,
                over: usize,
                group_by: &[usize])
                -> GroupedOperator<ExtremumOperator> {
        assert!(!group_by.iter().any(|&i| i == over),
                "cannot group by aggregation column");
        GroupedOperator::new(src,
                             ExtremumOperator {
                                 op: self,
                                 over: over,
                                 group: group_by.into(),
                             })
    }
}

/// ExtremumOperator implementas a Soup node that performans common aggregation operations such as
/// counts and sums.
///
/// `ExtremumOperator` nodes are constructed through `Extremum` variants using `Extremum::new`.
///
/// Logically, the aggregated value for all groups start out as `0`. Thus, when the first record is
/// received for a group, `ExtremumOperator` will output a negative for the *zero row*, followed by
/// a positive for the newly aggregated value.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column (`self.over`)
/// of the incoming record is then added to the current aggregation value according to the operator
/// in use (`COUNT` always adds/subtracts 1, `SUM` adds/subtracts the value of the value in the
/// incoming record. The output record is constructed by concatenating the columns identifying the
/// group, and appending the aggregated value. For example, for a sum with `self.over == 1`, a
/// previous sum of `3`, and an incoming record with `[a, 1, x]`, the output would be `[a, x, 4]`.
#[derive(Debug)]
pub struct ExtremumOperator {
    op: Extremum,
    over: usize,
    group: Vec<usize>,
}

pub enum DiffType {
    Insert(i64),
    Remove(i64),
}

impl GroupedOperation for ExtremumOperator {
    type Diff = DiffType;

    fn setup(&mut self, parent: &ops::V) {
        assert!(self.over < parent.args().len(),
                "cannot aggregate over non-existing column");
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn zero(&self) -> Option<query::DataType> {
        None
    }

    fn to_diff(&self, r: &[query::DataType], pos: bool) -> Self::Diff {
        let v = if let query::DataType::Number(n) = r[self.over] {
            n
        } else {
            unreachable!();
        };

        if pos {
            DiffType::Insert(v)
        } else {
            DiffType::Remove(v)
        }
    }

    fn apply(&self,
             current: &Option<query::DataType>,
             diffs: Vec<(Self::Diff, i64)>)
             -> query::DataType {
        // Extreme values are those that are at least as extreme as the current min/max (if any).
        // let mut is_extreme_value : Box<Fn(i64) -> bool> = Box::new(|_|true);
        let mut extreme_values: Vec<i64> = vec![];
        if let &Some(query::DataType::Number(n)) = current {
            extreme_values.push(n);
        };

        let is_extreme_value = |x: i64| {
            if let &Some(query::DataType::Number(n)) = current {
                match self.op {
                    Extremum::MAX => x >= n,
                    Extremum::MIN => x <= n,
                }
            } else {
                true
            }
        };

        for (d, _) in diffs {
            match d {
                DiffType::Insert(v) if is_extreme_value(v) => extreme_values.push(v),
                DiffType::Remove(v) if is_extreme_value(v) => {
                    if let Some(i) = extreme_values.iter().position(|x: &i64| *x == v) {
                        extreme_values.swap_remove(i);
                    }
                }
                _ => {}
            };
        }

        let extreme = match self.op {
            Extremum::MIN => extreme_values.into_iter().min(),
            Extremum::MAX => extreme_values.into_iter().max(),
        };

        if let Some(extreme) = extreme {
            return extreme.into();
        }

        // TODO: handle this case by querying into the parent.
        unimplemented!();
    }
    fn description(&self) -> String {
        let op_string = match self.op {
            Extremum::MIN => format!("min({})", self.over),
            Extremum::MAX => format!("max({})", self.over),
        };
        let group_cols = self.group
            .iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} γ[{}]", op_string, group_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use petgraph;
    use shortcut;

    use flow::View;
    use ops::NodeOp;

    fn setup(mat: bool, wide: bool) -> ops::Node {
        use std::sync;
        use flow::View;

        let mut g = petgraph::Graph::new();
        let mut s = if wide {
            ops::new("source", &["x", "y", "z"], true, ops::base::Base {})
        } else {
            ops::new("source", &["x", "y"], true, ops::base::Base {})
        };

        s.prime(&g);
        let s = g.add_node(Some(sync::Arc::new(s)));

        let mut c = if wide {
            Extremum::MAX.over(s, 1, &[0, 2])
        } else {
            Extremum::MAX.over(s, 1, &[0])
        };
        c.prime(&g);

        let node = if wide {
            ops::new("agg", &["x", "z", "ys"], mat, c)
        } else {
            ops::new("agg", &["x", "ys"], mat, c)
        };

        g[s].as_ref().unwrap().process(Some((vec![1.into(), 4.into()], 0).into()), s, 0, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 1.into()], 1).into()), s, 1, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 2.into()], 2).into()), s, 2, true);
        g[s].as_ref().unwrap().process(Some((vec![1.into(), 2.into()], 3).into()), s, 3, true);
        g[s].as_ref().unwrap().process(Some((vec![1.into(), 7.into()], 4).into()), s, 4, true);
        g[s].as_ref().unwrap().process(Some((vec![1.into(), 0.into()], 5).into()), s, 5, true);
        node
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let c = setup(true, false);

        let check_first_max = |group, val, out: flow::ProcessingResult<_>| {
            if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
                assert_eq!(rs.len(), 1);

                match rs.into_iter().next().unwrap() {
                    ops::Record::Positive(r, _) => {
                        assert_eq!(r[0], group);
                        assert_eq!(r[1], val);
                    }
                    _ => unreachable!(),
                }
            } else {
                unreachable!()
            }
        };

        let check_new_max = |group, old, new, out: flow::ProcessingResult<_>| {
            if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
                assert_eq!(rs.len(), 2);
                let mut rs = rs.into_iter();

                match rs.next().unwrap() {
                    ops::Record::Negative(r, _) => {
                        assert_eq!(r[0], group);
                        assert_eq!(r[1], old);
                    }
                    _ => unreachable!(),
                }
                match rs.next().unwrap() {
                    ops::Record::Positive(r, _) => {
                        assert_eq!(r[0], group);
                        assert_eq!(r[1], new);
                    }
                    _ => unreachable!(),
                }
            } else {
                unreachable!()
            }
        };

        // First insertion should trigger an update.
        let out = c.process(Some((vec![1.into(), 4.into()], 0).into()), src, 0, true);
        check_first_max(1.into(), 4.into(), out);

        // Larger value should also trigger an update.
        let out = c.process(Some((vec![1.into(), 7.into()], 1).into()), src, 1, true);
        check_new_max(1.into(), 4.into(), 7.into(), out);

        // No change if new value isn't the max.
        match c.process(Some((vec![1.into(), 2.into()], 2).into()), src, 2, true) {
            flow::ProcessingResult::Done(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }

        // Insertion into a different group should be independent.
        let out = c.process(Some((vec![2.into(), 3.into()], 3).into()), src, 3, true);
        check_first_max(2.into(), 3.into(), out);

        // Larger than last value, but not largest in group.
        match c.process(Some((vec![1.into(), 5.into()], 4).into()), src, 4, true) {
            flow::ProcessingResult::Done(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }

        // One more new max.
        let out = c.process(Some((vec![1.into(), 22.into()], 5).into()), src, 5, true);
        check_new_max(1.into(), 7.into(), 22.into(), out);

        // Negative for old max should be fine if there is a positive for a larger value.
        let u = ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 22.into()], 1),
                                          ops::Record::Positive(vec![1.into(), 23.into()], 5)]);
        let out = c.process(Some(u), src, 6, true);
        check_new_max(1.into(), 22.into(), 23.into(), out);

        // Competing positive and negative should cancel out.
        let u = ops::Update::Records(vec![ops::Record::Positive(vec![1.into(), 24.into()], 5),
                                          ops::Record::Negative(vec![1.into(), 24.into()], 1)]);
        match c.process(Some(u), src, 6, true) {
            flow::ProcessingResult::Done(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }
    }

    // TODO: also test MIN

    #[test]
    fn it_queries() {
        let c = setup(false, false);

        let hits = c.find(None, None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 7.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = c.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_queries_zeros() {
        let c = setup(false, false);

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(100.into())),
                         }]);

        let hits = c.find(Some(&q), None);
        assert!(hits.is_empty());
    }

    #[test]
    fn it_suggests_indices() {
        let c = setup(false, true);
        let idx = c.suggest_indexes(1.into());

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&1.into()));

        // should only index on group-by columns
        assert_eq!(idx[&1.into()].len(), 2);
        assert!(idx[&1.into()].iter().any(|&i| i == 0));
        assert!(idx[&1.into()].iter().any(|&i| i == 2));
    }

    #[test]
    fn it_resolves() {
        let c = setup(false, true);
        assert_eq!(c.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(c.resolve(1), Some(vec![(0.into(), 2)]));
        assert_eq!(c.resolve(2), None);
    }
}
