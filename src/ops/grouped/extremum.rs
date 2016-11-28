use query;

use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use flow::prelude::*;

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
    pub fn over<'a>(self,
                    src: NodeAddress,
                    over: usize,
                    group_by: &[usize])
                    -> GroupedOperator<'a, ExtremumOperator> {
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

    fn setup(&mut self, parent: &Node) {
        assert!(self.over < parent.fields().len(),
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

    fn apply(&self, current: &Option<query::DataType>, diffs: Vec<Self::Diff>) -> query::DataType {
        // Extreme values are those that are at least as extreme as the current min/max (if any).
        // let mut is_extreme_value : Box<Fn(i64) -> bool> = Box::new(|_|true);
        let mut extreme_values: Vec<i64> = vec![];
        if let &Some(query::DataType::Number(n)) = current {
            extreme_values.push(n);
        };

        let is_extreme_value = |x: i64| if let &Some(query::DataType::Number(n)) = current {
            match self.op {
                Extremum::MAX => x >= n,
                Extremum::MIN => x <= n,
            }
        } else {
            true
        };

        for d in diffs {
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
    use query;
    use shortcut;

    fn setup(mat: bool, wide: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = if wide {
            g.add_base("source", &["x", "y", "z"])
        } else {
            g.add_base("source", &["x", "y"])
        };
        if wide {
            g.seed(s, vec![1.into(), 4.into(), 1.into()]);
            g.seed(s, vec![2.into(), 1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into(), 1.into()]);
            g.seed(s, vec![1.into(), 2.into(), 1.into()]);
            g.seed(s, vec![1.into(), 7.into(), 1.into()]);
            g.seed(s, vec![1.into(), 0.into(), 1.into()]);
        } else {
            g.seed(s, vec![1.into(), 4.into()]);
            g.seed(s, vec![2.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into()]);
            g.seed(s, vec![1.into(), 2.into()]);
            g.seed(s, vec![1.into(), 7.into()]);
            g.seed(s, vec![1.into(), 0.into()]);
        }

        if wide {
            g.set_op("agg", &["x", "z", "ys"], Extremum::MAX.over(s, 1, &[0, 2]));
        } else {
            g.set_op("agg", &["x", "ys"], Extremum::MAX.over(s, 1, &[0]));
        }
        if mat {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(true, false);

        let check_first_max =
            |group, val, out: Option<ops::Update>| if let Some(ops::Update::Records(rs)) = out {
                assert_eq!(rs.len(), 1);

                match rs.into_iter().next().unwrap() {
                    ops::Record::Positive(r) => {
                        assert_eq!(r[0], group);
                        assert_eq!(r[1], val);
                    }
                    _ => unreachable!(),
                }
            } else {
                unreachable!()
            };

        let check_new_max = |group, old, new, out: Option<ops::Update>| {
            if let Some(ops::Update::Records(rs)) = out {
                assert_eq!(rs.len(), 2);
                let mut rs = rs.into_iter();

                match rs.next().unwrap() {
                    ops::Record::Negative(r) => {
                        assert_eq!(r[0], group);
                        assert_eq!(r[1], old);
                    }
                    _ => unreachable!(),
                }
                match rs.next().unwrap() {
                    ops::Record::Positive(r) => {
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
        let out = c.narrow_one_row(vec![1.into(), 4.into()], true);
        check_first_max(1.into(), 4.into(), out);

        // Larger value should also trigger an update.
        let out = c.narrow_one_row(vec![1.into(), 7.into()], true);
        check_new_max(1.into(), 4.into(), 7.into(), out);

        // No change if new value isn't the max.
        match c.narrow_one_row(vec![1.into(), 2.into()], true) {
            Some(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }

        // Insertion into a different group should be independent.
        let out = c.narrow_one_row(vec![2.into(), 3.into()], true);
        check_first_max(2.into(), 3.into(), out);

        // Larger than last value, but not largest in group.
        match c.narrow_one_row(vec![1.into(), 5.into()], true) {
            Some(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }

        // One more new max.
        let out = c.narrow_one_row(vec![1.into(), 22.into()], true);
        check_new_max(1.into(), 7.into(), 22.into(), out);

        // Negative for old max should be fine if there is a positive for a larger value.
        let u = ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 22.into()]),
                                          ops::Record::Positive(vec![1.into(), 23.into()])]);
        let out = c.narrow_one(u, true);
        check_new_max(1.into(), 22.into(), 23.into(), out);

        // Competing positive and negative should cancel out.
        let u = ops::Update::Records(vec![ops::Record::Positive(vec![1.into(), 24.into()]),
                                          ops::Record::Negative(vec![1.into(), 24.into()])]);
        match c.narrow_one(u, true) {
            Some(ops::Update::Records(rs)) => assert!(rs.is_empty()),
            _ => unreachable!(),
        }
    }

    // TODO: also test MIN

    #[test]
    fn it_queries() {
        let c = setup(false, false);

        let hits = c.query(None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 7.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let val = shortcut::Comparison::Equal(shortcut::Value::new(query::DataType::from(2)));
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                                           column: 0,
                                           cmp: val,
                                       }]);

        let hits = c.query(Some(&q));
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_queries_zeros() {
        let c = setup(false, false);

        let val = shortcut::Comparison::Equal(shortcut::Value::new(query::DataType::from(100)));
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                                           column: 0,
                                           cmp: val,
                                       }]);

        let hits = c.query(Some(&q));
        assert!(hits.is_empty());
    }

    #[test]
    fn it_suggests_indices() {
        let c = setup(false, true);
        let idx = c.node().suggest_indexes(1.into());

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&1.into()));

        // should only index on group-by columns
        assert_eq!(idx[&1.into()].len(), 2);
        assert!(idx[&1.into()].iter().any(|&i| i == 0));
        assert!(idx[&1.into()].iter().any(|&i| i == 1));
        // specifically, not last column, which is output
    }

    #[test]
    fn it_resolves() {
        let c = setup(false, true);
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), Some(vec![(c.narrow_base_id(), 2)]));
        assert_eq!(c.node().resolve(2), None);
    }
}
