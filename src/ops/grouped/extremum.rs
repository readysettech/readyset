use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use flow::prelude::*;

/// Supported kinds of extremum operators.
#[derive(Debug, Clone)]
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
                src: NodeAddress,
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

/// `ExtremumOperator` implementas a Soup node that performans common aggregation operations such
/// as counts and sums.
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
#[derive(Debug, Clone)]
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

    fn zero(&self) -> Option<DataType> {
        None
    }

    fn to_diff(&self, r: &[DataType], pos: bool) -> Self::Diff {
        let v = if let DataType::Number(n) = r[self.over] {
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

    fn apply(&self, current: Option<&DataType>, diffs: Vec<Self::Diff>) -> DataType {
        // Extreme values are those that are at least as extreme as the current min/max (if any).
        // let mut is_extreme_value : Box<Fn(i64) -> bool> = Box::new(|_|true);
        let mut extreme_values: Vec<i64> = vec![];
        if let Some(&DataType::Number(n)) = current {
            extreme_values.push(n);
        };

        let is_extreme_value = |x: i64| if let Some(&DataType::Number(n)) = current {
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

    fn setup(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        g.set_op("agg", &["x", "ys"], Extremum::MAX.over(s, 1, &[0]), mat);
        g
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(true);

        let check_first_max = |group, val, rs: ops::Records| {
            assert_eq!(rs.len(), 1);

            match rs.into_iter().next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], group);
                    assert_eq!(r[1], val);
                }
                _ => unreachable!(),
            }
        };

        let check_new_max = |group, old, new, rs: ops::Records| {
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
        };

        // First insertion should trigger an update.
        let out = c.narrow_one_row(vec![1.into(), 4.into()], true);
        check_first_max(1.into(), 4.into(), out);

        // Larger value should also trigger an update.
        let out = c.narrow_one_row(vec![1.into(), 7.into()], true);
        check_new_max(1.into(), 4.into(), 7.into(), out);

        // No change if new value isn't the max.
        let rs = c.narrow_one_row(vec![1.into(), 2.into()], true);
        assert!(rs.is_empty());

        // Insertion into a different group should be independent.
        let out = c.narrow_one_row(vec![2.into(), 3.into()], true);
        check_first_max(2.into(), 3.into(), out);

        // Larger than last value, but not largest in group.
        let rs = c.narrow_one_row(vec![1.into(), 5.into()], true);
        assert!(rs.is_empty());

        // One more new max.
        let out = c.narrow_one_row(vec![1.into(), 22.into()], true);
        check_new_max(1.into(), 7.into(), 22.into(), out);

        // Negative for old max should be fine if there is a positive for a larger value.
        let u = vec![(vec![1.into(), 22.into()], false), (vec![1.into(), 23.into()], true)];
        let out = c.narrow_one(u, true);
        check_new_max(1.into(), 22.into(), 23.into(), out);

        // Competing positive and negative should cancel out.
        let u = vec![(vec![1.into(), 24.into()], true), (vec![1.into(), 24.into()], false)];
        let rs = c.narrow_one(u, true);
        assert!(rs.is_empty());
    }

    // TODO: also test MIN

    #[test]
    fn it_suggests_indices() {
        let me = NodeAddress::mock_global(1.into());
        let c = setup(false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], vec![0]);
    }

    #[test]
    fn it_resolves() {
        let c = setup(false);
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), None);
    }
}
