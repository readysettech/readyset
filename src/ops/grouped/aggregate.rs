use query;

use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use flow::prelude::*;

/// Supported aggregation operators.
#[derive(Debug)]
pub enum Aggregation {
    /// Count the number of records for each group. The value for the `over` column is ignored.
    COUNT,
    /// Sum the value of the `over` column for all records of each group.
    SUM,
}

impl Aggregation {
    /// Construct a new `Aggregator` that performs this operation.
    ///
    /// The aggregation will be aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier. The `over` column should not be in the `group_by` array.
    pub fn over<'a>(self,
                    src: NodeIndex,
                    over: usize,
                    group_by: &[usize])
                    -> GroupedOperator<'a, Aggregator> {
        assert!(!group_by.iter().any(|&i| i == over),
                "cannot group by aggregation column");
        GroupedOperator::new(src,
                             Aggregator {
                                 op: self,
                                 over: over,
                                 group: group_by.into(),
                             })
    }
}

/// Aggregator implementas a Soup node that performans common aggregation operations such as counts
/// and sums.
///
/// `Aggregator` nodes are constructed through `Aggregation` variants using `Aggregation::new`.
///
/// Logically, the aggregated value for all groups start out as `0`. Thus, when the first record is
/// received for a group, `Aggregator` will output a negative for the *zero row*, followed by a
/// positive for the newly aggregated value.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column
/// (`self.over`) of the incoming record is then added to the current aggregation value according
/// to the operator in use (`COUNT` always adds/subtracts 1, `SUM` adds/subtracts the value of the
/// value in the incoming record. The output record is constructed by concatenating the columns
/// identifying the group, and appending the aggregated value. For example, for a sum with
/// `self.over == 1`, a previous sum of `3`, and an incoming record with `[a, 1, x]`, the output
/// would be `[a, x, 4]`.
#[derive(Debug)]
pub struct Aggregator {
    op: Aggregation,
    over: usize,
    group: Vec<usize>,
}

impl GroupedOperation for Aggregator {
    type Diff = i64;

    fn setup(&mut self, parent: &Node) {
        assert!(self.over < parent.fields().len(),
                "cannot aggregate over non-existing column");
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn zero(&self) -> Option<query::DataType> {
        Some(0i64.into())
    }

    fn to_diff(&self, r: &[query::DataType], pos: bool) -> Self::Diff {
        match self.op {
            Aggregation::COUNT if pos => 1,
            Aggregation::COUNT => -1,
            Aggregation::SUM => {
                let v = if let query::DataType::Number(n) = r[self.over] {
                    n
                } else {
                    unreachable!();
                };
                if pos { v } else { 0i64 - v }
            }
        }
    }

    fn apply(&self, current: &Option<query::DataType>, diffs: Vec<Self::Diff>) -> query::DataType {
        if let Some(query::DataType::Number(n)) = *current {
            diffs.into_iter().fold(n, |n, d| n + d).into()
        } else {
            unreachable!();
        }
    }

    fn description(&self) -> String {
        let op_string = match self.op {
            Aggregation::COUNT => "|*|".into(),
            Aggregation::SUM => format!("𝛴({})", self.over),
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
            g.seed(s, vec![1.into(), 1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into(), 1.into()]);
        } else {
            g.seed(s, vec![1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into()]);
        }

        if wide {
            g.set_op("identity",
                     &["x", "z", "ys"],
                     Aggregation::COUNT.over(s, 1, &[0, 2]));
        } else {
            g.set_op("identity",
                     &["x", "ys"],
                     Aggregation::COUNT.over(s, 1, &[0]));
        }
        if mat {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_describes() {
        let s = 0.into();

        let c = Aggregation::COUNT.over(s, 1, &[0, 2]);
        assert_eq!(c.description(), "|*| γ[0, 2]");

        let s = Aggregation::SUM.over(s, 1, &[2, 0]);
        assert_eq!(s.description(), "𝛴(1) γ[2, 0]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(true, false);

        let u: ops::Record = vec![1.into(), 1.into()].into();

        // first row for a group should emit -0 and +1 for that group
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 0.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u: ops::Record = vec![2.into(), 2.into()].into();

        // first row for a second group should emit -0 and +1 for that new group
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 0.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u: ops::Record = vec![1.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Record::Negative(vec![1.into(), 1.into()]);

        // negative row for a group should emit -2 and +1
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 1.into()]),
                                          ops::Record::Positive(vec![1.into(), 1.into()]),
                                          ops::Record::Positive(vec![1.into(), 2.into()]),
                                          ops::Record::Negative(vec![2.into(), 2.into()]),
                                          ops::Record::Positive(vec![2.into(), 2.into()]),
                                          ops::Record::Positive(vec![2.into(), 3.into()]),
                                          ops::Record::Positive(vec![2.into(), 1.into()]),
                                          ops::Record::Positive(vec![3.into(), 3.into()])]);

        // multiple positives and negatives should update aggregation value by appropriate amount
        // TODO: check for correct output ts'es
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 6); // one - and one + for each group
            // group 1 lost 1 and gained 2
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[0] == 1.into() && r[1] == 1.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[0] == 1.into() && r[1] == 2.into()
            } else {
                false
            }));
            // group 2 lost 1 and gained 3
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[0] == 2.into() && r[1] == 1.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[0] == 2.into() && r[1] == 3.into()
            } else {
                false
            }));
            // group 3 lost 1 (well, 0) and gained 1
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[0] == 3.into() && r[1] == 0.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[0] == 3.into() && r[1] == 1.into()
            } else {
                false
            }));
        } else {
            unreachable!();
        }
    }

    // TODO: also test SUM

    #[test]
    fn it_queries() {
        let c = setup(false, false);

        let hits = c.query(None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = c.query(Some(&q));
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    #[ignore]
    fn it_queries_zeros() {
        let c = setup(false, false);

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(100.into())),
                         }]);

        let hits = c.query(Some(&q));
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 100.into() && r[1] == 0.into()));
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
