use std::sync;

pub use nom_sql::{Operator, Literal};
use crate::ops::grouped::GroupedOperation;
use crate::ops::grouped::GroupedOperator;
use crate::ops::filter::{FilterCondition, Value};

use crate::prelude::*;

/// Supported aggregation operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterAggregation {
    /// Count the number of records for each filtered group. The value for the `over` column is ignored.
    COUNT,
    /// Sum the value of the `over` column for all records of each filtered group.
    SUM,
}

impl FilterAggregation {
    /// Construct a new `Aggregator` that performs this operation.
    ///
    /// The aggregation will aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier. The `over` column should not be in the `group_by` array.
    pub fn over(
        self,
        src: NodeIndex,
        filter: &[(usize, FilterCondition)],
        over: usize,
        over_else: Option<Literal>,
        group_by: &[usize],
    ) -> GroupedOperator<FilterAggregator> {
        assert!(
            !group_by.iter().any(|&i| i == over),
            "cannot group by aggregation column"
        );

        GroupedOperator::new(
            src,
            FilterAggregator {
                op: self,
                filter: sync::Arc::new(Vec::from(filter)),
                over,
                over_else,
                group: group_by.into(),
            },
        )
    }
}

/// FilterAggregator implements a Soup node that performs counts and sums over rows that meet a
/// condition, matching the behavior of SUM(CASE WHEN a=1 THEN 1 ELSE 0) or
/// COUNT(CASE WHEN <condition> THEN 1 END) in MySQL.
///
/// Nodes are constructed through `FilterAggregation` variants using `FilterAggregation::new`.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column
/// (`self.over`) of the incoming record is then added to the current aggregation value according
/// to the operator in use (`COUNT` always adds/subtracts 1, `SUM` adds/subtracts the value of the
/// value in the incoming record). The output record is constructed by concatenating the columns
/// identifying the group, and appending the aggregated value. For example, for a sum with
/// `self.over == 1`, a previous sum of `3`, and an incoming record with `[a, 1, x]`, the output
/// would be `[a, x, 4]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterAggregator {
    op: FilterAggregation,
    filter: sync::Arc<Vec<(usize, FilterCondition)>>,
    over: usize,
    over_else: Option<Literal>,
    group: Vec<usize>,
}

impl GroupedOperation for FilterAggregator {
    type Diff = i128;

    fn setup(&mut self, parent: &Node) {
        assert!(
            self.over < parent.fields().len(),
            "cannot aggregate over non-existing column"
        );
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DataType], pos: bool) -> Self::Diff {
        let passes_filter = self.filter.iter().all(|(i, cond)| {
            // check if this filter matches
            let d = &r[*i];
            match *cond {
                FilterCondition::Comparison(ref op, ref f) => {
                    let v = match *f {
                        Value::Constant(ref dt) => dt,
                        Value::Column(c) => &r[c],
                    };
                    match *op {
                        Operator::Equal => d == v,
                        Operator::NotEqual => d != v,
                        Operator::Greater => d > v,
                        Operator::GreaterOrEqual => d >= v,
                        Operator::Less => d < v,
                        Operator::LessOrEqual => d <= v,
                        Operator::In => unreachable!(),
                        _ => unimplemented!(),
                    }
                }
                FilterCondition::In(ref fs) => fs.contains(d),
            }
        });
        let v = if passes_filter {
            match self.op {
                FilterAggregation::COUNT => 1,
                FilterAggregation::SUM => {
                    match r[self.over] {
                        DataType::Int(n) => i128::from(n),
                        DataType::UnsignedInt(n) => i128::from(n),
                        DataType::BigInt(n) => i128::from(n),
                        DataType::UnsignedBigInt(n) => i128::from(n),
                        DataType::None => 0,
                        ref x => unreachable!("tried to aggregate over {:?} on {:?}", x, r),
                    }
                }
            }
        } else {
            // the filter returned false, so check whether we have an else case
            match self.over_else.clone() {
                Some(over_else) => {
                    match self.op {
                        FilterAggregation::COUNT => 1,
                        FilterAggregation::SUM => {
                            match over_else {
                                Literal::Integer(n) => i128::from(n),
                                Literal::UnsignedInteger(n) => i128::from(n),
                                ref x => unreachable!("tried to aggregate over {:?} on {:?}", x, r),
                            }
                        }
                    }
                }
                None => 0
            }
        };

        if pos {
            v
        } else {
            0i128 - v
        }
    }

    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
    ) -> DataType {
        let n = match current {
            Some(&DataType::Int(n)) => i128::from(n),
            Some(&DataType::UnsignedInt(n)) => i128::from(n),
            Some(&DataType::BigInt(n)) => i128::from(n),
            Some(&DataType::UnsignedBigInt(n)) => i128::from(n),
            None => 0,
            _ => unreachable!(),
        };
        diffs.fold(n, |n, d| n + d).into()
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from(match self.op {
                FilterAggregation::COUNT => "+σ",
                FilterAggregation::SUM => "𝛴σ",
            });
        }

        // TODO could include information about filter condition
        let op_string = match self.op {
            FilterAggregation::COUNT => format!("|σ({})|", self.over),
            FilterAggregation::SUM => format!("𝛴(σ({}))", self.over),
        };

        let group_cols = self
            .group
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} γ[{}]", op_string, group_cols)
    }

    fn over_columns(&self) -> Vec<usize> {
        vec![self.over]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            FilterAggregation::COUNT.over(
                s.as_global(),
                &[
                    (1, FilterCondition::Comparison(
                        Operator::Equal,
                        Value::Constant(2.into()),
                    )),
                ],
                1,
                None,
                &[0]),
            mat,
        );
        g
    }

    fn setup_multicolumn(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "identity",
            &["x", "z", "ys"],
            FilterAggregation::COUNT.over(
                s.as_global(),
                &[
                    (1, FilterCondition::Comparison(
                        Operator::Equal,
                        Value::Constant(2.into()),
                    )),
                ],
                1,
                None,
                &[0, 2]),
            mat,
        );
        g
    }

    fn setup_sum(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        // sum z's, grouped by y, where y!=x and z>1
        g.set_op(
            "identity",
            &["y", "zsum"],
            FilterAggregation::SUM.over(
                s.as_global(),
                &[
                    (1, FilterCondition::Comparison(
                        Operator::NotEqual,
                        Value::Column(0),
                    )),
                    (2, FilterCondition::Comparison(
                        Operator::Greater,
                        Value::Constant(1.into()),
                    )),
                ],
                2,
                None,
                &[1]
            ),
            mat,
        );
        g
    }

    fn setup_else(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z", "g"]);
        // sum z's if y >= 3, else x's, group by g
        g.set_op(
            "identity",
            &["g", "zsum"],
            FilterAggregation::SUM.over(
                s.as_global(),
                &[
                    (1, FilterCondition::Comparison(
                        Operator::GreaterOrEqual,
                        Value::Constant(3.into()),
                    )),
                ],
                2,
                Some(Literal::Integer(6)),
                &[3]
            ),
            mat,
        );
        g
    }

    #[test]
    fn it_describes() {
        let s = 0.into();

        let c = FilterAggregation::COUNT.over(s, &[], 1, None, &[0, 2]);
        assert_eq!(c.description(true), "|σ(1)| γ[0, 2]");

        let s = FilterAggregation::SUM.over(s, &[], 1, None, &[2, 0]);
        assert_eq!(s.description(true), "𝛴(σ(1)) γ[2, 0]");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn it_forwards() {
        let mut c = setup(true);

        let u: Record = vec![1.into(), 1.into()].into();

        // this should get filtered out
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 0.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into()].into();

        // first row for a second group should emit +1 for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = (vec![2.into(), 2.into()], false);

        // negative row for a group should emit -2 and +1
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u = vec![
            (vec![1.into(), 1.into()], false),
            (vec![1.into(), 1.into()], true),
            (vec![1.into(), 2.into()], true),  // these are the
            (vec![2.into(), 2.into()], false), // only rows that
            (vec![2.into(), 2.into()], true),  // aren't ignored
            (vec![2.into(), 3.into()], true),
            (vec![2.into(), 1.into()], true),
            (vec![3.into(), 3.into()], true),
        ];

        // multiple positives and negatives should update aggregation value by appropriate amount
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 3); // +- for 1, + for 3

        // group 1 lost 0 and gained 1
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == 0.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == 1.into()
        } else {
            false
        }));

        // group 2 lost 1 and gained 1

        // group 3 lost 0 and gained 1
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into() && r[1] == 0.into()
        } else {
            false
        }));
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn it_groups_by_multiple_columns() {
        let mut c = setup_multicolumn(true);

        let u: Record = vec![1.into(), 1.into(), 2.into()].into();

        // first row filtered, so should emit 0 for the group (1, 2)
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 0.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into(), 2.into()].into();

        // first row for a group should emit +1 for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }

        let u = (vec![2.into(), 2.into(), 2.into()], false);

        // negative row for a group should emit -2 and +1
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn it_sums() {
        // records [x, y, z]  --> [y, zsum]
        // sum z's, grouped by y, where y!=x and z>1

        // test:
        // filtered out by z=1, z=0, y=x
        // positive and negative updates

        let mut c = setup_sum(true);

        // some unfiltered updates first

        let u: Record = vec![1.into(), 2.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 0.into(), 3.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 3.into());
            }
            _ => unreachable!(),
        }

        // now filtered updates

        let u: Record = vec![1.into(), 1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 0.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 3.into(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 3.into());
                assert_eq!(r[1], 0.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 2.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 0);

        let u: Record = vec![2.into(), 2.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 0);

        // additional update to preexisting group

        let u: Record = vec![0.into(), 2.into(), 4.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 6.into());
            }
            _ => unreachable!(),
        }

        // now a negative update to a group
        let u = (vec![14.into(), 0.into(), 2.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 3.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn it_sums_with_else() {
        // records [x, y, z, g]  --> [g, zsum]
        // sum z's if y>=3 else 6s, grouped by g

        // test:
        // y<3, y=3, y>3
        // positive and negative updates

        let mut c = setup_else(true);

        // updates from z

        let u: Record = vec![1.into(), 5.into(), 2.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 3.into(), 3.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 5.into());
            }
            _ => unreachable!(),
        }

        // updates by literal 6

        let u: Record = vec![1.into(), 2.into(), 2.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 5.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 11.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 3.into(), 0.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 0);

        let u: Record = vec![3.into(), 0.into(), 0.into(), 0.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 11.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 17.into());
            }
            _ => unreachable!(),
        }

        // now a negative update

        let u = (vec![14.into(), 6.into(), 2.into(), 0.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 17.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 0.into());
                assert_eq!(r[1], 15.into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
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
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(c.node().resolve(1), None);
    }
}
