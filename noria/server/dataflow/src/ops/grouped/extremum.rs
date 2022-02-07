use crate::ops::grouped::{GroupedOperation, GroupedOperator};
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use noria_errors::{invariant, ReadySetResult};

/// Supported kinds of extremum operators.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Extremum {
    /// The minimum value that occurs in the `over` column in each group.
    Min,
    /// The maximum value of the `over` column for all records of each group.
    Max,
}

impl Extremum {
    /// Construct a new `ExtremumOperator` that performs this operation.
    ///
    /// The aggregation will be aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier.
    ///
    /// # Invariants
    ///
    /// * over argument must always be a valid column in the node.
    pub fn over(
        self,
        src: NodeIndex,
        over: usize,
        group_by: &[usize],
    ) -> GroupedOperator<ExtremumOperator> {
        GroupedOperator::new(
            src,
            ExtremumOperator {
                op: self,
                over,
                group: group_by.into(),
            },
        )
    }
}

/// `ExtremumOperator` implementas a Soup node that performans common aggregation operations such
/// as counts and sums.
///
/// `ExtremumOperator` nodes are constructed through `Extremum` variants using `Extremum::new`.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column (`self.over`)
/// of the incoming record is then added to the current aggregation value according to the operator
/// in use (`COUNT` always adds/subtracts 1, `SUM` adds/subtracts the value of the value in the
/// incoming record. The output record is constructed by concatenating the columns identifying the
/// group, and appending the aggregated value. For example, for a sum with `self.over == 1`, a
/// previous sum of `3`, and an incoming record with `[a, 1, x]`, the output would be `[a, x, 4]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtremumOperator {
    op: Extremum,
    over: usize,
    group: Vec<usize>,
}

pub enum DiffType {
    Insert(DataType),
    Remove(DataType),
    None,
}

impl GroupedOperation for ExtremumOperator {
    type Diff = DiffType;

    fn setup(&mut self, parent: &Node) -> ReadySetResult<()> {
        invariant!(
            self.over < parent.fields().len(),
            "cannot aggregate over non-existing column"
        );
        Ok(())
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DataType], pos: bool) -> ReadySetResult<Self::Diff> {
        #[allow(clippy::indexing_slicing)] // Invariant documented.
        let v = &r[self.over];
        if let DataType::None = *v {
            Ok(DiffType::None)
        } else if pos {
            Ok(DiffType::Insert(v.clone()))
        } else {
            Ok(DiffType::Remove(v.clone()))
        }
    }

    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
    ) -> ReadySetResult<DataType> {
        // Extreme values are those that are at least as extreme as the current min/max (if any).
        // let mut is_extreme_value : Box<dyn Fn(i64) -> bool> = Box::new(|_|true);
        let mut extreme_values: Vec<DataType> = vec![];
        if let Some(d) = current {
            extreme_values.push(d.clone());
        }

        let is_extreme_value = |x: &DataType| {
            if let Some(n) = current {
                match self.op {
                    Extremum::Max => x >= n,
                    Extremum::Min => x <= n,
                }
            } else {
                true
            }
        };

        for d in diffs {
            match d {
                DiffType::Insert(v) if is_extreme_value(&v) => extreme_values.push(v),
                DiffType::Remove(v) if is_extreme_value(&v) => {
                    if let Some(i) = extreme_values.iter().position(|x| *x == v) {
                        extreme_values.swap_remove(i);
                    }
                }
                _ => {}
            };
        }

        let extreme = match self.op {
            Extremum::Min => extreme_values.into_iter().min(),
            Extremum::Max => extreme_values.into_iter().max(),
        };

        extreme.ok_or(ReadySetError::GroupedStateLost)
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from(match self.op {
                Extremum::Min => "MIN",
                Extremum::Max => "MAX",
            });
        }

        let op_string = match self.op {
            Extremum::Min => format!("min({})", self.over),
            Extremum::Max => format!("max({})", self.over),
        };
        let group_cols = self
            .group
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} γ[{}]", op_string, group_cols)
    }

    fn over_column(&self) -> usize {
        self.over
    }

    fn output_col_type(&self) -> Option<nom_sql::SqlType> {
        None // Type of extremum relies on col type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{ops, SuggestedIndex};

    fn setup(op: Extremum, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        g.set_op("agg", &["x", "ys"], op.over(s.as_global(), 1, &[0]), mat);
        g
    }

    fn assert_positive_record(group: i32, new: i32, rs: Records) {
        assert_eq!(rs.len(), 1);

        match rs.into_iter().next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], group.into());
                assert_eq!(r[1], new.into());
            }
            _ => unreachable!(),
        }
    }

    fn assert_record_change(group: i32, old: DataType, new: DataType, rs: Records) {
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], group.into());
                assert_eq!(r[1], old);
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], group.into());
                assert_eq!(r[1], new);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn it_forwards_maximum() {
        let mut c = setup(Extremum::Max, true);
        let key = 1;

        // First insertion should trigger an update.
        let out = c.narrow_one_row(vec![key.into(), 4.into()], true);
        assert_positive_record(key, 4, out);

        // Larger value should also trigger an update.
        let out = c.narrow_one_row(vec![key.into(), 7.into()], true);
        assert_record_change(key, 4.into(), 7.into(), out);

        // No change if new value isn't the max.
        let rs = c.narrow_one_row(vec![key.into(), 2.into()], true);
        assert!(rs.is_empty());

        // Insertion into a different group should be independent.
        let out = c.narrow_one_row(vec![2.into(), 3.into()], true);
        assert_positive_record(2, 3, out);

        // Larger than last value, but not largest in group.
        let rs = c.narrow_one_row(vec![key.into(), 5.into()], true);
        assert!(rs.is_empty());

        // One more new max.
        let out = c.narrow_one_row(vec![key.into(), 22.into()], true);
        assert_record_change(key, 7.into(), 22.into(), out);

        // Negative for old max should be fine if there is a positive for a larger value.
        let u = vec![
            (vec![key.into(), 22.into()], false),
            (vec![key.into(), 23.into()], true),
        ];
        let out = c.narrow_one(u, true);
        assert_record_change(key, 22.into(), 23.into(), out);
    }

    #[test]
    fn it_forwards_minimum() {
        let mut c = setup(Extremum::Min, true);
        let key = 1;

        // First insertion should trigger an update.
        let out = c.narrow_one_row(vec![key.into(), 10.into()], true);
        assert_positive_record(key, 10, out);

        // Smaller value should also trigger an update.
        let out = c.narrow_one_row(vec![key.into(), 7.into()], true);
        assert_record_change(key, 10.into(), 7.into(), out);

        // No change if new value isn't the min.
        let rs = c.narrow_one_row(vec![key.into(), 9.into()], true);
        assert!(rs.is_empty());

        // Insertion into a different group should be independent.
        let out = c.narrow_one_row(vec![2.into(), 15.into()], true);
        assert_positive_record(2, 15, out);

        // Smaller than last value, but not smallest in group.
        let rs = c.narrow_one_row(vec![key.into(), 8.into()], true);
        assert!(rs.is_empty());

        // Negative for old min should be fine if there is a positive for a smaller value.
        let u = vec![
            (vec![key.into(), 7.into()], false),
            (vec![key.into(), 5.into()], true),
        ];
        let out = c.narrow_one(u, true);
        assert_record_change(key, 7.into(), 5.into(), out);
    }

    #[test]
    fn it_cancels_out_opposite_records() {
        let mut c = setup(Extremum::Max, true);
        c.narrow_one_row(vec![1.into(), 5.into()], true);
        // Competing positive and negative should cancel out.
        let u = vec![
            (vec![1.into(), 10.into()], true),
            (vec![1.into(), 10.into()], false),
        ];

        let out = c.narrow_one(u, true);
        assert!(out.is_empty());
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let c = setup(Extremum::Max, false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], SuggestedIndex::Strict(Index::hash_map(vec![0])));
    }

    #[test]
    fn it_resolves() {
        let c = setup(Extremum::Max, false);
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(c.node().resolve(1), None);
    }

    #[test]
    fn it_works_with_varying_types() {
        let mut c = setup(Extremum::Max, true);
        let key = 1;

        // First insertion should trigger an update.
        let out = c.narrow_one_row(vec![key.into(), 1.into()], true);
        assert_positive_record(key, 1, out);

        use std::convert::TryInto;
        let float_value = 1.2;
        let out = c.narrow_one_row(vec![key.into(), float_value.try_into().unwrap()], true);
        assert_record_change(key, 1.into(), float_value.try_into().unwrap(), out);

        let string_value = "yes";
        let out = c.narrow_one_row(vec![key.into(), string_value.try_into().unwrap()], true);
        assert_record_change(
            key,
            float_value.try_into().unwrap(),
            string_value.try_into().unwrap(),
            out,
        );
    }
}
