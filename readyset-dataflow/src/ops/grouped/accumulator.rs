//! Accumulation-based aggregation operations that collect values into composite results.

use std::collections::HashMap;

use common::DfValue;
use dataflow_expression::grouped::accumulator::{AccumulationOp, AccumulatorData};
use readyset_data::{Collation, DfType, Dialect};
use readyset_errors::{internal_err, invariant_eq, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::node::{AuxiliaryNodeState, Node};
use crate::ops::grouped::{GroupedOperation, GroupedOperator};
use crate::prelude::*;

use super::{hash_grouped_records, GroupHash};

/// The last stored state for a given group.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LastState {
    /// The last output value we emitted for this group.
    last_output: Option<DfValue>,
    /// The actual data
    data: AccumulatorData,
}

/// `Accumulator` implements accumulation-based aggregation operations that collect
/// multiple input values into a single composite output value.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Accumulator {
    op: AccumulationOp,
    over: usize,
    group: Vec<usize>,
    out_ty: DfType,
}

impl Accumulator {
    /// Construct a new `Accumulator` that performs this operation.
    pub fn over(
        op: AccumulationOp,
        src: NodeIndex,
        over: usize,
        group_by: &[usize],
        over_col_ty: &DfType,
        _dialect: &Dialect,
    ) -> ReadySetResult<GroupedOperator<Accumulator>> {
        let collation = over_col_ty.collation().unwrap_or(Collation::Utf8);

        let out_ty = match &op {
            AccumulationOp::ArrayAgg { .. } => {
                antithesis_sdk::assert_reachable!("Accumulation::ArrayAgg");
                DfType::Array(Box::new(over_col_ty.clone()))
            }
            AccumulationOp::GroupConcat { .. } => {
                antithesis_sdk::assert_reachable!("Accumulation::GroupConcat");
                DfType::Text(collation)
            }
            AccumulationOp::JsonObjectAgg { .. } => {
                antithesis_sdk::assert_reachable!("Accumulation::JsonObjectAgg");
                DfType::Text(collation)
            }
            AccumulationOp::StringAgg { .. } => {
                antithesis_sdk::assert_reachable!("Accumulation::StringAgg");
                DfType::Text(collation)
            }
        };

        Ok(GroupedOperator::new(
            src,
            Accumulator {
                op,
                over,
                group: group_by.into(),
                out_ty,
            },
        ))
    }
}

#[derive(Debug)]
pub struct AccumulationDiff {
    value: DfValue,
    is_positive: bool,
    group_hash: GroupHash,
}

impl GroupedOperation for Accumulator {
    type Diff = AccumulationDiff;

    fn setup(&mut self, _: &Node) -> ReadySetResult<()> {
        Ok(())
    }

    fn group_by(&self) -> &[usize] {
        &self.group
    }

    fn to_diff(&self, record: &[DfValue], is_positive: bool) -> ReadySetResult<Self::Diff> {
        let group_hash = hash_grouped_records(record, self.group_by());
        let value = record
            .get(self.over)
            .ok_or(ReadySetError::InvalidRecordLength)?
            .clone();
        Ok(AccumulationDiff {
            value,
            is_positive,
            group_hash,
        })
    }

    fn apply(
        &self,
        current: Option<&DfValue>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
        auxiliary_node_state: Option<&mut AuxiliaryNodeState>,
    ) -> ReadySetResult<Option<DfValue>> {
        let mut diffs = diffs.peekable();

        let first_diff = diffs
            .peek()
            .ok_or_else(|| internal_err!("accumulator received no diffs"))?;
        let group = first_diff.group_hash;

        let last_state = match auxiliary_node_state {
            Some(AuxiliaryNodeState::Accumulator(ref mut acc_state)) => &mut acc_state.last_state,
            Some(_) => internal!("Incorrect auxiliary state for Accumulator node"),
            None => internal!("Missing auxiliary state for Accumulator node"),
        };

        let mut ls = last_state.remove(&group);
        // check that the `current` value, as known by node owning operator,
        // is the same value that was last emitted from this function.
        // if they don't match, something's gone wrong and we need to rebuild.
        let mut prev_state = match current {
            #[allow(clippy::unwrap_used)] // check for is_some() before unwrapping
            Some(curr_val)
                if ls.is_some() && ls.as_ref().unwrap().last_output.as_ref() == Some(curr_val) =>
            {
                // if state matches, use it
                ls.take().unwrap()
            }
            // if state doesn't match, need to recreate it
            Some(_) => {
                return Ok(None);
            }
            // if we're recreating or this is the first record for the group, make a new state
            None => LastState {
                last_output: None,
                data: (&self.op).into(),
            },
        };

        for AccumulationDiff {
            value,
            is_positive,
            group_hash,
        } in diffs
        {
            if self.op.ignore_nulls() && value.is_none() {
                continue;
            }

            invariant_eq!(group_hash, group);
            if is_positive {
                prev_state.data.add(&self.op, value);
            } else {
                prev_state.data.remove(&self.op, value)?;
            }
        }
        let output_value = self.op.apply(&prev_state.data)?;
        prev_state.last_output = Some(output_value.clone());
        last_state.insert(group, prev_state);
        Ok(Some(output_value))
    }

    fn description(&self) -> String {
        macro_rules! order_by_str {
            ($order_by:expr) => {
                order_by_str!($order_by, true)
            };
            ($order_by:expr, $include_null_order:expr) => {
                match $order_by {
                    Some((order_type, null_order)) => {
                        // the '1' indicates the first column listed in the function args
                        if $include_null_order {
                            format!(" ORDER BY 1 {} {}", order_type, null_order)
                        } else {
                            format!(" ORDER BY 1 {}", order_type)
                        }
                    }
                    None => "".to_string(),
                }
            };
        }

        let op_string = match &self.op {
            AccumulationOp::ArrayAgg { distinct, order_by } => {
                format!(
                    "ArrayAgg({}{}{})",
                    distinct,
                    self.over,
                    order_by_str!(order_by)
                )
            }
            AccumulationOp::GroupConcat {
                separator,
                distinct,
                order_by,
            } => {
                format!(
                    "GroupConcat({}{}{}, {:?})",
                    distinct,
                    self.over,
                    order_by_str!(order_by, false),
                    separator,
                )
            }
            AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys,
            } => {
                if *allow_duplicate_keys {
                    format!("JsonObjectAgg({})", self.over)
                } else {
                    format!("JsonbObjectAgg({})", self.over)
                }
            }
            AccumulationOp::StringAgg {
                separator,
                distinct,
                order_by,
            } => {
                let sep = match separator {
                    Some(s) => format!("\"{s}\""),
                    None => "NULL".to_string(),
                };
                format!(
                    "StringAgg({}{}, {}{})",
                    distinct,
                    self.over,
                    sep,
                    order_by_str!(order_by),
                )
            }
        };
        format!("{} γ{:?}", op_string, self.group)
    }

    fn over_column(&self) -> usize {
        self.over
    }

    fn output_col_type(&self) -> DfType {
        self.out_ty.clone()
    }

    fn empty_value(&self) -> Option<DfValue> {
        Some("".into())
    }

    fn can_lose_state(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
/// Auxiliary State for accumulation operations, which is owned by a Domain.
pub struct AccumulatorState {
    last_state: HashMap<GroupHash, LastState>,
}

#[cfg(test)]
mod tests {
    use readyset_sql::ast::{NullOrder, OrderType};

    use super::*;
    use crate::{ops, LookupIndex};

    fn setup(mat: bool, op: Option<AccumulationOp>) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        let op = match op {
            Some(op) => op,
            None => AccumulationOp::GroupConcat {
                separator: "#".to_string(),
                distinct: false.into(),
                order_by: None,
            },
        };
        let c = Accumulator::over(
            op,
            s.as_global(),
            1,
            &[0],
            &DfType::Unknown,
            &Dialect::DEFAULT_MYSQL,
        )
        .unwrap();
        g.set_op("concat", &["x", "ys"], c, mat);
        g
    }

    #[test]
    fn describe_group_concat() {
        macro_rules! mk_op {
            ($d:expr, $o:expr) => {
                AccumulationOp::GroupConcat {
                    separator: "#".to_string(),
                    distinct: $d.into(),
                    order_by: $o,
                }
            };
        }

        let c = setup(true, Some(mk_op!(false, None)));
        assert_eq!(c.node().description(), "GroupConcat(1, \"#\") γ[0]",);
        let c = setup(true, Some(mk_op!(true, None)));
        assert_eq!(
            c.node().description(),
            "GroupConcat(DISTINCT 1, \"#\") γ[0]",
        );
        let c = setup(
            true,
            Some(mk_op!(
                true,
                Some((OrderType::OrderDescending, NullOrder::NullsFirst))
            )),
        );
        assert_eq!(
            c.node().description(),
            "GroupConcat(DISTINCT 1 ORDER BY 1 DESC, \"#\") γ[0]",
        );
    }

    #[test]
    fn describe_array_agg() {
        macro_rules! mk_op {
            ($d:expr, $o:expr) => {
                AccumulationOp::ArrayAgg {
                    distinct: $d.into(),
                    order_by: $o,
                }
            };
        }

        let c = setup(true, Some(mk_op!(false, None)));
        assert_eq!(c.node().description(), "ArrayAgg(1) γ[0]",);
        let c = setup(true, Some(mk_op!(true, None)));
        assert_eq!(c.node().description(), "ArrayAgg(DISTINCT 1) γ[0]",);
        let c = setup(
            true,
            Some(mk_op!(
                true,
                Some((OrderType::OrderDescending, NullOrder::NullsFirst))
            )),
        );
        assert_eq!(
            c.node().description(),
            "ArrayAgg(DISTINCT 1 ORDER BY 1 DESC NULLS FIRST) γ[0]",
        );
    }

    #[test]
    fn describe_string_agg() {
        macro_rules! mk_op {
            ($d:expr, $o:expr) => {
                AccumulationOp::StringAgg {
                    separator: Some("#".to_string()),
                    distinct: $d.into(),
                    order_by: $o,
                }
            };
        }

        let c = setup(true, Some(mk_op!(false, None)));
        assert_eq!(c.node().description(), "StringAgg(1, \"#\") γ[0]",);
        let c = setup(true, Some(mk_op!(true, None)));
        assert_eq!(c.node().description(), "StringAgg(DISTINCT 1, \"#\") γ[0]",);
        let c = setup(
            true,
            Some(mk_op!(
                true,
                Some((OrderType::OrderDescending, NullOrder::NullsFirst))
            )),
        );
        assert_eq!(
            c.node().description(),
            "StringAgg(DISTINCT 1, \"#\" ORDER BY 1 DESC NULLS FIRST) γ[0]",
        );

        let c = setup(
            true,
            Some(AccumulationOp::StringAgg {
                separator: None,
                distinct: false.into(),
                order_by: None,
            }),
        );
        assert_eq!(c.node().description(), "StringAgg(1, NULL) γ[0]",);
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn it_forwards() {
        let mut c = setup(true, None);

        let u: Record = vec![1.into(), 1.into()].into();

        // first row for a group should emit +"1" for that group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], "1".into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into()].into();

        // first row for a second group should emit +"2" for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], "2".into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 2.into()].into();

        // second row for a group should emit -"1" and +"1#2"
        let rs = c.narrow_one(u, true);
        eprintln!("{rs:?}");
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], "1".into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], "1#2".into());
            }
            _ => unreachable!(),
        }

        let u = (vec![1.into(), 1.into()], false);

        // negative row for a group should emit -"1#2" and +"2"
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], "1#2".into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], "2".into());
            }
            _ => unreachable!(),
        }

        let u = vec![
            // remove non-existing
            // (vec![1.into(), 1.into()], false),
            // add old
            (vec![1.into(), 1.into()], true),
            // add duplicate
            (vec![1.into(), 2.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![2.into(), 3.into()], true),
            (vec![2.into(), 2.into()], true),
            (vec![2.into(), 1.into()], true),
            // new group
            (vec![3.into(), 3.into()], true),
        ];

        // multiple positives and negatives should update aggregation value by appropriate amount
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5); // one - and one + for each group, except last (new) group
                                 // group 1 had [2], now has [1,2]
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            if r[0] == 1.into() {
                assert_eq!(r[1], "2".into());
                true
            } else {
                false
            }
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            if r[0] == 1.into() {
                assert_eq!(r[1], "2#1#2".into());
                true
            } else {
                false
            }
        } else {
            false
        }));
        // group 2 was [2], is now [1,2,3]
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            if r[0] == 2.into() {
                assert_eq!(r[1], "2".into());
                true
            } else {
                false
            }
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            if r[0] == 2.into() {
                assert_eq!(r[1], "3#2#1".into());
                true
            } else {
                false
            }
        } else {
            false
        }));
        // group 3 was [], is now [3]
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            if r[0] == 3.into() {
                assert_eq!(r[1], "3".into());
                true
            } else {
                false
            }
        } else {
            false
        }));

        // Test that NULL values are ignored
        let u: Record = vec![1.into(), DfValue::None].into();
        let rs = c.narrow_one(u, true);

        // Should still have the same output as before since NULL is ignored
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            if r[0] == 1.into() {
                // The output should still be "2#1#2" and not include NULL
                assert_eq!(r[1], "2#1#2".into());
                assert!(!r[1].to_string().contains("NULL"));
                true
            } else {
                false
            }
        } else {
            false
        }));
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let c = setup(false, None);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], LookupIndex::Strict(Index::hash_map(vec![0])));
    }

    #[test]
    fn it_resolves() {
        let c = setup(false, None);
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(c.node().resolve(1), None);
    }
}
