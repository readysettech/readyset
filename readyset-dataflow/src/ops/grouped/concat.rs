//! Kinda (s)crappy group_concat() implementation

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Write;

use common::DfValue;
use readyset_data::{Collation, DfType};
use readyset_errors::invariant_eq;
use readyset_util::Indices;
use serde::{Deserialize, Serialize};

use crate::node::{AuxiliaryNodeState, Node};
use crate::ops::grouped::{GroupedOperation, GroupedOperator};
use crate::prelude::*;

/// The last stored state for a given group.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LastState {
    /// The string representation we last emitted for this group.
    string_repr: String,
    /// A vector containing the actual data
    data: Vec<DfValue>,
}

impl Default for LastState {
    fn default() -> Self {
        Self {
            string_repr: "".to_owned(),
            data: vec![],
        }
    }
}

/// `GroupConcat` partially implements the `GROUP_CONCAT` SQL aggregate function, which
/// aggregates a set of arbitrary `DfValue`s into a string representation separated by
/// a user-defined separator.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GroupConcat {
    /// Which column to aggregate.
    source_col: usize,
    /// The columns to group by.
    group_by: Vec<usize>,
    /// The user-defined separator.
    separator: String,
}

fn concat_fmt<F: Write>(f: &mut F, dt: &DfValue) -> ReadySetResult<()> {
    match dt {
        DfValue::Text(..) | DfValue::TinyText(..) => {
            let text: &str = <&str>::try_from(dt)?;
            write!(f, "{}", text).unwrap();
        }
        x => write!(f, "{}", x).unwrap(),
    }
    Ok(())
}

impl GroupConcat {
    /// Construct a new `GroupConcat`, aggregating the provided `source_col` and separating
    /// aggregated data with the provided `separator`.
    pub fn new(
        src: NodeIndex,
        source_col: usize,
        group_by: Vec<usize>,
        separator: String,
    ) -> ReadySetResult<GroupedOperator<GroupConcat>> {
        Ok(GroupedOperator::new(
            src,
            GroupConcat {
                source_col,
                group_by,
                separator,
            },
        ))
    }
}

pub struct ConcatDiff {
    value: DfValue,
    is_positive: bool,
    group_by: Vec<DfValue>,
}

impl GroupedOperation for GroupConcat {
    type Diff = ConcatDiff;

    fn setup(&mut self, _: &Node) -> ReadySetResult<()> {
        Ok(())
    }

    fn group_by(&self) -> &[usize] {
        &self.group_by
    }

    fn to_diff(&self, record: &[DfValue], is_positive: bool) -> ReadySetResult<Self::Diff> {
        let value = record
            .get(self.source_col)
            .ok_or(ReadySetError::InvalidRecordLength)?
            .clone();
        // We need this to figure out which state to use.
        let group_by = record
            .cloned_indices(self.group_by.iter().cloned())
            .map_err(|_| ReadySetError::InvalidRecordLength)?;
        Ok(ConcatDiff {
            value,
            is_positive,
            group_by,
        })
    }

    fn apply(
        &self,
        current: Option<&DfValue>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
        auxiliary_node_state: Option<&mut AuxiliaryNodeState>,
    ) -> ReadySetResult<Option<DfValue>> {
        let current: Option<&str> = current
            .filter(|dt| matches!(dt, &DfValue::Text(..) | &DfValue::TinyText(..)))
            .and_then(|dt| <&str>::try_from(dt).ok());

        let mut diffs = diffs.peekable();

        let first_diff = diffs
            .peek()
            .ok_or_else(|| internal_err!("group_concat got no diffs"))?;
        let group = first_diff.group_by.clone();

        let last_state = match auxiliary_node_state {
            Some(AuxiliaryNodeState::Concat(ref mut gcs)) => &mut gcs.last_state,
            Some(_) => internal!("Incorrect auxiliary state for Concat node"),
            None => internal!("Missing auxiliary state for Concat node"),
        };

        let mut ls = last_state.remove(&group);
        let mut prev_state = match current {
            #[allow(clippy::unwrap_used)] // check for is_some() before unwrapping
            Some(text) if ls.is_some() && text == ls.as_ref().unwrap().string_repr => {
                // if state matches, use it
                ls.take().unwrap()
            }
            // if state doesn't match, need to recreate it
            Some(_) => {
                return Ok(None);
            }
            // if we're recreating or this is the first record for the group, make a new state
            None => LastState::default(),
        };
        for ConcatDiff {
            value,
            is_positive,
            group_by,
        } in diffs
        {
            invariant_eq!(group_by, group);
            if is_positive {
                prev_state.data.push(value);
            } else {
                let item_pos = prev_state
                    .data
                    .iter()
                    .rposition(|x| x == &value)
                    .ok_or_else(|| {
                        #[cfg(feature = "display_literals")]
                        {
                            internal_err!(
                                "group_concat couldn't remove {:?} from {:?}",
                                value,
                                prev_state.data
                            )
                        }
                        #[cfg(not(feature = "display_literals"))]
                        internal_err!("group_concat couldn't remove value from data")
                    })?;
                prev_state.data.remove(item_pos);
            }
        }
        // what I *really* want here is Haskell's "intercalate" ~eta
        let mut out_str = String::new();
        for (i, piece) in prev_state.data.iter().enumerate() {
            // TODO(eta): not unwrap, maybe
            concat_fmt(&mut out_str, piece)?;
            if i < prev_state.data.len() - 1 {
                write!(&mut out_str, "{}", self.separator).unwrap();
            }
        }
        prev_state.string_repr.clone_from(&out_str);
        last_state.insert(group, prev_state);
        Ok(Some(out_str.into()))
    }

    fn description(&self) -> String {
        format!(
            "||({}, {:?}) γ{:?}",
            self.source_col, self.separator, self.group_by
        )
    }

    fn over_column(&self) -> usize {
        self.source_col
    }

    fn output_col_type(&self) -> DfType {
        DfType::Text(/* TODO */ Collation::default())
    }

    fn empty_value(&self) -> Option<DfValue> {
        Some("".into())
    }

    fn can_lose_state(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
/// Auxiliary State for a single GroupConcat Node, which is owned by a Domain.
pub struct GroupConcatState {
    last_state: HashMap<Vec<DfValue>, LastState>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ops, LookupIndex};

    fn setup(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        let c = GroupConcat::new(s.as_global(), 1, vec![0], String::from("#")).unwrap();

        g.set_op("concat", &["x", "ys"], c, mat);
        g
    }

    #[test]
    fn it_describes() {
        let c = setup(true);
        assert_eq!(c.node().description(), "||(1, \"#\") γ[0]",);
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn it_forwards() {
        let mut c = setup(true);

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
        eprintln!("{:?}", rs);
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
        assert_eq!(idx[&me], LookupIndex::Strict(Index::hash_map(vec![0])));
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
