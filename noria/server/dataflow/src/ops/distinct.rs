use std::cmp::Ordering;
use std::collections::HashMap;

use crate::prelude::*;

/// This will get distinct records from a set of records compared over a given set of columns
#[derive(Clone, Serialize, Deserialize)]
pub struct Distinct {
    // Parent Node
    src: IndexPair,

    us: Option<IndexPair>,

    group_by: Vec<usize>,
}

impl Distinct {
    pub fn new(src: NodeIndex, group_by: Vec<usize>) -> Self {
        let mut group_by = group_by;
        group_by.sort();
        Distinct {
            src: src.into(),
            us: None,
            group_by,
        }
    }
}

impl Ingredient for Distinct {
    /// Returns a clone of the node
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        // Check that the records aren't empty
        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                ..Default::default()
            };
        }

        let us = self.us.unwrap();
        let db = state
            .get(*us)
            .expect("Distinct must have its own state initialized");

        let pos_comp = |a: &Record, b: &Record| a.is_positive().cmp(&b.is_positive());

        let mut rs: Vec<_> = rs.into();
        // Sort by positive or negative
        rs.sort_by(&pos_comp);

        let group_by = &self.group_by;

        let group_cmp = |a: &Record, b: &Record| {
            group_by
                .iter()
                .map(|&col| &a[col])
                .cmp(group_by.iter().map(|&col| &b[col]))
        };

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        rs.sort_by(&group_cmp);

        let mut output = Vec::new();
        let mut prev_grp = Vec::new();
        let mut prev_pos = false;

        for rec in rs {
            let group_by = &self.group_by[..];
            let group = rec
                .iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if self.group_by.iter().any(|col| col == &i) {
                        Some(v)
                    } else {
                        None
                    }
                })
                .cloned()
                .collect::<Vec<_>>();

            // Do not take overlapping elements
            if prev_grp.iter().cmp(group_by.iter().map(|&col| &rec[col])) == Ordering::Equal
                && prev_pos == rec.is_positive()
            {
                continue;
            }

            // make ready for the new one
            prev_grp.clear();
            prev_grp.extend(group_by.iter().map(|&col| &rec[col]).cloned());
            prev_pos = rec.is_positive();

            let positive = rec.is_positive();
            match db.lookup(group_by, &KeyType::from(&group[..])) {
                LookupResult::Some(rr) => {
                    if positive {
                        //println!("record {:?}", rr);
                        if rr.is_empty() {
                            output.push(rec.clone());
                        }
                    } else if !rr.is_empty() {
                        output.push(rec.clone());
                    }
                }
                LookupResult::Missing => unimplemented!("Distinct does not yet support partial"),
            }
        }

        ProcessingResult {
            results: output.into(),
            ..Default::default()
        }
    }

    fn description(&self, _: bool) -> String {
        "Distinct".into()
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn requires_full_materialization(&self) -> bool {
        true
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        vec![(this, self.group_by.clone())].into_iter().collect()
    }
}

/// Tests for the Distinct Operator
#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();

        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "distinct",
            &["x", "y", "z"],
            Distinct::new(s.as_global(), vec![1, 2]),
            materialized,
        );
        g
    }

    #[test]
    fn simple_distinct() {
        let mut g = setup(true);

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 1.into()];
        let r2: Vec<DataType> = vec![1.into(), "z".into(), 1.into()];
        let r3: Vec<DataType> = vec![1.into(), "c".into(), 2.into()];

        let a = g.narrow_one_row(r1.clone(), true);
        assert_eq!(a, vec![r1.clone()].into());

        let a = g.narrow_one_row(r2.clone(), false);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r3.clone(), true);
        assert_eq!(a, vec![r3.clone()].into());
    }

    #[test]
    fn distinct_neg_record() {
        let mut g = setup(true);

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 1.into()];
        let r2: Vec<DataType> = vec![2.into(), "a".into(), 2.into()];
        let r3: Vec<DataType> = vec![3.into(), "c".into(), 2.into()];

        let a = g.narrow_one_row(r1.clone(), true);
        println!("{:?}", a);
        assert_eq!(a, vec![r1.clone()].into());

        let a = g.narrow_one_row(r2.clone(), true);
        println!("{:?}", a);
        assert_eq!(a, vec![r2.clone()].into());

        let a = g.narrow_one_row(r3.clone(), true);
        assert_eq!(a, vec![r3.clone()].into());

        let a = g.narrow_one_row((r1.clone(), false), true);
        println!("{:?}", a);

        let a = g.narrow_one_row((r1.clone(), true), true);
        println!("{:?}", a);
        assert_eq!(a, vec![r1.clone()].into());
    }

    #[test]
    fn multiple_records_distinct() {
        let mut g = setup(true);

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 1.into()];
        let r2: Vec<DataType> = vec![2.into(), "a".into(), 2.into()];
        let r3: Vec<DataType> = vec![3.into(), "c".into(), 2.into()];

        let a = g.narrow_one(
            vec![
                (r2.clone(), true),
                (r1.clone(), true),
                (r1.clone(), true),
                (r3.clone(), true),
            ],
            true,
        );
        assert!(a.iter().any(|r| r == &(r1.clone(), true).into()));
        assert!(a.iter().any(|r| r == &(r2.clone(), true).into()));
        assert!(a.iter().any(|r| r == &(r3.clone(), true).into()));

        let a = g.narrow_one(vec![(r1.clone(), false), (r3.clone(), true)], true);
        assert!(a.iter().any(|r| r == &(r1.clone(), false).into()));
        assert!(!a.iter().any(|r| r == &(r3.clone(), true).into()));
    }
}
