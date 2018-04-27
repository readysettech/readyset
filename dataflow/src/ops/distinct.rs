use std::collections::HashMap;
use std::cmp::Ordering;

use prelude::*;

/// This will get Distinct elements from each group
/// TODO

#[derive(Clone, Serialize, Deserialize)]
pub struct Distinct {
    // Parent Node
    src: IndexPair,

    us: Option<IndexPair>,

    group_by: Vec<usize>,

    // A list of records that have been seen
    seen: HashMap<Vec<DataType>, bool>,
}

impl Distinct {
    pub fn new(
        src: NodeIndex,
        group_by: Vec<usize>,
        ) -> Self {

        // TODO add implementation for group by
        let group_by = group_by;
        Distinct {
            src: src.into(),
            us: None,
            group_by: group_by,
            seen: HashMap::new(),
        }
    }
}

impl Ingredient for Distinct {
    /// Returns a clone of the node
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        // I think this is right?
        vec![self.src.as_global()]
    }

    /// TODO
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_col: Option<usize>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        // Check that the records aren't empty
        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        // TODO
        // Preproc the records
        // Sort to get the column we want
        // check if we have seen it if so then keep it
        let us = self.us.unwrap();
        let db = state
            .get(&*us)
            .expect("Distinct must have its own state initialized");

        let mut output = Vec::new();
        for rec in rs.iter() {
            let group_by = &self.group_by[..];
            let group = rec.iter()
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
            let positive = rec.is_positive();
            if positive {
                match db.lookup(group_by, &KeyType::from(&group[..])) {
                    LookupResult::Some(rs) => {
                        println!("{}", rs.is_empty());
                        if rs.is_empty(){
                            output.push(rec.clone());
                        }
                    },
                    LookupResult::Missing => {
                        // TODO
                        ()
                    }
                }
            } else {
                match db.lookup(group_by, &KeyType::from(&group[..])) {
                    LookupResult::Some(_) => {
                        println!("{}", rs.is_empty());
                        if !rs.is_empty(){
                            output.push(rec.clone());
                        }
                    },
                    LookupResult::Missing => {
                        ()
                    }
                }
            }

        }

        //for rec in rs.iter() {
        //    let group = rec.iter()
        //        .enumerate()
        //        .filter_map(|(i, v)| {
        //            if self.group_by.iter().any(|col| col == &i) {
        //                Some(v)
        //            } else {
        //                None
        //            }
        //        })
        //        .cloned()
        //        .collect::<Vec<_>>();

        //    let positive = rec.is_positive();
        //    if self.seen.contains_key(&group){
        //        if !positive{
        //            self.seen.remove(&group);
        //            //output.push(rec.clone());
        //        }
        //    } else {
        //        if positive {
        //            //output.push(rec.clone());
        //            // self.seen.insert(group.clone(), true);
        //        }
        //    }
        //}
        //
        ProcessingResult {
            results: output.into(),
            misses: Vec::new(),
        }
    }

    fn description(&self) -> String {
        // TODO
        "Distinct".into()
    }


    fn on_connected(&mut self, _: &Graph) {
        // TODO I think something is meant to go here, but I'm not sure what?
    }

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

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        vec![
            (this, (self.group_by.clone(), true)),
        ].into_iter()
            .collect()
    }
}

/// Tests for the Distinct Operator
#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();


        let s = g.add_base("source", &["x", "y", "z"]);
        // TODO add groupby to test
        g.set_op(
            "distinct",
            &["x", "y", "z"],
            Distinct::new(s.as_global(), vec![1]),
            materialized,
        );
        g
    }

    #[test]
    fn simple_distinct() {
        let mut g = setup(true);

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 1.into()];
        let r2: Vec<DataType> = vec![1.into(), "z".into(), 2.into()];
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
        println!("{:?}",a);
        assert_eq!(a, vec![r1.clone()].into());

        let a = g.narrow_one_row(r2.clone(), true);
        println!("{:?}",a);
        assert_eq!(a, vec![r2.clone()].into());

        let a = g.narrow_one_row(r3.clone(), true);
        assert_eq!(a, vec![r3.clone()].into());

        let a = g.narrow_one_row((r1.clone(), false), true);
        let a = g.narrow_one_row((r1.clone(), true), true);
        println!("{:?}",a);
        assert_eq!(a, vec![r1.clone()].into());
    }
}
