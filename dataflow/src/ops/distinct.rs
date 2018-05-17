use std::collections::HashMap;

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
        let mut group_by = group_by;
        group_by.sort();
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

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
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

        let us = self.us.unwrap();
        let db = state
            .get(&*us)
            .expect("Distinct must have its own state initialized");

        let mut seen = HashMap::new();
        let mut cleaned = Vec::new();

        // Preprocess current batch to remove duplicates
        // since those won't be caught by checking state below.
        for rec in rs.iter() {
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
            if seen.contains_key(&group){
                if !positive{
                    seen.remove(&group);
                    cleaned.push(rec.clone());
                }
            } else {
                cleaned.push(rec.clone());
                seen.insert(group.clone(), true);
            }
        }

        let mut misses = Vec::new();
        let mut output = Vec::new();
        for rec in cleaned.iter() {
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
                    LookupResult::Some(rr) => {
                        //println!("record {:?}", rr);
                        if rr.len() == 0 {
                            output.push(rec.clone());
                        }
                    },
                    LookupResult::Missing => {
                        println!("Missing");
                        ()
                    }
                }
            } else {
                match db.lookup(group_by, &KeyType::from(&group[..])) {
                    LookupResult::Some(_) => {
                        if !rs.is_empty(){
                            output.push(rec.clone());
                        }
                    },
                    LookupResult::Missing => {
                        println!("Missing");
                        ()
                    }
                }
            }

        }

        
        ProcessingResult {
            results: output.into(),
            misses: misses,
        }
    }

    fn description(&self) -> String {
        "Distinct".into()
    }


    fn on_connected(&mut self, _: &Graph) {
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

    fn requires_full_materialization(&self) -> bool {
        true
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
