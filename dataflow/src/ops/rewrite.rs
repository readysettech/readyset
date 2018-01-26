use std::collections::HashMap;
use prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rewrite {
    src: IndexPair,
    should_rewrite: IndexPair,

    rw_col: usize,
    value: DataType,

    key: usize,
}

impl Rewrite {
    pub fn new(src: NodeIndex, should_rewrite: NodeIndex, rw_col: usize, value: DataType, key: usize) -> Rewrite {
        Rewrite {
            src: src.into(),
            should_rewrite: should_rewrite.into(),
            rw_col: rw_col,
            value: value,
            key: key,
        }
    }

    fn rewrite(&self, r: Vec<DataType>) -> Vec<DataType> {
        let mut r = r.clone();
        r[self.rw_col] = self.value.clone();
        r
    }

    fn generate_row(&self, r: Vec<DataType>, positive: bool) -> Record {
        if positive {
            Record::Positive(r.clone())
        } else {
            Record::Negative(r.clone())
        }
    }
}


impl Ingredient for Rewrite {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global(), self.should_rewrite.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.should_rewrite.remap(remap);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        assert!(from == *self.src || from == *self.should_rewrite);
        let mut misses = Vec::new();
        let mut emit_rs = Vec::with_capacity(rs.len());

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        for r in rs {
            if from == *self.src {
                let key = r[self.key].clone();
                // ask should_rewrite if column should be rewritten
                let rc = self.lookup(
                    *self.should_rewrite,
                    &[0],
                    &KeyType::Single(&key),
                    nodes,
                    state,
                ).unwrap();


                if rc.is_none() {
                    // todo (larat): might need to do some stuff here
                    continue;
                }

                let mut rc = rc.unwrap().peekable();
                if rc.peek().is_none() {
                    emit_rs.push(r);
                } else {
                    let (r, positive) = r.extract();
                    let row = self.generate_row(self.rewrite(r), positive);
                    emit_rs.push(row);
                }

            } else if from == *self.should_rewrite {
                let key = r[0].clone();
                let other_rows = self.lookup(
                    *self.src,
                    &[self.key],
                    &KeyType::Single(&key),
                    nodes,
                    state,
                ).unwrap();

                if other_rows.is_none() {
                    // todo (larat): might need to do some stuff here
                    continue;
                }

                let mut other_rows = other_rows.unwrap().peekable();
                if other_rows.peek().is_none() {
                    continue;
                } else {
                    let mut other;;
                    while other_rows.peek().is_some() {
                        other = other_rows.next().unwrap();
                        if r.is_positive() {
                            // emit negatives for other
                            emit_rs.push(self.generate_row(other.to_vec(), false));
                            // emit positives for rewritten other
                            emit_rs.push(self.generate_row(self.rewrite(other.to_vec()), true));
                        } else {
                            // emit positives for other
                            emit_rs.push(self.generate_row(other.to_vec(), true));
                            // emit negatives for rewritten other
                            emit_rs.push(self.generate_row(self.rewrite(other.to_vec()), false));
                        }

                    }
                }

            }
        }

        ProcessingResult {
            results: emit_rs.into(),
            misses: misses,
        }

    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        vec![
            (self.should_rewrite.as_global(), (vec![0], true)),
            (self.src.as_global(), (vec![self.key], true)),
        ].into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self) -> String {
        // TODO: better description
        "R".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let src = g.add_base("src", &["id", "rw_col"]);
        let should_rewrite = g.add_base("should_rewrite", &["id"]);

        let rw = Rewrite::new(
            src.as_global(),
            should_rewrite.as_global(),
            1,
            "NONE".into(),
            0,
        );

        g.set_op("rewrite", &["rw0", "rw1"], rw, false);
        (g, src, should_rewrite)
    }

    #[test]
    fn it_works() {
        let (mut rw, src, should_rw) = setup();

        let src_a1 = vec![1.into(), "a".into()];
        let src_b2 = vec![2.into(), "b".into()];

        let rw1 = vec![1.into()];
        let rw2 = vec![2.into()];

        let result = vec![((vec![1.into(), "a".into()], true))].into();
        rw.seed(src, src_a1.clone());
        let rs = rw.one_row(src, src_a1.clone(), false);
        assert_eq!(rs, result);

        rw.seed(should_rw, rw2.clone());
        let rs = rw.one_row(should_rw, rw2.clone(), false);

        // forward [2, b] to src; should be rewritten and produce [2, "NONE"].
        let result = vec![((vec![2.into(), "NONE".into()], true))].into();
        rw.seed(src, src_b2.clone());
        let rs = rw.one_row(src, src_b2.clone(), false);
        assert_eq!(rs, result);

        // forward 1 to should_rewrite; should produce Positive([1, "NONE"]) and Negative([1, "a"]).
        let result = vec![((vec![1.into(), "a".into()], false)), ((vec![1.into(), "NONE".into()], true))].into();
        rw.seed(should_rw, rw1.clone());
        let rs = rw.one_row(should_rw, rw1.clone(), false);
        assert_eq!(rs, result);
    }
}
