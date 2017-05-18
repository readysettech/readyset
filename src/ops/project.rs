use std::collections::HashMap;
use std::sync;

use flow::prelude::*;

/// Permutes or omits columns from its source node, or adds additional literal value columns.
#[derive(Debug, Clone)]
pub struct Project {
    us: Option<NodeAddress>,
    emit: Option<Vec<usize>>,
    additional: Option<Vec<DataType>>,
    src: NodeAddress,
    cols: usize,
}

impl Project {
    /// Construct a new permuter operator.
    pub fn new(src: NodeAddress, emit: &[usize], additional: Option<Vec<DataType>>) -> Project {
        Project {
            emit: Some(emit.into()),
            additional: additional,
            src: src,
            cols: 0,
            us: None,
        }
    }

    fn resolve_col(&self, col: usize) -> usize {
        if self.emit.is_some() && col >= self.emit.as_ref().unwrap().len() {
            panic!("can't resolve literal column {} that doesn't come from parent node!",
                   col);
        } else {
            self.emit.as_ref().map_or(col, |emit| emit[col])
        }
    }
}

impl Ingredient for Project {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _materialized: bool) -> bool {
        false
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols = g[*self.src.as_global()].fields().len();
    }

    fn on_commit(&mut self, us: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
        self.src = remap[&self.src];

        // Eliminate emit specifications which require no permutation of
        // the inputs, so we don't needlessly perform extra work on each
        // update.
        self.emit = self.emit.take().and_then(|emit| {
            let complete = emit.len() == self.cols && self.additional.is_none();
            let sequential = emit.iter().enumerate().all(|(i, &j)| i == j);
            if complete && sequential {
                None
            } else {
                Some(emit)
            }
        });
    }

    fn on_input(&mut self,
                from: NodeAddress,
                mut rs: Records,
                _: &mut Tracer,
                _: &DomainNodes,
                _: &StateMap)
                -> ProcessingResult {
        debug_assert_eq!(from, self.src);

        if self.emit.is_some() {
            for r in &mut *rs {
                if self.emit.is_none() {
                    continue;
                }

                let mut new_r = Vec::with_capacity(r.len());
                let e = self.emit.as_ref().unwrap();
                for i in e {
                    new_r.push(r[*i].clone());
                }
                let a = self.additional.as_ref().unwrap();
                for i in a {
                    new_r.push(i.clone());
                }
                **r = sync::Arc::new(new_r);
            }
        }
        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        // TODO
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, self.resolve_col(col))])
    }

    fn description(&self) -> String {
        let emit_cols = match self.emit.as_ref() {
            None => "*".into(),
            Some(emit) => {
                match self.additional {
                    None => {
                        emit.iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                    Some(ref add) => {
                        emit.iter()
                            .map(|e| e.to_string())
                            .chain(add.iter().map(|e| format!("lit: {}", e.to_string())))
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                }
            }
        };
        format!("π[{}]", emit_cols)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        let result = if self.emit.is_some() && column >= self.emit.as_ref().unwrap().len() {
            None
        } else {
            Some(self.resolve_col(column))
        };
        vec![(self.src, result)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool, all: bool, add: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        let permutation = if all { vec![0, 1, 2] } else { vec![2, 0] };
        let additional = if add {
            Some(vec![DataType::from("hello"), DataType::Int(42)])
        } else {
            None
        };
        g.set_op("permute",
                 &["x", "y", "z"],
                 Project::new(s, &permutation[..], additional),
                 materialized);
        g
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false, true);
        assert_eq!(p.node().description(), "π[2, 0, lit: \"hello\", lit: 42]");
    }

    #[test]
    fn it_describes_all() {
        let p = setup(false, true, false);
        assert_eq!(p.node().description(), "π[*]");
    }

    #[test]
    fn it_describes_all_w_literals() {
        let p = setup(false, true, true);
        assert_eq!(p.node().description(),
                   "π[0, 1, 2, lit: \"hello\", lit: 42]");
    }

    #[test]
    fn it_forwards_some() {
        let mut p = setup(false, false, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(p.narrow_one_row(rec, false),
                   vec![vec!["c".into(), "a".into(), "hello".into(), 42.into()]].into());
    }

    #[test]
    fn it_forwards_all() {
        let mut p = setup(false, true, false);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(p.narrow_one_row(rec, false),
                   vec![vec!["a".into(), "b".into(), "c".into()]].into());
    }

    #[test]
    fn it_forwards_all_w_literals() {
        let mut p = setup(false, true, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(p.narrow_one_row(rec, false),
                   vec![vec!["a".into(),
                             "b".into(),
                             "c".into(),
                             "hello".into(),
                             42.into()]]
                           .into());
    }

    #[test]
    fn it_suggests_indices() {
        let me = NodeAddress::mock_global(1.into());
        let p = setup(false, false, true);
        let idx = p.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let p = setup(false, false, true);
        assert_eq!(p.node().resolve(0), Some(vec![(p.narrow_base_id(), 2)]));
        assert_eq!(p.node().resolve(1), Some(vec![(p.narrow_base_id(), 0)]));
    }

    #[test]
    fn it_resolves_all() {
        let p = setup(false, true, true);
        assert_eq!(p.node().resolve(0), Some(vec![(p.narrow_base_id(), 0)]));
        assert_eq!(p.node().resolve(1), Some(vec![(p.narrow_base_id(), 1)]));
        assert_eq!(p.node().resolve(2), Some(vec![(p.narrow_base_id(), 2)]));
    }

    #[test]
    #[should_panic(expected = "can't resolve literal column")]
    fn it_fails_to_resolve_literal() {
        let p = setup(false, false, true);
        p.node().resolve(2);
    }
}
