use ops;
use query;
use shortcut;

use std::collections::HashMap;
use std::iter;

use flow::prelude::*;

/// Permutes or omits columns from its source node.
#[derive(Debug)]
pub struct Permute {
    emit: Option<Vec<usize>>,
    src: NodeIndex,
    cols: usize,
    us: NodeIndex,
}

impl Permute {
    /// Construct a new permuter operator.
    pub fn new(src: NodeIndex, emit: &[usize]) -> Permute {
        Permute {
            emit: Some(emit.into()),
            src: src,
            cols: 0,
            us: 0.into(),
        }
    }

    fn resolve_col(&self, col: usize) -> usize {
        self.emit.as_ref().map_or(col, |emit| emit[col])
    }

    fn permute(&self, data: &mut [query::DataType]) {
        if let Some(ref emit) = self.emit {
            use std::iter;
            // http://stackoverflow.com/a/1683662/472927
            // TODO: compute the swaps in advance instead
            let mut done: Vec<_> = iter::repeat(false).take(emit.len()).collect();
            for i in 0..emit.len() {
                if done[i] {
                    continue;
                }
                if emit[i] == i {
                    continue;
                }

                let t = data[i].clone();
                let mut j = i;
                loop {
                    done[j] = true;
                    if emit[j] != i {
                        data[j] = data[emit[j]].clone();
                        j = emit[j];
                    } else {
                        data[j] = t;
                        break;
                    }
                }
            }
        }
    }
}

impl Ingredient for Permute {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols = g[self.src].fields().len();
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        self.us = us;
        self.src = remap[&self.src];

        // Eliminate emit specifications which require no permutation of
        // the inputs, so we don't needlessly perform extra work on each
        // update.
        self.emit = self.emit.take().and_then(|emit| {
            let complete = emit.len() == self.cols;
            let sequential = emit.iter().enumerate().all(|(i, &j)| i == j);
            if complete && sequential {
                None
            } else {
                Some(emit)
            }
        });
    }

    fn on_input(&mut self, mut input: Message, _: &NodeList, _: &StateMap) -> Option<Update> {
        debug_assert_eq!(input.from, self.src);

        if self.emit.is_some() {
            match input.data {
                ops::Update::Records(ref mut rs) => {
                    for r in rs {
                        self.permute(&mut *r);
                    }
                }
            }
        }
        input.data.into()
    }

    fn query(&self, q: Option<&query::Query>, domain: &NodeList, states: &StateMap) -> ops::Datas {
        use shortcut::cmp::Comparison::Equal;
        use shortcut::cmp::Value::{Const, Column};

        // TODO: We don't need to select all fields if our permutation
        // drops some fields--`self.permute` will end up dropping them
        // anyway--but it's not worth the trouble.
        let select = iter::repeat(true)
            .take(domain.lookup(self.src).fields().len())
            .collect::<Vec<_>>();

        let q = q.map(|q| {
            let having = q.having.iter().map(|c| {
                shortcut::Condition {
                    column: self.resolve_col(c.column),
                    cmp: match c.cmp {
                        Equal(Const(_)) => c.cmp.clone(),
                        Equal(Column(idx)) => Equal(Column(self.resolve_col(idx))),
                    },
                }
            });
            query::Query::new(&select, having.collect())
        });

        let mut rx = if let Some(state) = states.get(&self.src) {
            // other node is materialized
            state.find(q.as_ref().map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // other node is not materialized, query instead
            domain.lookup(self.src).query(q.as_ref(), domain, states)
        };

        if self.emit.is_some() {
            for r in rx.iter_mut() {
                self.permute(&mut *r);
            }
        }
        rx
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // TODO
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src, self.resolve_col(col))])
    }

    fn description(&self) -> String {
        let emit_cols = match self.emit.as_ref() {
            None => "*".into(),
            Some(emit) => {
                emit.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        format!("π[{}]", emit_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    fn setup(materialized: bool, all: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.seed(s, vec![1.into(), 0.into(), 1.into()]);
        g.seed(s, vec![2.into(), 0.into(), 1.into()]);
        g.seed(s, vec![2.into(), 0.into(), 2.into()]);

        let permutation = if all { vec![0, 1, 2] } else { vec![2, 0] };
        g.set_op("permute",
                 &["x", "y", "z"],
                 Permute::new(s, &permutation[..]));
        if materialized {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false);
        assert_eq!(p.node().description(), "π[2, 0]");
    }

    #[test]
    fn it_describes_all() {
        let p = setup(false, true);
        assert_eq!(p.node().description(), "π[*]");
    }

    #[test]
    fn it_forwards_some() {
        let mut p = setup(false, false);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        match p.narrow_one_row(rec, false).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec!["c".into(), "a".into()])]);
            }
        }
    }

    #[test]
    fn it_forwards_all() {
        let mut p = setup(false, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        match p.narrow_one_row(rec, false).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec!["a".into(), "b".into(), "c".into()])]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let p = setup(false, false);

        let hits = p.query(None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(1)),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_queries_all() {
        let p = setup(false, true);

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(2)),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|r| r[0] == 1.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let p = setup(false, false);
        let idx = p.node().suggest_indexes(1.into());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let p = setup(false, false);
        assert_eq!(p.node().resolve(0), Some(vec![(0.into(), 2)]));
        assert_eq!(p.node().resolve(1), Some(vec![(0.into(), 0)]));
    }

    #[test]
    fn it_resolves_all() {
        let p = setup(false, true);
        assert_eq!(p.node().resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(p.node().resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(p.node().resolve(2), Some(vec![(0.into(), 2)]));
    }
}
