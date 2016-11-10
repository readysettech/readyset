use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::iter;

use shortcut;

/// Permutes or omits columns from its source node.
#[derive(Debug)]
pub struct Permute {
    emit: Option<Vec<usize>>,
    src: flow::NodeIndex,
    srcn: Option<ops::V>,
}

impl Permute {
    /// Construct a new permuter operator.
    pub fn new(src: flow::NodeIndex, emit: &[usize]) -> Permute {
        Permute {
            emit: Some(emit.into()),
            src: src,
            srcn: None,
        }
    }

    fn resolve_col(&self, col: usize) -> usize {
        self.emit.as_ref().map_or(col, |emit| emit[col])
    }

    fn permute(&self, data: Vec<query::DataType>) -> Vec<query::DataType> {
        self.emit.as_ref()
            .map(|emit| {
                // TODO: Avoid this clone when the permutation doesn't
                // duplicate source columns. The borrow checker makes this
                // quite hard.
                emit.iter().map(|&col| data[col].clone()).collect()
            })
            .unwrap_or(data)
    }
}

impl From<Permute> for NodeType {
    fn from(b: Permute) -> NodeType {
        NodeType::Permute(b)
    }
}

impl NodeOp for Permute {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        self.srcn = g[self.src].as_ref().cloned();

        // Eliminate emit specifications which require no permutation of
        // the inputs, so we don't needlessly perform extra work on each
        // update.
        self.emit = self.emit.take().and_then(|emit| {
            let complete = emit.len() == self.srcn.as_ref().unwrap().args().len();
            let sequential = emit.iter().enumerate().all(|(i, &j)| i == j);
            if complete && sequential {
                None
            } else {
                Some(emit)
            }
        });

        vec![self.src]
    }

    #[allow(unused_variables)]
    fn forward(&self,
               update: Option<ops::Update>,
               src: flow::NodeIndex,
               timestamp: i64,
               last: bool,
               materialized_view: Option<&backlog::BufferedStore>)
               -> flow::ProcessingResult<ops::Update> {

        if update.is_none() {
            debug_assert!(last);
            return update.into();
        }

        if let Some(ref emit) = self.emit {
            let update = update.unwrap();
            match update {
                ops::Update::Records(rs) => {
                    if rs.is_empty() {
                        return flow::ProcessingResult::Skip;
                    }

                    let out = rs.into_iter().map(|r| {
                        let (data, pos, ts) = r.extract();
                        (self.permute(data), ts, pos).into()
                    }).collect::<Vec<_>>();

                    flow::ProcessingResult::Done(ops::Update::Records(out))
                }
            }
        } else {
            update.into()
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use shortcut::cmp::Comparison::Equal;
        use shortcut::cmp::Value::{Const, Column};

        // TODO: We don't need to select all fields if our permutation
        // drops some fields--`self.permute` will end up dropping them
        // anyway--but it's not worth the trouble.
        let select = iter::repeat(true)
            .take(self.srcn.as_ref().unwrap().args().len())
            .collect::<Vec<_>>();

        let q = q.map(|q| {
            let having = q.having.iter().map(|c| shortcut::Condition {
                column: self.resolve_col(c.column),
                cmp: match c.cmp {
                    Equal(Const(_)) => c.cmp.clone(),
                    Equal(Column(idx)) => Equal(Column(self.resolve_col(idx)))
                }
            });
            query::Query::new(&select, having.collect())
        });

        self.srcn.as_ref().unwrap().find(q.as_ref(), Some(ts))
            .into_iter()
            .map(|(r, ts)| (self.permute(r), ts))
            .collect()
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        self.srcn.as_ref().unwrap().suggest_indexes(self.src)
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        Some(vec![(self.src, self.resolve_col(col))])
    }

    fn description(&self) -> String {
        let emit_cols = match self.emit.as_ref() {
            None => "*".into(),
            Some(emit) => {
                emit.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>().join(", ")
            }
        };
        format!("π[{}]", emit_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use petgraph;
    use shortcut;

    use flow::View;
    use ops::NodeOp;

    fn setup(materialized: bool, all: bool) -> ops::Node {
        use std::sync;

        let mut g = petgraph::Graph::new();
        let mut s = ops::new("source", &["x", "y", "z"], true, ops::base::Base {});
        s.prime(&g);
        let s = g.add_node(Some(sync::Arc::new(s)));

        g[s].as_ref().unwrap().process(Some((vec![1.into(), 0.into(), 1.into()], 0).into()), s, 0, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 0.into(), 1.into()], 1).into()), s, 1, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 0.into(), 2.into()], 2).into()), s, 2, true);

        let permutation = if all {
            vec![0, 1, 2]
        } else {
            vec![2, 0]
        };

        let mut p = Permute::new(s, &permutation[..]);
        p.prime(&g);

        ops::new("latest", &["x", "y", "z"], materialized, p)
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false);
        assert_eq!(p.inner.description(), "π[2, 0]");

        let p = setup(false, true);
        assert_eq!(p.inner.description(), "π[*]");
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let p = Permute::new(src, &[2, 1]);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        match p.forward(Some(rec.clone().into()), src, 0, true, None).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(vec!["c".into(), "b".into()], 0)]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let p = setup(false, false);

        let hits = p.find(None, None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(1)),
                         }]);

        let hits = p.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));


        let p = setup(false, true);

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(2)),
                         }]);

        let hits = p.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let p = setup(false, false);
        let idx = p.suggest_indexes(1.into());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let p = setup(false, false);
        assert_eq!(p.resolve(0), Some(vec![(0.into(), 2)]));
        assert_eq!(p.resolve(1), Some(vec![(0.into(), 0)]));

        let p = setup(false, true);
        assert_eq!(p.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(p.resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(p.resolve(2), Some(vec![(0.into(), 2)]));
    }
}
