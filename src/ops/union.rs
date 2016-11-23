use ops;
use query;
use shortcut;

use std::collections::HashMap;

use flow::prelude::*;

/// A union of a set of views.
#[derive(Debug)]
pub struct Union {
    emit: HashMap<NodeIndex, Vec<usize>>,
    cols: HashMap<NodeIndex, usize>,
}

// gather isn't normally Sync, but we know that we're only
// accessing it from one place at any given time, so it's fine..
unsafe impl Sync for Union {}

impl Union {
    /// Construct a new union operator.
    ///
    /// When receiving an update from node `a`, a union will emit the columns selected in `emit[a]`.
    /// `emit` only supports omitting columns, not rearranging them.
    pub fn new(emit: HashMap<NodeIndex, Vec<usize>>) -> Union {
        for emit in emit.values() {
            let mut last = &emit[0];
            for i in emit {
                if i < last {
                    unimplemented!();
                }
                last = i;
            }
        }
        Union {
            emit: emit,
            cols: HashMap::new(),
        }
    }
}

impl Ingredient for Union {
    fn ancestors(&self) -> Vec<NodeIndex> {
        self.emit.keys().cloned().collect()
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols.extend(self.emit.keys().map(|&n| (n, g[n].fields().len())));
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        for (from, to) in remap {
            if from == to {
                continue;
            }

            if let Some(e) = self.emit.remove(from) {
                assert!(self.emit.insert(*to, e).is_none());
            }
            if let Some(e) = self.cols.remove(from) {
                assert!(self.cols.insert(*to, e).is_none());
            }
        }
    }

    fn on_input(&mut self, input: Message, _: &NodeList, _: &StateMap) -> Option<Update> {
        match input.data {
            Update::Records(rs) => {
                let from = input.from;
                let rs = rs.into_iter()
                    .map(move |rec| {
                        let (r, pos) = rec.extract();

                        // yield selected columns for this source
                        // TODO: avoid the .clone() here
                        let res = self.emit[&from].iter().map(|&col| r[col].clone()).collect();

                        // return new row with appropriate sign
                        if pos {
                            ops::Record::Positive(res)
                        } else {
                            ops::Record::Negative(res)
                        }
                    })
                    .collect();
                Some(Update::Records(rs))
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, domain: &NodeList, states: &StateMap) -> ops::Datas {
        use std::iter;

        let mut params = HashMap::new();
        for src in self.emit.keys() {
            params.insert(*src, None);

            // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
            // conditions that filter over a field present in left, and use those as parameters.
            let emit = &self.emit[src];
            if let Some(q) = q {
                let p: Vec<_> = q.having
                    .iter()
                    .map(|c| {
                        shortcut::Condition {
                            column: emit[c.column],
                            cmp: c.cmp.clone(),
                        }
                    })
                    .collect();

                if !p.is_empty() {
                    params.insert(*src, Some(p));
                }
            }
        }

        // we select from each source in turn
        params.into_iter()
            .flat_map(move |(src, params)| {
                let emit = &self.emit[&src];
                let mut select: Vec<_> = iter::repeat(false).take(self.cols[&src]).collect();
                for c in emit {
                    select[*c] = true;
                }
                let cs = params.unwrap_or_else(Vec::new);
                if let Some(state) = states.get(&src) {
                    // parent is materialized
                    state.find(&cs[..]).map(|r| r.iter().cloned().collect()).collect()
                } else {
                    // parent is not materialized, query into parent
                    // TODO: if we're selecting all and have no conds, we could pass q = None
                    domain.lookup(src)
                        .query(Some(&query::Query::new(&select[..], cs)), domain, states)
                }

            })
            .filter_map(move |r| if let Some(q) = q { q.feed(r) } else { Some(r) })
            .collect()
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(self.emit.iter().map(|(src, emit)| (*src, emit[col])).collect())
    }

    fn description(&self) -> String {
        // Ensure we get a consistent output by sorting.
        let mut emit = self.emit.iter().collect::<Vec<_>>();
        emit.sort();
        emit.iter()
            .map(|&(src, emit)| {
                let cols = emit.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}:[{}]", src.index(), cols)
            })
            .collect::<Vec<_>>()
            .join(" ⋃ ")
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
    use std::collections::HashMap;

    fn setup() -> (ops::Node, flow::NodeIndex, flow::NodeIndex) {
        use std::sync;

        let mut g = petgraph::Graph::new();
        let mut l = ops::new("left", &["l0", "l1"], true, ops::base::Base {});
        let mut r = ops::new("right", &["r0", "r1", "r2"], true, ops::base::Base {});

        l.prime(&g);
        r.prime(&g);

        let l = g.add_node(Some(sync::Arc::new(l)));
        let r = g.add_node(Some(sync::Arc::new(r)));

        g[l].as_ref().unwrap().process(Some((vec![1.into(), "a".into()], 0).into()), l, 0, true);
        g[l].as_ref().unwrap().process(Some((vec![2.into(), "b".into()], 1).into()), l, 1, true);
        g[r].as_ref()
            .unwrap()
            .process(Some((vec![1.into(), "skipped".into(), "x".into()], 2).into()),
                     r,
                     2,
                     true);

        let mut emits = HashMap::new();
        emits.insert(l, vec![0, 1]);
        emits.insert(r, vec![0, 2]);

        let mut c = Union::new(emits);
        c.prime(&g);
        (ops::new("union", &["u0", "u1"], false, c), l, r)
    }

    #[test]
    fn it_describes() {
        let (u, _, _) = setup();
        assert_eq!(u.inner.description(), "0:[0, 1] ⋃ 1:[0, 2]");
    }

    #[test]
    fn it_works() {
        let (u, l, r) = setup();

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        match u.process(Some(left.clone().into()), l, 0, true).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(left, 0)]);
            }
        }

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        match u.process(Some(right.clone().into()), r, 0, true).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![1.into(), "x".into()], 0)]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let (u, _, _) = setup();

        // do a full query, which should return left + right:
        // [a, b, x]
        let hits = u.find(None, None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameters matching on both sides
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(1.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter matching only on left
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into()));

        // query with parameter matching only on right
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("x".into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter with no matches
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(3.into())),
                         }]);

        let hits = u.find(Some(&q), None);
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup();
        assert_eq!(u.suggest_indexes(1.into()), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup();
        let r0 = u.resolve(0);
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 0));
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 0));
        let r1 = u.resolve(1);
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 1));
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 2));
    }
}
