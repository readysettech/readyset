use ops;

use std::collections::HashMap;
use std::sync;

use flow::prelude::*;

/// A union of a set of views.
#[derive(Debug, Clone)]
pub struct Union {
    emit: HashMap<NodeAddress, Vec<usize>>,
    cols: HashMap<NodeAddress, usize>,
}

// gather isn't normally Sync, but we know that we're only
// accessing it from one place at any given time, so it's fine..
unsafe impl Sync for Union {}

impl Union {
    /// Construct a new union operator.
    ///
    /// When receiving an update from node `a`, a union will emit the columns selected in `emit[a]`.
    /// `emit` only supports omitting columns, not rearranging them.
    pub fn new(emit: HashMap<NodeAddress, Vec<usize>>) -> Union {
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
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        self.emit.keys().cloned().collect()
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols.extend(self.emit.keys().map(|&n| (n, g[*n.as_global()].fields().len())));
    }

    fn on_commit(&mut self, _: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
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

    fn on_input(&mut self, input: Message, _: &DomainNodes, _: &StateMap) -> Option<Update> {
        match input.data {
            Update::Records(rs) => {
                let from = input.from;
                let rs = rs.into_iter()
                    .map(move |rec| {
                        let (r, pos) = rec.extract();

                        // yield selected columns for this source
                        // TODO: if emitting all in same order then avoid clone
                        let res = self.emit[&from].iter().map(|&col| r[col].clone()).collect();

                        // return new row with appropriate sign
                        if pos {
                            ops::Record::Positive(sync::Arc::new(res))
                        } else {
                            ops::Record::Negative(sync::Arc::new(res))
                        }
                    })
                    .collect();
                Some(Update::Records(rs))
            }
        }
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, usize> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
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
                format!("{}:[{}]", src, cols)
            })
            .collect::<Vec<_>>()
            .join(" ⋃ ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, NodeAddress, NodeAddress) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1", "r2"]);

        let mut emits = HashMap::new();
        emits.insert(l, vec![0, 1]);
        emits.insert(r, vec![0, 2]);
        g.set_op("union", &["u0", "u1"], Union::new(emits), false);

        let (l, r) = (g.to_local(l), g.to_local(r));
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (u, l, r) = setup();
        assert_eq!(u.node().description(),
                   format!("{}:[0, 1] ⋃ {}:[0, 2]", l, r));
    }

    #[test]
    fn it_works() {
        let (mut u, l, r) = setup();

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        assert_eq!(u.one_row(l, left.clone(), false), Some(vec![left].into()));

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        assert_eq!(u.one_row(r, right.clone(), false),
                   Some(vec![vec![1.into(), "x".into()]].into()));
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup();
        let me = NodeAddress::mock_global(1.into());
        assert_eq!(u.node().suggest_indexes(me), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup();
        let r0 = u.node().resolve(0);
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 0));
        assert!(r0.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 0));
        let r1 = u.node().resolve(1);
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == l && c == 1));
        assert!(r1.as_ref().unwrap().iter().any(|&(n, c)| n == r && c == 2));
    }
}
