use ops;
use query;

use std::collections::HashMap;

use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Mutex;

use flow::prelude::*;

/// Applies the identity operation to the view but waits for an recv on a
/// channel before forwarding. This is useful for writing tests because it
/// enables precise control of the propogation of updates through the graph.
#[derive(Debug)]
pub struct GatedIdentity {
    src: NodeIndex,
    rx: Mutex<Receiver<()>>,
}

impl GatedIdentity {
    /// Construct a new gated identity operator.
    pub fn new(src: NodeIndex) -> (GatedIdentity, Sender<()>) {
        let (tx, rx) = channel();
        let g = GatedIdentity {
            src: src,
            rx: Mutex::new(rx),
        };
        (g, tx)
    }
}

impl Ingredient for GatedIdentity {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        self.src = remap[&self.src];
    }

    fn on_input(&mut self, input: Message, _: &NodeList, _: &StateMap) -> Option<Update> {
        self.rx.lock().unwrap().recv().unwrap();
        input.data.into()
    }

    fn query(&self, q: Option<&query::Query>, domain: &NodeList, states: &StateMap) -> ops::Datas {
        if let Some(state) = states.get(&self.src) {
            // parent is materialized
            state.find(q.map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // parent is not materialized, query into parent
            domain.lookup(self.src).query(q, domain, states)
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // TODO
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        "GatedIdentity".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use petgraph;

    use flow::View;
    use ops::NodeOp;
    use std::thread;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::sync::mpsc::Sender;

    fn setup(materialized: bool) -> (ops::Node, Sender<()>) {
        use std::sync;

        let mut g = petgraph::Graph::new();
        let mut s = ops::new("source", &["x", "y", "z"], true, ops::base::Base {});
        s.prime(&g);
        let s = g.add_node(Some(sync::Arc::new(s)));

        g[s].as_ref().unwrap().process(Some((vec![1.into(), 1.into()], 0).into()), s, 0, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 1.into()], 1).into()), s, 1, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 2.into()], 2).into()), s, 2, true);
        g[s].as_ref().unwrap().process(Some((vec![1.into(), 2.into()], 3).into()), s, 3, true);
        g[s].as_ref().unwrap().process(Some((vec![3.into(), 3.into()], 4).into()), s, 4, true);

        let (mut i, tx) = GatedIdentity::new(s);
        i.prime(&g);

        let op = ops::new("latest", &["x", "y", "z"], materialized, i);
        (op, tx)
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let (i, tx) = GatedIdentity::new(src);
        let left = vec![1.into(), "a".into()];

        let done = Arc::new(AtomicBool::new(false));
        let child_done = done.clone();
        let child = thread::spawn(move || {
            match i.forward(Some(left.clone().into()), src, 0, true, None).unwrap() {
                ops::Update::Records(rs) => {
                    assert_eq!(rs, vec![ops::Record::Positive(left, 0)]);
                }
            };
            &done.store(true, Ordering::SeqCst);
        });

        assert_eq!((&child_done).load(Ordering::SeqCst), false);
        tx.send(()).unwrap();
        child.join().unwrap();
        assert_eq!((&child_done).load(Ordering::SeqCst), true);
    }

    #[test]
    fn it_queries() {
        let (i, _) = setup(false);
        let hits = i.find(None, None);
        println!("{:?}", hits);
        assert_eq!(hits.len(), 5);
    }

    #[test]
    fn it_suggests_indices() {
        let (i, _) = setup(false);
        let idx = i.suggest_indexes(1.into());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let (i, _) = setup(false);
        assert_eq!(i.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(i.resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(i.resolve(2), Some(vec![(0.into(), 2)]));
    }
}
