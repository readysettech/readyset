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
#[allow(dead_code)]
pub struct GatedIdentity {
    src: NodeAddress,
    rx: Mutex<Receiver<()>>,
}

impl GatedIdentity {
    /// Construct a new gated identity operator.
    #[allow(dead_code)]
    pub fn new(src: NodeAddress) -> (GatedIdentity, Sender<()>) {
        let (tx, rx) = channel();
        let g = GatedIdentity {
            src: src,
            rx: Mutex::new(rx),
        };
        (g, tx)
    }
}

impl Ingredient for GatedIdentity {
    fn take(&mut self) -> Box<Ingredient> {
        use std::mem;
        // we cheat a little here because rx can't be cloned. we just construct a new GatedIdentity
        // (with a separate channel), and leave that behind in the graph. this is fine, since that
        // channel will never be used for anything.
        let src = self.src;
        Box::new(mem::replace(self, Self::new(src).0))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        self.src = remap[&self.src];
    }

    fn on_input(&mut self,
                _: NodeAddress,
                rs: Records,
                _: &DomainNodes,
                _: &StateMap)
                -> ProcessingResult {
        self.rx
            .lock()
            .unwrap()
            .recv()
            .unwrap();
        ProcessingResult::Done(rs, 0)
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        // TODO
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        "GatedIdentity".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    use std::thread;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::sync::mpsc::Sender;

    fn setup() -> (ops::test::MockGraph, Sender<()>) {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        let (i, tx) = GatedIdentity::new(s);
        g.set_op("identity", &["x", "y", "z"], i, false);
        (g, tx)
    }

    #[test]
    fn it_forwards() {
        let (mut i, tx) = setup();
        let left = vec![1.into(), "a".into()];

        let done = Arc::new(AtomicBool::new(false));
        let child_done = done.clone();
        let child = thread::spawn(move || {
                                      assert_eq!(i.narrow_one_row(left.clone(), false),
                                                 vec![left].into());
                                      &done.store(true, Ordering::SeqCst);
                                  });

        assert_eq!((&child_done).load(Ordering::SeqCst), false);
        tx.send(()).unwrap();
        child.join().unwrap();
        assert_eq!((&child_done).load(Ordering::SeqCst), true);
    }

    #[test]
    fn it_suggests_indices() {
        let (i, _) = setup();
        let me = NodeAddress::mock_global(1.into());
        let idx = i.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let (i, _) = setup();
        assert_eq!(i.node().resolve(0), Some(vec![(i.narrow_base_id(), 0)]));
        assert_eq!(i.node().resolve(1), Some(vec![(i.narrow_base_id(), 1)]));
        assert_eq!(i.node().resolve(2), Some(vec![(i.narrow_base_id(), 2)]));
    }
}
