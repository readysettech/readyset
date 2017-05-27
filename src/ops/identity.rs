use std::collections::HashMap;

use flow::prelude::*;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone)]
pub struct Identity {
    src: NodeAddress,
}

impl Identity {
    /// Construct a new identity operator.
    pub fn new(src: NodeAddress) -> Identity {
        Identity { src: src }
    }
}

impl Ingredient for Identity {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
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

    fn on_commit(&mut self, me: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        // an identity is sometimes injected into the graph (to de-shard), which causes its src
        // address to be remapped to its address in  all of its (new) children. unfortunately, this
        // means that *we* will also be told to remap our parent to ourselves, which is
        // non-sensical and should be ignored.
        if remap[&self.src] != me {
            self.src = remap[&self.src];
        }
    }

    fn on_input(&mut self,
                _: NodeAddress,
                rs: Records,
                _: &mut Tracer,
                _: &DomainNodes,
                _: &StateMap)
                -> ProcessingResult {
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
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        "≡".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op("identity", &["x", "y", "z"], Identity::new(s), materialized);
        g
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false);

        let left: Vec<DataType> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false);
        let me = NodeAddress::mock_global(1.into());
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false);
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(g.narrow_base_id(), 2)]));
    }
}
