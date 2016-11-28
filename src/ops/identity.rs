use ops;
use query;

use std::collections::HashMap;

use flow::prelude::*;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug)]
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

    fn on_input(&mut self, input: Message, _: &DomainNodes, _: &StateMap) -> Option<Update> {
        input.data.into()
    }

    fn query(&self,
             q: Option<&query::Query>,
             domain: &DomainNodes,
             states: &StateMap)
             -> ops::Datas {
        if let Some(state) = states.get(self.src.as_local()) {
            // parent is materialized
            state.find(q.map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // parent is not materialized, query into parent
            domain[self.src.as_local()].borrow().query(q, domain, states)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.seed(s, vec![1.into(), 1.into(), 1.into()]);
        g.seed(s, vec![2.into(), 1.into(), 1.into()]);
        g.seed(s, vec![2.into(), 2.into(), 1.into()]);
        g.seed(s, vec![1.into(), 2.into(), 1.into()]);
        g.seed(s, vec![3.into(), 3.into(), 1.into()]);
        g.set_op("identity", &["x", "y", "z"], Identity::new(s));
        if materialized {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false);

        let left = ops::Record::Positive(vec![1.into(), "a".into()]);
        match g.narrow_one(vec![left.clone()], false).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![left]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let g = setup(false);
        let hits = g.query(None);
        println!("{:?}", hits);
        assert_eq!(hits.len(), 5);
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
