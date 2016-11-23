use ops;
use query;

use std::collections::HashMap;

use flow::prelude::*;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug)]
pub struct Identity {
    src: NodeIndex,
}

impl Identity {
    /// Construct a new identity operator.
    pub fn new(src: NodeIndex) -> Identity {
        Identity { src: src }
    }
}

impl Ingredient for Identity {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        self.src = remap[&self.src];
    }

    fn on_input(&mut self, input: Message, _: &NodeList, _: &StateMap) -> Option<Update> {
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
        "≡".into()
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

    fn setup(materialized: bool) -> ops::Node {
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

        let mut i = Identity::new(s);
        i.prime(&g);

        ops::new("latest", &["x", "y", "z"], materialized, i)
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let i = Identity::new(src);

        let left = vec![1.into(), "a".into()];
        match i.forward(Some(left.clone().into()), src, 0, true, None).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(left, 0)]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let i = setup(false);
        let hits = i.find(None, None);
        println!("{:?}", hits);
        assert_eq!(hits.len(), 5);
    }

    #[test]
    fn it_suggests_indices() {
        let i = setup(false);
        let idx = i.suggest_indexes(1.into());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let i = setup(false);
        assert_eq!(i.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(i.resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(i.resolve(2), Some(vec![(0.into(), 2)]));
    }
}
