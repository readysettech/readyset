use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug)]
pub struct Identity {
    src: flow::NodeIndex,
    srcn: Option<ops::V>,
}

impl Identity {
    /// Construct a new identity operator.
    pub fn new(src: flow::NodeIndex) -> Identity {
        Identity {
            src: src,
            srcn: None,
        }
    }
}

impl From<Identity> for NodeType {
    fn from(b: Identity) -> NodeType {
        NodeType::Identity(b)
    }
}

impl NodeOp for Identity {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        self.srcn = g[self.src].as_ref().cloned();

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
        update.into()
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        self.srcn.as_ref().unwrap().find(q, Some(ts))
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        self.srcn.as_ref().unwrap().suggest_indexes(self.src)
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        Some(vec![(self.src, col)])
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
