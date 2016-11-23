use petgraph;
use shortcut;

use std::collections::HashMap;
use std::collections::VecDeque;

use flow;
use flow::prelude::*;
pub mod list;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
pub struct Index(usize);

impl From<usize> for Index {
    fn from(i: usize) -> Self {
        Index(i)
    }
}

macro_rules! broadcast {
    ($handoffs:ident, $m:expr, $children:expr) => {{
        let c = $children;
        let mut m = Some($m); // so we can .take() below
        for (i, to) in c.iter().enumerate() {
            let u = if i == c.len() - 1 {
                m.take()
            } else {
                m.clone()
            };

            $handoffs.get_mut(to).unwrap().push_back(u.unwrap());
        }
    }}
}

mod single;

pub struct Domain {
    domain: Index,
    nodes: NodeList,
    state: StateMap,
    handoffs: HashMap<NodeIndex, VecDeque<Message>>,
}

impl Domain {
    pub fn from_graph(domain: Index, nodes: Vec<NodeIndex>, graph: &mut Graph) -> Self {
        let nodes: Vec<_> = nodes.into_iter()
            .map(|ni| {
                (ni, graph.node_weight_mut(ni).unwrap().take())
            })
            .collect::<Vec<_>>() // because above closure mutably borrows self.mainline
            .into_iter()
            .map(|(ni, n)| {
                // also include all *internal* descendants
                let children: Vec<_> = graph
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&c| graph[c].domain().unwrap() == domain)
                    .collect();

                single::NodeDescriptor {
                    index: ni,
                    inner: n,
                    children: children,
                }
            })
            .collect();


        let state = nodes.iter()
            .filter_map(|n| {
                // materialized state for any nodes that need it
                // in particular, we keep state for any node that requires its own state to be
                // materialized, or that has an outgoing edge marked as materialized (we know that
                // that edge has to be internal, since ingress/egress nodes have already been
                // added, and they make sure that there are no cross-domain materialized edges).
                match *n.inner {
                    flow::node::Type::Internal(_, ref i) => {
                        if i.should_materialize() ||
                           graph.edges_directed(n.index, petgraph::EdgeDirection::Outgoing)
                            .any(|e| *e.weight()) {
                            Some((n.index, shortcut::Store::new(n.inner.fields().len())))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .collect();

        let handoffs = nodes.iter().map(|n| (n.index, VecDeque::new())).collect();

        Domain {
            domain: domain,
            nodes: nodes.into(),
            state: state,
            handoffs: handoffs,
        }
    }

    pub fn boot(mut self) {
        use std::thread;

        thread::spawn(move || {
            loop {
                // `nodes` is already in topological order, so we just walk over them in order and
                // do the appropriate action for each one.
                for node in &self.nodes {
                    node.borrow_mut().iterate(&mut self.handoffs, &mut self.state, &self.nodes);
                }
            }
        });
    }
}
