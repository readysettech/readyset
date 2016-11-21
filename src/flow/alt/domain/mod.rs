use petgraph;
use petgraph::graph::NodeIndex;
use query;
use shortcut;

use std::collections::HashMap;
use std::collections::VecDeque;

use flow::alt;

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

mod list;
mod single;

pub struct Domain {
    domain: Index,
    nodes: list::NodeList,
    state: HashMap<NodeIndex, shortcut::Store<query::DataType>>,
    handoffs: HashMap<NodeIndex, VecDeque<alt::Message>>,
}

impl Domain {
    pub fn from_graph(domain: Index,
                      nodes: Vec<NodeIndex>,
                      graph: &mut petgraph::Graph<alt::Node, alt::Edge>)
                      -> Self {
        let nodes: Vec<_> = nodes.into_iter()
            .map(|ni| {
                use std::mem;
                let n = match *graph.node_weight_mut(ni).unwrap() {
                    alt::Node::Egress(d, ref txs) => {
                        // egress nodes can still be modified externally if subgraphs are added
                        // so we just make a new one with a clone of the Mutex-protected Vec
                        alt::Node::Egress(d, txs.clone())
                    }
                    ref mut n @ alt::Node::Ingress(..) => {
                        // no-one else will be using our ingress node, so we take it from the graph
                        mem::replace(n, alt::Node::Taken)
                    }
                    ref mut n @ alt::Node::Internal(..) => {
                        // same with internal nodes
                        mem::replace(n, alt::Node::Taken)
                    }
                    _ => unreachable!(),
                };
                (ni, n)
            })
            .collect::<Vec<_>>() // because above closure mutably borrows self.mainline
            .into_iter()
            .map(|(ni, n)| {
                // also include all *internal* descendants
                let children: Vec<_> = graph
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&c| graph[c].domain() == domain)
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
                match n.inner {
                    alt::Node::Internal(_, ref i) => {
                        if i.should_materialize() ||
                           graph.edges_directed(n.index, petgraph::EdgeDirection::Outgoing)
                            .any(|e| *e.weight()) {
                            Some((n.index, shortcut::Store::new(i.fields().len())))
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
