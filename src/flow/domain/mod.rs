use petgraph;
use shortcut;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc;

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

impl Into<usize> for Index {
    fn into(self) -> usize {
        self.0
    }
}

pub mod single;

pub struct Domain {
    domain: Index,
    nodes: NodeList,
    state: StateMap,
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
                    .filter(|&c| {
                        graph[c].domain().unwrap() == domain
                    })
                    .collect();

                single::NodeDescriptor {
                    index: ni,
                    inner: n,
                    children: children,
                }
            })
            .collect();

        let mut state: HashMap<_, _> = nodes.iter()
            .filter_map(|n| {
                // materialized state for any nodes that need it
                // in particular, we keep state for
                //
                //  - any internal node that requires its own state to be materialized
                //  - any internal node that has an outgoing edge marked as materialized (we know
                //    that that edge has to be internal, since ingress/egress nodes have already
                //    been added, and they make sure that there are no cross-domain materialized
                //    edges).
                //  - any ingress node with children that say that they may query their ancestors
                //
                // that last point needs to be checked *after* we have determined if all internal
                // nodes should be materialized
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

        let inquisitive_children: HashSet<_> = nodes.iter()
            .filter_map(|n| {
                if let flow::node::Type::Internal(..) = *n.inner {
                    if n.will_query(state.contains_key(&n.index)) {
                        return Some(n.index);
                    }
                }
                None
            })
            .collect();


        for n in &nodes {
            if let flow::node::Type::Ingress(..) = *n.inner {
                if graph.neighbors_directed(n.index, petgraph::EdgeDirection::Outgoing)
                    .any(|child| inquisitive_children.contains(&child)) {
                    state.insert(n.index, shortcut::Store::new(n.inner.fields().len()));
                }
            }
        }

        Domain {
            domain: domain,
            nodes: nodes.into(),
            state: state,
        }
    }

    pub fn dispatch(m: Message, states: &mut StateMap, nodes: &NodeList) {
        let me = m.to;

        let mut n = nodes.lookup_mut(me);
        let mut u = n.process(m, states, nodes);
        drop(n);

        let n = nodes.lookup(me);
        for i in 0..n.children.len() {
            // avoid cloning if we can
            let data = if i == n.children.len() - 1 {
                u.take()
            } else {
                u.clone()
            };

            let m = Message {
                from: me,
                to: n.children[i],
                data: data.unwrap(),
            };

            Self::dispatch(m, states, nodes)
        }
    }

    pub fn boot(self, rx: mpsc::Receiver<Message>) {
        use std::thread;

        thread::spawn(move || {
            let mut states = self.state;
            let nodes = self.nodes;
            for m in rx {
                Self::dispatch(m, &mut states, &nodes);
            }
        });
    }
}
