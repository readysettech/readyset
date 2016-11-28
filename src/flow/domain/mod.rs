use petgraph;
use shortcut;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc;
use std::cell;

use flow;
use flow::prelude::*;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
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
    nodes: DomainNodes,
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

        // Now let's talk indices.
        //
        // We need to query all our nodes for what indices they believe should be maintained, and
        // apply those to the stores in state. However, this is somewhat complicated by the fact
        // that we need to push indices through non-materialized views so that they end up on the
        // columns of the views that will actually query into a table of some sort.
        {
            let nodes: HashMap<_, _> = nodes.iter().map(|n| (n.index, n)).collect();
            let mut indices = nodes.iter()
                .filter(|&(_, node)| node.is_internal()) // only internal nodes can suggest indices
                .filter(|&(_, node)| {
                    // under what circumstances might a node need indices to be placed?
                    // there are two cases:
                    //
                    //  - if makes queries into its ancestors regardless of whether it's
                    //    materialized or not
                    //  - if it queries its ancestors when it is *not* materialized (implying that
                    //    it queries into its own output)
                    //
                    //  unless we come up with a weird operators that *doesn't* need indices when
                    //  it is *not* materialized, but *does* when is, we can therefore just use
                    //  will_query(false) as an indicator of whether indices are necessary.
                    node.will_query(false)
                })
                .flat_map(|(ni, node)| node.suggest_indexes(*ni).into_iter())
                .filter(|&(ref node, _)| nodes.contains_key(node))
                .fold(HashMap::new(), |mut hm, (v, idxs)| {
                    hm.entry(v).or_insert_with(HashSet::new).extend(idxs.into_iter());
                    hm
                });

            // push down indices
            let mut leftover_indices: HashMap<_, _> = indices.drain().collect();
            let mut tmp = HashMap::new();
            while !leftover_indices.is_empty() {
                for (v, cols) in leftover_indices.drain() {
                    if let Some(ref mut state) = state.get_mut(&v) {
                        // this node is materialized! add the indices!
                        for col in cols {
                            println!("adding index on column {:?} of view {:?}", col, v);
                            // TODO: don't re-add indices that already exist
                            state.index(col, shortcut::idx::HashIndex::new());
                        }
                    } else if let Some(ref node) = nodes.get(&v) {
                        // this node is not materialized
                        // we need to push the index up to its ancestor(s)
                        if let flow::node::Type::Ingress(..) = *node.inner {
                            // we can't push further up!
                            unreachable!("node suggested index outside domain, and ingress isn't \
                                          materalized");
                        }

                        assert!(node.is_internal());
                        for col in cols {
                            let really = node.resolve(col);
                            if let Some(really) = really {
                                // the index should instead be placed on the corresponding
                                // columns of this view's inputs
                                for (v, col) in really {
                                    tmp.entry(v).or_insert_with(HashSet::new).insert(col);
                                }
                            } else {
                                // this view is materialized, so we should index this column
                                indices.entry(v).or_insert_with(HashSet::new).insert(col);
                            }
                        }
                    } else {
                        unreachable!("node suggested index outside domain");
                    }
                }
                leftover_indices.extend(tmp.drain());
            }
        }

        let nodes = nodes.into_iter().map(|n| (n.index, cell::RefCell::new(n))).collect();
        Domain {
            domain: domain,
            nodes: nodes,
            state: state,
        }
    }

    pub fn dispatch(m: Message, states: &mut StateMap, nodes: &DomainNodes) {
        let me = m.to;

        let mut n = nodes[&me].borrow_mut();
        let mut u = n.process(m, states, nodes);
        drop(n);

        let n = nodes[&me].borrow();
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
