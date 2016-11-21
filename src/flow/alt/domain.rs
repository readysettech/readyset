use petgraph;
use petgraph::graph::NodeIndex;
use ops;
use query;
use shortcut;

use std::collections::VecDeque;
use std::collections::HashMap;

use std::cell;

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

struct NodeDescriptor {
    index: NodeIndex,
    inner: alt::Node,
    children: Vec<NodeIndex>,
}

impl NodeDescriptor {
    pub fn iterate(&mut self,
                   handoffs: &mut HashMap<NodeIndex, VecDeque<alt::Message>>,
                   state: &mut HashMap<NodeIndex, shortcut::Store<query::DataType>>) {
        match self.inner {
            alt::Node::Ingress(_, ref mut rx) => {
                // receive an update
                debug_assert!(handoffs[&self.index].is_empty());
                broadcast!(handoffs, rx.recv().unwrap(), &self.children[..]);
            }
            alt::Node::Egress(_, ref txs) => {
                // send any queued updates to all external children
                let mut txs = txs.lock().unwrap();
                let txn = txs.len() - 1;
                while let Some(m) = handoffs.get_mut(&self.index).unwrap().pop_front() {
                    let mut m = Some(m); // so we can use .take()
                    for (txi, tx) in txs.iter_mut().enumerate() {
                        if txi == txn && self.children.is_empty() {
                            tx.send(m.take().unwrap()).unwrap();
                        } else {
                            tx.send(m.clone().unwrap()).unwrap();
                        }
                    }

                    if let Some(m) = m {
                        broadcast!(handoffs, m, &self.children[..]);
                    } else {
                        debug_assert!(self.children.is_empty());
                    }
                }
            }
            alt::Node::Internal(..) => {
                while let Some(m) = handoffs.get_mut(&self.index).unwrap().pop_front() {
                    if let Some(u) = self.process_one(m, state) {
                        broadcast!(handoffs,
                                   alt::Message {
                                       from: self.index,
                                       data: u,
                                   },
                                   &self.children[..]);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn process_one(&mut self,
                   m: alt::Message,
                   state: &mut HashMap<NodeIndex, shortcut::Store<query::DataType>>)
                   -> Option<ops::Update> {

        // first, process the incoming message
        let u = match self.inner {
            alt::Node::Internal(_, ref mut i) => i.process(m),
            _ => unreachable!(),
        };

        // if our output didn't change there's nothing more to do
        if u.is_none() {
            return u;
        }

        // our output changed -- do we need to modify materialized state?
        let state = state.get_mut(&self.index);
        if state.is_none() {
            // nope
            return u;
        }

        // yes!
        let mut state = state.unwrap();
        if let Some(ops::Update::Records(ref rs)) = u {
            for r in rs.iter().cloned() {
                match r {
                    ops::Record::Positive(r, _) => state.insert(r),
                    ops::Record::Negative(r, _) => {
                        // we need a cond that will match this row.
                        let conds = r.into_iter()
                            .enumerate()
                            .map(|(coli, v)| {
                                shortcut::Condition {
                                    column: coli,
                                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(v)),
                                }
                            })
                            .collect::<Vec<_>>();

                        // however, multiple rows may have the same values as this row for every
                        // column. afaict, it is safe to delete any one of these rows. we do this
                        // by returning true for the first invocation of the filter function, and
                        // false for all subsequent invocations.
                        let mut first = true;
                        state.delete_filter(&conds[..], |_| {
                            if first {
                                first = false;
                                true
                            } else {
                                false
                            }
                        });
                    }
                }
            }
        }

        u
    }
}


pub struct Domain {
    domain: Index,
    nodes: Vec<cell::RefCell<NodeDescriptor>>,
    map: HashMap<NodeIndex, usize>,
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

                NodeDescriptor {
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

        // okay, this deserves some explanation...
        //
        // we're going to be iterating over nodes one at a time, processing updates, each time
        // having a mutable borrow of the *current* node. however, in order to support queries
        // on non-materialized nodes, we also want that node to be able to access *other* nodes
        // using read-only borrows. since they are all in the same vector, this is tricky to do
        // simply using the borrow checker.
        //
        // instead, we make a RefCell of each node to track the borrows at runtime. when we hit
        // a given node, we can lend it the RefCell of all nodes in this domain, and it can
        // freely take out borrows on all nodes (except itself, which will be checked at
        // runtime).
        //
        // as if that wasn't enough, we also want an efficient way for a node to look up its
        // ancestors by node index if it needs to do so. unfortunately, nodes are stored in a
        // Vec (and need to be, since we want to maintain the topological order). we therefore
        // construct a map from node index to each node's index in the Vec, which nodes can use
        // to quickly find ancestor RefCells. this mapping can even be inspected at set-up
        // time, and all NodeIndexes translated to Vec indices instead, to remove the
        // performance penalty of the map.
        //
        // it's unfortunate that we have to resort to refcounting to solve this, as it means
        // every query is a bit more expensive. luckily, since we construct the cells inside
        let map = nodes.iter().enumerate().map(|(i, n)| (n.index, i)).collect();
        let nodes = nodes.into_iter().map(|n| cell::RefCell::new(n)).collect();

        Domain {
            domain: domain,
            nodes: nodes,
            map: map,
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
                    node.borrow_mut().iterate(&mut self.handoffs, &mut self.state);
                }
            }
        });
    }
}
