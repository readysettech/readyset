use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::cell;

use std::collections::hash_map::Entry;

use flow;
use flow::prelude::*;

use ops;
use checktable;

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
pub mod local;

pub struct Domain {
    domain: Index,
    nodes: DomainNodes,
    state: StateMap,

    /// Map from timestamp to vector of messages buffered for that timestamp. None in place of
    /// NodeIndex means that the timestamp will be skipped (because it is from a base node that
    /// never sends updates to this domain).
    buffered_transactions: HashMap<i64, (Option<NodeIndex>, Vec<Message>)>,
    /// Number of ingress nodes in the domain that receive updates from each base node. Base nodes
    /// that are only connected by timestamp ingress nodes are not included.
    ingress_from_base: HashMap<NodeIndex, usize>,
    /// Timestamp that the domain has seen all transactions up to.
    ts: i64,

    checktable: Arc<Mutex<checktable::CheckTable>>,
}

impl Domain {
    pub fn from_graph(domain: Index,
                      nodes: Vec<(NodeIndex, NodeAddress)>,
                      checktable: Arc<Mutex<checktable::CheckTable>>,
                      base_nodes: &Vec<NodeIndex>,
                      graph: &mut Graph)
                      -> Self {
        let ni2na: HashMap<NodeIndex, NodeAddress> = nodes.iter().cloned().collect();

        let nodes: Vec<_> = nodes.into_iter()
            .map(|(ni, _)| {
                // also include all *internal* descendants
                let children: Vec<_> = graph
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&c| {
                        graph[c].domain().unwrap() == domain
                    })
                    .map(|ni| ni2na[&ni])
                    .collect();
                    (ni, children)
            })
            .collect::<Vec<_>>() // because above closure mutably borrows self.mainline
            .into_iter()
            .map(|(ni, children)| {
                single::NodeDescriptor {
                    index: ni,
                    addr: ni2na[&ni],
                    inner: graph.node_weight_mut(ni).unwrap().take(),
                    children: children,
                }
            })
            .collect();

        let mut state: StateMap = nodes.iter()
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
                            Some((*n.addr.as_local(), State::default()))
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
                    if n.will_query(state.contains_key(n.addr.as_local())) {
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
                    // we have children that may query us, so our output should be materialized
                    state.insert(*n.addr.as_local(), State::default());
                }
            }
        }

        // find all nodes that can be queried through, and where any of its outgoing edges are
        // materialized. for those nodes, we should instead materialize the input to that node.
        for n in &nodes {
            if let flow::node::Type::Internal(..) = *n.inner {
                if !n.can_query_through() {
                    continue;
                }

                if !state.contains_key(n.addr.as_local()) {
                    // we're not materialized, so no materialization shifting necessary
                    continue;
                }

                if graph.edges_directed(n.index, petgraph::EdgeDirection::Outgoing)
                    .any(|e| *e.weight()) {
                    // our output is materialized! what a waste. instead, materialize our input.
                    state.remove(n.addr.as_local());
                    println!("hoisting materialization past {}", n.addr);

                    // TODO: unclear if we need *all* our parents to be materialized. it's
                    // certainly the case for filter, which is our only use-case for now...
                    for p in graph.neighbors_directed(n.index, petgraph::EdgeDirection::Incoming) {
                        state.insert(*ni2na[&p].as_local(), State::default());
                    }
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
            let nodes: HashMap<_, _> = nodes.iter().map(|n| (n.addr, n)).collect();
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
                .fold(HashMap::new(), |mut hm, (v, idx)| {
                    hm.entry(v).or_insert_with(HashSet::new).insert(idx);
                    hm
                });

            // push down indices
            let mut leftover_indices: HashMap<_, _> = indices.drain().collect();
            let mut tmp = HashMap::new();
            while !leftover_indices.is_empty() {
                for (v, cols) in leftover_indices.drain() {
                    if let Some(ref mut state) = state.get_mut(v.as_local()) {
                        // this node is materialized! add the indices!
                        // we *currently* only support keeping one materialization per node
                        assert_eq!(cols.len(), 1, "conflicting index requirements for {}", v);
                        let col = cols.into_iter().next().unwrap();
                        println!("adding index on column {:?} of view {:?}", col, v);
                        state.set_pkey(col);
                    } else if let Some(node) = nodes.get(&v) {
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

        for n in &nodes {
            if !state.contains_key(n.addr.as_local()) {
                continue;
            }

            if !state.get(n.addr.as_local()).unwrap().is_useful() {
                // this materialization doesn't have any primary key,
                // so we assume it's not in use.
                println!("removing unnecessary materialization on {}", n.addr);
                state.remove(n.addr.as_local());
            }
        }

        let nodes: DomainNodes =
            nodes.into_iter().map(|n| (*n.addr.as_local(), cell::RefCell::new(n))).collect();

        let ingress_nodes: Vec<NodeIndex> =
            nodes.iter().filter(|n| n.borrow().is_ingress()).map(|n| n.borrow().index).collect();

        let ingress_from_base = base_nodes.iter()
            .map(|base| {
                let num_paths = ingress_nodes.iter()
                    .filter(|ingress| {
                        petgraph::algo::has_path_connecting(&*graph, *base, **ingress, None)
                    })
                    .count();
                (*base, num_paths)
            })
            .collect();

        Domain {
            domain: domain,
            nodes: nodes,
            state: state,
            buffered_transactions: HashMap::new(),
            ingress_from_base: ingress_from_base,
            ts: -1,
            checktable: checktable.clone(),
        }
    }

    pub fn dispatch(m: Message,
                    states: &mut StateMap,
                    nodes: &DomainNodes,
                    enable_output: bool,
                    checktable: &Arc<Mutex<checktable::CheckTable>>)
                    -> HashMap<NodeAddress, Vec<ops::Record>> {
        let me = m.to;
        let ts = m.ts;
        let mut output_messages = HashMap::new();

        let mut n = nodes[me.as_local()].borrow_mut();
        let mut u = n.process(m, states, nodes, checktable);
        drop(n);

        if ts.is_some() {
            // Any message with a timestamp (ie part of a transaction) must flow through the entire
            // graph, even if there are no updates associated with it.
            u = u.or(Some((Update::Records(vec![]), ts, None)));
        }

        if u.is_none() {
            // no need to deal with our children if we're not sending them anything
            return output_messages;
        }

        let n = nodes[me.as_local()].borrow();
        for i in 0..n.children.len() {
            // avoid cloning if we can
            let (data, ts, token) = if i == n.children.len() - 1 {
                u.take().unwrap()
            } else {
                u.clone().unwrap()
            };

            if enable_output || !nodes[n.children[i].as_local()].borrow().is_output() {
                let m = Message {
                    from: me,
                    to: n.children[i],
                    data: data,
                    ts: ts,
                    token: token,
                };

                for (k, v) in Self::dispatch(m, states, nodes, enable_output, checktable)
                    .into_iter() {
                    output_messages.insert(k, v);
                }
            } else {
                let ops::Update::Records(mut data) = data;
                match output_messages.entry(n.children[i]) {
                    Entry::Occupied(entry) => {
                        entry.into_mut().append(&mut data);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(data);
                    }
                };
            }
        }

        output_messages
    }

    pub fn transactional_dispatch(&mut self, messages: Vec<Message>) {
        if messages.len() == 0 {
            return;
        }

        let mut egress_messages = HashMap::new();
        let ts = messages.iter().next().unwrap().ts;

        for m in messages {
            let new_messages =
                Self::dispatch(m, &mut self.state, &self.nodes, false, &self.checktable)
                    .into_iter();

            for (key, value) in new_messages.into_iter() {
                egress_messages.insert(key, value);
            }
        }

        for n in self.nodes.iter().filter(|n| n.borrow().is_output()) {
            let data = match egress_messages.entry(n.borrow().addr) {
                Entry::Occupied(entry) => Update::Records(entry.remove()),
                _ => Update::Records(vec![]),
            };

            let m = Message {
                from: n.borrow().addr, // TODO: message should be from actual parent, not self.
                to: n.borrow().addr,
                data: data,
                ts: ts,
                token: None,
            };

            self.nodes[m.to.as_local()]
                .borrow_mut()
                .process(m, &mut self.state, &self.nodes, &self.checktable);
            assert_eq!(n.borrow().children.len(), 0);
        }
    }

    fn apply_transactions(&mut self) {
        while !self.buffered_transactions.is_empty() {
            // Extract a complete set of messages for timestep (self.ts+1) if one exists.
            let messages = match self.buffered_transactions.entry(self.ts + 1) {
                Entry::Occupied(entry) => {
                    match entry.get().0 {
                        Some(base) => {
                            let messages_needed = self.ingress_from_base.get(&base).unwrap();
                            // If we don't have all the messages for this timestamp, then stop.
                            if entry.get().1.len() < *messages_needed {
                                break;
                            }

                            // Otherwise, extract this set of messages.
                            entry.remove().1
                        }
                        None => {
                            // If we learned about this timestamp from our timestamp ingress node,
                            // then skip it and move on to the next.
                            entry.remove();
                            self.ts += 1;
                            continue;
                        }
                    }
                }
                Entry::Vacant(_) => break,
            };

            // Process messages and advance to the next timestep.
            self.transactional_dispatch(messages);
            self.ts += 1;
        }
    }

    fn buffer_transaction(&mut self, m: Message) {
        let (ts, base) = m.ts.unwrap();

        // Insert message into buffer.
        self.buffered_transactions.entry(ts).or_insert((Some(base), vec![])).1.push(m);

        if ts == self.ts + 1 {
            self.apply_transactions();
        }
    }

    pub fn boot(mut self, rx: mpsc::Receiver<Message>, timestamp_rx: mpsc::Receiver<i64>) {
        use std::thread;

        thread::spawn(move || {
            let sel = mpsc::Select::new();
            let mut rx_handle = sel.handle(&rx);
            let mut timestamp_rx_handle = sel.handle(&timestamp_rx);

            unsafe {
                rx_handle.add();
                timestamp_rx_handle.add();
            }

            loop {
                let id = sel.wait();
                if id == timestamp_rx_handle.id() {
                    let ts = timestamp_rx_handle.recv();
                    if ts.is_err() {
                        return;
                    }
                    let ts = ts.unwrap();

                    self.buffered_transactions.insert(ts, (None, vec![]));
                    self.apply_transactions();
                } else if id == rx_handle.id() {
                    let m = rx_handle.recv();
                    if m.is_err() {
                        return;
                    }
                    let m = m.unwrap();

                    match m.ts {
                        None => {
                            Self::dispatch(m, &mut self.state, &self.nodes, true, &self.checktable);
                        }
                        Some(_) => {
                            self.buffer_transaction(m);
                        }
                    }
                }
            }
        });
    }
}
