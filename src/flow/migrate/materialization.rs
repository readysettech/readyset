//! Functions for identifying which nodes should be materialized, and what indices should be used
//! for those materializations.
//!
//! This module also holds the logic for *identifying* state that must be transfered from other
//! domains, but does not perform that copying itself (that is the role of the `augmentation`
//! module).

use flow;
use flow::domain;
use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::{HashSet, HashMap};
use std::sync::mpsc;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

pub fn pick(graph: &Graph, nodes: &[(NodeIndex, bool)]) -> HashSet<LocalNodeIndex> {
    let nodes: Vec<_> = nodes.iter()
        .map(|&(ni, new)| (ni, &graph[ni], new))
        .collect();

    let mut materialize: HashSet<_> = nodes.iter()
        .filter_map(|&(ni, n, _)| {
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
            match **n {
                flow::node::Type::Internal(ref i) => {
                    if i.should_materialize() ||
                       graph.edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                        .any(|e| *e.weight()) {
                        Some(*n.addr().as_local())
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect();

    let inquisitive_children: HashSet<_> = nodes.iter()
        .filter_map(|&(ni, n, _)| {
            if let flow::node::Type::Internal(..) = **n {
                if n.will_query(materialize.contains(n.addr().as_local())) {
                    return Some(ni);
                }
            }
            None
        })
        .collect();


    for &(ni, n, _) in &nodes {
        if let flow::node::Type::Ingress = **n {
            if graph.neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                .any(|child| inquisitive_children.contains(&child)) {
                // we have children that may query us, so our output should be materialized
                materialize.insert(*n.addr().as_local());
            }
        }
    }

    // find all nodes that can be queried through, and where any of its outgoing edges are
    // materialized. for those nodes, we should instead materialize the input to that node.
    for &(ni, n, _) in &nodes {
        if let flow::node::Type::Internal(..) = **n {
            if !n.can_query_through() {
                continue;
            }

            if !materialize.contains(n.addr().as_local()) {
                // we're not materialized, so no materialization shifting necessary
                continue;
            }

            if graph.edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                .any(|e| *e.weight()) {
                // our output is materialized! what a waste. instead, materialize our input.
                materialize.remove(n.addr().as_local());
                println!("hoisting materialization past {}", n.addr());

                // TODO: unclear if we need *all* our parents to be materialized. it's
                // certainly the case for filter, which is our only use-case for now...
                for p in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                    materialize.insert(*graph[p].addr().as_local());
                }
            }
        }
    }

    materialize
}

pub fn index(graph: &Graph,
             nodes: &[(NodeIndex, bool)],
             materialize: HashSet<LocalNodeIndex>)
             -> HashMap<LocalNodeIndex, usize> {

    let map: HashMap<_, _> =
        nodes.iter().map(|&(ni, _)| (*graph[ni].addr().as_local(), ni)).collect();
    let nodes: Vec<_> = nodes.iter()
        .map(|&(ni, new)| (&graph[ni], new))
        .collect();

    let mut state: HashMap<_, Option<usize>> = materialize.into_iter().map(|n| (n, None)).collect();

    // Now let's talk indices.
    //
    // We need to query all our nodes for what indices they believe should be maintained, and
    // apply those to the stores in state. However, this is somewhat complicated by the fact
    // that we need to push indices through non-materialized views so that they end up on the
    // columns of the views that will actually query into a table of some sort.
    {
        let nodes: HashMap<_, _> = nodes.iter().map(|&(n, _)| (n.addr(), n)).collect();
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
                if let Some(mut state) = state.get_mut(v.as_local()) {
                    // this node is materialized! add the indices!
                    // we *currently* only support keeping one materialization per node
                    assert_eq!(cols.len(), 1, "conflicting index requirements for {}", v);
                    let col = cols.into_iter().next().unwrap();
                    println!("adding index on column {:?} of view {:?}", col, v);
                    *state = Some(col);
                } else if let Some(node) = nodes.get(&v) {
                    // this node is not materialized
                    // we need to push the index up to its ancestor(s)
                    if let flow::node::Type::Ingress = ***node {
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

    state.into_iter()
        .filter_map(|(n, col)| {
            if let Some(col) = col {
                Some((n, col))
            } else {
                // this materialization doesn't have any primary key,
                // so we assume it's not in use.

                if graph[map[&n]].is_base() {
                    // but it's a base nodes!
                    // we must *always* materialize base nodes
                    // so, just make up some column to index on
                    return Some((n, 0));
                }

                println!("removing unnecessary materialization on {:?}", map[&n]);
                None
            }
        })
        .collect()
}

pub fn initialize(graph: &Graph,
                  source: NodeIndex,
                  new: &HashSet<NodeIndex>,
                  mut materialize: HashMap<domain::Index, HashMap<LocalNodeIndex, usize>>,
                  control_txs: &mut HashMap<domain::Index, mpsc::SyncSender<domain::Control>>) {
    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&*graph);
    while let Some(node) = topo.next(&*graph) {
        if node == source {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }

    // TODO: what about adding materialization to *existing* views?

    let mut empty = HashSet::new();
    for node in topo_list {
        let n = &graph[node];
        let d = n.domain();

        let index_on = materialize.get_mut(&d)
            .and_then(|ss| ss.get(n.addr().as_local()))
            .cloned();
        let mut has_state = index_on.is_some();

        if let flow::node::Type::Reader(_, ref r) = **n {
            if r.state.is_some() {
                has_state = true;
            }
        }

        // ready communicates to the domain in charge of a particular node that it should start
        // delivering updates to a given new node. note that we wait for the domain to acknowledge
        // the change. this is important so that we don't ready a child in a different domain
        // before the parent has been readied. it's also important to avoid us returning before the
        // graph is actually fully operational.
        let ready = |control_txs: &mut HashMap<_, mpsc::SyncSender<_>>, index_on: Option<usize>| {
            let (ack_tx, ack_rx) = mpsc::sync_channel(0);
            control_txs[&d]
                .send(domain::Control::Ready(*n.addr().as_local(), index_on, ack_tx))
                .unwrap();
            match ack_rx.recv() {
                Err(mpsc::RecvError) => (),
                _ => unreachable!(),
            }
        };

        if graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .filter(|&ni| ni != source)
            .all(|n| empty.contains(&n)) {
            // all parents are empty, so we can materialize it immediately
            empty.insert(node);
            ready(control_txs, index_on);
        } else {
            // if this node doesn't need to be materialized, then we're done. note that this check
            // needs to happen *after* the empty parents check so that we keep tracking whether or
            // not nodes are empty.
            if !has_state {
                ready(control_txs, index_on);
                continue;
            }

            // we have a parent that has data, so we need to replay and reconstruct
            let start = ::std::time::Instant::now();
            reconstruct(graph, source, &materialize, control_txs, node, index_on);
            // NOTE: the state has already been marked ready by the replay completing,
            // but we want to wait for the domain to finish replay, which a Ready does.
            ready(control_txs, None);
            println!("reconstruction of {:?} {:?} took {}us",
                     *graph[node],
                     node,
                     dur_to_ns!(start.elapsed()) / 1000);
        }
    }
}

pub fn reconstruct(graph: &Graph,
                   source: NodeIndex,
                   materialized: &HashMap<domain::Index, HashMap<LocalNodeIndex, usize>>,
                   control_txs: &mut HashMap<domain::Index, mpsc::SyncSender<domain::Control>>,
                   node: NodeIndex,
                   index_on: Option<usize>) {

    // okay, so here's the situation: `node` is a node that
    //
    //   a) was not previously materialized, and
    //   b) now needs to be materialized, and
    //   c) at least one of node's parents has existing data
    //
    // because of the topological traversal done by `initialize`, we know that all our ancestors
    // that should be materialized have been.
    //
    // our plan is as follows:
    //
    //   1. search our ancestors for the closest materialization points along each path
    //   2. for each such path, identify the domains along that path and pause them
    //   3. construct a daisy-chain of channels, and pass them to each domain along the path
    //   4. tell the domain nearest to the root to start replaying
    //
    // so, first things first, let's find our closest materialized parents
    let paths = trace(graph, source, node, materialized, vec![node]);

    if let flow::node::Type::Reader(..) = *graph[node] {
        // readers have their own internal state
    } else {
        let index_on = index_on.expect("all non-reader nodes must have a state key");

        // tell the domain in question to create an empty state for the node in question
        control_txs[&graph[node].domain()]
            .send(domain::Control::PrepareState(*graph[node].addr().as_local(), index_on))
            .unwrap();
    }

    // set up channels for replay along each path
    for mut path in paths {
        // we want path to have the ancestor closest to the root *first*
        path.reverse();

        // first, find out which domains we are crossing
        let mut segments = Vec::new();
        let mut last_domain = None;
        for node in path {
            let domain = graph[node].domain();
            if last_domain.is_none() || domain != last_domain.unwrap() {
                segments.push((domain, Vec::new()));
                last_domain = Some(domain);
            }

            if graph[node].is_egress() {
                // we don't want replayed records to spill out into data channels
            } else {
                segments.last_mut().unwrap().1.push(node);
            }
        }

        // next, daisy chain channels between them
        // this includes the final domain, which will automatically populate `node` during replay
        let (root_tx, mut next_rx) = mpsc::sync_channel(10);
        let root_tx = if segments.len() == 1 {
            // no channels needed
            None
        } else {
            Some(root_tx)
        };

        let locals = |i: usize| -> Vec<NodeAddress> {
            segments[i]
                .1
                .iter()
                .map(|&ni| graph[ni].addr())
                .collect::<Vec<_>>()
        };

        let (wait_tx, wait_rx) = mpsc::sync_channel(0);

        // first, tell the root domain to start replaying
        control_txs[&segments[0].0]
            .send(domain::Control::Replay(locals(0), root_tx, wait_tx.clone()))
            .unwrap();

        // wait for the root to start replay.
        //
        // NOTE:
        // this is crucial to prevent deadlock.
        // if we *didn't* do this, we could run into the following situation:
        //
        //  - next domain in chain (let's call it [1]) receives ReplayThrough
        //  - [1] enters replay loop (and crucially, stops running its main loop)
        //  - [0] continues receiving records on its data channel
        //  - [0] processes those records all the way to egress node connected to [1]
        //  - [0] fills up channel between [0] egress and [1] ingress since [1] isn't reading
        //  - [0] blocks on said channel
        //
        // now, [1] is blocking on [0] reading from its control channel, and [0] is blocking on [1]
        // reading from its data channel. deadlock. yay!
        //
        // by having [n] wait for [0..n-1], we know that this won't happen.
        wait_rx.recv().unwrap();

        // next, replay through the later domains one by one, linking up the daisy-chain
        let mut seen = HashSet::new();
        for (i, &(ref domain, _)) in segments.iter().skip(1).enumerate() {
            // TODO:
            //  a domain may appear multiple times in this list if a path crosses into the same
            //  domain more than once. currently, that will cause a deadlock.
            assert!(!seen.contains(domain),
                    "a-b-a domain replays are not yet supported");
            seen.insert(*domain);

            let (tx, rx) = mpsc::sync_channel(10);
            let tx = if i == segments.len() - 1 - 1 {
                // last segment shouldn't emit anything
                None
            } else {
                Some(tx)
            };

            control_txs[domain]
                .send(domain::Control::ReplayThrough(locals(i + 1), next_rx, tx, wait_tx.clone()))
                .unwrap();
            next_rx = rx;

            // wait for this domain too -- see explanation above
            wait_rx.recv().unwrap();
        }
    }
}

fn trace(graph: &Graph,
         source: NodeIndex,
         node: NodeIndex,
         materialized: &HashMap<domain::Index, HashMap<LocalNodeIndex, usize>>,
         path: Vec<NodeIndex>)
         -> Vec<Vec<NodeIndex>> {

    if node == source {
        unreachable!("base node was not materialized!");
    }

    let n = &graph[node];
    let is_materialized = if path.len() == 1 {
        // the start node is the one we're trying to replay to, so while it'll be marked as
        // materialized in the map, it isn't really
        false
    } else {
        materialized.get(&n.domain())
            .map(|dm| dm.contains_key(n.addr().as_local()))
            .unwrap_or(false)
    };

    if is_materialized {
        vec![path]
    } else {
        let mut parents: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect();
        if parents.len() != 1 {
            // there are two cases where we have multiple parents: joins and unions
            // for unions, we should replay *all* paths. for joins, we should only replay one path.
            // in particular, for a join, we should only replay the ancestor that yields the full
            // result-set (i.e., the left side of a left join).
            assert!(graph[node].is_internal());
            if let Some(picked_ancestor) = graph[node].replay_ancestor() {
                // join, only replay picked ancestor
                parents.retain(|&parent| graph[parent].addr() == picked_ancestor);
            } else {
                // union; just replay all
            }
        }

        parents.into_iter()
            .flat_map(|parent| {
                let mut path = path.clone();
                path.push(parent);
                trace(graph, source, parent, materialized, path)
            })
            .collect()
    }
}
