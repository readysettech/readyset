//! Functions for adding ingress/egress nodes.
//!
//! In particular:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Egress nodes that gain new children must gain channels to facilitate forwarding
//!  - Timestamp ingress nodes for existing domains must be connected to new base nodes
//!  - Timestamp ingress nodes must be added to all new domains

use flow::prelude::*;
use flow::domain;
use flow::node;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::{HashSet, HashMap};
use std::sync::mpsc;

pub fn add(graph: &mut Graph,
           source: NodeIndex,
           new: &mut HashSet<NodeIndex>)
           -> HashMap<domain::Index, HashMap<NodeIndex, NodeIndex>> {
    // We want to add in ingress and egress nodes as appropriate in the graph to facilitate
    // communication between domains.

    // find all new nodes in topological order. we collect first since we'll be mutating the graph
    // below. we need them to be in topological order here so that we know that parents have been
    // processed, and that children have not
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

    let mut swaps = HashMap::new();
    for node in topo_list {
        let domain = graph[node].domain();

        // add ingress nodes
        for (parent, ingress) in add_ingress_for(graph, source, node, domain) {
            new.insert(ingress);
            swaps.entry(domain).or_insert_with(HashMap::new).insert(parent, ingress);
        }

        // add egress nodes
        for egress in add_egress_for(graph, node, domain) {
            new.insert(egress);
        }
    }

    swaps
}

pub fn connect(graph: &mut Graph,
               data_txs: &HashMap<domain::Index, mpsc::SyncSender<Message>>,
               new: &HashSet<NodeIndex>) {

    // ensure all egress nodes contain the tx channel of the domains of their child ingress nodes
    for &node in new {
        let n = &graph[node];
        if let node::Type::Ingress = **n {
            // check the egress connected to this ingress
        } else {
            continue;
        }

        for egress in graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming) {
            if let node::Type::Egress(ref txs) = *graph[egress] {
                txs.lock()
                    .unwrap()
                    .push((n.addr(), data_txs[&n.domain()].clone()));
                continue;
            }

            unreachable!("ingress parent is not egress");
        }
    }
}

fn add_ingress_for(graph: &mut Graph,
                   source: NodeIndex,
                   node: NodeIndex,
                   domain: domain::Index)
                   -> Vec<(NodeIndex, NodeIndex)> {

    let parents: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
        .collect(); // collect so we can mutate graph

    let mut new = Vec::new();
    for parent in parents {
        if parent == source || domain != graph[parent].domain() {
            // parent is in a different domain
            // create an ingress node to handle that
            let proxy = graph[parent].mirror(node::Type::Ingress);
            let ingress = graph.add_node(proxy);

            // note that, since we are traversing in topological order, our parent in this
            // case should either be the source node, or it should be an egress node!
            if cfg!(debug_assertions) && parent != source {
                if let node::Type::Egress(..) = *graph[parent] {
                } else {
                    unreachable!("parent of ingress is not an egress");
                }
            }

            // we need to hook the ingress node in between us and this parent
            let old = graph.find_edge(parent, node).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(parent, ingress, false);
            graph.add_edge(ingress, node, was_materialized);
            new.push((parent, ingress));
        }
    }
    new
}

pub fn add_egress_for(graph: &mut Graph, node: NodeIndex, domain: domain::Index) -> Vec<NodeIndex> {
    let children: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
        .collect(); // collect so we can mutate graph

    let mut new = Vec::new();
    for child in children {
        if domain != graph[child].domain() {
            // child is in a different domain
            // create an egress node to handle that
            // NOTE: technically, this doesn't need to mirror its parent, but meh
            let proxy = graph[node].mirror(node::Type::Egress(Default::default()));
            let egress = graph.add_node(proxy);

            // we need to hook that node in between us and this child
            let old = graph.find_edge(node, child).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(node, egress, false);
            graph.add_edge(egress, child, was_materialized);
            new.push(egress);
        }
    }
    new
}
