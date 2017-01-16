use flow::node;
use flow::domain;
use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

fn add_time_egress(nodes: &mut Vec<(NodeIndex, bool)>, graph: &mut Graph) -> Vec<NodeIndex> {
    let new_base_nodes: Vec<_> = nodes.iter()
        .filter_map(|&(ni, new)| {
            if !new {
                return None;
            }

            if let node::Type::Internal(ref ingredient) = *graph[ni] {
                if ingredient.is_base() {
                    return Some(ni);
                }
            }
            None

        })
        .collect();

    new_base_nodes.into_iter()
        .map(|node| {
            let proxy = graph[node].mirror(node::Type::TimestampEgress(Default::default()));

            // we need to hook that node into the graph
            let time_egress = graph.add_node(proxy);
            graph.add_edge(node, time_egress, false);

            // and to this domain
            nodes.push((time_egress, true));

            time_egress
        })
        .collect()

}

/// Returns a list of newly added TimeEgress nodes that need to be registered with pre-existing
/// TimestampIngress nodes.
pub fn add_time_nodes(nodes: &mut HashMap<domain::Index, Vec<(NodeIndex, bool)>>,
                      graph: &mut Graph,
                      time_txs: &HashMap<domain::Index, mpsc::SyncSender<i64>>) {

    // For every *new* base node, add a TimeEgress node after it so that it can communicate to
    // other domains about a new assigned timestamp. This is to ensure that every domain learns
    // about every timestamp, even if it is not (transitively) connected to the base node that
    // generated each timestamp.
    let mut new_time_egress = Vec::new();
    for (_, nodes) in nodes.iter_mut() {
        new_time_egress.extend(add_time_egress(nodes, graph));
    }

    // Connect all these new TimeEgress nodes to the TimeIngress nodes of all pre-existing domains
    // iff there isn't already a path between the TimeEgress's base node and the TimeIngress'
    // domain.

    // add_to keeps track of edges we need to add to the graph, but can't yet because we already
    // have a read-only reference to the graph in the if let
    let mut add_to = Vec::new();
    for ingress in graph.node_indices() {
        if let node::Type::TimestampIngress(ref arc) = *graph[ingress] {
            let tx = arc.lock().unwrap().clone();
            let domain = graph[ingress].domain();
            for &egress in &new_time_egress {
                let base = graph.neighbors_directed(egress, petgraph::EdgeDirection::Incoming)
                    .next()
                    .expect("ts egress must be child of base node");

                // we now want to check if the domain holding this time ingress node is somehow
                // connected to the new base node that this new TimeEgress node is connected to.
                // that can *only* be the case if at least one node was added to the domain in this
                // migration. there is no point in checking *old* nodes, because they cannot have
                // been connected to this new base node
                if nodes.contains_key(&domain) &&
                   nodes[&domain].iter().any(|&(node, new)| {
                    new && petgraph::algo::has_path_connecting(&*graph, base, node, None)
                }) {
                    // yes! no need for time channel
                    continue;
                }

                // nope, we need to tell this domain about new updates from this base
                if let node::Type::TimestampEgress(ref arc) = *graph[egress] {
                    arc.lock().unwrap().push(tx.clone());
                    add_to.push(egress);
                } else {
                    unreachable!();
                }
            }
        }

        for egress in add_to.drain(..) {
            graph.add_edge(egress, ingress, false);
        }
    }

    // Add a TimeIngress node to every new domain so it can receive these timestamp messages
    let mut new_time_ingress = Vec::new();
    for (domain, nodes) in nodes.iter() {
        if !nodes.iter().all(|&(_, new)| new) {
            // existing domains already have a TimeIngress node
            continue;
        }

        let tx = time_txs[domain].clone();
        let t = node::Type::TimestampIngress(Arc::new(Mutex::new(tx)));
        let mut proxy = node::Node::new::<_, Vec<String>, _>("time-node", vec![], t);
        proxy.add_to(*domain);
        let time_ingress = graph.add_node(proxy);
        new_time_ingress.push((*domain, time_ingress));
    }

    // Ensure these new TimeIngress nodes are actually adopted by the domain
    for &(domain, time_in) in &new_time_ingress {
        nodes.get_mut(&domain).unwrap().push((time_in, true));
    }

    // Connect every TimeEgress node in the graph to each such added TimeIngress node iff there is
    // no *other* path from that TimeEgress' base node to the TimeIngress node's domain.
    let mut add_to = Vec::new();
    for egress in graph.node_indices() {
        if let node::Type::TimestampEgress(ref arc) = *graph[egress] {
            let base = graph.neighbors_directed(egress, petgraph::EdgeDirection::Incoming)
                .next()
                .expect("ts egress must be child of base node");

            let mut txs = arc.lock().unwrap();
            for &(domain, ingress) in &new_time_ingress {
                // is this egress' base node already connected to this domain somehow?
                if 
                    nodes[&domain].iter().any(|&(node, _)| {
                        petgraph::algo::has_path_connecting(&*graph, base, node, None)
                    }) {
                    // yes! no need for time channel
                    continue;
                }

                txs.push(time_txs[&domain].clone());
                add_to.push(ingress);
            }
        }

        for ingress in add_to.drain(..) {
            graph.add_edge(egress, ingress, false);
        }
    }
}
