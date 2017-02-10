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

use slog::Logger;

/// Add in ingress and egress nodes as appropriate in the graph to facilitate cross-domain
/// communication.
pub fn add(log: &Logger,
           graph: &mut Graph,
           source: NodeIndex,
           new: &mut HashSet<NodeIndex>)
           -> HashMap<domain::Index, HashMap<NodeIndex, NodeIndex>> {

    // find all new nodes in topological order. we collect first since we'll be mutating the graph
    // below. it's convenient to have the nodes in topological order, because we then know that
    // we'll first add egress nodes, and then the related ingress nodes. if we're ever required to
    // add an ingress node, and its parent isn't an egress node, we know that we're seeing a
    // connection between an old node in one domain, and a new node in a different domain.
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

    // we need to keep track of all the times we change the parent of a node (by replacing it with
    // an egress, and then with an ingress), since this remapping must be communicated to the nodes
    // so they know the true identifier of their parent in the graph.
    let mut swaps = HashMap::new();

    for node in topo_list {
        let domain = graph[node].domain();

        // First, we add egress nodes for any of our cross-domain children
        let children: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            .collect(); // collect so we can mutate graph

        for child in children {
            let cdomain = graph[child].domain();
            if domain != cdomain {
                // child is in a different domain
                // create an egress node to handle that
                // NOTE: technically, this doesn't need to mirror its parent, but meh
                let proxy = graph[node].mirror(node::Type::Egress(Default::default()));
                let egress = graph.add_node(proxy);

                trace!(log, "adding cross-domain egress to new node"; "node" => node.index(), "egress" => egress.index());

                // we need to hook that node in between us and this child
                let old = graph.find_edge(node, child).unwrap();
                let was_materialized = graph.remove_edge(old).unwrap();
                graph.add_edge(node, egress, false);
                graph.add_edge(egress, child, was_materialized);
                new.insert(egress);
                swaps.entry(cdomain).or_insert_with(HashMap::new).insert(node, egress);
            }
        }

        // Then, we look for any parents in the graph that
        //
        //   a) are in a different domain, and
        //   b) aren't egress nodes
        //
        // This situation arises whenever a cross-domain edge is added as the result of a
        // migration. We need to find or make an egress domain in that other domain, and hook that
        // up as the parent of this node instead of the original internal foreign domain node.
        //
        // Note that same-domain parents are never interesting to us for this purpose.
        let mut parents: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .filter(|&ni| ni == source || graph[ni].domain() != domain)
            .collect(); // collect so we can mutate graph

        for parent in &mut parents {
            if *parent == source {
                // no egress needed
                continue;
            }

            // since we are traversing in topological order, egress nodes should have been added to
            // all our parents, and our incoming edges should have been updated. if that *isn't*
            // the case for a given parent, it must be a pre-existing parent.
            if let node::Type::Egress(..) = *graph[*parent] {
                continue;
            }

            // let's first see if this parent already has an egress we can use
            let egress = graph.neighbors_directed(*parent, petgraph::EdgeDirection::Outgoing)
                .find(|&ni| graph[ni].is_egress());

            let egress = egress.unwrap_or_else(|| {
                // no, okay, so we need to add an egress for that other node,
                let proxy = graph[*parent].mirror(node::Type::Egress(Default::default()));
                let egress = graph.add_node(proxy);

                trace!(log, "adding cross-domain egress to existing node"; "node" => parent.index(), "egress" => egress.index());

                graph.add_edge(*parent, egress, false);
                new.insert(egress);
                egress
            });

            // now, let's use that egress as our parent instead
            let old = graph.find_edge(*parent, node).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(egress, node, was_materialized);
            // all references to our original parent should now refer to the egress
            swaps.entry(domain).or_insert_with(HashMap::new).insert(*parent, egress);
            // and we should now just consider the egress our parent instead
            *parent = egress;
        }

        // Now that we know all our foreign parents are egress nodes, we can add ingress nodes.
        for parent in parents {
            // create our new ingress node
            let mut ingress = graph[parent].mirror(node::Type::Ingress);
            ingress.add_to(domain); // it belongs to this domain, not that of the parent
            let ingress = graph.add_node(ingress);
            new.insert(ingress);

            if parent == source {
                trace!(log, "adding source ingress"; "base" => node.index(), "ingress" => ingress.index());
            } else {
                trace!(log, "adding cross-domain ingress"; "to" => node.index(), "from" => parent.index(), "ingress" => ingress.index());
            }

            // we need to hook the ingress node in between us and the parent
            let old = graph.find_edge(parent, node).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(parent, ingress, false);
            graph.add_edge(ingress, node, was_materialized);

            // tracking swaps here is a bit tricky because we've already swapped the "true" parents
            // of `node` with the ids of the egress nodes. thus, we actually need to do swaps on
            // the values in `swaps`, not insert new entries (that, or we'd need to change the
            // resolution process to be recursive, which is painful and unnecessary). note that we
            // *also* need to special-case handing base nodes, because there there *won't* be a
            // parent egress swap
            if parent != source {
                for (_, to) in swaps.get_mut(&domain).unwrap().iter_mut() {
                    if *to == parent {
                        *to = ingress;
                    }
                }
            }
        }

    }

    swaps
}

pub fn connect(log: &Logger,
               graph: &mut Graph,
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
            match *graph[egress] {
                node::Type::Egress(ref txs) => {
                    trace!(log, "connecting"; "egress" => egress.index(), "ingress" => node.index());
                    txs.lock()
                        .unwrap()
                        .push((n.addr(), data_txs[&n.domain()].clone()));
                    continue;
                }
                node::Type::Source => continue,
                _ => unreachable!("ingress parent is not egress"),
            }
        }
    }
}
