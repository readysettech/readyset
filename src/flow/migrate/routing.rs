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

    // we also need to keep track of the ingress nodes we've added to each domain so that we don't
    // end up with two ingress nodes for a given egress node. that would cause unnecessary
    // cross-domain communication. this is domain => source => NodeIndex (of ingress). note that
    // `source` here is actually the *egress* node. this is because by the time we add ingress
    // nodes, we know our incoming edges have already been updated to point to the egress nodes.
    let mut ingresses = HashMap::new();

    for node in topo_list {
        let domain = graph[node].domain();

        // First, we add egress nodes for any of our cross-domain children
        let children: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            .collect(); // collect so we can mutate graph

        // We then need to make sure that we're acting on up-to-date information about existing
        // egress/ingress pairs. In particular, we want to know about egresses this node already
        // has (and to which domains). In the process we also populate the information about
        // ingress nodes in other domains that point here (those shouldn't be re-created if new
        // nodes are created in the corresponding domains).
        let mut egresses = HashMap::new();
        for child in &children {
            if !new.contains(child) {
                continue;
            }
            if let node::Type::Egress { .. } = *graph[*child] {
                for ingress in graph.neighbors_directed(*child, petgraph::EdgeDirection::Outgoing) {
                    // this egress already contains this node to the ingress' domain
                    egresses.insert(graph[ingress].domain(), *child);
                    // also keep track of the corresponding ingress node so we can re-use it
                    ingresses.entry(graph[ingress].domain())
                        .or_insert_with(HashMap::new)
                        .insert(node, ingress);
                }
            }
        }

        for child in children {
            let cdomain = graph[child].domain();
            if domain != cdomain {
                // child is in a different domain
                if !egresses.contains_key(&cdomain) {
                    // create an egress node to handle that
                    // NOTE: technically, this doesn't need to mirror its parent, but meh
                    let proxy = graph[node].mirror(node::Type::Egress {
                        tags: Default::default(),
                        txs: Default::default(),
                    });
                    let egress = graph.add_node(proxy);
                    graph.add_edge(node, egress, false);

                    new.insert(egress);
                    egresses.insert(cdomain, egress);

                    trace!(log, "adding cross-domain egress to new node"; "node" => node.index(), "egress" => egress.index());
                } else {
                    trace!(log, "re-using cross-domain egress to new node"; "node" => node.index(), "egress" => egresses[&cdomain].index());
                }

                // we need to hook that node in between us and this child
                let egress = egresses[&cdomain];
                let old = graph.find_edge(node, child).unwrap();
                let was_materialized = graph.remove_edge(old).unwrap();
                graph.add_edge(egress, child, was_materialized);
                // this ends up being re-executed, but that's okay
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
            if let node::Type::Egress { .. } = *graph[*parent] {
                continue;
            }

            // let's first see if this parent already has an egress we can use
            let egress = graph.neighbors_directed(*parent, petgraph::EdgeDirection::Outgoing)
                .find(|&ni| graph[ni].is_egress());

            let egress = egress.unwrap_or_else(|| {
                // no, okay, so we need to add an egress for that other node,
                let proxy = graph[*parent].mirror(node::Type::Egress{
                    txs: Default::default(),
                    tags: Default::default()
                });
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
        // Note that by this time (due to the topological walk), we know that `ingresses` has been
        // sufficiently populated to contain any relevant existing ingress nodes.
        for parent in parents {

            // is there already an ingress node we can re-use?
            let mut ingress =
                ingresses.get(&domain).and_then(|ingresses| ingresses.get(&parent)).map(|ni| *ni);

            if ingress.is_none() {
                // nope -- create our new ingress node
                let mut i = graph[parent].mirror(node::Type::Ingress);
                i.add_to(domain); // it belongs to this domain, not that of the parent
                let i = graph.add_node(i);
                graph.add_edge(parent, i, false);

                // we also now need to deal with this ingress node
                new.insert(i);

                if parent == source {
                    trace!(log, "adding source ingress"; "base" => node.index(), "ingress" => i.index());
                    // we don't re-use source ingress nodes
                } else {
                    trace!(log, "adding cross-domain ingress"; "to" => node.index(), "from" => parent.index(), "ingress" => i.index());
                    ingresses.entry(domain).or_insert_with(HashMap::new).insert(parent, i);
                }
                ingress = Some(i);
            } else {
                trace!(log, "re-using cross-domain ingress"; "to" => node.index(), "from" => parent.index(), "ingress" => ingress.unwrap().index());
            }
            let ingress = ingress.unwrap();

            // we need to hook the ingress node in between us and the parent
            let old = graph.find_edge(parent, node).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
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
                node::Type::Egress { ref txs, .. } => {
                    trace!(log, "connecting"; "egress" => egress.index(), "ingress" => node.index());
                    txs.lock()
                        .unwrap()
                        .push((node.into(), n.addr(), data_txs[&n.domain()].clone()));
                    continue;
                }
                node::Type::Source => continue,
                _ => unreachable!("ingress parent is not egress"),
            }
        }
    }
}
