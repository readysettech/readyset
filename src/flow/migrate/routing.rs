//! Functions for adding ingress/egress nodes.
//!
//! In particular:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Egress nodes that gain new children must gain channels to facilitate forwarding

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

    // in the code below, there are three node type of interest: ingress, egress, and sharder. we
    // want to ensure the following properties:
    //
    //  - every time an edge crosses a domain boundary, the target of the edge is an ingress node.
    //  - every ingress node has a parent that is either a sharder or an egress node.
    //  - if an ingress does *not* have such a parent, we add an egress node to the ingress'
    //    ancestor's domain, and interject it between the ingress and its old parent.
    //  - every domain has at most one egress node as a child of any other node.
    //  - every domain has at most one ingress node connected to any single egress node.
    //
    // this is a lot to keep track of. the last two invariants (which are mostly for efficiency) in
    // particular require some extra bookkeeping, especially considering that they may end up
    // causing re-use of ingress and egress nodes that were added in a *previous* migration.
    //
    // we do this in a couple of passes, as described below.
    //
    // first, we look at all other-domain parents of new nodes. if a parent does not have an egress
    // node child, we *don't* add one at this point (this is done at a later stage, because we also
    // need to handle the case where the parent is a sharder). when the parent does not have any
    // egress children, the node's domain *cannot* have an ingress for that parent already, so we
    // also make an ingress node. if the parent does have an egress child, we check the children of
    // that egress node for any ingress nodes that are in the domain of the current node. if there
    // aren't any, we make one. if there are, we only need to redirect the node's parent edge to
    // the ingress.
    for &node in &topo_list {
        let domain = graph[node].domain();
        let parents: Vec<_> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect(); // collect so we can mutate graph

        for parent in parents {
            if !graph[parent].is_source() && graph[parent].domain() == domain {
                continue;
            }

            // parent is in other domain! does it already have an egress?
            let mut ingress = None;
            if parent != source {
                'search: for pchild in
                    graph.neighbors_directed(parent, petgraph::EdgeDirection::Outgoing) {
                    if graph[pchild].is_egress() {
                        // it does! does `domain` have an ingress already listed there?
                        for i in
                            graph.neighbors_directed(pchild, petgraph::EdgeDirection::Outgoing) {
                            assert!(graph[i].is_ingress());
                            if graph[i].domain() == domain {
                                // it does! we can just reuse that ingress :D
                                ingress = Some(i);
                                trace!(log,
                                       "re-using cross-domain ingress";
                                       "to" => node.index(),
                                       "from" => parent.index(),
                                       "ingress" => i.index()
                                );
                                break 'search;
                            }
                        }
                    }
                }
            }

            let ingress = ingress.unwrap_or_else(|| {
                // we need to make a new ingress
                let mut i = graph[parent].mirror(node::special::Ingress);

                // it belongs to this domain, not that of the parent
                i.add_to(domain);

                // we also now need to deal with this ingress node
                let ingress = graph.add_node(i);
                new.insert(ingress);

                if parent == source {
                    trace!(log,
                               "adding source ingress";
                               "base" => node.index(),
                               "ingress" => ingress.index()
                        );
                } else {
                    trace!(log,
                               "adding cross-domain ingress";
                               "to" => node.index(),
                               "from" => parent.index(),
                               "ingress" => ingress.index()
                        );
                }

                ingress
            });

            // we need to hook the ingress node in between us and the parent
            graph.add_edge(parent, ingress, false);
            let old = graph.find_edge(parent, node).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(ingress, node, was_materialized);

            // we now need to refer to the ingress instead of the "real" parent
            swaps
                .entry(domain)
                .or_insert_with(HashMap::new)
                .insert(node, ingress);
        }
    }

    // we now have all the ingress nodes we need. it's time to check that they are all connected to
    // an egress or a sharder (otherwise they would never receive anything!).
    //
    // TODO:
    // we may actually be able to merge this loop with the one above due to topological traversal..
    for &node in &topo_list {
        let parents: Vec<_> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect(); // collect so we can mutate graph

        for ingress in parents {
            if !graph[ingress].is_ingress() {
                continue;
            }

            let sender = {
                let mut senders =
                    graph.neighbors_directed(ingress, petgraph::EdgeDirection::Incoming);
                let sender = senders.next().expect("ingress has no parents");
                assert_eq!(senders.count(), 0, "ingress had more than one parent");
                sender
            };

            if graph[sender].is_sender() {
                // all good -- we're already hooked up with an egress or sharder!
                if graph[sender].is_egress() {
                    trace!(log,
                           "re-using cross-domain egress to new node";
                           "node" => node.index(),
                           "egress" => sender.index()
                    );
                }
                continue;
            }

            // need to inject an egress above us
            // NOTE: technically, this doesn't need to mirror its parent, but meh
            let egress = graph[sender].mirror(node::special::Egress::default());
            let egress = graph.add_node(egress);

            // we need to hook the egress in between the ingress and its "real" parent
            graph.add_edge(sender, egress, false);
            let old = graph.find_edge(sender, ingress).unwrap();
            let was_materialized = graph.remove_edge(old).unwrap();
            graph.add_edge(egress, ingress, was_materialized);

            // we also now need to deal with this egress node
            new.insert(egress);

            trace!(log,
                   "adding cross-domain egress to send to new ingress";
                   "ingress" => node.index(),
                   "egress" => egress.index()
            );

            // NOTE: we *don't* need to update swaps here, because ingress doesn't care
        }
    }

    swaps
}

pub fn connect(log: &Logger,
               graph: &mut Graph,
               main_txs: &HashMap<domain::Index, mpsc::SyncSender<Box<Packet>>>,
               new: &HashSet<NodeIndex>) {

    // ensure all egress nodes contain the tx channel of the domains of their child ingress nodes
    for &node in new {
        let n = &graph[node];
        if n.is_ingress() {
            // check the egress or sharder connected to this ingress
        } else {
            continue;
        }

        for sender in graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming) {
            let sender_node = &graph[sender];
            if sender_node.is_egress() {
                trace!(log,
                           "connecting";
                           "egress" => sender.index(),
                           "ingress" => node.index()
                    );
                main_txs[&sender_node.domain()]
                    .send(box Packet::UpdateEgress {
                              node: sender_node.local_addr().as_local().clone(),
                              new_tx: Some((node.into(),
                                            *n.local_addr(),
                                            main_txs[&n.domain()].clone())),
                              new_tag: None,
                          })
                    .unwrap();
            } else if sender_node.is_sharder() {
                trace!(log,
                           "connecting";
                           "sharder" => sender.index(),
                           "ingress" => node.index()
                    );
                main_txs[&sender_node.domain()]
                    .send(box Packet::UpdateSharder {
                              node: sender_node.local_addr().as_local().clone(),
                              new_tx: (*n.local_addr(), main_txs[&n.domain()].clone()),
                          })
                    .unwrap();
            } else if sender_node.is_source() {
            } else {
                unreachable!("ingress parent is not a sender");
            }
        }
    }
}
