//! Functions for adding ingress/egress nodes.
//!
//! In particular:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Egress nodes that gain new children must gain channels to facilitate forwarding

use std::collections::{HashMap, HashSet};

use dataflow::prelude::*;
use dataflow::{node, DomainRequest};
use noria_errors::{
    internal, internal_err, invariant, invariant_eq, ReadySetError, ReadySetResult,
};
use petgraph::graph::NodeIndex;
use tracing::trace;

use crate::controller::migrate::DomainMigrationPlan;
use crate::controller::state::DataflowState;

/// Add in ingress and egress nodes as appropriate in the graph to facilitate cross-domain
/// communication.
pub fn add(
    dataflow_state: &mut DataflowState,
    new: &mut HashSet<NodeIndex>,
    topo_list: &[NodeIndex],
) -> Result<HashMap<(NodeIndex, NodeIndex), NodeIndex>, ReadySetError> {
    // find all new nodes in topological order. we collect first since we'll be mutating the graph
    // below. it's convenient to have the nodes in topological order, because we then know that
    // we'll first add egress nodes, and then the related ingress nodes. if we're ever required to
    // add an ingress node, and its parent isn't an egress node, we know that we're seeing a
    // connection between an old node in one domain, and a new node in a different domain.

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
    for &node in topo_list {
        let domain = dataflow_state.ingredients[node].domain();
        let parents: Vec<_> = dataflow_state
            .ingredients
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect(); // collect so we can mutate graph

        // first, we look at all other-domain parents of new nodes. if a parent does not have an
        // egress node child, we *don't* add one at this point (this is done at a later stage,
        // because we also need to handle the case where the parent is a sharder). when the parent
        // does not have any egress children, the node's domain *cannot* have an ingress for that
        // parent already, so we also make an ingress node. if the parent does have an egress
        // child, we check the children of that egress node for any ingress nodes that are in the
        // domain of the current node. if there aren't any, we make one. if there are, we only need
        // to redirect the node's parent edge to the ingress.
        for parent in parents {
            if dataflow_state.ingredients[parent].is_source()
                || dataflow_state.ingredients[parent].domain() == domain
            {
                continue;
            }

            // parent is in other domain! does it already have an egress?
            let mut ingress: Option<ReadySetResult<NodeIndex>> = None;
            if parent != dataflow_state.source {
                'search: for pchild in dataflow_state
                    .ingredients
                    .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                {
                    if dataflow_state.ingredients[pchild].is_egress() {
                        // it does! does `domain` have an ingress already listed there?
                        for i in dataflow_state
                            .ingredients
                            .neighbors_directed(pchild, petgraph::EdgeDirection::Outgoing)
                        {
                            invariant!(dataflow_state.ingredients[i].is_ingress());
                            if dataflow_state.ingredients[i].domain() == domain {
                                // it does! we can just reuse that ingress :D
                                ingress = Some(Ok(i));
                                // FIXME(malte): this is buggy! it will re-use ingress nodes even if
                                // they have a different sharding to the one we're about to add
                                // (whose sharding is only determined below).
                                trace!(
                                    to = node.index(),
                                    from = parent.index(),
                                    ingress = i.index(),
                                    "re-using cross-domain ingress"
                                );
                                break 'search;
                            }
                        }
                    }
                }
            }

            let ingress = ingress.unwrap_or_else(|| {
                // we need to make a new ingress
                let mut i = dataflow_state.ingredients[parent].mirror(node::special::Ingress);

                // it belongs to this domain, not that of the parent
                i.add_to(domain);

                // the ingress is sharded the same way as its target, but with remappings of parent
                // columns applied
                let sharding = if let Some(s) = dataflow_state.ingredients[parent].as_sharder() {
                    let parent_out_sharding = s.sharded_by();
                    // TODO(malte): below is ugly, but the only way to get the sharding width at
                    // this point; the sharder parent does not currently have the information.
                    // Change this once we support per-subgraph sharding widths and
                    // the sharder knows how many children it is supposed to have.
                    if let Sharding::ByColumn(_, width) =
                        dataflow_state.ingredients[node].sharded_by()
                    {
                        Sharding::ByColumn(parent_out_sharding, width)
                    } else {
                        internal!()
                    }
                } else {
                    dataflow_state.ingredients[parent].sharded_by()
                };
                i.shard_by(sharding);

                // insert the new ingress node
                let ingress = dataflow_state.ingredients.add_node(i);
                dataflow_state.ingredients.add_edge(parent, ingress, ());

                // we also now need to deal with this ingress node
                new.insert(ingress);

                if parent == dataflow_state.source {
                    trace!(
                        base = node.index(),
                        ingress = ingress.index(),
                        "adding source ingress"
                    );
                } else {
                    trace!(
                        to = node.index(),
                        from = parent.index(),
                        ingress = ingress.index(),
                        "adding cross-domain ingress"
                    );
                }

                Ok(ingress)
            })?;

            // we need to hook the ingress node in between us and our remote parent
            #[allow(clippy::unit_arg)]
            #[allow(clippy::let_unit_value)]
            {
                let old = dataflow_state.ingredients.find_edge(parent, node).unwrap();
                let was_materialized = dataflow_state.ingredients.remove_edge(old).unwrap();
                dataflow_state
                    .ingredients
                    .add_edge(ingress, node, was_materialized);
            }

            // we now need to refer to the ingress instead of the "real" parent
            swaps.insert((node, parent), ingress);
        }

        // we now have all the ingress nodes we need. it's time to check that they are all
        // connected to an egress or a sharder (otherwise they would never receive anything!).
        // Note that we need to re-load the list of parents, because it might have changed as a
        // result of adding ingress nodes.
        let parents: Vec<_> = dataflow_state
            .ingredients
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .collect(); // collect so we can mutate graph
        for ingress in parents {
            if !dataflow_state.ingredients[ingress].is_ingress() {
                continue;
            }

            let sender = {
                let mut senders = dataflow_state
                    .ingredients
                    .neighbors_directed(ingress, petgraph::EdgeDirection::Incoming);
                let sender = senders
                    .next()
                    .ok_or_else(|| internal_err("ingress has no parents"))?;
                if senders.count() != 0 {
                    internal!("ingress had more than one parent");
                }
                sender
            };

            if sender == dataflow_state.source {
                // no need for egress from source
                continue;
            }

            if dataflow_state.ingredients[sender].is_sender() {
                // all good -- we're already hooked up with an egress or sharder!
                if dataflow_state.ingredients[sender].is_egress() {
                    trace!(
                        node = node.index(),
                        egress = sender.index(),
                        "re-using cross-domain egress to new node"
                    );
                }
                continue;
            }

            // ingress is not already connected to egress/sharder
            // next, check if source node already has an egress
            let egress = {
                let mut es = dataflow_state
                    .ingredients
                    .neighbors_directed(sender, petgraph::EdgeDirection::Outgoing)
                    .filter(|&ni| dataflow_state.ingredients[ni].is_egress());
                let egress = es.next();
                if es.count() != 0 {
                    internal!("node has more than one egress")
                }
                egress
            };

            if let Some(egress) = egress {
                trace!(
                    ingress = ingress.index(),
                    egress = egress.index(),
                    "re-using cross-domain egress to ingress"
                );
            }

            let egress = egress.unwrap_or_else(|| {
                // need to inject an egress above us

                // NOTE: technically, this doesn't need to mirror its parent, but meh
                let mut egress =
                    dataflow_state.ingredients[sender].mirror(node::special::Egress::default());
                egress.add_to(dataflow_state.ingredients[sender].domain());
                egress.shard_by(dataflow_state.ingredients[sender].sharded_by());
                let egress = dataflow_state.ingredients.add_node(egress);
                dataflow_state.ingredients.add_edge(sender, egress, ());

                // we also now need to deal with this egress node
                new.insert(egress);

                trace!(
                    ingress = ingress.index(),
                    egress = egress.index(),
                    "adding cross-domain egress to send to new ingress"
                );

                egress
            });

            // we need to hook the egress in between the ingress and its "real" parent
            #[allow(clippy::unit_arg)]
            #[allow(clippy::let_unit_value)]
            {
                let old = dataflow_state
                    .ingredients
                    .find_edge(sender, ingress)
                    .unwrap();
                let was_materialized = dataflow_state.ingredients.remove_edge(old).unwrap();
                dataflow_state
                    .ingredients
                    .add_edge(egress, ingress, was_materialized);
            }

            // NOTE: we *don't* need to update swaps here, because ingress doesn't care
        }
    }

    Ok(swaps)
}

pub(in crate::controller) fn connect(
    graph: &Graph,
    dmp: &mut DomainMigrationPlan,
    new: &HashSet<NodeIndex>,
) -> ReadySetResult<()> {
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
                trace!(
                    egress = sender.index(),
                    ingress = node.index(),
                    "connecting"
                );

                let shards = dmp.num_shards(n.domain())?;
                if shards != 1 && !sender_node.sharded_by().is_none() {
                    // we need to be a bit careful here in the particular case where we have a
                    // sharded egress that sends to another domain sharded by the same key.
                    // specifically, in that case we shouldn't have each shard of domain A send to
                    // all the shards of B. instead A[0] should send to B[0], A[1] to B[1], etc.
                    // note that we don't have to check the sharding of both src and dst here,
                    // because an egress implies that no shuffle was necessary, which again means
                    // that the sharding must be the same.
                    for shard in 0..shards {
                        dmp.add_message_for_shard(
                            sender_node.domain(),
                            shard,
                            DomainRequest::UpdateEgress {
                                node: sender_node.local_addr(),
                                new_tx: Some((
                                    node,
                                    n.local_addr(),
                                    ReplicaAddress {
                                        domain_index: n.domain(),
                                        shard,
                                    },
                                )),
                                new_tag: None,
                            },
                        )?;
                    }
                } else {
                    // consider the case where len != 1. that must mean that the
                    // sender_node.sharded_by() == Sharding::None. so, we have an unsharded egress
                    // sending to a sharded child. but that shouldn't be allowed -- such a node
                    // *must* be a Sharder.
                    invariant_eq!(shards, 1);
                    dmp.add_message(
                        sender_node.domain(),
                        DomainRequest::UpdateEgress {
                            node: sender_node.local_addr(),
                            new_tx: Some((
                                node,
                                n.local_addr(),
                                ReplicaAddress {
                                    domain_index: n.domain(),
                                    shard: 0,
                                },
                            )),
                            new_tag: None,
                        },
                    )?;
                }
            } else if sender_node.is_sharder() {
                trace!(
                    sharder = sender.index(),
                    ingress = node.index(),
                    "connecting"
                );

                let shards = dmp.num_shards(n.domain())?;
                let txs = (0..shards)
                    .map(|shard| ReplicaAddress {
                        domain_index: n.domain(),
                        shard,
                    })
                    .collect();
                dmp.add_message(
                    sender_node.domain(),
                    DomainRequest::UpdateSharder {
                        node: sender_node.local_addr(),
                        new_txs: (n.local_addr(), txs),
                    },
                )?;
            } else if sender_node.is_source() {
            } else {
                internal!("ingress parent is not a sender");
            }
        }
    }
    Ok(())
}
