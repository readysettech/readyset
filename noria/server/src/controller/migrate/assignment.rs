//! Functions for assigning new nodes to thread domains.

use dataflow::prelude::*;
use tracing::debug;

use crate::controller::state::DataflowState;
use crate::controller::NodeRestrictionKey;

/// Assigns domains to all the new nodes.
///
/// # Domain assigment heuristics
/// The main idea is to have as few domains as possible.
/// However, domains are assigned or created depending on the type of each node, heuristically.
/// There are some nodes that have special invariants that must be held. See the Invariants section
/// below
///
/// ## Shard Merger
/// Shard Mergers (nodes that are of type [`dataflow::node::NodeType::Internal`] with a
/// [`dataflow::ops::NodeOperator::Union`] operator, where the union has a
/// [`dataflow::ops::union::Emit::AllFrom`]), need their own separate domain from their
/// sharded ancestors.
/// A Shard Merger is then assigned a new domain.
///
/// ## [`dataflow::node::NodeType::Reader`]
/// Since readers always re-materialize, sharing a domain doesn't help them much. Having them in
/// their own domain also means that they get to aggregate reader replay requests in their own
/// thread, and not interfere as much with other internal traffic.
/// A reader node is then assigned a new domain.
///
/// ## [`dataflow::node::NodeType::Base`]
/// Base nodes are assigned domains depending on sharding.
///
/// ### Sharding disabled
/// All base nodes are assigned to the same domain.
///
/// ### Sharding enabled
/// In this situation, the following happens:
/// 1. We traverse down the graph and gather all the children nodes from the base node,
/// until we hit a sharder or shard merger.
/// 2. From all of those children, we traverse the graph up until we encounter another base node
/// without traversing any sharders or shard mergers.
///
/// The set of those base node will be grouped together in the same domain, as long as their shards
/// are compatible with each other (based on the [`DataflowState::node_restrictions`]).
///
/// ## Node name starts with 'BOUNDARY_'
/// It is assigned a new domain.
///
/// ## Any other case
/// The node is assigned the same domain as its first non-source, non-sharder parent.
/// If no such parent exists, it is assigned a new domain.
///
/// # Invariants
/// ## Shard Mergers
/// Shard Mergers are the only nodes that MUST be assigned to a different node than their sharded
/// ancestors.
/// The reason behind this is that it is not possible to change the shard key of the dataflow
/// except at a domain boundary.
pub fn assign(dataflow_state: &mut DataflowState, new_nodes: &[NodeIndex]) -> ReadySetResult<()> {
    // we need to walk the data flow graph and assign domains to all new nodes.
    // we generally want as few domains as possible, but in *some* cases we must make new ones.
    // specifically:
    //
    //  - the child of a Sharder is always in a different domain from the sharder
    //  - shard merge nodes are never in the same domain as their sharded ancestors
    let mut ndomains = dataflow_state.ndomains;

    let mut next_domain = || -> ReadySetResult<usize> {
        ndomains += 1;
        Ok(ndomains - 1)
    };

    for &node in new_nodes {
        #[allow(clippy::cognitive_complexity)]
        let assignment = (|| {
            let graph = &dataflow_state.ingredients;
            let node_restrictions = &dataflow_state.node_restrictions;
            let n = &graph[node];

            // TODO: the code below is probably _too_ good at keeping things in one domain.
            // having all bases in one domain (e.g., if sharding is disabled) isn't great because
            // write performance will suffer terribly.

            if n.is_shard_merger() {
                // shard mergers are always in their own domain.
                // we *could* use the same domain for multiple separate shard mergers
                // but it's unlikely that would do us any good.
                return next_domain();
            }

            if n.is_reader() {
                // readers always re-materialize, so sharing a domain doesn't help them much.
                // having them in their own domain also means that they get to aggregate reader
                // replay requests in their own little thread, and not interfere as much with other
                // internal traffic.
                return next_domain();
            }

            if n.is_base() {
                // bases are in a little bit of an awkward position becuase they can't just blindly
                // join in domains of other bases in the face of sharding. consider the case of two
                // bases, A and B, where A is sharded by A[0] and B by B[0]. Can they share a
                // domain? The way we deal with this is that we walk *down* from the base until we
                // hit any sharders or shard mergers, and then we walk *up* from each node visited
                // on the way down until we hit a base without traversing a sharding.
                // XXX: maybe also do this extended walk for non-bases?
                let mut children_same_shard = Vec::new();
                let mut frontier: Vec<_> = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                    .collect();
                while !frontier.is_empty() {
                    for cni in frontier.split_off(0) {
                        let c = &graph[cni];
                        if !c.is_sharder() && !c.is_shard_merger() {
                            invariant_eq!(n.sharded_by().is_none(), c.sharded_by().is_none());
                            children_same_shard.push(cni);
                            frontier.extend(
                                graph.neighbors_directed(cni, petgraph::EdgeDirection::Outgoing),
                            );
                        }
                    }
                }

                let mut friendly_base = None;
                frontier = children_same_shard;

                // We search to see if there's another base with the same shard constraints.
                // TODO(fran): Why are we looking for a single base? Couldn't we group more bases
                //  together? Or maybe not group bases at all, since having too many bases in one
                //  domain can hurt write performance.
                'search: while !frontier.is_empty() {
                    for pni in frontier.split_off(0) {
                        if pni == node {
                            continue;
                        }

                        let p = &graph[pni];
                        if p.is_base() && p.has_domain() {
                            friendly_base = Some(p);
                            break 'search;
                        } else if !p.is_source() && !p.is_sharder() && !p.is_shard_merger() {
                            invariant_eq!(n.sharded_by().is_none(), p.sharded_by().is_none());
                            frontier.extend(
                                graph.neighbors_directed(pni, petgraph::EdgeDirection::Incoming),
                            );
                        }
                    }
                }

                // A base table is only friendly with another if their shards also
                // do not have conflicting domain placement restrictions. If a node
                // has more shards than another, these shards will be placed
                // in a separate domain shard and placement restrictions do not
                // overlap.
                return Ok(if let Some(friendly_base) = friendly_base {
                    let num_shards = std::cmp::min(
                        n.sharded_by().shards().unwrap_or(1),
                        friendly_base.sharded_by().shards().unwrap_or(1),
                    );

                    // TODO(fran): Is it possible that to have a scneario in which we have two nodes N1 and N2
                    //  with shards N1S1, N1S2 and N2S1, N2S2 and N2S3 where:
                    //   - N1S1 is compatible with N2S2
                    //   - N1S2 is compatible with N2S3
                    //   - Any other combination is incompatible
                    //  There is a valid combination there, by leaving N2S1 into it's own domain, but
                    //  we are not contemplating that here.
                    let compatible = |new_node: &Node, existing_node: &Node| {
                        for i in 0..num_shards {
                            let new_node_key = NodeRestrictionKey {
                                node_name: new_node.name().into(),
                                shard: i,
                            };
                            let existing_node_key = NodeRestrictionKey {
                                node_name: existing_node.name().into(),
                                shard: i,
                            };

                            let compatible = match (
                                node_restrictions.get(&new_node_key),
                                node_restrictions.get(&existing_node_key),
                            ) {
                                // If two nodes each have domain placement restrictions.
                                // The two need to be compatible to be placed in the
                                // same domain. Otherwise, the domain would not be placed
                                // on a valid server.
                                (Some(new_node), Some(existing_node)) => {
                                    // A server can only have one worker_volume, as a
                                    // result, these two nodes should require the same
                                    // worker_volume.
                                    new_node.worker_volume == existing_node.worker_volume
                                }
                                // If we have placement restrictions, don't place the node
                                // in a domain without. We technically can if the worker
                                // matches, but requires more checks.
                                (Some(_), None) => false,
                                // If we have no domain placemnet restrictions, we can
                                // be placed anywhere.
                                (None, Some(_)) => true,
                                (None, None) => true,
                            };

                            if !compatible {
                                return false;
                            }
                        }

                        true
                    };

                    if compatible(n, friendly_base) {
                        friendly_base.domain().index()
                    } else {
                        next_domain()?
                    }
                } else {
                    // there are no bases like us, so we need a new domain :'(
                    next_domain()?
                });
            }

            if graph[node].name().starts_with("BOUNDARY_") {
                return next_domain();
            }

            let any_parents = move |prime: &dyn Fn(&Node) -> bool,
                                    check: &dyn Fn(&Node) -> bool| {
                let mut stack: Vec<_> = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                    .filter(move |&p| prime(&graph[p]))
                    .collect();
                while let Some(p) = stack.pop() {
                    if graph[p].is_source() {
                        continue;
                    }
                    if check(&graph[p]) {
                        return true;
                    }
                    stack.extend(graph.neighbors_directed(p, petgraph::EdgeDirection::Incoming));
                }
                false
            };

            let parents: Vec<_> = graph
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .map(|ni| (ni, &graph[ni]))
                .collect();

            let mut assignment = None;
            for &(_, p) in &parents {
                if p.is_source() {
                    // the source isn't a useful source of truth
                    continue;
                }
                if p.is_sharder() {
                    // we're a child of a sharder (which currently has to be unsharded). we
                    // can't be in the same domain as the sharder (because we're starting a new
                    // sharding)
                    invariant!(p.sharded_by().is_none());
                } else if assignment.is_none() {
                    // the key may move to a different column, so we can't actually check for
                    // ByColumn equality. this'll do for now.
                    invariant_eq!(p.sharded_by().is_none(), n.sharded_by().is_none());
                    if p.has_domain() {
                        assignment = Some(p.domain().index())
                    }
                }

                if let Some(candidate) = assignment {
                    // let's make sure we don't construct a-b-a path
                    if any_parents(
                        &|p| p.has_domain() && p.domain().index() != candidate,
                        &|pp| pp.domain().index() == candidate,
                    ) {
                        assignment = None;
                        continue;
                    }
                    break;
                }
            }

            if assignment.is_none() {
                // check our siblings too
                // XXX: we could keep traversing here to find cousins and such
                for &(pni, _) in &parents {
                    let siblings = graph
                        .neighbors_directed(pni, petgraph::EdgeDirection::Outgoing)
                        .map(|ni| &graph[ni]);
                    for s in siblings {
                        if !s.has_domain() {
                            continue;
                        }
                        if s.sharded_by().is_none() != n.sharded_by().is_none() {
                            continue;
                        }
                        let candidate = s.domain().index();
                        if any_parents(
                            &|p| p.has_domain() && p.domain().index() != candidate,
                            &|pp| pp.domain().index() == candidate,
                        ) {
                            continue;
                        }
                        assignment = Some(candidate);
                        break;
                    }
                }
            }

            Ok(assignment.unwrap_or_else(|| {
                // no other options left -- we need a new domain
                next_domain().unwrap()
            }))
        })()?;

        debug!(
            node = node.index(),
            node_type = ?dataflow_state.ingredients[node],
            domain = ?assignment,
            "node added to domain"
        );
        dataflow_state.ingredients[node].add_to(assignment.into());
    }
    dataflow_state.ndomains = ndomains;
    Ok(())
}
