//! Functions for assigning new nodes to thread domains.

use dataflow::prelude::*;
use tracing::debug;

use crate::controller::state::DfState;

/// Assigns domains to all the new nodes.
///
/// # Domain assignment heuristics
/// The main idea is to have as few domains as possible, but with one structural exception: each
/// base table is assigned its own domain.
///
/// ## [`dataflow::node::NodeType::Reader`]
/// Since readers always re-materialize, sharing a domain doesn't help them much. Having them in
/// their own domain also means that they get to aggregate reader replay requests in their own
/// thread, and not interfere as much with other internal traffic.
/// A reader node is then assigned a new domain.
///
/// ## [`dataflow::node::NodeType::Base`] / [`dataflow::node::NodeType::Constant`]
/// Each base/constant node is assigned its own domain, subject to compatible volume placement
/// restrictions on the candidate (see [`DfState::node_restrictions`]).
///
/// ## Node name starts with 'BOUNDARY_'
/// It is assigned a new domain.
///
/// ## Any other case
/// The node is assigned the same domain as its first non-source parent.
/// If no such parent exists, it is assigned a new domain.
pub fn assign(dataflow_state: &mut DfState, new_nodes: &[NodeIndex]) -> ReadySetResult<()> {
    let mut ndomains = dataflow_state.ndomains;

    let mut next_domain = || -> ReadySetResult<usize> {
        ndomains += 1;
        Ok(ndomains - 1)
    };

    for &node in new_nodes {
        #[allow(clippy::cognitive_complexity)]
        let assignment = (|| {
            let graph = &dataflow_state.ingredients;
            let n = &graph[node];

            if n.is_reader() {
                // readers always re-materialize, so sharing a domain doesn't help them much.
                // having them in their own domain also means that they get to aggregate reader
                // replay requests in their own little thread, and not interfere as much with other
                // internal traffic.
                return next_domain();
            }

            if n.is_source() {
                // Each base / constant gets its own domain. Volume placement restrictions on the
                // node are honored downstream when scheduling the resulting domain.
                return next_domain();
            }

            if graph[node].name().name.starts_with("BOUNDARY_") {
                return next_domain();
            }

            let any_parents = move |prime: &dyn Fn(&Node) -> bool,
                                    check: &dyn Fn(&Node) -> bool| {
                let mut stack: Vec<_> = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                    .filter(move |&p| prime(&graph[p]))
                    .collect();
                while let Some(p) = stack.pop() {
                    if graph[p].is_graph_root() {
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
                if p.is_graph_root() {
                    // the source isn't a useful source of truth
                    continue;
                }
                if assignment.is_none() && p.has_domain() {
                    assignment = Some(p.domain().index())
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
