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
    assign_inner(
        &mut dataflow_state.ingredients,
        &mut dataflow_state.ndomains,
        new_nodes,
    )
}

/// Inner implementation of [`assign`] that takes only the graph and domain counter directly,
/// so it can be exercised by unit tests without standing up a full [`DfState`].
fn assign_inner(
    graph: &mut Graph,
    ndomains: &mut usize,
    new_nodes: &[NodeIndex],
) -> ReadySetResult<()> {
    let mut next_domain = || -> ReadySetResult<usize> {
        *ndomains += 1;
        Ok(*ndomains - 1)
    };

    for &node in new_nodes {
        #[allow(clippy::cognitive_complexity)]
        let assignment = (|| {
            // Shared reborrow so the `move` closure below can capture `&Graph` (Copy) instead
            // of consuming our `&mut Graph`.
            let graph: &Graph = &*graph;
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
            node_type = ?graph[node],
            domain = ?assignment,
            "node added to domain"
        );
        graph[node].add_to(assignment.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use dataflow::node::{self, Column, Node};
    use dataflow::ops::join::{Join, JoinType};
    use dataflow::ops::{NodeOperator, Side};
    use dataflow::utils::make_columns;

    use super::*;

    /// Two base tables connected by a join in a single migration must land in distinct
    /// domains. Pre-REA-6610 the `friendly-base` search co-located them, which serialized
    /// their writes through a shared message queue. Regression test for the failure mode
    /// that hit `it_works_basic` and `correct_nested_view_schema`.
    #[test]
    fn joined_base_tables_get_distinct_domains() {
        let mut graph: Graph = petgraph::Graph::new();
        let source = graph.add_node(Node::new::<_, _, Vec<Column>, _>(
            "source",
            Vec::new(),
            node::special::Source,
        ));

        let a = graph.add_node(Node::new(
            "a",
            make_columns(&["a0", "a1"]),
            node::special::Base::new(),
        ));
        let b = graph.add_node(Node::new(
            "b",
            make_columns(&["b0", "b1"]),
            node::special::Base::new(),
        ));
        graph.add_edge(source, a, ());
        graph.add_edge(source, b, ());

        let join: NodeOperator = Join::new(
            a,
            b,
            JoinType::Inner,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Right, 1)],
            false,
            None,
        )
        .into();
        let j = graph.add_node(Node::new("j", make_columns(&["a0", "b1"]), join));
        graph.add_edge(a, j, ());
        graph.add_edge(b, j, ());

        let mut ndomains = 0;
        assign_inner(&mut graph, &mut ndomains, &[a, b, j]).unwrap();

        assert_ne!(
            graph[a].domain(),
            graph[b].domain(),
            "two base tables joined together must land in distinct domains",
        );
    }
}
