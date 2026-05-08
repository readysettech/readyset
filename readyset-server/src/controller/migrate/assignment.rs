//! Functions for assigning new nodes to thread domains.

use dataflow::prelude::*;
use tracing::debug;

use crate::controller::state::DfState;

/// Assigns domains to all the new nodes.
///
/// # Domain assignment heuristics
/// The main idea is to have as few domains as possible, but with one structural exception: each
/// base table is assigned its own domain. Constants (`VALUES`) are placed in a deferred second
/// pass — see [`dataflow::node::NodeType::Constant`] below.
///
/// ## [`dataflow::node::NodeType::Reader`]
/// Since readers always re-materialize, sharing a domain doesn't help them much. Having them in
/// their own domain also means that they get to aggregate reader replay requests in their own
/// thread, and not interfere as much with other internal traffic.
/// A reader node is then assigned a new domain.
///
/// ## [`dataflow::node::NodeType::Base`]
/// Each base table is assigned its own domain.
///
/// ## [`dataflow::node::NodeType::Constant`]
/// Constants (`VALUES` clauses) are co-located with their consumer's domain in a deferred
/// second pass. Structurally a Constant looks like a Base — both originate data and have only
/// the graph root as a parent — but operationally a Constant does no ongoing work: it receives
/// no writes, owns no I/O, and just emits a fixed set of rows once at materialization time.
/// Giving it its own domain costs an OS thread for no benefit and forces cross-domain message
/// passing for static rows the consumer needs. Co-locating turns Constant→Join row delivery
/// into intra-domain message passing and eliminates the Egress/Ingress pair between them.
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

    // Constants are deferred to pass 2 (see below) so they can co-locate with their consumer.
    let mut deferred_constants: Vec<NodeIndex> = Vec::new();

    for &node in new_nodes {
        if graph[node].is_constant() {
            deferred_constants.push(node);
            continue;
        }

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

            if n.is_base() {
                // Each base table gets its own domain.
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
            placement = "primary",
            "node added to domain"
        );
        graph[node].add_to(assignment.into());
    }

    // Pass 2: place each deferred Constant in its consumer's domain. By processing every
    // non-Constant in pass 1, every consumer of a Constant has been assigned a domain by
    // the time pass 2 runs — regardless of where the Constant fell in topo order.
    // MIR-to-graph compilation always wires a Constant to at least one downstream operator
    // before this function runs; if neither holds, the graph is malformed and we bail.
    //
    // Multi-consumer Constants are not produced by the current MIR pipeline. If that
    // changes, the placement policy below (pick the first consumer) needs revisiting —
    // unchosen consumers would pay a cross-domain hop, partially undoing this fix.
    for node in deferred_constants {
        debug_assert!(
            graph
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .count()
                <= 1,
            "Constant {} has multiple consumers; placement policy is undefined for that case",
            node.index()
        );
        let assignment = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            .find(|&c| graph[c].has_domain())
            .map(|c| graph[c].domain().index())
            .ok_or_else(|| {
                internal_err!(
                    "Constant node {} has no downstream consumer with an assigned domain",
                    node.index()
                )
            })?;
        debug!(
            node = node.index(),
            node_type = ?graph[node],
            domain = ?assignment,
            placement = "deferred_constant",
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
    use dataflow::prelude::DfValue;
    use dataflow::utils::make_columns;

    use super::*;

    /// Build an empty graph with the singular graph-root node, mirroring how `DfState`
    /// initializes `ingredients`. Returns the graph and the root node index so callers can
    /// wire base/constant nodes under it.
    fn graph_with_source() -> (Graph, NodeIndex) {
        let mut graph: Graph = petgraph::Graph::new();
        let source = graph.add_node(Node::new::<_, _, Vec<Column>, _>(
            "source",
            Vec::new(),
            node::special::Source,
        ));
        (graph, source)
    }

    /// Two base tables connected by a join in a single migration must land in distinct
    /// domains. Pre-REA-6610 the `friendly-base` search co-located them, which serialized
    /// their writes through a shared message queue. Regression test for the failure mode
    /// that hit `it_works_basic` and `correct_nested_view_schema`.
    #[test]
    fn joined_base_tables_get_distinct_domains() {
        let (mut graph, source) = graph_with_source();

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

    /// A Constant feeding a Join must land in the Join's domain rather than its own. The
    /// previous behavior — driven by `is_source()` returning true for both Bases and
    /// Constants — gave every VALUES clause an OS thread for a node that does no ongoing
    /// work, and forced cross-domain message passing for static rows the Join needed.
    #[test]
    fn constant_inherits_consumer_domain() {
        let (mut graph, source) = graph_with_source();

        let t = graph.add_node(Node::new(
            "t",
            make_columns(&["id"]),
            node::special::Base::new(),
        ));
        let v = graph.add_node(Node::new(
            "v",
            make_columns(&["x"]),
            node::special::Constant::new(vec![
                vec![DfValue::from(1)],
                vec![DfValue::from(2)],
                vec![DfValue::from(3)],
            ]),
        ));
        graph.add_edge(source, t, ());
        graph.add_edge(source, v, ());

        let join: NodeOperator = Join::new(
            t,
            v,
            JoinType::Inner,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Right, 0)],
            false,
            None,
        )
        .into();
        let j = graph.add_node(Node::new("j", make_columns(&["id"]), join));
        graph.add_edge(t, j, ());
        graph.add_edge(v, j, ());

        let mut ndomains = 0;
        // Real topo order places the Constant `v` before its consumer `j`. Pass 2 handles
        // this by deferring Constant placement until after pass 1 assigns every non-Constant.
        assign_inner(&mut graph, &mut ndomains, &[t, v, j]).unwrap();

        assert_eq!(
            graph[v].domain(),
            graph[j].domain(),
            "Constant must inherit the consuming Join's domain, not get its own",
        );
    }
}
