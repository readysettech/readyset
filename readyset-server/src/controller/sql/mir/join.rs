use std::collections::{HashMap, HashSet};

use mir::NodeIndex;
use nom_sql::Relation;
use readyset_errors::{internal, internal_err, invariant, unsupported, ReadySetResult};

use super::JoinKind;
use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::{QueryGraph, QueryGraphEdge};

struct JoinChain {
    tables: HashSet<Relation>,
    last_node: NodeIndex,
}

impl JoinChain {
    pub(super) fn merge_chain(self, other: JoinChain, last_node: NodeIndex) -> JoinChain {
        let tables = self.tables.union(&other.tables).cloned().collect();

        JoinChain { tables, last_node }
    }

    pub(super) fn has_table(&self, table: &Relation) -> bool {
        self.tables.contains(table)
    }
}

// Generate join nodes for the query.
// This is done by creating/merging join chains as each predicate is added.
// If a predicate's parent tables appear in a previous predicate, the
// current predicate is added to the on-going join chain of the previous
// predicate.
// If a predicate's parent tables haven't been used by any previous predicate,
// a new join chain is started for the current predicate. And we assume that
// a future predicate will bring these chains together.
pub(super) fn make_joins(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: Relation,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&Relation, NodeIndex>,
    correlated_nodes: &HashSet<NodeIndex>,
) -> ReadySetResult<Vec<NodeIndex>> {
    let mut join_nodes: Vec<NodeIndex> = Vec::new();
    let mut join_chains = Vec::new();

    for jref in qg.join_order.iter() {
        let (mut join_kind, jps) = match &qg.edges[&(jref.src.clone(), jref.dst.clone())] {
            QueryGraphEdge::Join { on } => (JoinKind::Inner, on),
            QueryGraphEdge::LeftJoin { on, extra_preds } => {
                if !extra_preds.is_empty() {
                    unsupported!("Non-equal predicates not (yet) supported in left joins");
                }
                (JoinKind::Left, on)
            }
        };

        let (left_chain, right_chain) =
            pick_join_chains(&jref.src, &jref.dst, &mut join_chains, node_for_rel)?;

        // TODO(fran): Use NodeIndex instead of name.
        if correlated_nodes.contains(&right_chain.last_node) {
            match join_kind {
                JoinKind::Left => internal!(
                    "Dependent left join not yet supported (when joining to {})",
                    jref.dst.display_unquoted()
                ),
                JoinKind::Inner => {
                    join_kind = JoinKind::Dependent;
                }
                JoinKind::Dependent => {}
            }
        }

        let jn = mir_converter.make_join_node(
            query_name,
            mir_converter.generate_label(&name),
            jps,
            left_chain.last_node,
            right_chain.last_node,
            join_kind,
        )?;

        // merge node chains
        let new_chain = left_chain.merge_chain(right_chain, jn);
        join_chains.push(new_chain);

        join_nodes.push(jn);
    }

    Ok(join_nodes)
}

/// Make cartesian (cross) joins for the given list of nodes, returning a list of join nodes created
/// in order
///
/// Will return an error if passed an empty list of `nodes`.
///
/// Will never return an empty list.
pub(super) fn make_cross_joins(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: &str,
    nodes: Vec<NodeIndex>,
    correlated_nodes: &HashSet<NodeIndex>,
) -> ReadySetResult<Vec<NodeIndex>> {
    let mut join_nodes = vec![];
    let mut nodes = nodes.into_iter();
    let first_node = nodes
        .next()
        .ok_or_else(|| internal_err!("make_cross_joins called with empty nodes"))?;
    nodes.try_fold(first_node, |n1, n2| -> ReadySetResult<_> {
        // TODO(fran): Use NodeIndex instead of name.
        let join_kind = if correlated_nodes.contains(&n2) {
            JoinKind::Dependent
        } else {
            JoinKind::Inner
        };

        let node = mir_converter.make_join_node(
            query_name,
            mir_converter.generate_label(&name.into()),
            &[],
            n1,
            n2,
            join_kind,
        )?;
        join_nodes.push(node);
        Ok(node)
    })?;

    Ok(join_nodes)
}

// Generate join nodes for the query aggregates. This will call
// `mir_converter.make_join_aggregates_node` only once if there are only two parents, and otherwise
// create multiple nodes of type `MirNodeInner::JoinAggregates`.
pub(super) fn make_joins_for_aggregates(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: &str,
    ancestors: &[NodeIndex],
) -> ReadySetResult<Vec<NodeIndex>> {
    invariant!(ancestors.len() >= 2);

    let parent_join = mir_converter.make_join_aggregates_node(
        query_name,
        mir_converter.generate_label(&name.into()),
        ancestors[0],
        ancestors[1],
    )?;

    let mut join_nodes = vec![parent_join];

    // We skip the first two because those were used for the initial parent join.
    for ancestor in ancestors.iter().skip(2) {
        // We want top join our most recent join node to our next ancestor.
        let jn = mir_converter.make_join_aggregates_node(
            query_name,
            mir_converter.generate_label(&name.into()),
            *join_nodes.last().unwrap(),
            *ancestor,
        )?;

        join_nodes.push(jn);
    }

    Ok(join_nodes)
}

fn pick_join_chains(
    src: &Relation,
    dst: &Relation,
    join_chains: &mut Vec<JoinChain>,
    node_for_rel: &HashMap<&Relation, NodeIndex>,
) -> ReadySetResult<(JoinChain, JoinChain)> {
    let left_chain = match join_chains
        .iter()
        .position(|chain| chain.has_table(&src.clone()))
    {
        Some(idx) => join_chains.swap_remove(idx),
        None => JoinChain {
            tables: std::iter::once(src.clone()).collect(),
            last_node: node_for_rel[src],
        },
    };

    let right_chain = match join_chains
        .iter()
        .position(|chain| chain.has_table(&dst.clone()))
    {
        Some(idx) => join_chains.swap_remove(idx),
        None => JoinChain {
            tables: std::iter::once(dst.clone()).collect(),
            last_node: node_for_rel[dst],
        },
    };

    Ok((left_chain, right_chain))
}
