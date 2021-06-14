use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::{JoinPredicate, JoinRef, QueryGraph, QueryGraphEdge};
use crate::ReadySetResult;
use dataflow::ops::join::JoinType;
use mir::MirNodeRef;
use noria::invariant;
use std::collections::{HashMap, HashSet};

struct JoinChain {
    tables: HashSet<String>,
    last_node: MirNodeRef,
}

impl JoinChain {
    pub(super) fn merge_chain(self, other: JoinChain, last_node: MirNodeRef) -> JoinChain {
        let tables = self.tables.union(&other.tables).cloned().collect();

        JoinChain { tables, last_node }
    }

    pub(super) fn has_table(&self, table: &str) -> bool {
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
    mir_converter: &SqlToMirConverter,
    name: &str,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&str, MirNodeRef>,
    node_count: usize,
) -> ReadySetResult<Vec<MirNodeRef>> {
    let mut join_nodes: Vec<MirNodeRef> = Vec::new();
    let mut join_chains = Vec::new();
    let mut node_count = node_count;

    for jref in qg.join_order.iter() {
        let (join_type, jps) = from_join_ref(jref, &qg);
        let (left_chain, right_chain) =
            pick_join_chains(&jref.src, &jref.dst, &mut join_chains, node_for_rel);

        let jn = mir_converter.make_join_node(
            &format!("{}_n{}", name, node_count),
            jps,
            left_chain.last_node.clone(),
            right_chain.last_node.clone(),
            join_type,
        )?;

        // merge node chains
        let new_chain = left_chain.merge_chain(right_chain, jn.clone());
        join_chains.push(new_chain);

        node_count += 1;

        join_nodes.push(jn);
    }

    Ok(join_nodes)
}

// Generate join nodes for the query aggregates. This will call `mir_converter.make_join_aggregates_node` only
// once if there are only two parents, and otherwise create multiple nodes of type
// `MirNodeInner::JoinAggregates`.
pub(super) fn make_joins_for_aggregates(
    mir_converter: &SqlToMirConverter,
    name: &str,
    ancestors: &[MirNodeRef],
    node_count: usize,
) -> ReadySetResult<Vec<MirNodeRef>> {
    invariant!(ancestors.len() >= 2);

    let parent_join = mir_converter.make_join_aggregates_node(
        &format!("{}_n{}", name, node_count),
        &[ancestors[0].clone(), ancestors[1].clone()],
    )?;

    let mut node_count = node_count + 1;

    let mut join_nodes = vec![parent_join];

    // We skip the first two because those were used for the initial parent join.
    for ancestor in ancestors.iter().skip(2) {
        // We want top join our most recent join node to our next ancestor.
        let jn = mir_converter.make_join_aggregates_node(
            &format!("{}_n{}", name, node_count),
            &[join_nodes.last().unwrap().clone(), ancestor.clone()],
        )?;

        node_count += 1;

        join_nodes.push(jn);
    }

    Ok(join_nodes)
}

fn from_join_ref<'a>(jref: &JoinRef, qg: &'a QueryGraph) -> (JoinType, &'a [JoinPredicate]) {
    match &qg.edges[&(jref.src.clone(), jref.dst.clone())] {
        QueryGraphEdge::Join { on } => (JoinType::Inner, on),
        QueryGraphEdge::LeftJoin { on } => (JoinType::Left, on),
        QueryGraphEdge::GroupBy(_) => unreachable!(),
    }
}

fn pick_join_chains(
    src: &str,
    dst: &str,
    join_chains: &mut Vec<JoinChain>,
    node_for_rel: &HashMap<&str, MirNodeRef>,
) -> (JoinChain, JoinChain) {
    let left_chain = match join_chains.iter().position(|chain| chain.has_table(src)) {
        Some(idx) => join_chains.swap_remove(idx),
        None => JoinChain {
            tables: std::iter::once(src.to_owned()).collect(),
            last_node: node_for_rel[src].clone(),
        },
    };

    let right_chain = match join_chains.iter().position(|chain| chain.has_table(dst)) {
        Some(idx) => join_chains.swap_remove(idx),
        None => JoinChain {
            tables: std::iter::once(dst.to_owned()).collect(),
            last_node: node_for_rel[dst].clone(),
        },
    };

    (left_chain, right_chain)
}
