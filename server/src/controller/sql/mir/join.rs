use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::{JoinRef, QueryGraph, QueryGraphEdge};
use dataflow::ops::join::JoinType;
use mir::MirNodeRef;
use nom_sql::ConditionTree;
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
) -> Vec<MirNodeRef> {
    let mut join_nodes: Vec<MirNodeRef> = Vec::new();
    let mut join_chains = Vec::new();
    let mut node_count = node_count;

    for jref in qg.join_order.iter() {
        let (join_type, jp) = from_join_ref(jref, &qg);
        let (left_chain, right_chain) =
            pick_join_chains(&jref.src, &jref.dst, &mut join_chains, node_for_rel);

        let jn = mir_converter.make_join_node(
            &format!("{}_n{}", name, node_count),
            jp,
            left_chain.last_node.clone(),
            right_chain.last_node.clone(),
            join_type,
        );

        // merge node chains
        let new_chain = left_chain.merge_chain(right_chain, jn.clone());
        join_chains.push(new_chain);

        node_count += 1;

        join_nodes.push(jn);
    }

    join_nodes
}

fn from_join_ref<'a>(jref: &JoinRef, qg: &'a QueryGraph) -> (JoinType, &'a ConditionTree) {
    match qg.edges[&(jref.src.clone(), jref.dst.clone())] {
        QueryGraphEdge::Join(ref jps) => (JoinType::Inner, &jps[jref.index]),
        QueryGraphEdge::LeftJoin(ref jps) => (JoinType::Left, &jps[jref.index]),
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
