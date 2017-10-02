use sql::reuse::helpers::predicate_implication::predicate_is_equivalent;
use sql::query_graph::{JoinRef, QueryGraph, QueryGraphEdge};
use std::vec::Vec;
use std::collections::HashSet;
use nom_sql::ConditionTree;

use std::mem;

use sql::reuse::ReuseType;

#[derive(Debug, Clone)]
struct JoinChain {
    pub join_order: Vec<JoinRef>,
    tables: HashSet<String>,
    pub stopped: bool,
}

impl JoinChain {
    fn empty() -> JoinChain {
        JoinChain {
            tables: HashSet::new(),
            join_order: vec![],
            stopped: false,
        }
    }

    fn join_count(&self) -> usize {
        self.join_order.len()
    }

    fn add(&mut self, join_ref: JoinRef) {
        self.tables.insert(join_ref.src.clone());
        self.tables.insert(join_ref.dst.clone());
        self.join_order.push(join_ref);
    }

    fn conflicts(&self, other: &JoinChain) -> bool {
        !self.tables.is_disjoint(&other.tables)
    }

    fn has_table(&self, table: &String) -> bool {
        self.tables.contains(table)
    }

    fn merge_chain(self, other: JoinChain) -> JoinChain {
        let tables = self.tables.union(&other.tables).cloned().collect();
        let join_order = self.join_order
            .into_iter()
            .chain(other.join_order.into_iter())
            .collect();
        let stopped = self.stopped && other.stopped;

        JoinChain {
            tables: tables,
            join_order: join_order,
            stopped: stopped,
        }
    }
}

/// Update a list of join chains by adding a new predicate.
/// The most recently modified chain will be at the end of
/// the list.
fn extend_chains(chains: &mut Vec<JoinChain>, jref: &JoinRef) {
    let src_chain = match chains.iter().position(|ref c| c.has_table(&jref.src)) {
        Some(idx) => chains.swap_remove(idx),
        None => JoinChain::empty(),
    };

    let dst_chain = match chains.iter().position(|ref c| c.has_table(&jref.dst)) {
        Some(idx) => chains.swap_remove(idx),
        None => JoinChain::empty(),
    };

    let mut new_chain = src_chain.merge_chain(dst_chain);
    new_chain.add(jref.clone());
    chains.push(new_chain);
}

fn greedy_merge(mc: JoinChain, existing_chains: &mut Vec<JoinChain>) {
    // find the existing chains that conflict with `mc` and count
    // the number of join predicates
    let reused_joins = existing_chains
        .iter()
        .filter(|ref c| mc.conflicts(c))
        .fold(0, |acc, ref c| acc + c.join_count());

    // if `mc` has more join predicates than the conflicting chains,
    // delete the conflicting chains and add `mc`
    if reused_joins < mc.join_count() {
        existing_chains.retain(|c| !mc.conflicts(c));
        existing_chains.push(mc);
    }
}

// Creates a join order from a list of join chains.
fn chains_to_order(chains: Vec<JoinChain>, order: &mut Vec<JoinRef>) {
    // Join chains act on disjoint tables, so it doesn't matter the
    // order in which they are added, as long as the chain's join
    // order is preserved.
    let mut new_order = chains.iter().fold(vec![], |acc, ref c| {
        acc.iter().chain(c.join_order.iter()).cloned().collect()
    });

    // finds predicates that are not present in any chain and add them
    // to the end.
    for jref in order.iter() {
        if !new_order.contains(&jref) {
            new_order.push(jref.clone());
        }
    }

    assert_eq!(new_order.len(), order.len());

    // replace the current order for the new order
    mem::replace(order, new_order);
}

fn from_join_ref<'a>(jref: &JoinRef, qg: &'a QueryGraph) -> &'a ConditionTree {
    let edge = qg.edges.get(&(jref.src.clone(), jref.dst.clone())).unwrap();
    match *edge {
        QueryGraphEdge::Join(ref jps) => jps.get(jref.index).unwrap(),
        QueryGraphEdge::LeftJoin(ref jps) => jps.get(jref.index).unwrap(),
        QueryGraphEdge::GroupBy(_) => unreachable!(),
    }
}


pub fn reorder_joins(qg: &mut QueryGraph, reuse_candidates: &Vec<(ReuseType, &QueryGraph)>) {
    let mut join_chains = Vec::new();
    // For each reuse candidate, let's find the common join
    // chains it has with the new query graph.
    for &(_, eqg) in reuse_candidates {
        let mut existing_join_chains = Vec::new();
        let mut shared_join_chains = Vec::new();

        for existing_jref in eqg.join_order.iter() {
            // update the join chains of the existing qg
            extend_chains(&mut existing_join_chains, existing_jref);

            // this is the chain we will explore
            let existing_chain = existing_join_chains.last_mut().unwrap();

            // if the chain has been stopped, maybe because it diverged
            // from the chains in the new query graph, move on to next predicate
            if existing_chain.stopped {
                continue;
            }

            let ejp = from_join_ref(&existing_jref, eqg);

            // look in the new query graph for an equivalent join predicate.
            let mut found = false;
            for new_jref in qg.join_order.iter() {
                let njp = from_join_ref(&new_jref, qg);
                // if we find an equivalent join, add it to the new query's join chains
                if predicate_is_equivalent(njp, ejp) {
                    extend_chains(&mut shared_join_chains, new_jref);
                    found = true;
                    break;
                }
            }
            // if no equivalent predicate was found, the chain in
            // the existing query graph has diverged from the chain
            // in the new query graph, so we stop exploring it
            if !found {
                existing_chain.stopped = true;
            }
        }

        // now we have a set of join chains shared between the
        // existing and the new query graph. so we try to greedily
        // merge each chain into our current set of chains.
        for chain in shared_join_chains {
            greedy_merge(chain, &mut join_chains);
        }
    }

    chains_to_order(join_chains, &mut qg.join_order);
}
