use nom_sql::{Table, ConditionTree};
use sql::query_graph::{QueryGraph, QueryGraphEdge, JoinRef};
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;
use sql::reuse::helpers::predicate_implication::predicate_is_equivalent;

mod finkelstein;
mod relaxed;
mod full;
mod helpers;

#[derive(Clone, Debug)]
pub enum ReuseType {
    DirectExtension,
    PrefixReuse,
    #[allow(dead_code)] BackjoinRequired(Vec<Table>),
}

enum ReuseConfigType {
    Finkelstein,
    Relaxed,
    Full,
}

// TODO(larat): make this a dynamic option
const REUSE_CONFIG: ReuseConfigType = ReuseConfigType::Full;

pub struct ReuseConfig {
    config: ReuseConfigType,
}

impl ReuseConfig {
    pub fn reuse_candidates<'a>(
        &self,
        qg: &mut QueryGraph,
        query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>,
    ) -> Vec<(ReuseType, &'a QueryGraph)> {
        let reuse_candidates = match self.config {
            ReuseConfigType::Finkelstein => {
                finkelstein::Finkelstein::reuse_candidates(qg, query_graphs)
            }
            ReuseConfigType::Relaxed => relaxed::Relaxed::reuse_candidates(qg, query_graphs),
            ReuseConfigType::Full => full::Full::reuse_candidates(qg, query_graphs),
        };
        self.reorder_joins(qg, &reuse_candidates);

        reuse_candidates

    }

    pub fn reorder_joins(&self, qg: &mut QueryGraph, reuse_candidates: &Vec<(ReuseType, &QueryGraph)>) {

        let mut max = 0;
        for &(_, eqg) in reuse_candidates {
            let mut new_join_order = qg.join_order.clone();
            let mut matched = 0;
            for (i, existing_jref) in eqg.join_order.iter().enumerate() {
                if i > qg.join_order.len() || matched < i { break; }

                let ejp = Self::from_join_ref(existing_jref, eqg);
                for j in i..qg.join_order.len() {
                    let njp = Self::from_join_ref(&qg.join_order[j], qg);
                    if predicate_is_equivalent(njp, ejp) {
                        new_join_order.swap(i, j);
                        matched = matched + 1;
                        break;
                    }
                }
            }

            if matched > max {
                max = matched;
                qg.join_order = new_join_order;
            }
        }

        println!("query has {} matching joins", max);
    }

    fn from_join_ref<'a>(jref: &JoinRef, qg: &'a QueryGraph) -> &'a ConditionTree {
        let edge = qg.edges.get(&(jref.src.clone(), jref.dst.clone())).unwrap();
        match *edge {
            QueryGraphEdge::Join(ref jps) => jps.get(jref.index).unwrap(),
            QueryGraphEdge::LeftJoin(ref jps) => jps.get(jref.index).unwrap(),
            QueryGraphEdge::GroupBy(_) => unreachable!(),
        }
    }

    pub fn default() -> ReuseConfig {
        match REUSE_CONFIG {
            ReuseConfigType::Finkelstein => ReuseConfig::finkelstein(),
            ReuseConfigType::Relaxed => ReuseConfig::relaxed(),
            ReuseConfigType::Full => ReuseConfig::full(),
        }
    }

    pub fn full() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Full,
        }
    }

    pub fn finkelstein() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Finkelstein,
        }
    }

    pub fn relaxed() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Relaxed,
        }
    }
}

pub trait ReuseConfiguration {
    fn reuse_candidates<'a>(
        qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>,
    ) -> Vec<(ReuseType, &'a QueryGraph)>;
}
