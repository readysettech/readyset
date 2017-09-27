use nom_sql::Table;
use sql::query_graph::QueryGraph;
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;

use sql::reuse::join_order::reorder_joins;

mod finkelstein;
mod relaxed;
mod full;
mod helpers;
mod join_order;

#[derive(Clone, Debug)]
pub enum ReuseType {
    DirectExtension,
    PrefixReuse,
    #[allow(dead_code)]
    BackjoinRequired(Vec<Table>),
}

#[derive(Clone, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum ReuseConfigType {
    Finkelstein,
    Relaxed,
    Full,
    NoReuse,
}

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
            _ => unreachable!(),
        };
        self.reorder_joins(qg, &reuse_candidates);

        reuse_candidates

    }

    pub fn reorder_joins(&self, qg: &mut QueryGraph, reuse_candidates: &Vec<(ReuseType, &QueryGraph)>) {
        reorder_joins(qg, reuse_candidates);
    }

    pub fn new(reuse_type: ReuseConfigType) -> ReuseConfig {
        match reuse_type {
            ReuseConfigType::Finkelstein => ReuseConfig::finkelstein(),
            ReuseConfigType::Relaxed => ReuseConfig::relaxed(),
            ReuseConfigType::Full => ReuseConfig::full(),
            _ => unreachable!(),
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
    fn reuse_candidates<'a>(qg: &QueryGraph, query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>) -> Vec<(ReuseType, &'a QueryGraph)>;
}
