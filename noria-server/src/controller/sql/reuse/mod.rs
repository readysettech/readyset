use crate::controller::sql::query_graph::QueryGraph;
use nom_sql::Table;

use std::collections::HashMap;
use std::vec::Vec;

use crate::controller::sql::reuse::join_order::reorder_joins;
use crate::controller::sql::UniverseId;

use dataflow::prelude::DataType;

mod finkelstein;
mod full;
mod helpers;
mod join_order;
mod relaxed;

#[derive(Clone, Debug)]
pub enum ReuseType {
    DirectExtension,
    PrefixReuse,
    #[allow(dead_code)]
    BackjoinRequired(Vec<Table>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Vec<(ReuseType, (u64, &'a QueryGraph))> {
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

    pub fn reorder_joins(
        &self,
        qg: &mut QueryGraph,
        reuse_candidates: &[(ReuseType, (u64, &QueryGraph))],
    ) {
        reorder_joins(qg, reuse_candidates);
    }

    // Return which universes are available for reuse opportunities
    pub fn reuse_universes(
        &self,
        universe: UniverseId,
        universes: &HashMap<Option<DataType>, Vec<UniverseId>>,
    ) -> Vec<UniverseId> {
        let global = ("global".into(), None);
        let mut reuse_universes = vec![global, universe.clone()];
        let (_, group) = universe;

        // Find one universe that belongs to the same group
        if let Some(ref uids) = universes.get(&group) {
            let grouped = uids.first().unwrap().clone();
            reuse_universes.push(grouped);
        }

        reuse_universes
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
    fn reuse_candidates<'a>(
        qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Vec<(ReuseType, (u64, &'a QueryGraph))>;
}
