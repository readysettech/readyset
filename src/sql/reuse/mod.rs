use nom_sql::Table;
use sql::query_graph::QueryGraph;
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;

mod finkelstein;
mod relaxed;
mod full;
mod helpers;

#[derive(Clone, Debug)]
pub enum ReuseType {
    DirectExtension,
    PrefixReuse,
    #[allow(dead_code)]
    BackjoinRequired(Vec<Table>),
}

enum ReuseConfigType {
    Finkelstein,
    Relaxed,
    Full,
}

// TODO(larat): make this a dynamic option
const REUSE_CONFIG: ReuseConfigType = ReuseConfigType::Finkelstein;

pub struct ReuseConfig {
    config: ReuseConfigType,
}

impl ReuseConfig {
    pub fn reuse_candidates<'a>(&self, qg: &QueryGraph, query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>) -> Vec<(ReuseType, &'a QueryGraph)> {
        match self.config {
            ReuseConfigType::Finkelstein => finkelstein::Finkelstein::reuse_candidates(qg, query_graphs),
            ReuseConfigType::Relaxed => relaxed::Relaxed::reuse_candidates(qg, query_graphs),
            ReuseConfigType::Full => full::Full::reuse_candidates(qg, query_graphs),
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
    fn reuse_candidates<'a>(qg: &QueryGraph, query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>) -> Vec<(ReuseType, &'a QueryGraph)>;
}
