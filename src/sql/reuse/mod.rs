use nom_sql::Table;
use sql::query_graph::QueryGraph;
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;

mod finkelstein;
mod weak;
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
    Weak,
}

const REUSE_CONFIG: ReuseConfigType = ReuseConfigType::Finkelstein;

pub struct ReuseConfig {
    config: ReuseConfigType,
}

impl ReuseConfig {
    pub fn reuse_candidates<'a>(&self, qg: &QueryGraph, query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>) -> Vec<(ReuseType, &'a QueryGraph)> {
        match self.config {
            ReuseConfigType::Finkelstein => finkelstein::Finkelstein::reuse_candidates(qg, query_graphs),
            ReuseConfigType::Weak => weak::Weak::reuse_candidates(qg, query_graphs),
        }
    }

    pub fn choose_best_option<'a>(&self, options: Vec<(ReuseType, &'a QueryGraph)>) -> (ReuseType, &'a QueryGraph) {
         match self.config {
            ReuseConfigType::Finkelstein => finkelstein::Finkelstein::choose_best_option(options),
            ReuseConfigType::Weak => weak::Weak::choose_best_option(options)
        }
    }

    pub fn default() -> ReuseConfig {
        match REUSE_CONFIG {
            ReuseConfigType::Finkelstein => ReuseConfig::finkelstein(),
            ReuseConfigType::Weak => ReuseConfig::weak(),
        }
    }

    pub fn finkelstein() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Finkelstein,
        }
    }

    pub fn weak() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::Weak,
        }
    }
}

pub trait ReuseConfiguration {
    fn reuse_candidates<'a>(qg: &QueryGraph, query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>) -> Vec<(ReuseType, &'a QueryGraph)>;

    fn choose_best_option<'a>(options: Vec<(ReuseType, &'a QueryGraph)>) -> (ReuseType, &'a QueryGraph);
}
