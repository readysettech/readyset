use nom_sql::Table;
use sql::query_graph::QueryGraph;
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;

mod finkelstein;
mod weak;
mod all;
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
    All,
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
            ReuseConfigType::All => all::All::reuse_candidates(qg, query_graphs),
        }
    }

    pub fn default() -> ReuseConfig {
        match REUSE_CONFIG {
            ReuseConfigType::Finkelstein => ReuseConfig::finkelstein(),
            ReuseConfigType::Weak => ReuseConfig::weak(),
            ReuseConfigType::All => ReuseConfig::all(),
        }
    }

    pub fn all() -> ReuseConfig {
        ReuseConfig {
            config: ReuseConfigType::All,
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
}
