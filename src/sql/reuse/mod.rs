use nom_sql::Table;
use sql::query_graph::{QueryGraph, QueryGraphEdge};
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
    #[allow(dead_code)] BackjoinRequired(Vec<Table>),
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
    pub fn reuse_candidates<'a>(
        &self,
        qg: &mut QueryGraph,
        query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>,
    ) -> Vec<(ReuseType, &'a QueryGraph)> {
        self.make_join_order(qg);
        match self.config {
            ReuseConfigType::Finkelstein => {
                finkelstein::Finkelstein::reuse_candidates(qg, query_graphs)
            }
            ReuseConfigType::Relaxed => relaxed::Relaxed::reuse_candidates(qg, query_graphs),
            ReuseConfigType::Full => full::Full::reuse_candidates(qg, query_graphs),
        }

    }

    pub fn make_join_order(&self, qg: &mut QueryGraph) {
        let mut join_order: Vec<(String, String, usize)> = Vec::new();
        for (&(ref src, ref dst), edge) in qg.edges.iter() {
            match *edge {
                QueryGraphEdge::Join(ref jps) => join_order.extend(jps.iter().enumerate().map(|(idx, _)| (src.clone(), dst.clone(), idx)).collect::<Vec<_>>()),
                QueryGraphEdge::LeftJoin(ref jps) => join_order.extend(jps.iter().enumerate().map(|(idx, _)| (src.clone(), dst.clone(), idx)).collect::<Vec<_>>()),
                QueryGraphEdge::GroupBy(_) => continue
            }
        }

        qg.join_order = join_order;
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
