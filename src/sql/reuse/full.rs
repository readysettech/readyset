use sql::reuse::{ReuseConfiguration, ReuseType};
use sql::query_graph::QueryGraph;
use mir::MirQuery;

use std::vec::Vec;
use std::collections::HashMap;


/// Full reuse algorithm
/// Implementation of reuse algorithm that checks all available reuse options.
/// This algorithm yields maximum reuse, since it checks all possible options.
pub struct Full;

impl ReuseConfiguration for Full {
    fn reuse_candidates<'a>(
        _qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, (QueryGraph, MirQuery)>,
    ) -> Vec<(ReuseType, &'a QueryGraph)> {
        query_graphs
            .values()
            .map(|c| (ReuseType::DirectExtension, &c.0))
            .collect()
    }
}
