use std::collections::HashMap;
use std::vec::Vec;

use readyset::ReadySetError;

use crate::controller::sql::query_graph::QueryGraph;
use crate::controller::sql::reuse::{ReuseConfiguration, ReuseType};

/// Full reuse algorithm
/// Implementation of reuse algorithm that checks all available reuse options.
/// This algorithm yields maximum reuse, since it checks all possible options.
pub struct Full;

impl ReuseConfiguration for Full {
    fn reuse_candidates<'a>(
        _qg: &QueryGraph,
        query_graphs: &'a HashMap<u64, QueryGraph>,
    ) -> Result<Vec<(ReuseType, (u64, &'a QueryGraph))>, ReadySetError> {
        // sort keys to make reuse deterministic
        let mut sorted_keys: Vec<u64> = query_graphs.keys().cloned().collect();
        sorted_keys.sort_unstable();
        Ok(sorted_keys
            .iter()
            .map(|k| (ReuseType::DirectExtension, (*k, &query_graphs[k])))
            .collect())
    }
}
