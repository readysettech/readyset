use std::collections::HashMap;

use prelude::*;
use domain;

/// Struct holding statistics about a domain. All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStats {
    pub total_time: u64,
    pub total_ptime: u64,
    pub wait_time: u64,
}

/// Struct holding statistics about a node. All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStats {
    pub process_time: u64,
    pub process_ptime: u64,
}

/// Struct holding statistics about an entire graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphStats {
    pub domains: HashMap<(domain::Index, usize), (DomainStats, HashMap<NodeIndex, NodeStats>)>,
}
