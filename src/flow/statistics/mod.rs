
use std::collections::HashMap;

use flow::prelude::*;
use flow::domain;

/// Struct holding statistics about a domain. All times are in nanoseconds.
#[derive(Debug)]
pub struct DomainStats {
    pub total_time: u64,
    pub total_ptime: u64,
    pub wait_time: u64,
}

/// Struct holding statistics about a node. All times are in nanoseconds.
#[derive(Debug)]
pub struct NodeStats {
    pub process_time: u64,
    pub process_ptime: u64,
}

/// Struct holding statistics about an entire graph.
#[derive(Debug)]
pub struct GraphStats {
    pub domains: HashMap<domain::Index, (DomainStats, HashMap<NodeAddress, NodeStats>)>
}
