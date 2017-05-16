use flow::prelude::*;
use flow::domain;
use petgraph::graph::NodeIndex;
use std::collections::{HashSet, HashMap};
use slog::Logger;

pub fn shard(log: &Logger,
             graph: &mut Graph,
             source: NodeIndex,
             new: &mut HashSet<NodeIndex>)
             -> HashMap<domain::Index, HashMap<NodeIndex, NodeIndex>> {
    HashMap::default()
}
