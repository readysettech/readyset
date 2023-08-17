use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::internal::*;
use crate::MaterializationStatus;

type DomainMap = HashMap<ReplicaAddress, Option<(DomainStats, HashMap<NodeIndex, NodeStats>)>>;

/// Statistics about a domain.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStats {
    /// Total wall-clock time elapsed while processing in this domain.
    pub total_time: u64,
    /// Total thread time elapsed while processing in this domain.
    pub total_ptime: u64,
    /// Total wall-clock time spent processing replays in this domain.
    pub total_replay_time: u64,
    /// Total wall-clock time spent processing forward updates in this domain.
    pub total_forward_time: u64,
    /// Total wall-clock time spent waiting for work in this domain.
    pub wait_time: u64,
}

/// Statistics about a node.
///
/// All times are in nanoseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStats {
    /// A textual description of this node.
    pub desc: String,
    /// Total wall-clock time elapsed while processing in this node.
    pub process_time: u64,
    /// Total thread time elapsed while processing in this node.
    pub process_ptime: u64,
    /// Total memory size of this node's state.
    pub mem_size: u64,
    /// The materialization type of this node's state.
    pub materialized: MaterializationStatus,
    /// The value returned from Ingredient::probe.
    pub probe_result: HashMap<String, String>,
}

/// Status that we persist in the Authority to make it available across restarts.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PersistentStats {
    /// Time in millis when the controller last started up.
    pub last_controller_startup: Option<u64>,
    /// Time in millis when the last snapshot was completed.
    pub last_completed_snapshot: Option<u64>,
    /// Time in millis when we last started the main replication loop.
    pub last_started_replication: Option<u64>,
    /// Last error reported by the replicator that caused it to restart. This message is cleared
    /// when we enter the main replication loop, because it is primarily intended to help debug
    /// issues with starting replication and holding onto errors forever can be confusing.
    pub last_replicator_error: Option<String>,
}

/// Statistics about the Soup data-flow.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphStats {
    pub domains: DomainMap,
}

use std::ops::Deref;
impl Deref for GraphStats {
    type Target = DomainMap;
    fn deref(&self) -> &Self::Target {
        &self.domains
    }
}
