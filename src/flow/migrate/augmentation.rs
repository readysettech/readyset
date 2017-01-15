//! Functions for modifying or otherwise interacting with existing domains to complete a migration.
//!
//! In particular:
//!
//!  - New nodes for existing domains must be sent to those domains
//!  - Existing egress nodes that gain new children must gain channels to facilitate forwarding
//!  - State must be replayed for materializations in other domains that need it

use flow::prelude::*;
use flow::domain;

use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use petgraph::graph::NodeIndex;

#[allow(unused_variables)]
pub fn inform(graph: &mut Graph,
              source: NodeIndex,
              control_txs: &mut HashMap<domain::Index, mpsc::SyncSender<domain::Control>>,
              new: &HashSet<NodeIndex>) {
}
