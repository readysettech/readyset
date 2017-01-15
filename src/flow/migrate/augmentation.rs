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

use petgraph;
use petgraph::graph::NodeIndex;

pub fn inform(graph: &mut Graph,
              source: NodeIndex,
              control_txs: &mut HashMap<domain::Index, mpsc::SyncSender<domain::Control>>,
              nodes: HashMap<domain::Index, Vec<(NodeIndex, bool)>>) {

    for (domain, nodes) in nodes {
        let old_nodes: HashSet<_> =
            nodes.iter().filter(|&&(_, new)| !new).map(|&(ni, _)| ni).collect();

        let ctx = control_txs.get_mut(&domain).unwrap();
        for (ni, new) in nodes {
            if !new {
                continue;
            }

            let node = domain::single::NodeDescriptor::new(graph, ni);
            // new parents already have the right child list
            let old_parents = graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
                .filter(|&ni| ni != source)
                .filter(|ni| old_nodes.contains(ni))
                .map(|ni| &graph[ni])
                .filter(|n| n.domain() == domain)
                .map(|n| *n.addr().as_local())
                .collect();

            ctx.send(domain::Control::AddNode(node, old_parents)).unwrap();

            // TODO: count_base_ingress
        }
    }
}
