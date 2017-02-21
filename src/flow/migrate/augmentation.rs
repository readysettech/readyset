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

use slog::Logger;

pub fn inform(log: &Logger,
              graph: &mut Graph,
              source: NodeIndex,
              txs: &mut HashMap<domain::Index, mpsc::SyncSender<Packet>>,
              nodes: HashMap<domain::Index, Vec<(NodeIndex, bool)>>,
              ts: i64) {

    for (domain, nodes) in nodes {
        let log = log.new(o!("domain" => domain.index()));
        let ctx = txs.get_mut(&domain).unwrap();

        let (ready_tx, ready_rx) = mpsc::sync_channel(1);

        trace!(log, "informing domain of migration start");
        let _ = ctx.send(Packet::StartMigration {
            at: ts,
            ack: ready_tx,
        });
        let _ = ready_rx.recv();
        trace!(log, "domain ready for migration");

        let old_nodes: HashSet<_> =
            nodes.iter().filter(|&&(_, new)| !new).map(|&(ni, _)| ni).collect();

        if old_nodes.len() == nodes.len() {
            // some domains haven't changed at all
            continue;
        }

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

            trace!(log, "request addition of node"; "node" => ni.index());
            ctx.send(Packet::AddNode {
                    node: node,
                    parents: old_parents,
                })
                .unwrap();
        }
    }
}
