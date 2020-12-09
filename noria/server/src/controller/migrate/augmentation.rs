//! Functions for modifying or otherwise interacting with existing domains to complete a migration.
//!
//! In particular:
//!
//!  - New nodes for existing domains must be sent to those domains
//!  - Existing egress nodes that gain new children must gain channels to facilitate forwarding
//!  - State must be replayed for materializations in other domains that need it

use crate::controller;
use dataflow::prelude::*;

use std::collections::{HashMap, HashSet};

use petgraph;
use petgraph::graph::NodeIndex;

use slog::Logger;

pub(super) fn inform(
    log: &Logger,
    controller: &mut controller::ControllerInner,
    nodes: HashMap<DomainIndex, Vec<(NodeIndex, bool)>>,
) {
    let source = controller.source;
    for (domain, nodes) in nodes {
        let log = log.new(o!("domain" => domain.index()));
        let ctx = controller.domains.get_mut(&domain).unwrap();

        trace!(log, "domain ready for migration");

        let old_nodes: HashSet<_> = nodes
            .iter()
            .filter(|&&(_, new)| !new)
            .map(|&(ni, _)| ni)
            .collect();

        assert_ne!(old_nodes.len(), nodes.len());
        for (ni, new) in nodes {
            if !new {
                continue;
            }

            let node = controller.ingredients.node_weight_mut(ni).unwrap().take();
            let node = node.finalize(&controller.ingredients);
            let graph = &controller.ingredients;
            // new parents already have the right child list
            let old_parents = graph
                .neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
                .filter(|&ni| ni != source)
                .filter(|ni| old_nodes.contains(ni))
                .map(|ni| &graph[ni])
                .filter(|n| n.domain() == domain)
                .map(|n| n.local_addr())
                .collect();

            trace!(log, "request addition of node"; "node" => ni.index());
            ctx.send_to_healthy(
                Box::new(Packet::AddNode {
                    node,
                    parents: old_parents,
                }),
                &controller.workers,
            )
            .unwrap();
        }
    }
}
