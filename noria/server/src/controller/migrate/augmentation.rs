//! Functions for modifying or otherwise interacting with existing domains to complete a migration.
//!
//! In particular:
//!
//!  - New nodes for existing domains must be sent to those domains
//!  - Existing egress nodes that gain new children must gain channels to facilitate forwarding
//!  - State must be replayed for materializations in other domains that need it

use dataflow::prelude::*;
use tracing::{debug_span, trace};

use std::collections::{HashMap, HashSet};

use petgraph::graph::NodeIndex;

use crate::controller::migrate::DomainMigrationPlan;
use dataflow::DomainRequest;

pub(super) fn inform(
    source: NodeIndex,
    ingredients: &mut Graph,
    dmp: &mut DomainMigrationPlan,
    nodes: HashMap<DomainIndex, Vec<(NodeIndex, bool)>>,
) -> ReadySetResult<()> {
    for (domain, nodes) in nodes {
        let span = debug_span!("informing domain", domain = domain.index());
        let _g = span.enter();

        let old_nodes: HashSet<_> = nodes
            .iter()
            .filter(|&&(_, new)| !new)
            .map(|&(ni, _)| ni)
            .collect();

        invariant!(old_nodes.len() != nodes.len());
        for (ni, new) in nodes {
            if !new {
                continue;
            }

            let node = ingredients.node_weight_mut(ni).unwrap().take();
            let node = node.finalize(ingredients);
            // new parents already have the right child list
            let old_parents = ingredients
                .neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
                .filter(|&ni| ni != source)
                .filter(|ni| old_nodes.contains(ni))
                .map(|ni| &ingredients[ni])
                .filter(|n| n.domain() == domain)
                .map(|n| n.local_addr())
                .collect();

            trace!(node = ni.index(), "request addition of node");
            dmp.add_message(
                domain,
                DomainRequest::AddNode {
                    node,
                    parents: old_parents,
                },
            )?;
        }
    }
    Ok(())
}
