use dataflow::prelude::{Graph, NodeIndex};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Description for how to decide how many times a domain should be replicated
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    /// Never replicate domains
    #[default]
    Never,
    /// Replicate domains that contain reader nodes this many times.
    ///
    /// Since we're not currently able to replicate domains containing base nodes, this will only
    /// replicate a domain that contains both a reader and a base node once.
    ReaderDomains(usize),
    /// Replicate domains that don't contain base nodes this many times
    NonBaseDomains(usize),
}

impl ReplicationStrategy {
    /// Determine the number of times a domain with the given nodes should be replicated
    ///
    /// # Invariants
    ///
    /// * Each of the nodes in `domain_nodes` must be present in `ingredients`
    pub fn replicate_domain(&self, ingredients: &Graph, domain_nodes: &[NodeIndex]) -> usize {
        let has_reader = || domain_nodes.iter().any(|n| ingredients[*n].is_reader());
        let has_base = || domain_nodes.iter().any(|n| ingredients[*n].is_base());

        match *self {
            ReplicationStrategy::Never => 1,
            ReplicationStrategy::ReaderDomains(num_reader_replicas) => {
                if has_reader() {
                    if has_base() {
                        // This currently never happens, but doesn't hurt to stop it from causing us
                        // problems anyway
                        warn!("Found domain with both reader and base, not replicating");
                        1
                    } else {
                        num_reader_replicas
                    }
                } else {
                    1
                }
            }
            ReplicationStrategy::NonBaseDomains(num_non_base_replicas) => {
                if has_base() {
                    1
                } else {
                    num_non_base_replicas
                }
            }
        }
    }
}
