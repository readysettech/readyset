use clap::Parser;
use dataflow::prelude::{Graph, NodeIndex};
use serde::{Deserialize, Serialize};
use tracing::warn;

// Command-line options for configuring the domain replication strategy
#[allow(missing_docs)] // Allows us to exclude docs (from doc comments) from --help text
#[derive(Debug, Parser, Clone)]
pub struct ReplicationOptions {
    /// Number of times to replicate domains that contain readers.
    ///
    /// This flag should be set to the number of adapter instances in the ReadySet deployment when
    /// running in embbedded readers mode (eg for high availability)
    #[clap(
        long,
        conflicts_with = "non_base_replicas",
        env = "READER_REPLICAS",
        hide = true
    )]
    reader_replicas: Option<usize>,

    /// Number of times to replicate domains that don't contain base nodes
    #[clap(long, hide = true, conflicts_with = "reader_replicas")]
    non_base_replicas: Option<usize>,
}

/// Description for how to decide how many times a domain should be replicated
///
/// This configuration is specified for an entire cluster, and can be built from command-line
/// options by converting from [`ReplicationOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    /// Never replicate domains
    Never,
    /// Replicate domains that contain reader nodes this many times.
    ///
    /// Since we're not currently able to replicate domains containing base nodes, this will only
    /// replicate a domain that contains both a reader and a base node once.
    ReaderDomains(usize),
    /// Replicate domains that don't contain base nodes this many times
    NonBaseDomains(usize),
}

impl Default for ReplicationStrategy {
    fn default() -> Self {
        Self::Never
    }
}

impl From<ReplicationOptions> for ReplicationStrategy {
    fn from(opts: ReplicationOptions) -> Self {
        if let Some(reader_replicas) = opts.reader_replicas {
            Self::ReaderDomains(reader_replicas)
        } else if let Some(non_base_replicas) = opts.non_base_replicas {
            Self::NonBaseDomains(non_base_replicas)
        } else {
            Self::Never
        }
    }
}

impl ReplicationStrategy {
    /// Determine the number of times a domain with the given nodes should be replicated
    ///
    /// # Invariants
    ///
    /// * Each of the nodes in `domain_nodes` must be present in `ingredients`
    #[allow(clippy::indexing_slicing)] // Invariant
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
