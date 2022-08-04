use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct DomainIndex(usize);

impl From<usize> for DomainIndex {
    fn from(i: usize) -> Self {
        DomainIndex(i)
    }
}

impl From<DomainIndex> for usize {
    fn from(val: DomainIndex) -> usize {
        val.0
    }
}

impl DomainIndex {
    pub fn index(self) -> usize {
        self.0
    }
}

impl Display for DomainIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ReplicaAddress {
    pub domain_index: DomainIndex,
    pub shard: usize,
    pub replica: usize,
}

impl Display for ReplicaAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.domain_index, self.shard, self.replica)
    }
}

/// A domain-local node identifier.
///
/// After migration is complete, every node in the dataflow graph gets two IDs, one
/// ([`petgraph::graph::NodeIndex`]) which is globally unique across the entire dataflow graph, and
/// one (this type) that's local to a particular domain. Having these local indices allows us to
/// efficiently store node-specific information within a domain in efficient sparse vectors called
/// [`NodeMap`]s.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct LocalNodeIndex {
    id: u32, // not a tuple struct so this field can be made private
}

impl LocalNodeIndex {
    /// Create a new [`LocalNodeIndex`] from a `u32` node ID.
    ///
    /// Users of this method should show an abundance of caution so they do not cause hard-to-debug
    /// runtime errors. Local node indices **must** be 0-indexed, contiguous, and distinct within
    /// each domain, otherwise bad things will happen.
    pub fn make(id: u32) -> LocalNodeIndex {
        LocalNodeIndex { id }
    }

    pub fn id(self) -> usize {
        self.id as usize
    }
}

impl Display for LocalNodeIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "l{}", self.id)
    }
}
