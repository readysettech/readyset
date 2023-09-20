use std::collections::HashMap;
use std::fmt::{self, Display};
use std::ops::{AddAssign, Deref};

use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::internal::*;

/// [`HashMap`] that has a pair of [`DomainIndex`] and [`usize`] as keys.
/// Useful since it already implements the Serialization/Deserialization traits.
type DomainMap<V> = HashMap<ReplicaAddress, V>;
type WorkersInfo = HashMap<Url, DomainMap<Vec<NodeIndex>>>;

/// Information about the dataflow graph.
#[derive(Debug, Serialize, Deserialize)]
pub struct GraphInfo {
    pub workers: WorkersInfo,
}

impl Deref for GraphInfo {
    type Target = WorkersInfo;
    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}

/// Used to wrap key counts since we use row count estimates as a rough correlate of the key count
/// in the case of RocksDB nodes, and we want to keep track of when we do that so as to avoid any
/// confusion in other parts of the code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyCount {
    /// An exact key count, pulled from a memory node
    ExactKeyCount(usize),
    /// An estimate of the row count, pulled from a persistent storage node
    EstimatedRowCount(usize),
    /// This node does not keep rows, it uses materialization from another node
    ExternalMaterialization,
}

/// Used to wrap the materialized size of a node's state
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMaterializedSize(usize);

/// Use to aggregate various node stats that describe its size
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSize {
    /// The number of keys materialized for a node
    pub key_count: KeyCount,
    /// The approximate size of the materialized state in bytes
    pub bytes: NodeMaterializedSize,
}

impl Display for KeyCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyCount::ExactKeyCount(count) => write!(f, "{}", count),
            KeyCount::EstimatedRowCount(count) => write!(f, "~{}", count),
            KeyCount::ExternalMaterialization => write!(f, ""),
        }
    }
}

impl Display for NodeMaterializedSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size_b = self.0 as f64;
        let size_kb = size_b / 1024.;
        let size_mb = size_kb / 1024.;
        if size_kb < 1. {
            write!(f, "{size_b:.0} B")
        } else if size_mb < 1. {
            write!(f, "{size_kb:.2} KiB")
        } else {
            write!(f, "{size_mb:.2} MiB")
        }
    }
}

impl AddAssign for NodeSize {
    /// Adds the node size for the rhs node size to ourselves.
    fn add_assign(&mut self, rhs: Self) {
        self.key_count += rhs.key_count;
        self.bytes += rhs.bytes;
    }
}

impl AddAssign for KeyCount {
    /// Adds the key count for the rhs KeyCount to ourselves.
    ///
    /// # Panics
    ///
    /// Panics if the caller attempts to add an `ExactKeyCount` to an `EstimatedRowCount` or vice
    /// versa.
    #[track_caller]
    fn add_assign(&mut self, rhs: Self) {
        match (&self, rhs) {
            (KeyCount::ExactKeyCount(self_count), KeyCount::ExactKeyCount(rhs_count)) => {
                *self = KeyCount::ExactKeyCount(*self_count + rhs_count)
            }
            (KeyCount::EstimatedRowCount(self_count), KeyCount::EstimatedRowCount(rhs_count)) => {
                *self = KeyCount::EstimatedRowCount(*self_count + rhs_count)
            }
            _ => panic!(
                "Cannot add mismatched KeyCount types for values {}/{}",
                self, rhs
            ),
        };
    }
}

impl AddAssign for NodeMaterializedSize {
    /// Adds the node size for the rhs node size to ourselves.
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_count_formatting() {
        assert_eq!("42", KeyCount::ExactKeyCount(42).to_string());
        assert_eq!("~42", KeyCount::EstimatedRowCount(42).to_string());
    }

    #[test]
    fn key_count_add_assign() {
        let mut kc = KeyCount::ExactKeyCount(1);
        kc += KeyCount::ExactKeyCount(99);
        assert_eq!(KeyCount::ExactKeyCount(100), kc);
    }

    #[test]
    #[should_panic]
    fn key_count_add_assign_panic() {
        let mut kc = KeyCount::ExactKeyCount(1);
        kc += KeyCount::EstimatedRowCount(1);
    }
}
