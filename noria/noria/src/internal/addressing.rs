use std::fmt;

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

impl fmt::Display for DomainIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A domain-local node identifier.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct LocalNodeIndex {
    id: u32, // not a tuple struct so this field can be made private
}

impl LocalNodeIndex {
    /// # Safety
    ///
    /// This function is not _memory_ unsafe, but users of this method should show an abundance of
    /// caution so they do not cause hard-to-debug runtime errors. Local node indices **must** be
    /// 0-indexed, contiguous, and distinct within each domain, otherwise bad things will happen.
    pub unsafe fn make(id: u32) -> LocalNodeIndex {
        LocalNodeIndex { id }
    }

    pub fn id(self) -> usize {
        self.id as usize
    }
}

impl fmt::Display for LocalNodeIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "l{}", self.id)
    }
}
