use std::fmt;
use LocalNodeIndex;

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub src: LocalNodeIndex,
    pub dst: LocalNodeIndex,
}

impl Link {
    pub fn new(src: LocalNodeIndex, dst: LocalNodeIndex) -> Self {
        Link { src: src, dst: dst }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
    }
}

/// Describe the materialization state of an operator.
#[derive(Debug, Serialize, Deserialize)]
pub enum MaterializationStatus {
    /// Operator's state is not materialized.
    Not,
    /// Operator's state is fully materialized.
    Full,
    /// Operator's state is partially materialized.
    Partial,
}
