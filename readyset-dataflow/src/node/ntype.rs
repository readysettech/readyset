use std::fmt;

use serde::{Deserialize, Serialize};

use crate::node::special;
use crate::ops;
use crate::processing::Ingredient;

#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum NodeType {
    Ingress,
    Base(special::Base),
    Internal(ops::NodeOperator),
    Egress(Option<special::Egress>),
    Sharder(special::Sharder),
    Reader(special::Reader),
    /// The root node in the graph. There is a single outgoing edge from Source to all base table
    /// nodes.
    Source,
    Dropped,
}

impl NodeType {
    pub(super) fn take(&mut self) -> Self {
        match self {
            NodeType::Base(b) => NodeType::Base(b.take()),
            NodeType::Egress(e) => NodeType::Egress(e.take()),
            NodeType::Reader(r) => NodeType::Reader(r.take()),
            NodeType::Sharder(s) => NodeType::Sharder(s.take()),
            NodeType::Ingress => NodeType::Ingress,
            NodeType::Internal(i) => NodeType::Internal(i.clone()),
            NodeType::Source => NodeType::Source,
            NodeType::Dropped => NodeType::Dropped,
        }
    }

    /// Produce a compact, human-readable description of this node for Graphviz.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    ⊥    |  Source
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    ///    →|   |  Ingress
    ///    |→   |  Egress
    ///    ÷    |  Sharder
    ///    R    |  Reader
    ///    ☒    |  Dropped
    pub(super) fn description(&self) -> String {
        match self {
            NodeType::Base(_) => "B".to_string(),
            NodeType::Egress(_) => "|→".to_string(),
            NodeType::Reader(_) => "R".to_string(),
            NodeType::Sharder(_) => "÷".to_string(),
            NodeType::Ingress => "→|".to_string(),
            NodeType::Internal(i) => Ingredient::description(i),
            NodeType::Source => "⊥".to_string(),
            NodeType::Dropped => "☒".to_string(),
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::Ingress => write!(f, "Ingress"),
            NodeType::Base(_) => write!(f, "Base"),
            NodeType::Internal(o) => write!(f, "Internal ({o})"),
            NodeType::Egress(_) => write!(f, "Egress"),
            NodeType::Sharder(_) => write!(f, "Sharder"),
            NodeType::Reader(_) => write!(f, "Reader"),
            NodeType::Source => write!(f, "Source"),
            NodeType::Dropped => write!(f, "Dropped"),
        }
    }
}

impl From<ops::NodeOperator> for NodeType {
    fn from(op: ops::NodeOperator) -> Self {
        NodeType::Internal(op)
    }
}

impl From<special::Base> for NodeType {
    fn from(b: special::Base) -> Self {
        NodeType::Base(b)
    }
}

impl From<special::Egress> for NodeType {
    fn from(e: special::Egress) -> Self {
        NodeType::Egress(Some(e))
    }
}

impl From<special::Reader> for NodeType {
    fn from(r: special::Reader) -> Self {
        NodeType::Reader(r)
    }
}

impl From<special::Ingress> for NodeType {
    fn from(_: special::Ingress) -> Self {
        NodeType::Ingress
    }
}

impl From<special::Source> for NodeType {
    fn from(_: special::Source) -> Self {
        NodeType::Source
    }
}

impl From<special::Sharder> for NodeType {
    fn from(s: special::Sharder) -> Self {
        NodeType::Sharder(s)
    }
}
