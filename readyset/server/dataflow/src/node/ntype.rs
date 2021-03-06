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
        match *self {
            NodeType::Base(ref mut b) => NodeType::Base(b.take()),
            NodeType::Egress(ref mut e) => NodeType::Egress(e.take()),
            NodeType::Reader(ref mut r) => NodeType::Reader(r.take()),
            NodeType::Sharder(ref mut s) => NodeType::Sharder(s.take()),
            NodeType::Ingress => NodeType::Ingress,
            NodeType::Internal(ref mut i) => NodeType::Internal(i.take()),
            NodeType::Source => NodeType::Source,
            NodeType::Dropped => NodeType::Dropped,
        }
    }

    /// Produce a compact, human-readable description of this node for Graphviz.
    ///
    /// If `detailed` is true, and node type is `Internal`,  emit more info.
    ///  Symbol   Description
    /// --------|-------------
    ///    โฅ    |  Source
    ///    B    |  Base
    ///    ||   |  Concat
    ///    โง    |  Latest
    ///    ฮณ    |  Group by
    ///   |*|   |  Count
    ///    ๐ด    |  Sum
    ///    โ    |  Join
    ///    โ    |  Left join
    ///    โ    |  Union
    ///    โ|   |  Ingress
    ///    |โ   |  Egress
    ///    รท    |  Sharder
    ///    R    |  Reader
    ///    โ    |  Dropped
    pub(super) fn description(&self, detailed: bool) -> String {
        match self {
            NodeType::Base(_) => "B".to_string(),
            NodeType::Egress(_) => "|โ".to_string(),
            NodeType::Reader(_) => "R".to_string(),
            NodeType::Sharder(_) => "รท".to_string(),
            NodeType::Ingress => "โ|".to_string(),
            NodeType::Internal(ref i) => Ingredient::description(i, detailed),
            NodeType::Source => "โฅ".to_string(),
            NodeType::Dropped => "โ".to_string(),
        }
    }
}

impl ToString for NodeType {
    fn to_string(&self) -> String {
        match self {
            NodeType::Ingress => "Ingress".to_string(),
            NodeType::Base(_) => "Base".to_string(),
            NodeType::Internal(o) => format!("Internal ({})", o.to_string()),
            NodeType::Egress(_) => "Egress".to_string(),
            NodeType::Sharder(_) => "Sharder".to_string(),
            NodeType::Reader(_) => "Reader".to_string(),
            NodeType::Source => "Source".to_string(),
            NodeType::Dropped => "Dropped".to_string(),
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
