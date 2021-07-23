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
            NodeType::Source => unreachable!(),
            NodeType::Dropped => unreachable!(),
        }
    }
}

impl ToString for NodeType {
    fn to_string(&self) -> String {
        match *self {
            NodeType::Ingress => "Ingress",
            NodeType::Base(_) => "Base",
            NodeType::Internal(_) => "Internal",
            NodeType::Egress(_) => "Egress",
            NodeType::Sharder(_) => "Sharder",
            NodeType::Reader(_) => "Reader",
            NodeType::Source => "Source",
            NodeType::Dropped => "Dropped",
        }
        .to_string()
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
