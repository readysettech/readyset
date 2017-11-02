use ops;
use flow::node::special;
use flow::processing::Ingredient;

#[derive(Clone, Serialize, Deserialize)]
pub enum NodeType {
    Ingress,
    Internal(ops::NodeOperator),
    Egress(Option<special::Egress>),
    Sharder(special::Sharder),
    Reader(special::Reader),
    Source,
    Dropped,
}

impl NodeType {
    pub fn take(&mut self) -> Self {
        match *self {
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

impl From<ops::NodeOperator> for NodeType {
    fn from(op: ops::NodeOperator) -> Self {
        NodeType::Internal(op)
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
