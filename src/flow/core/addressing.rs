use std::fmt;
use petgraph::graph::NodeIndex;

use serde::{Serialize, Serializer, Deserialize, Deserializer};

/// A domain-local node identifier.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LocalNodeIndex {
    id: usize, // not a tuple struct so this field can be made private
}

impl LocalNodeIndex {
    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
pub enum NodeAddress_ {
    Global(NodeIndex),
    Local(LocalNodeIndex), // XXX: maybe include domain here?
}

/// Variant of NodeAddress_ used for serialization
#[derive(Serialize, Deserialize)]
enum NodeAddressDef {
    Global(usize),
    Local(usize),
}

impl Serialize for NodeAddress_ {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let def = match *self {
            NodeAddress_::Global(i) => NodeAddressDef::Global(i.index()),
            NodeAddress_::Local(i) => NodeAddressDef::Local(i.id()),
        };

        def.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeAddress_ {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        NodeAddressDef::deserialize(deserializer).map(|def|match def {
            NodeAddressDef::Local(idx) => NodeAddress_::Local(LocalNodeIndex{id: idx}),
            NodeAddressDef::Global(idx) => NodeAddress_::Global(NodeIndex::new(idx)),
        })
    }
}

/// `NodeAddress` is a unique identifier that can be used to refer to nodes in the graph across
/// migrations.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct NodeAddress {
    addr: NodeAddress_, // wrap the enum so people can't create these accidentally
}

impl fmt::Debug for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.addr {
            NodeAddress_::Global(ref ni) => write!(f, "NodeAddress::Global({})", ni.index()),
            NodeAddress_::Local(ref li) => write!(f, "NodeAddress::Local({})", li.id()),
        }
    }
}

impl NodeAddress {
    // https://github.com/rust-lang-nursery/rustfmt/issues/1394
    #[cfg_attr(rustfmt, rustfmt_skip)]
    pub(crate) unsafe fn make_local(id: usize) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Local(LocalNodeIndex { id: id }) }
    }


    pub(crate) fn is_global(&self) -> bool {
        match self.addr {
            NodeAddress_::Global(_) => true,
            _ => false,
        }
    }

    #[cfg(test)]
    pub fn mock_local(id: usize) -> NodeAddress {
        unsafe { Self::make_local(id) }
    }

    #[cfg(test)]
    pub fn mock_global(id: NodeIndex) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Global(id) }
    }
}

impl Into<usize> for NodeAddress {
    fn into(self) -> usize {
        match self.addr {
            NodeAddress_::Global(ni) => ni.index(),
            _ => unreachable!(),
        }
    }
}

impl From<NodeIndex> for NodeAddress {
    fn from(o: NodeIndex) -> Self {
        NodeAddress { addr: NodeAddress_::Global(o) }
    }
}

impl From<usize> for NodeAddress {
    fn from(o: usize) -> Self {
        NodeAddress::from(NodeIndex::new(o))
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.addr {
            NodeAddress_::Global(ref ni) => write!(f, "g{}", ni.index()),
            NodeAddress_::Local(ref ni) => write!(f, "l{}", ni.id),
        }
    }
}

impl NodeAddress {
    pub(crate) fn as_global(&self) -> &NodeIndex {
        match self.addr {
            NodeAddress_::Global(ref ni) => ni,
            NodeAddress_::Local(_) => unreachable!("tried to use local address as global"),
        }
    }

    pub(crate) fn as_local(&self) -> &LocalNodeIndex {
        match self.addr {
            NodeAddress_::Local(ref i) => i,
            NodeAddress_::Global(_) => unreachable!("tried to use global address as local"),
        }
    }
}
