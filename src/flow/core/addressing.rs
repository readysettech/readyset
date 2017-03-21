use std::fmt;
use petgraph::graph::NodeIndex;

/// A domain-local node identifier.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
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

/// `NodeAddress` is a unique identifier that can be used to refer to nodes in the graph across
/// migrations.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
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
