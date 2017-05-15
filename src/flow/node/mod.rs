use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use flow::domain;
use flow::prelude::*;
use petgraph;
use ops;

mod process;
pub use self::process::materialize;

pub mod special;
pub use self::special::StreamUpdate;

mod ntype;
pub use self::ntype::NodeType;

mod debug;

pub struct Node {
    name: String,
    index: Option<NodeAddress>,
    domain: Option<domain::Index>,
    addr: Option<NodeAddress>,
    transactional: bool,

    fields: Vec<String>,
    children: Vec<NodeAddress>,
    inner: NodeType,
    taken: bool,
}

// constructors
impl Node {
    pub fn new<S1, FS, S2, NT>(name: S1, fields: FS, inner: NT, transactional: bool) -> Node
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>,
              NT: Into<NodeType>
    {
        Node {
            name: name.to_string(),
            index: None,
            domain: None,
            addr: None,
            transactional: transactional,

            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            children: Vec::new(),
            inner: inner.into(),
            taken: false,
        }
    }

    pub fn mirror<NT: Into<NodeType>>(&self, n: NT) -> Node {
        let mut n = Self::new(&*self.name, &self.fields, n, self.transactional);
        n.domain = self.domain;
        n
    }
}

#[must_use]
pub struct DanglingDomainNode(Node);

impl DanglingDomainNode {
    pub fn finalize(self, graph: &Graph) -> Node {
        let mut n = self.0;
        let ni = *n.global_addr().as_global();
        let dm = n.domain();
        n.children = graph
            .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
            .filter(|&c| graph[c].domain() == dm)
            .map(|ni| *graph[ni].local_addr())
            .collect();
        n
    }
}

// events
impl Node {
    pub fn take(&mut self) -> DanglingDomainNode {
        assert!(!self.taken);
        assert!(!self.is_internal() || self.domain.is_some(),
                "tried to take unassigned node");

        let inner = self.inner.take();
        let mut n = self.mirror(inner);
        n.addr = self.addr;
        self.taken = true;

        DanglingDomainNode(n)
    }

    pub fn on_commit(&mut self, remap: &HashMap<NodeAddress, NodeAddress>) {
        // this is *only* overwritten for these asserts.
        assert!(self.addr.is_some());
        assert!(!self.taken);
        assert!(self.is_internal());
        if let NodeType::Internal(ref mut i) = self.inner {
            i.on_commit(self.addr.unwrap(), remap)
        }
    }
}

// derefs
impl Node {
    pub fn with_egress_mut<F>(&mut self, f: F)
        where F: FnOnce(&mut special::Egress)
    {
        match self.inner {
            NodeType::Egress(Some(ref mut e)) => f(e),
            _ => unreachable!(),
        }
    }

    pub fn with_reader_mut<F>(&mut self, f: F)
        where F: FnOnce(&mut special::Reader)
    {
        match self.inner {
            NodeType::Reader(ref mut r) => f(r),
            _ => unreachable!(),
        }
    }

    pub fn with_reader<'a, F, R>(&'a self, f: F) -> Option<R>
        where F: FnOnce(&'a special::Reader) -> R,
              R: 'a
    {
        match self.inner {
            NodeType::Reader(ref r) => Some(f(r)),
            _ => None,
        }
    }

    /// For mutating even though you *know* it's been taken
    pub(crate) fn inner_mut(&mut self) -> &mut ops::NodeOperator {
        assert!(self.taken);
        match self.inner {
            NodeType::Internal(ref mut i) => i,
            _ => unreachable!(),
        }
    }
}

impl Deref for Node {
    type Target = ops::NodeOperator;
    fn deref(&self) -> &Self::Target {
        match self.inner {
            NodeType::Internal(ref i) => i,
            _ => unreachable!(),
        }
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        assert!(!self.taken);
        match self.inner {
            NodeType::Internal(ref mut i) => i,
            _ => unreachable!(),
        }
    }
}

// children
impl Node {
    pub fn children(&self) -> &[NodeAddress] {
        &self.children[..]
    }

    pub fn child(&self, i: usize) -> &NodeAddress {
        &self.children[i]
    }

    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    pub fn nchildren(&self) -> usize {
        self.children.len()
    }
}

// attributes
impl Node {
    pub fn add_child(&mut self, child: NodeAddress) {
        assert!(child.is_local());
        self.children.push(child);
    }

    pub fn add_column(&mut self, field: &str) -> usize {
        self.fields.push(field.to_string());
        self.fields.len() - 1
    }

    pub fn name(&self) -> &str {
        &*self.name
    }

    pub fn fields(&self) -> &[String] {
        &self.fields[..]
    }

    pub fn domain(&self) -> domain::Index {
        match self.domain {
            Some(domain) => domain,
            None => {
                unreachable!("asked for unset domain for {:?}", self);
            }
        }
    }

    pub fn local_addr(&self) -> &NodeAddress {
        match self.addr {
            Some(ref addr) => addr,
            None => {
                unreachable!("asked for unset addr for {:?}", self);
            }
        }
    }

    pub fn global_addr(&self) -> &NodeAddress {
        match self.index {
            Some(ref index) => index,
            None => {
                unreachable!("asked for unset index for {:?}", self);
            }
        }
    }

    pub fn is_localized(&self) -> bool {
        self.addr.is_some() && self.domain.is_some()
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        self.domain = Some(domain);
    }

    pub fn set_local_addr(&mut self, addr: NodeAddress) {
        addr.as_local();
        self.addr = Some(addr);
    }

    pub fn set_global_addr(&mut self, addr: NodeAddress) {
        addr.as_global();
        self.index = Some(addr);
    }
}

// is this or that?
impl Node {
    pub fn is_source(&self) -> bool {
        if let NodeType::Source { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_egress(&self) -> bool {
        if let NodeType::Egress { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_reader(&self) -> bool {
        if let NodeType::Reader { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_ingress(&self) -> bool {
        if let NodeType::Ingress = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_internal(&self) -> bool {
        if let NodeType::Internal(..) = self.inner {
            true
        } else {
            false
        }
    }

    /// A node is considered to be an output node if changes to its state are visible outside of
    /// its domain.
    pub fn is_output(&self) -> bool {
        match self.inner {
            NodeType::Egress { .. } |
            NodeType::Reader(..) |
            NodeType::Hook(..) => true,
            _ => false,
        }
    }

    pub fn is_transactional(&self) -> bool {
        self.transactional
    }
}
