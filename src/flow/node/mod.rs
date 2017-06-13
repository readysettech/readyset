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

#[derive(Clone)]
pub struct Node {
    name: String,
    index: Option<IndexPair>,
    domain: Option<domain::Index>,
    transactional: bool,

    fields: Vec<String>,
    children: Vec<LocalNodeIndex>,
    inner: NodeType,
    taken: bool,

    sharded_by: Sharding,
    is_shard_merge: bool,
}

// constructors
impl Node {
    pub fn new<S1, FS, S2, NT>(name: S1, fields: FS, inner: NT, transactional: bool) -> Node
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        NT: Into<NodeType>,
    {
        Node {
            name: name.to_string(),
            index: None,
            domain: None,
            transactional: transactional,

            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            children: Vec::new(),
            inner: inner.into(),
            taken: false,

            sharded_by: Sharding::None,
            is_shard_merge: false,
        }
    }

    pub fn mirror<NT: Into<NodeType>>(&self, n: NT) -> Node {
        Self::new(&*self.name, &self.fields, n, self.transactional)
    }
}

#[must_use]
pub struct DanglingDomainNode(Node);

impl DanglingDomainNode {
    pub fn finalize(self, graph: &Graph) -> Node {
        let mut n = self.0;
        let ni = n.global_addr();
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
        assert!(
            !self.is_internal() || self.domain.is_some(),
            "tried to take unassigned node"
        );

        let inner = self.inner.take();
        let mut n = self.mirror(inner);
        n.index = self.index;
        n.domain = self.domain;
        self.taken = true;

        DanglingDomainNode(n)
    }

    pub fn remove(&mut self) {
        self.inner = NodeType::Dropped;
    }

    /// Set this node's sharding property.
    pub fn shard_by(&mut self, s: Sharding) {
        self.sharded_by = s;
    }

    pub fn on_commit(&mut self, remap: &HashMap<NodeIndex, IndexPair>) {
        // this is *only* overwritten for these asserts.
        assert!(!self.taken);
        if let NodeType::Internal(ref mut i) = self.inner {
            i.on_commit(self.index.unwrap().as_global(), remap)
        }
    }
}

// derefs
impl Node {
    pub fn with_sharder_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut special::Sharder),
    {
        match self.inner {
            NodeType::Sharder(ref mut s) => f(s),
            _ => unreachable!(),
        }
    }

    pub fn with_sharder<'a, F, R>(&'a self, f: F) -> Option<R>
    where
        F: FnOnce(&'a special::Sharder) -> R,
        R: 'a,
    {
        match self.inner {
            NodeType::Sharder(ref s) => Some(f(s)),
            _ => None,
        }
    }

    pub fn with_egress_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut special::Egress),
    {
        match self.inner {
            NodeType::Egress(Some(ref mut e)) => f(e),
            _ => unreachable!(),
        }
    }

    pub fn with_reader_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut special::Reader),
    {
        match self.inner {
            NodeType::Reader(ref mut r) => f(r),
            _ => unreachable!(),
        }
    }

    pub fn with_reader<'a, F, R>(&'a self, f: F) -> Option<R>
    where
        F: FnOnce(&'a special::Reader) -> R,
        R: 'a,
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
    pub fn children(&self) -> &[LocalNodeIndex] {
        &self.children[..]
    }

    pub fn child(&self, i: usize) -> &LocalNodeIndex {
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
    pub fn sharded_by(&self) -> Sharding {
        self.sharded_by
    }

    pub fn add_child(&mut self, child: LocalNodeIndex) {
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

    pub fn has_domain(&self) -> bool {
        self.domain.is_some()
    }

    pub fn domain(&self) -> domain::Index {
        match self.domain {
            Some(domain) => domain,
            None => {
                unreachable!("asked for unset domain for {:?}", self);
            }
        }
    }

    pub fn local_addr(&self) -> &LocalNodeIndex {
        match self.index {
            Some(ref idx) if idx.has_local() => &*idx,
            Some(_) | None => unreachable!("asked for unset addr for {:?}", self),
        }
    }

    pub fn global_addr(&self) -> NodeIndex {
        match self.index {
            Some(ref index) => index.as_global(),
            None => {
                unreachable!("asked for unset index for {:?}", self);
            }
        }
    }

    pub fn get_index(&self) -> &IndexPair {
        self.index.as_ref().unwrap()
    }

    pub fn is_localized(&self) -> bool {
        self.index.as_ref().map(|idx| idx.has_local()).unwrap_or(
            false,
        ) && self.domain.is_some()
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        assert_eq!(self.domain, None);
        assert!(!self.is_dropped());
        self.domain = Some(domain);
    }

    pub fn set_finalized_addr(&mut self, addr: IndexPair) {
        self.index = Some(addr);
    }

    pub fn mark_as_shard_merger(&mut self, is: bool) {
        self.is_shard_merge = is;
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

    pub fn is_dropped(&self) -> bool {
        if let NodeType::Dropped = self.inner {
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

    pub fn is_sharder(&self) -> bool {
        if let NodeType::Sharder { .. } = self.inner {
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

    pub fn is_sender(&self) -> bool {
        match self.inner {
            NodeType::Egress { .. } |
            NodeType::Sharder(..) => true,
            _ => false,
        }
    }

    pub fn is_shard_merger(&self) -> bool {
        self.is_shard_merge
    }

    /// A node is considered to be an output node if changes to its state are visible outside of
    /// its domain.
    pub fn is_output(&self) -> bool {
        match self.inner {
            NodeType::Egress { .. } |
            NodeType::Reader(..) |
            NodeType::Sharder(..) |
            NodeType::Hook(..) => true,
            _ => false,
        }
    }

    pub fn is_transactional(&self) -> bool {
        self.transactional
    }
}
