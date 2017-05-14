use petgraph;
use petgraph::graph::NodeIndex;

use std::fmt;
use std::collections::HashMap;

use std::ops::{Deref, DerefMut};

use flow::domain;
use flow::keys;
use flow::prelude::*;
use flow::hook;

pub(crate) enum NodeHandle {
    Owned(Type),
    Taken(Type),
}

impl NodeHandle {
    pub fn mark_taken(&mut self) {
        use std::mem;
        match mem::replace(self, NodeHandle::Owned(Type::Source)) {
            NodeHandle::Owned(t) => {
                mem::replace(self, NodeHandle::Taken(t));
            }
            NodeHandle::Taken(_) => {
                unreachable!("tried to take already taken value");
            }
        }
    }
}

impl Deref for NodeHandle {
    type Target = Type;
    fn deref(&self) -> &Self::Target {
        match *self {
            NodeHandle::Owned(ref t) |
            NodeHandle::Taken(ref t) => t,
        }
    }
}

impl DerefMut for NodeHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            NodeHandle::Owned(ref mut t) => t,
            NodeHandle::Taken(_) => unreachable!("cannot mutate taken node"),
        }
    }
}

mod special;
pub use self::special::StreamUpdate;

pub enum Type {
    Ingress,
    Internal(Box<Ingredient>),
    Egress(Option<special::Egress>),
    Reader(special::Reader),
    Hook(Option<hook::Hook>),
    Source,
}

impl Type {
    // Returns a map from base node to the column in that base node whose value must match the value
    // of this node's column to cause a conflict. Returns None for a given base node if any write to
    // that base node might cause a conflict.
    pub fn base_columns(&self,
                        column: usize,
                        graph: &petgraph::Graph<Node, Edge>,
                        index: NodeIndex)
                        -> Vec<(NodeIndex, Option<usize>)> {

        keys::provenance_of(graph, index, column, |_, _| None)
            .into_iter()
            .map(|path| {
                     // we want the base node corresponding to each path
                     path.into_iter().last().unwrap()
                 })
            .collect()
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Type::Source => write!(f, "source node"),
            Type::Ingress => write!(f, "ingress node"),
            Type::Egress { .. } => write!(f, "egress node"),
            Type::Reader(..) => write!(f, "reader node"),
            Type::Internal(ref i) => write!(f, "internal {} node", i.description()),
            Type::Hook(..) => write!(f, "hook node"),
        }
    }
}

impl Deref for Type {
    type Target = Ingredient;
    fn deref(&self) -> &Self::Target {
        match *self {
            Type::Internal(ref i) => i.deref(),
            _ => unreachable!(),
        }
    }
}

impl DerefMut for Type {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Type::Internal(ref mut i) => i.deref_mut(),
            _ => unreachable!(),
        }
    }
}

impl<I> From<I> for Type
    where I: Ingredient + 'static
{
    fn from(i: I) -> Type {
        Type::Internal(Box::new(i))
    }
}

pub struct Node {
    name: String,
    domain: Option<domain::Index>,
    addr: Option<NodeAddress>,
    transactional: bool,

    fields: Vec<String>,
    inner: NodeHandle,
}

impl Node {
    pub fn new<S1, FS, S2>(name: S1, fields: FS, inner: Type, transactional: bool) -> Node
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>
    {
        Node {
            name: name.to_string(),
            domain: None,
            addr: None,
            transactional: transactional,

            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            inner: NodeHandle::Owned(inner),
        }
    }

    pub fn mirror(&self, n: Type) -> Node {
        let mut n = Self::new(&*self.name, &self.fields, n, self.transactional);
        n.domain = self.domain;
        n
    }

    pub(crate) fn inner_mut(&mut self) -> &mut NodeHandle {
        &mut self.inner
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
                unreachable!("asked for unset domain for {:?}", &*self.inner);
            }
        }
    }

    pub fn addr(&self) -> NodeAddress {
        match self.addr {
            Some(addr) => addr,
            None => {
                unreachable!("asked for unset addr for {:?}", &*self.inner);
            }
        }
    }

    pub fn is_localized(&self) -> bool {
        self.addr.is_some() && self.domain.is_some()
    }

    pub fn take(&mut self) -> Node {
        let inner = match *self.inner {
            Type::Egress(ref mut e) => Type::Egress(e.take()),
            Type::Reader(ref mut r) => Type::Reader(r.take()),
            Type::Ingress => Type::Ingress,
            Type::Internal(ref mut i) if self.domain.is_some() => Type::Internal(i.take()),
            Type::Hook(ref mut h) => Type::Hook(h.take()),
            Type::Internal(_) |
            Type::Source => unreachable!(),
        };
        self.inner.mark_taken();

        let mut n = self.mirror(inner);
        n.addr = self.addr;
        n
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        self.domain = Some(domain);
    }

    pub fn set_addr(&mut self, addr: NodeAddress) {
        self.addr = Some(addr);
    }

    pub fn on_commit(&mut self, remap: &HashMap<NodeAddress, NodeAddress>) {
        assert!(self.addr.is_some());
        self.inner.on_commit(self.addr.unwrap(), remap)
    }

    pub fn describe(&self, f: &mut fmt::Write, idx: NodeIndex) -> fmt::Result {
        use regex::Regex;

        let escape = |s: &str| {
            Regex::new("([\"|{}])")
                .unwrap()
                .replace_all(s, "\\$1")
                .to_string()
        };
        write!(f,
               " [style=filled, fillcolor={}, label=\"",
               self.domain
                   .map(|d| -> usize { d.into() })
                   .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                   .unwrap_or("white".into()))?;

        match *self.inner {
            Type::Source => write!(f, "(source)"),
            Type::Ingress => write!(f, "{{ {} | (ingress) }}", idx.index()),
            Type::Egress { .. } => write!(f, "{{ {} | (egress) }}", idx.index()),
            Type::Hook(..) => write!(f, "{{ {} | (hook) }}", idx.index()),
            Type::Reader(ref r) => {
                let key = match r.key() {
                    None => String::from("none"),
                    Some(k) => format!("{}", k),
                };
                use flow::VIEW_READERS;
                let size = match VIEW_READERS
                          .lock()
                          .unwrap()
                          .get(&idx)
                          .map(|state| state.len()) {
                    None => String::from("empty"),
                    Some(s) => format!("{} distinct keys", s),
                };
                write!(f,
                       "{{ {} | (reader / key: {}) | {} }}",
                       idx.index(),
                       key,
                       size)
            }
            Type::Internal(ref i) => {
                write!(f, "{{")?;

                // Output node name and description. First row.
                write!(f,
                       "{{ {} / {} | {} }}",
                       idx.index(),
                       escape(self.name()),
                       escape(&i.description()))?;

                // Output node outputs. Second row.
                write!(f, " | {}", self.fields().join(", \\n"))?;

                // Maybe output node's HAVING conditions. Optional third row.
                // TODO
                // if let Some(conds) = n.node().unwrap().having_conditions() {
                //     let conds = conds.iter()
                //         .map(|c| format!("{}", c))
                //         .collect::<Vec<_>>()
                //         .join(" ∧ ");
                //     write!(f, " | σ({})", escape(&conds))?;
                // }

                write!(f, " }}")
            }
        }?;

        writeln!(f, "\"]")
    }

    pub fn is_egress(&self) -> bool {
        if let Type::Egress { .. } = *self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_reader(&self) -> bool {
        if let Type::Reader { .. } = *self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_ingress(&self) -> bool {
        if let Type::Ingress = *self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_internal(&self) -> bool {
        if let Type::Internal(..) = *self.inner {
            true
        } else {
            false
        }
    }

    /// A node is considered to be an output node if changes to its state are visible outside of
    /// its domain.
    pub fn is_output(&self) -> bool {
        match *self.inner {
            Type::Egress { .. } |
            Type::Reader(..) |
            Type::Hook(..) => true,
            _ => false,
        }
    }

    pub fn is_transactional(&self) -> bool {
        self.transactional
    }
}

impl Deref for Node {
    type Target = Type;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
