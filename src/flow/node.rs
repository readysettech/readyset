use petgraph::graph::NodeIndex;

use std::sync::mpsc;
use std::sync;
use std::fmt;

use std::ops::{Deref, DerefMut};

use checktable;

use ops::Records;
use flow::domain;
use flow::{Message, Ingredient, NodeAddress};

use backlog;

#[derive(Clone)]
pub struct Reader {
    pub streamers: sync::Arc<sync::Mutex<Vec<mpsc::Sender<Records>>>>,
    pub state: Option<backlog::BufferedStore>,
    pub token_generator: checktable::TokenGenerator,
}

impl Reader {
    pub fn new(token_generator: checktable::TokenGenerator) -> Self {
        Reader {
            streamers: sync::Arc::default(),
            state: None,
            token_generator: token_generator,
        }
    }
}

enum NodeHandle {
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

pub enum Type {
    Ingress(domain::Index),
    Internal(domain::Index, Box<Ingredient>),
    Egress(domain::Index, sync::Arc<sync::Mutex<Vec<(NodeAddress, mpsc::SyncSender<Message>)>>>),
    TimestampIngress(domain::Index, sync::Arc<sync::Mutex<mpsc::SyncSender<i64>>>),
    TimestampEgress(domain::Index, sync::Arc<sync::Mutex<Vec<mpsc::SyncSender<i64>>>>),
    Reader(Option<domain::Index>, Option<backlog::WriteHandle>, Reader), /* domain only known at commit time! */
    Unassigned(Box<Ingredient>),
    Source,
}

impl Type {
    fn domain(&self) -> Option<domain::Index> {
        match *self {
            Type::Ingress(d) |
            Type::Internal(d, _) |
            Type::TimestampIngress(d, _) |
            Type::TimestampEgress(d, _) |
            Type::Egress(d, _) => Some(d),
            Type::Reader(d, _, _) => d,
            Type::Unassigned(_) |
            Type::Source => None,
        }
    }

    fn assign(&mut self, domain: domain::Index) {
        use std::mem;
        match mem::replace(self, Type::Source) {
            Type::Unassigned(inner) => {
                mem::replace(self, Type::Internal(domain, inner));
            }
            _ => unreachable!(),
        }
    }
}

impl Deref for Type {
    type Target = Ingredient;
    fn deref(&self) -> &Self::Target {
        match *self {
            Type::Internal(_, ref i) |
            Type::Unassigned(ref i) => i.deref(),
            _ => unreachable!(),
        }
    }
}

impl DerefMut for Type {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Type::Internal(_, ref mut i) |
            Type::Unassigned(ref mut i) => i.deref_mut(),
            _ => unreachable!(),
        }
    }
}

impl<I> From<I> for Type
    where I: Ingredient + 'static
{
    fn from(i: I) -> Type {
        Type::Unassigned(Box::new(i))
    }
}

pub struct Node {
    inner: NodeHandle,
    name: String,
    fields: Vec<String>,
}

impl Node {
    pub fn new<S1, FS, S2>(name: S1, fields: FS, inner: Type) -> Node
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>
    {
        Node {
            name: name.to_string(),
            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            inner: NodeHandle::Owned(inner),
        }
    }

    pub fn mirror(&self, n: Type) -> Node {
        Self::new(&*self.name, &self.fields, n)
    }

    pub fn name(&self) -> &str {
        &*self.name
    }

    pub fn fields(&self) -> &[String] {
        &self.fields[..]
    }

    pub fn domain(&self) -> Option<domain::Index> {
        self.inner.domain()
    }

    pub fn take(&mut self) -> Node {
        let inner = match *self.inner {
            Type::Egress(d, ref txs) => {
                // egress nodes can still be modified externally if subgraphs are added
                // so we just make a new one with a clone of the Mutex-protected Vec
                Type::Egress(d, txs.clone())
            }
            Type::Reader(d, ref mut w, ref r) => {
                // reader nodes can still be modified externally if txs are added
                Type::Reader(d, w.take(), r.clone())
            }
            Type::TimestampEgress(d, ref arc) => Type::TimestampEgress(d, arc.clone()),
            Type::TimestampIngress(d, ref arc) => Type::TimestampIngress(d, arc.clone()),
            Type::Ingress(d) => Type::Ingress(d),
            Type::Internal(d, ref mut i) => Type::Internal(d, i.take()),
            Type::Unassigned(_) |
            Type::Source => unreachable!(),
        };
        self.inner.mark_taken();

        self.mirror(inner)
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        self.inner.assign(domain);
    }

    pub fn describe(&self, f: &mut fmt::Formatter, idx: NodeIndex) -> fmt::Result {
        use regex::Regex;

        let escape = |s: &str| Regex::new("([\"|{}])").unwrap().replace_all(s, "\\$1");
        write!(f,
               " [style=filled, fillcolor={}, label=\"",
               self.domain()
                   .map(|d| -> usize { d.into() })
                   .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                   .unwrap_or("white".into()))?;

        match *self.inner {
            Type::Source => write!(f, "(source)"),
            Type::Ingress(..) => write!(f, "{{ {} | (ingress) }}", idx.index()),
            Type::Egress(..) => write!(f, "{{ {} | (egress) }}", idx.index()),
            Type::TimestampIngress(..) => write!(f, "{{ {} | (timestamp-ingress) }}", idx.index()),
            Type::TimestampEgress(..) => write!(f, "{{ {} | (timestamp-egress) }}", idx.index()),
            Type::Reader(..) => write!(f, "{{ {} | (reader) }}", idx.index()),
            Type::Unassigned(ref i) |
            Type::Internal(_, ref i) => {
                write!(f, "{{")?;

                // Output node name and description. First row.
                write!(f,
                       "{{ {} / {} | {} }}",
                       idx.index(),
                       escape(self.name()),
                       escape(&i.description()))?;

                // Output node outputs. Second row.
                write!(f, " | {}", self.fields().join(", "))?;

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
