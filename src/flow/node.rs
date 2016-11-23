use petgraph::graph::NodeIndex;

use std::sync::mpsc;
use std::sync;
use std::fmt;

use std::ops::{Deref, DerefMut};

use flow::domain;
use flow::{Message, Ingredient};

pub enum Type {
    Ingress(domain::Index, mpsc::Receiver<Message>),
    Internal(domain::Index, Box<Ingredient>),
    Egress(domain::Index, sync::Arc<sync::Mutex<Vec<mpsc::Sender<Message>>>>),
    Unassigned(Box<Ingredient>),
    Taken(domain::Index),
    Source,
}

impl Type {
    fn domain(&self) -> Option<domain::Index> {
        match *self {
            Type::Taken(d) |
            Type::Ingress(d, _) |
            Type::Internal(d, _) |
            Type::Egress(d, _) => Some(d),
            _ => None,
        }
    }
}

impl Deref for Type {
    type Target = Ingredient;
    fn deref(&self) -> &Self::Target {
        match self {
            &Type::Internal(_, ref i) |
            &Type::Unassigned(ref i) => i.deref(),
            _ => unreachable!(),
        }
    }
}

impl DerefMut for Type {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            &mut Type::Internal(_, ref mut i) |
            &mut Type::Unassigned(ref mut i) => i.deref_mut(),
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
    inner: Type,
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
            inner: inner,
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
        use std::mem;
        let domain = self.domain();
        let inner = match self.inner {
            Type::Egress(d, ref txs) => {
                // egress nodes can still be modified externally if subgraphs are added
                // so we just make a new one with a clone of the Mutex-protected Vec
                Type::Egress(d, txs.clone())
            }
            ref mut n @ Type::Ingress(..) => {
                // no-one else will be using our ingress node, so we take it from the graph
                mem::replace(n, Type::Taken(domain.unwrap()))
            }
            ref mut n @ Type::Internal(..) => {
                // same with internal nodes
                mem::replace(n, Type::Taken(domain.unwrap()))
            }
            _ => unreachable!(),
        };

        self.mirror(inner)
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        use std::mem;
        match mem::replace(&mut self.inner, Type::Taken(domain)) {
            Type::Unassigned(inner) => {
                self.inner = Type::Internal(domain, inner);
            }
            _ => unreachable!(),
        }
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

        match self.inner {
            Type::Source => write!(f, "(source)"),
            Type::Ingress(..) => write!(f, "{{ {} | (ingress) }}", idx.index()),
            Type::Egress(..) => write!(f, "{{ {} | (egress) }}", idx.index()),
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
            Type::Taken(_) => write!(f, "(taken)"),
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

// TODO: what do we do about .having?
// impl Node {
//     /// Add an output filter to this node.
//     ///
//     /// Only records matching the given conditions will be output from this node. This filtering
//     /// applies both to feed-forward and to queries. Note that adding conditions in this way does
//     /// *not* modify a node's input, and so the node may end up performing computation whose result
//     /// will simply be discarded.
//     ///
//     /// Adding a HAVING condition will not reduce the size of the node's materialized state.
//     pub fn having(mut self, cond: Vec<shortcut::Condition<query::DataType>>) -> Self {
//         self.having = Some(query::Query::new(&[], cond));
//         self
//     }
//
//     /// Retrieve a list of this node's output filters.
//     pub fn having_conditions(&self) -> Option<&[shortcut::Condition<query::DataType>]> {
//         self.having.as_ref().map(|q| &q.having[..])
//     }
//
//     pub fn operator(&self) -> &Type {
//         &*self.inner
//     }
// }
