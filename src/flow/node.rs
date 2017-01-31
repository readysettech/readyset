use petgraph;
use petgraph::graph::NodeIndex;

use std::sync::mpsc;
use std::sync;
use std::fmt;
use std::collections::HashMap;

use std::ops::{Deref, DerefMut};

use checktable;

use query::DataType;
use ops::{Records, Datas};
use flow::domain;
use flow::{Message, Ingredient, NodeAddress, Edge};

use backlog;

#[derive(Clone)]
pub struct Reader {
    pub streamers: sync::Arc<sync::Mutex<Vec<mpsc::Sender<Records>>>>,
    pub state: Option<backlog::BufferedStore>,
    pub token_generator: Option<checktable::TokenGenerator>,
}

impl Reader {
    pub fn new() -> Self {
        Reader {
            streamers: sync::Arc::default(),
            state: None,
            token_generator: None,
        }
    }

    pub fn get_reader
        (&self)
         -> Option<Box<Fn(&DataType) -> Result<Vec<Vec<DataType>>, ()> + Send + Sync>> {
        self.state.clone().map(|arc| {
            Box::new(move |q: &DataType| -> Result<Datas, ()> {
                arc.find_and(q,
                              |rs| rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>())
                    .map(|r| r.0)
            }) as Box<_>
        })
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
    Ingress,
    Internal(Box<Ingredient>),
    Egress(sync::Arc<sync::Mutex<Vec<(NodeAddress, mpsc::SyncSender<Message>)>>>),
    TimestampIngress(sync::Arc<sync::Mutex<mpsc::SyncSender<i64>>>),
    TimestampEgress(sync::Arc<sync::Mutex<Vec<mpsc::SyncSender<i64>>>>),
    Reader(Option<backlog::WriteHandle>, Reader),
    Source,
}

impl Type {
    // Returns a map from base node to the column in that base node whose value must match the value
    // of this node's column to cause a conflict. Returns None for a given base node if any write to
    // that base node might cause a conflict.
    pub fn base_columns(&self, column: usize, graph: &petgraph::Graph<Node, Edge>, index: NodeIndex)
                    ->  Vec<(NodeIndex, Option<usize>)>{

        fn base_parents(graph: &petgraph::Graph<Node, Edge>, index: NodeIndex) -> Vec<(NodeIndex, Option<usize>)> {
            if let &Type::Internal(ref i) = graph[index].deref() {
                if i.is_base() {
                    return vec![(index, None)]
                }
            }
            graph.neighbors_directed(index, petgraph::EdgeDirection::Incoming).flat_map(|n|{
                base_parents(graph, n)
            }).collect()
        }

        let parents:Vec<_> = graph.neighbors_directed(index, petgraph::EdgeDirection::Incoming).collect();

        match *self {
            Type::Ingress |
            Type::Reader(..) |
            Type::Egress(..) => {
                assert_eq!(parents.len(), 1);
                graph[parents[0]].base_columns(column, graph, parents[0])
            }
            Type::Internal(ref i) => {
                if i.is_base() {
                    vec![(index, Some(column))]
                } else {
                    i.parent_columns(column).into_iter().flat_map(|(n, c)|{
                        let n = if n.is_global() {
                            *n.as_global()
                        } else {
                            // Find the parent with node address matching the result from parent_columns.
                            *parents.iter().filter(|p|{
                                match graph[**p].addr {
                                    Some(a) if a == n => true,
                                    _ => false,
                                }
                            }).next().unwrap()
                        };

                        match c {
                            Some(c) => graph[n].base_columns(c, graph, n),
                            None => base_parents(graph, n),
                        }
                    }).collect()
                }
            }
            Type::TimestampIngress(..) |
            Type::TimestampEgress(..) |
            Type::Source => unreachable!(),
        }
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Type::Source => write!(f, "source node"),
            &Type::Ingress => write!(f, "ingress node"),
            &Type::Egress(..) => write!(f, "egress node"),
            &Type::TimestampIngress(..) => write!(f, "time ingress node"),
            &Type::TimestampEgress(..) => write!(f, "time egress node"),
            &Type::Reader(..) => write!(f, "reader node"),
            &Type::Internal(ref i) => write!(f, "internal {} node", i.description()),
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

    fields: Vec<String>,
    inner: NodeHandle,
}

impl Node {
    pub fn new<S1, FS, S2>(name: S1, fields: FS, inner: Type) -> Node
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>
    {
        Node {
            name: name.to_string(),
            domain: None,
            addr: None,

            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            inner: NodeHandle::Owned(inner),
        }
    }

    pub fn mirror(&self, n: Type) -> Node {
        let mut n = Self::new(&*self.name, &self.fields, n);
        n.domain = self.domain;
        n
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

    pub fn take(&mut self) -> Node {
        let inner = match *self.inner {
            Type::Egress(ref txs) => {
                // egress nodes can still be modified externally if subgraphs are added
                // so we just make a new one with a clone of the Mutex-protected Vec
                Type::Egress(txs.clone())
            }
            Type::Reader(ref mut w, ref r) => {
                // reader nodes can still be modified externally if txs are added
                Type::Reader(w.take(), r.clone())
            }
            Type::TimestampEgress(ref arc) => Type::TimestampEgress(arc.clone()),
            Type::TimestampIngress(ref arc) => Type::TimestampIngress(arc.clone()),
            Type::Ingress => Type::Ingress,
            Type::Internal(ref mut i) if self.domain.is_some() => Type::Internal(i.take()),
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

        let escape = |s: &str| Regex::new("([\"|{}])").unwrap().replace_all(s, "\\$1");
        write!(f,
               " [style=filled, fillcolor={}, label=\"",
               self.domain
                   .map(|d| -> usize { d.into() })
                   .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                   .unwrap_or("white".into()))?;

        match *self.inner {
            Type::Source => write!(f, "(source)"),
            Type::Ingress => write!(f, "{{ {} | (ingress) }}", idx.index()),
            Type::Egress(..) => write!(f, "{{ {} | (egress) }}", idx.index()),
            Type::TimestampIngress(..) => write!(f, "{{ {} | (timestamp-ingress) }}", idx.index()),
            Type::TimestampEgress(..) => write!(f, "{{ {} | (timestamp-egress) }}", idx.index()),
            Type::Reader(..) => write!(f, "{{ {} | (reader) }}", idx.index()),
            Type::Internal(ref i) => {
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

    pub fn is_egress(&self) -> bool {
        if let Type::Egress(..) = *self.inner {
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

    /// A node is considered to be an output node if changes to its state are visible outside of its domain.
    pub fn is_output(&self) -> bool {
        match *self.inner {
            Type::Egress(..) | Type::Reader(..) => true,
            _ => false,
        }
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
