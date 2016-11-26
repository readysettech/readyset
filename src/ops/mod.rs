pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod permute;
pub mod union;
pub mod identity;
#[cfg(test)]
pub mod gatedid;

use query;

use std::ops::{Deref, DerefMut};

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<query::DataType>),
    Negative(Vec<query::DataType>),
}

impl Record {
    pub fn rec(&self) -> &[query::DataType] {
        match *self {
            Record::Positive(ref v) |
            Record::Negative(ref v) => &v[..],
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (Vec<query::DataType>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
        }
    }
}

impl Deref for Record {
    type Target = Vec<query::DataType>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) |
            Record::Negative(ref r) => r,
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) |
            Record::Negative(ref mut r) => r,
        }
    }
}

impl From<Vec<query::DataType>> for Record {
    fn from(other: Vec<query::DataType>) -> Self {
        Record::Positive(other)
    }
}

/// Update is the smallest unit of data transmitted over edges in a data flow graph.
#[derive(Clone)]
pub enum Update {
    /// This update holds a set of records.
    Records(Vec<Record>),
}

impl From<Vec<Record>> for Update {
    fn from(others: Vec<Record>) -> Self {
        Update::Records(others)
    }
}

impl From<Record> for Update {
    fn from(other: Record) -> Self {
        Update::Records(vec![other])
    }
}

impl From<Vec<query::DataType>> for Update {
    fn from(other: Vec<query::DataType>) -> Self {
        Update::Records(vec![other.into()])
    }
}

impl From<Vec<Vec<query::DataType>>> for Update {
    fn from(other: Vec<Vec<query::DataType>>) -> Self {
        Update::Records(other.into_iter().map(|r| r.into()).collect())
    }
}

pub type Datas = Vec<Vec<query::DataType>>;

#[cfg(test)]
pub mod test {
    use super::*;

    use std::collections::HashMap;
    use std::cell;

    use flow::prelude::*;
    use flow::domain::single;
    use flow::node;
    use query;

    pub struct MockGraph {
        graph: Graph,
        source: NodeIndex,
        nut: Option<NodeIndex>, // node under test
        states: StateMap,
        nodes: NodeList,
        bases: Vec<NodeIndex>,
    }

    impl MockGraph {
        pub fn new() -> MockGraph {
            let mut graph = Graph::new();
            let source = graph.add_node(node::Node::new("source",
                                                        &["because-type-inference"],
                                                        node::Type::Source));
            MockGraph {
                graph: graph,
                source: source,
                nut: None,
                states: StateMap::new(),
                nodes: NodeList::from(vec![]),
                bases: Vec::new(),
            }
        }

        pub fn add_base(&mut self, name: &str, fields: &[&str]) -> NodeIndex {
            use ops::base::Base;
            let mut i: node::Type = Base {}.into();
            i.on_connected(&self.graph);
            let ni = self.graph.add_node(Node::new(name, fields, i));
            self.graph.add_edge(self.source, ni, false);
            let mut remap = HashMap::new();
            remap.insert(ni, ni);
            self.graph.node_weight_mut(ni).unwrap().on_commit(ni, &remap);
            self.states.insert(ni, State::new(fields.len()));
            self.bases.push(ni);
            ni
        }

        pub fn set_op<I>(&mut self, name: &str, fields: &[&str], i: I)
            where I: Into<node::Type>
        {
            use petgraph;
            assert!(self.nut.is_none(), "only one node under test is supported");

            let mut i: node::Type = i.into();
            i.on_connected(&self.graph);

            let parents = i.ancestors();
            assert!(!parents.is_empty(), "node under test should have ancestors");

            let ni = self.graph.add_node(node::Node::new(name, fields, i));
            for parent in parents {
                self.graph.add_edge(parent, ni, false);
            }
            let mut remap = HashMap::new();
            for b in &self.bases {
                remap.insert(*b, *b);
            }
            remap.insert(ni, ni);
            self.graph.node_weight_mut(ni).unwrap().on_commit(ni, &remap);

            // we're now committing to testing this op
            // store the id
            self.nut = Some(ni);
            // and also set up the node list
            let mut nodes = vec![];
            let mut topo = petgraph::visit::Topo::new(&self.graph);
            while let Some(node) = topo.next(&self.graph) {
                if node == self.source {
                    continue;
                }
                self.graph[node].add_to(0.into());
                nodes.push((node, self.graph[node].take()));
            }

            let nodes: Vec<_> = nodes.into_iter()
                .map(|(ni, n)| {
                    // also include all *internal* descendants
                    let children: Vec<_> = self.graph
                        .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                        .collect();

                    single::NodeDescriptor {
                        index: ni,
                        inner: n,
                        children: children,
                    }
                })
                .collect();
            self.nodes = NodeList::from(nodes);
        }

        pub fn seed(&mut self, base: NodeIndex, data: Vec<query::DataType>) {
            let m = Message {
                from: self.source,
                to: base,
                data: data.into(),
            };

            let u = self.graph
                .node_weight_mut(base)
                .unwrap()
                .on_input(m, &self.nodes, &self.states);

            let state = self.states.get_mut(&base).unwrap();
            match u {
                Some(Update::Records(rs)) => {
                    for r in rs {
                        match r {
                            Record::Positive(r) => state.insert(r),
                            Record::Negative(_) => unreachable!(),
                        }
                    }
                }
                None => unreachable!(),
            }
        }

        pub fn set_materialized(&mut self) {
            assert!(self.nut.is_some());
            let fs = self.graph[self.nut.unwrap()].fields().len();
            self.states.insert(self.nut.unwrap(), State::new(fs));
        }

        pub fn one<U: Into<Update>>(&mut self,
                                    src: NodeIndex,
                                    u: Update,
                                    remember: bool)
                                    -> Option<Update> {
            use shortcut;

            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(&self.nut.unwrap()));

            let m = Message {
                from: src,
                to: self.nut.unwrap(),
                data: u.into(),
            };

            let u = self.nodes
                .lookup_mut(self.nut.unwrap())
                .inner
                .on_input(m, &self.nodes, &self.states);

            if !remember || !self.states.contains_key(&self.nut.unwrap()) {
                return u;
            }

            let state = self.states.get_mut(&self.nut.unwrap()).unwrap();
            if let Some(Update::Records(ref rs)) = u {
                for r in rs.iter().cloned() {
                    // TODO: avoid duplication with Domain::materialize
                    match r {
                        Record::Positive(r) => state.insert(r),
                        Record::Negative(r) => {
                            // we need a cond that will match this row.
                            let conds = r.into_iter()
                                .enumerate()
                                .map(|(coli, v)| {
                                    shortcut::Condition {
                                        column: coli,
                                        cmp: shortcut::Comparison::Equal(shortcut::Value::Const(v)),
                                    }
                                })
                                .collect::<Vec<_>>();

                            // however, multiple rows may have the same values as this row for
                            // every column. afaict, it is safe to delete any one of these rows. we
                            // do this by returning true for the first invocation of the filter
                            // function, and false for all subsequent invocations.
                            let mut first = true;
                            state.delete_filter(&conds[..], |_| if first {
                                first = false;
                                true
                            } else {
                                false
                            });
                        }
                    }
                }
            }

            u
        }

        pub fn one_row(&mut self,
                       src: NodeIndex,
                       d: Vec<query::DataType>,
                       remember: bool)
                       -> Option<Update> {
            self.one::<Record>(src, d.into(), remember)
        }

        pub fn narrow_one<U: Into<Update>>(&mut self, u: U, remember: bool) -> Option<Update> {
            assert_eq!(self.bases.len(), 1);
            let src = self.bases[0];
            self.one::<Update>(src, u.into(), remember)
        }

        pub fn narrow_one_row(&mut self,
                              d: Vec<query::DataType>,
                              remember: bool)
                              -> Option<Update> {
            self.narrow_one::<Record>(d.into(), remember)
        }

        pub fn query(&self, q: Option<&query::Query>) -> Datas {
            self.node().query(q, &self.nodes, &self.states)
        }

        pub fn node(&self) -> cell::Ref<single::NodeDescriptor> {
            self.nodes.lookup(self.nut.unwrap())
        }

        pub fn narrow_base_id(&self) -> NodeIndex {
            assert_eq!(self.bases.len(), 1);
            self.bases[0]
        }
    }
}
