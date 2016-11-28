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

    use petgraph::graph::NodeIndex;

    pub struct MockGraph {
        graph: Graph,
        source: NodeIndex,
        nut: Option<(NodeIndex, NodeAddress)>, // node under test
        states: StateMap,
        nodes: DomainNodes,
        remap: HashMap<NodeAddress, NodeAddress>,
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
                nodes: DomainNodes::default(),
                remap: HashMap::new(),
            }
        }

        pub fn add_base(&mut self, name: &str, fields: &[&str]) -> NodeAddress {
            use ops::base::Base;
            let mut i: node::Type = Base {}.into();
            i.on_connected(&self.graph);
            let ni = self.graph.add_node(Node::new(name, fields, i));
            self.graph.add_edge(self.source, ni, false);
            let mut remap = HashMap::new();
            let global = NodeAddress::mock_global(ni);
            let local = NodeAddress::mock_local(self.remap.len());
            remap.insert(global, local);
            self.graph.node_weight_mut(ni).unwrap().on_commit(local, &remap);
            self.states.insert(local, State::new(fields.len()));
            self.remap.insert(global, local);
            global
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
            let global = NodeAddress::mock_global(ni);
            let local = NodeAddress::mock_local(self.remap.len());
            for parent in parents {
                self.graph.add_edge(parent.as_global(), ni, false);
            }
            self.remap.insert(global, local);
            self.graph.node_weight_mut(ni).unwrap().on_commit(local, &self.remap);

            // we're now committing to testing this op
            // store the id
            self.nut = Some((ni, local));
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
                .enumerate()
                .map(|(i, (ni, n))| {
                    single::NodeDescriptor {
                        index: ni,
                        addr: NodeAddress::mock_local(i),
                        inner: n,
                        children: Vec::default(),
                    }
                })
                .collect();

            self.nodes = nodes.into_iter()
                .map(|n| {
                    use std::cell;
                    (n.addr, cell::RefCell::new(n))
                })
                .collect();
        }

        pub fn seed(&mut self, base: NodeAddress, data: Vec<query::DataType>) {
            // base here is some identifier that was returned by Self::add_base.
            // which means it's a global address (and has to be so that it will correctly refer to
            // ancestors pre on_commit). we need to translate it into a local address.
            // since we set up the graph, we actually know that the NodeIndex is simply one greater
            // than the local index (since bases are added first, and assigned local + global
            // indices in order, but global ids are prefixed by the id of the source node).
            let local = self.to_local(base);

            let m = Message {
                from: NodeAddress::mock_global(self.source),
                to: local,
                data: data.into(),
            };

            let u = self.graph
                .node_weight_mut(base.as_global())
                .unwrap()
                .on_input(m, &self.nodes, &self.states);

            let state = self.states.get_mut(&local).unwrap();
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
            let fs = self.graph[self.nut.unwrap().0].fields().len();
            self.states.insert(self.nut.unwrap().1, State::new(fs));
        }

        pub fn one<U: Into<Update>>(&mut self,
                                    src: NodeAddress,
                                    u: Update,
                                    remember: bool)
                                    -> Option<Update> {
            use shortcut;

            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(&self.nut.unwrap().1));

            let m = Message {
                from: src,
                to: self.nut.unwrap().1,
                data: u.into(),
            };

            let u = self.nodes[&self.nut.unwrap().1]
                .borrow_mut()
                .inner
                .on_input(m, &self.nodes, &self.states);

            if !remember || !self.states.contains_key(&self.nut.unwrap().1) {
                return u;
            }

            let state = self.states.get_mut(&self.nut.unwrap().1).unwrap();
            if let Some(Update::Records(ref rs)) = u {
                for r in rs {
                    // TODO: avoid duplication with Domain::materialize
                    match *r {
                        Record::Positive(ref r) => state.insert(r.clone()),
                        Record::Negative(ref r) => {
                            // we need a cond that will match this row.
                            let conds = r.iter()
                                .enumerate()
                                .map(|(coli, v)| {
                                    shortcut::Condition {
                                        column: coli,
                                        cmp: shortcut::Comparison::Equal(shortcut::Value::using(v)),
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
                       src: NodeAddress,
                       d: Vec<query::DataType>,
                       remember: bool)
                       -> Option<Update> {
            self.one::<Record>(src, d.into(), remember)
        }

        pub fn narrow_one<U: Into<Update>>(&mut self, u: U, remember: bool) -> Option<Update> {
            let src = self.narrow_base_id();
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
            self.nodes[&self.nut.unwrap().1].borrow()
        }

        pub fn narrow_base_id(&self) -> NodeAddress {
            assert_eq!(self.remap.len(), 2 /* base + nut */);
            *self.remap.values().skip_while(|&&n| n == self.nut.unwrap().1).next().unwrap()
        }

        pub fn to_local(&self, global: NodeAddress) -> NodeAddress {
            NodeAddress::mock_local(global.as_global().index() - 1)
        }
    }
}
