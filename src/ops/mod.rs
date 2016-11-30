pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod permute;
pub mod union;
pub mod identity;
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
            self.states.insert(*local.as_local(), State::default());
            self.remap.insert(global, local);
            global
        }

        pub fn set_op<I>(&mut self, name: &str, fields: &[&str], i: I, materialized: bool)
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
            if materialized {
                self.states.insert(*local.as_local(), State::default());
            }
            for parent in parents {
                self.graph.add_edge(*parent.as_global(), ni, false);
            }
            self.remap.insert(global, local);
            self.graph.node_weight_mut(ni).unwrap().on_commit(local, &self.remap);

            // we need to set the indices for all the base tables so they *actually* store things.
            let idx = self.graph[ni].suggest_indexes(local);
            for (tbl, col) in idx {
                if let Some(ref mut s) = self.states.get_mut(tbl.as_local()) {
                    s.set_pkey(col);
                }
            }
            // and get rid of states we don't need
            let unused: Vec<_> = self.remap
                .values()
                .filter_map(|ni| self.states.get(ni.as_local()).map(move |s| (ni, !s.is_useful())))
                .filter(|&(_, x)| x)
                .collect();
            for (ni, _) in unused {
                self.states.remove(ni.as_local());
            }

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
                    (*n.addr.as_local(), cell::RefCell::new(n))
                })
                .collect();
        }

        pub fn seed(&mut self, base: NodeAddress, data: Vec<query::DataType>) {
            assert!(self.nut.is_some(), "seed must happen after set_op");

            // base here is some identifier that was returned by Self::add_base.
            // which means it's a global address (and has to be so that it will correctly refer to
            // ancestors pre on_commit). we need to translate it into a local address.
            // since we set up the graph, we actually know that the NodeIndex is simply one greater
            // than the local index (since bases are added first, and assigned local + global
            // indices in order, but global ids are prefixed by the id of the source node).
            let local = self.to_local(base);

            // no need to call on_input since base tables just forward anyway

            // if the base node has state, keep it
            if let Some(ref mut state) = self.states.get_mut(local.as_local()) {
                match data.into() {
                    Update::Records(rs) => {
                        for r in rs {
                            match r {
                                Record::Positive(r) => state.insert(r),
                                Record::Negative(_) => unreachable!(),
                            }
                        }
                    }
                }
            } else {
                assert!(false,
                        "unnecessary seed value for {} (never used by any node)",
                        base);
            }
        }

        pub fn one<U: Into<Update>>(&mut self,
                                    src: NodeAddress,
                                    u: Update,
                                    remember: bool)
                                    -> Option<Update> {
            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(self.nut.unwrap().1.as_local()));

            let m = Message {
                from: src,
                to: self.nut.unwrap().1,
                data: u.into(),
            };

            let u = self.nodes[self.nut.unwrap().1.as_local()]
                .borrow_mut()
                .inner
                .on_input(m, &self.nodes, &self.states);

            if !remember || !self.states.contains_key(self.nut.unwrap().1.as_local()) {
                return u;
            }

            if let Some(ref u) = u {
                single::materialize(u, self.states.get_mut(self.nut.unwrap().1.as_local()));
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

        pub fn node(&self) -> cell::Ref<single::NodeDescriptor> {
            self.nodes[self.nut.unwrap().1.as_local()].borrow()
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
