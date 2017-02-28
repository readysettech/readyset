pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod permute;
pub mod project;
pub mod union;
pub mod identity;
pub mod gatedid;
pub mod filter;
pub mod topk;

use flow::data::DataType;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync;

/// A record is a single positive or negative data record with an associated time stamp.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(sync::Arc<Vec<DataType>>),
    Negative(sync::Arc<Vec<DataType>>),
    DeleteRequest(Vec<DataType>),
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // TODO(jmftrindade): Retrieve timestamps.
            Record::Positive(..) => write!(f, "positive: {:?}", self.rec()),
            Record::Negative(..) => write!(f, "negative: {:?}", self.rec()),
            Record::DeleteRequest(..) => write!(f, "delete request")
        }
    }
}

impl Record {
    pub fn rec(&self) -> &[DataType] {
        match *self {
            Record::Positive(ref v) |
            Record::Negative(ref v) => &v[..],
            Record::DeleteRequest(..) => unreachable!(),
        }
    }

    pub fn is_positive(&self) -> bool {
        if let Record::Positive(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn extract(self) -> (sync::Arc<Vec<DataType>>, bool) {
        match self {
            Record::Positive(v) => (v, true),
            Record::Negative(v) => (v, false),
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl Deref for Record {
    type Target = sync::Arc<Vec<DataType>>;
    fn deref(&self) -> &Self::Target {
        match *self {
            Record::Positive(ref r) |
            Record::Negative(ref r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Record::Positive(ref mut r) |
            Record::Negative(ref mut r) => r,
            Record::DeleteRequest(..) => unreachable!(),
        }
    }
}

impl From<sync::Arc<Vec<DataType>>> for Record {
    fn from(other: sync::Arc<Vec<DataType>>) -> Self {
        Record::Positive(other)
    }
}

impl From<Vec<DataType>> for Record {
    fn from(other: Vec<DataType>) -> Self {
        Record::Positive(sync::Arc::new(other))
    }
}

impl From<(Vec<DataType>, bool)> for Record {
    fn from(other: (Vec<DataType>, bool)) -> Self {
        if other.1 {
            Record::Positive(sync::Arc::new(other.0))
        } else {
            Record::Negative(sync::Arc::new(other.0))
        }
    }
}

impl Into<Vec<Record>> for Records {
    fn into(self) -> Vec<Record> {
        self.0
    }
}

use std::iter::FromIterator;
impl FromIterator<Record> for Records {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = Record>
    {
        Records(iter.into_iter().collect())
    }
}
impl FromIterator<sync::Arc<Vec<DataType>>> for Records {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = sync::Arc<Vec<DataType>>>
    {
        Records(iter.into_iter().map(Record::Positive).collect())
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = ::std::vec::IntoIter<Record>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Represents a set of records returned from a query.
pub type Datas = Vec<Vec<DataType>>;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct Records(Vec<Record>);

impl Deref for Records {
    type Target = Vec<Record>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Into<Records> for Record {
    fn into(self) -> Records {
        Records(vec![self])
    }
}

impl Into<Records> for Vec<Record> {
    fn into(self) -> Records {
        Records(self)
    }
}

impl Into<Records> for Vec<sync::Arc<Vec<DataType>>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

impl Into<Records> for Vec<Vec<DataType>> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

impl Into<Records> for Vec<(Vec<DataType>, bool)> {
    fn into(self) -> Records {
        Records(self.into_iter().map(|r| r.into()).collect())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use std::collections::HashMap;
    use std::cell;

    use flow::prelude::*;
    use flow::domain::single;
    use flow::node;

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
            let mut i: node::Type = Base::default().into();
            i.on_connected(&self.graph);
            let ni = self.graph.add_node(Node::new(name, fields, i));
            self.graph.add_edge(self.source, ni, false);
            let mut remap = HashMap::new();
            let global = NodeAddress::mock_global(ni);
            let local = NodeAddress::mock_local(self.remap.len());
            self.graph.node_weight_mut(ni).unwrap().set_addr(local);
            remap.insert(global, local);
            self.graph.node_weight_mut(ni).unwrap().on_commit(&remap);
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
            self.graph.node_weight_mut(ni).unwrap().set_addr(local);
            self.graph.node_weight_mut(ni).unwrap().on_commit(&self.remap);

            // we need to set the indices for all the base tables so they *actually* store things.
            let idx = self.graph[ni].suggest_indexes(local);
            for (tbl, col) in idx {
                if let Some(ref mut s) = self.states.get_mut(tbl.as_local()) {
                    s.add_key(&col[..]);
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
                .map(|(ni, n)| {
                    single::NodeDescriptor {
                        index: ni,
                        inner: n,
                        children: Vec::default(),
                    }
                })
                .collect();

            self.nodes = nodes.into_iter()
                .map(|n| {
                    use std::cell;
                    (*n.addr().as_local(), cell::RefCell::new(n))
                })
                .collect();
        }

        pub fn seed(&mut self, base: NodeAddress, data: Vec<DataType>) {
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
                    Record::Positive(r) => state.insert(r),
                    Record::Negative(_) => unreachable!(),
                    Record::DeleteRequest(..) => unreachable!(),
                }
            } else {
                assert!(false,
                        "unnecessary seed value for {} (never used by any node)",
                        base);
            }
        }

        pub fn unseed(&mut self, base: NodeAddress) {
            assert!(self.nut.is_some(), "unseed must happen after set_op");

            let local = self.to_local(base);
            self.states.get_mut(local.as_local()).unwrap().clear();
        }

        pub fn one<U: Into<Records>>(&mut self, src: NodeAddress, u: U, remember: bool) -> Records {
            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(self.nut.unwrap().1.as_local()));

            let u = self.nodes[self.nut.unwrap().1.as_local()]
                .borrow_mut()
                .inner
                .on_input(src, u.into(), &self.nodes, &self.states);

            if !remember || !self.states.contains_key(self.nut.unwrap().1.as_local()) {
                return u;
            }

            single::materialize(&u, self.states.get_mut(self.nut.unwrap().1.as_local()));
            u
        }

        pub fn one_row<R: Into<Record>>(&mut self,
                                        src: NodeAddress,
                                        d: R,
                                        remember: bool)
                                        -> Records {
            self.one::<Record>(src, d.into(), remember)
        }

        pub fn narrow_one<U: Into<Records>>(&mut self, u: U, remember: bool) -> Records {
            let src = self.narrow_base_id();
            self.one::<Records>(src, u.into(), remember)
        }

        pub fn narrow_one_row<R: Into<Record>>(&mut self, d: R, remember: bool) -> Records {
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
