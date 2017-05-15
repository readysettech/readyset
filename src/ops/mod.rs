use flow::core::processing::Ingredient;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use flow::prelude::*;

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

pub enum NodeOperator {
    Base(base::Base),
    Sum(grouped::GroupedOperator<grouped::aggregate::Aggregator>),
    Extremum(grouped::GroupedOperator<grouped::extremum::ExtremumOperator>),
    Concat(grouped::GroupedOperator<grouped::concat::GroupConcat>),
    Join(join::Join),
    Latest(latest::Latest),
    Permute(permute::Permute),
    Project(project::Project),
    Union(union::Union),
    Identity(identity::Identity),
    GatedId(gatedid::GatedIdentity),
    Filter(filter::Filter),
    TopK(topk::TopK),
}

macro_rules! nodeop_from_impl {
    ($variant:path, $type:ty) => {
        impl From<$type> for NodeOperator {
            fn from(other: $type) -> Self {
                $variant(other)
            }
        }
    }
}

nodeop_from_impl!(NodeOperator::Base, base::Base);
nodeop_from_impl!(NodeOperator::Sum, grouped::GroupedOperator<grouped::aggregate::Aggregator>);
nodeop_from_impl!(NodeOperator::Extremum, grouped::GroupedOperator<grouped::extremum::ExtremumOperator>);
nodeop_from_impl!(NodeOperator::Concat, grouped::GroupedOperator<grouped::concat::GroupConcat>);
nodeop_from_impl!(NodeOperator::Join, join::Join);
nodeop_from_impl!(NodeOperator::Latest, latest::Latest);
nodeop_from_impl!(NodeOperator::Permute, permute::Permute);
nodeop_from_impl!(NodeOperator::Project, project::Project);
nodeop_from_impl!(NodeOperator::Union, union::Union);
nodeop_from_impl!(NodeOperator::Identity, identity::Identity);
nodeop_from_impl!(NodeOperator::GatedId, gatedid::GatedIdentity);
nodeop_from_impl!(NodeOperator::Filter, filter::Filter);
nodeop_from_impl!(NodeOperator::TopK, topk::TopK);

macro_rules! impl_ingredient_fn_mut {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Base(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Sum(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Join(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Permute(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Project(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Union(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref mut i) => i.$fn($($arg),*),
            NodeOperator::GatedId(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref mut i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref mut i) => i.$fn($($arg),*),
        }
    }
}

macro_rules! impl_ingredient_fn_ref {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Base(ref i) => i.$fn($($arg),*),
            NodeOperator::Sum(ref i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref i) => i.$fn($($arg),*),
            NodeOperator::Join(ref i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref i) => i.$fn($($arg),*),
            NodeOperator::Permute(ref i) => i.$fn($($arg),*),
            NodeOperator::Project(ref i) => i.$fn($($arg),*),
            NodeOperator::Union(ref i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref i) => i.$fn($($arg),*),
            NodeOperator::GatedId(ref i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref i) => i.$fn($($arg),*),
        }
    }
}

impl Ingredient for NodeOperator {
    fn take(&mut self) -> NodeOperator {
        impl_ingredient_fn_mut!(self,take,)
    }
    fn ancestors(&self) -> Vec<NodeAddress> {
        impl_ingredient_fn_ref!(self,ancestors,)
    }
    fn should_materialize(&self) -> bool {
        impl_ingredient_fn_ref!(self,should_materialize,)
    }
    fn must_replay_among(&self) -> Option<HashSet<NodeAddress>> {
        impl_ingredient_fn_ref!(self,must_replay_among,)
    }
    fn will_query(&self, materialized: bool) -> bool {
        impl_ingredient_fn_ref!(self, will_query, materialized)
    }
    fn suggest_indexes(&self, you: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        impl_ingredient_fn_ref!(self, suggest_indexes, you)
    }
    fn resolve(&self, i: usize) -> Option<Vec<(NodeAddress, usize)>> {
        impl_ingredient_fn_ref!(self, resolve, i)
    }
    fn get_base(&self) -> Option<&base::Base> {
        impl_ingredient_fn_ref!(self, get_base,)
    }
    fn get_base_mut(&mut self) -> Option<&mut base::Base> {
        impl_ingredient_fn_mut!(self, get_base_mut,)
    }
    fn is_join(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_join,)
    }
    fn description(&self) -> String {
        impl_ingredient_fn_ref!(self, description,)
    }
    fn on_connected(&mut self, graph: &Graph) {
        impl_ingredient_fn_mut!(self, on_connected, graph)
    }
    fn on_commit(&mut self, you: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        impl_ingredient_fn_mut!(self, on_commit, you, remap)
    }
    fn on_input(&mut self,
                from: NodeAddress,
                data: Records,
                tracer: &mut Tracer,
                domain: &DomainNodes,
                states: &StateMap)
                -> ProcessingResult {
        impl_ingredient_fn_mut!(self, on_input, from, data, tracer, domain, states)
    }
    fn on_input_raw(&mut self,
                    from: NodeAddress,
                    data: Records,
                    tracer: &mut Tracer,
                    is_replay_of: Option<(usize, DataType)>,
                    domain: &DomainNodes,
                    states: &StateMap)
                    -> RawProcessingResult {
        impl_ingredient_fn_mut!(self,
                                on_input_raw,
                                from,
                                data,
                                tracer,
                                is_replay_of,
                                domain,
                                states)
    }
    fn can_query_through(&self) -> bool {
        impl_ingredient_fn_ref!(self, can_query_through, )
    }
    fn query_through<'a>(&self,
                         columns: &[usize],
                         key: &KeyType<DataType>,
                         states: &'a StateMap)
                         -> Option<Option<Box<Iterator<Item = &'a Arc<Vec<DataType>>> + 'a>>> {
        impl_ingredient_fn_ref!(self, query_through, columns, key, states)
    }
    fn lookup<'a>(&self,
                  parent: NodeAddress,
                  columns: &[usize],
                  key: &KeyType<DataType>,
                  domain: &DomainNodes,
                  states: &'a StateMap)
                  -> Option<Option<Box<Iterator<Item = &'a Arc<Vec<DataType>>> + 'a>>> {
        impl_ingredient_fn_ref!(self, lookup, parent, columns, key, domain, states)
    }
    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        impl_ingredient_fn_ref!(self, parent_columns, column)
    }
    fn is_selective(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_selective,)
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::cell;

    use flow::prelude::*;
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
                                                        node::special::Source,
                                                        true));
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
            self.add_base_defaults(name, fields, vec![])
        }

        pub fn add_base_defaults(&mut self,
                                 name: &str,
                                 fields: &[&str],
                                 defaults: Vec<DataType>)
                                 -> NodeAddress {
            use ops::base::Base;
            let mut i = Base::new(defaults).into();
            i.on_connected(&self.graph);
            let ni = self.graph.add_node(Node::new(name, fields, i, false));
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
            where I: Into<node::NodeType>
        {
            use petgraph;
            assert!(self.nut.is_none(), "only one node under test is supported");

            let mut i = i.into();
            i.on_connected(&self.graph);

            let parents = i.ancestors();
            assert!(!parents.is_empty(), "node under test should have ancestors");

            let ni = self.graph.add_node(node::Node::new(name, fields, i, false));
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
            self.graph
                .node_weight_mut(ni)
                .unwrap()
                .on_commit(&self.remap);

            // we need to set the indices for all the base tables so they *actually* store things.
            let idx = self.graph[ni].suggest_indexes(local);
            for (tbl, col) in idx {
                if let Some(ref mut s) = self.states.get_mut(tbl.as_local()) {
                    s.add_key(&col[..], false);
                }
            }
            // and get rid of states we don't need
            let unused: Vec<_> = self.remap
                .values()
                .filter_map(|ni| {
                                self.states
                                    .get(ni.as_local())
                                    .map(move |s| (ni, !s.is_useful()))
                            })
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

            let nodes: Vec<_> = self.nodes = nodes
                .into_iter()
                .map(|(_, n)| {
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
            let local = if NodeAddress::is_global(&base) {
                self.to_local(base)
            } else {
                base
            };

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

            let mut u = {
                let id = self.nut.unwrap().1;
                let mut n = self.nodes[id.as_local()].borrow_mut();
                let m = n.inner
                    .on_input(src, u.into(), &mut None, &self.nodes, &self.states);
                assert_eq!(m.misses, vec![]);
                m.results
            };

            if !remember || !self.states.contains_key(self.nut.unwrap().1.as_local()) {
                return u;
            }

            let misses = node::materialize(&mut u,
                                           *self.nut.unwrap().1.as_local(),
                                           self.states.get_mut(self.nut.unwrap().1.as_local()));
            assert_eq!(misses, vec![]);
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

        pub fn node(&self) -> cell::Ref<node::Node> {
            self.nodes[self.nut.unwrap().1.as_local()].borrow()
        }

        pub fn narrow_base_id(&self) -> NodeAddress {
            assert_eq!(self.remap.len(), 2 /* base + nut */);
            *self.remap
                 .values()
                 .skip_while(|&&n| n == self.nut.unwrap().1)
                 .next()
                 .unwrap()
        }

        pub fn to_local(&self, global: NodeAddress) -> NodeAddress {
            NodeAddress::mock_local(global.as_global().index() - 1)
        }
    }
}
