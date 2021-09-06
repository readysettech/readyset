use derive_more::From;
use noria::KeyComparison;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::prelude::*;
use noria::errors::ReadySetResult;

pub mod filter;
pub mod grouped;
pub mod identity;
pub mod join;
pub mod latest;
pub mod param_filter;
pub mod project;
pub mod topk;
pub mod union;

use crate::ops::grouped::concat::GroupConcat;
use crate::processing::{ColumnMiss, ColumnSource, LookupMode, SuggestedIndex};

/// Enum for distinguishing between the two parents of a union or join
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Serialize, Deserialize)]
pub(self) enum Side {
    Left,
    Right,
}

impl Side {
    fn other_side(&self) -> Self {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, From)]
#[allow(clippy::large_enum_variant)]
pub enum NodeOperator {
    // Aggregation was previously named "Sum", but now supports Sum, Count, Avg
    // Aggregation supports both filtered and normal Aggregations
    Aggregation(grouped::GroupedOperator<grouped::aggregate::Aggregator>),
    Extremum(grouped::GroupedOperator<grouped::extremum::ExtremumOperator>),
    Concat(grouped::GroupedOperator<GroupConcat>),
    Join(join::Join),
    ParamFilter(param_filter::ParamFilter),
    Latest(latest::Latest),
    Project(project::Project),
    Union(union::Union),
    Identity(identity::Identity),
    Filter(filter::Filter),
    TopK(topk::TopK),
}

impl ToString for NodeOperator {
    fn to_string(&self) -> String {
        match *self {
            NodeOperator::Aggregation(_) => "Aggregation",
            NodeOperator::Extremum(_) => "Extermum",
            NodeOperator::Concat(_) => "Concat",
            NodeOperator::Join(_) => "Join",
            NodeOperator::ParamFilter(_) => "ParamFilter",
            NodeOperator::Latest(_) => "Latest",
            NodeOperator::Project(_) => "Project",
            NodeOperator::Union(_) => "Union",
            NodeOperator::Identity(_) => "Identity",
            NodeOperator::Filter(_) => "Filter",
            NodeOperator::TopK(_) => "TopK",
        }
        .to_string()
    }
}

macro_rules! impl_ingredient_fn_mut {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Aggregation(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Join(ref mut i) => i.$fn($($arg),*),
            NodeOperator::ParamFilter(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Project(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Union(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref mut i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref mut i) => i.$fn($($arg),*),
        }
    }
}

macro_rules! impl_ingredient_fn_ref {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Aggregation(ref i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref i) => i.$fn($($arg),*),
            NodeOperator::Join(ref i) => i.$fn($($arg),*),
            NodeOperator::ParamFilter(ref i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref i) => i.$fn($($arg),*),
            NodeOperator::Project(ref i) => i.$fn($($arg),*),
            NodeOperator::Union(ref i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref i) => i.$fn($($arg),*),
        }
    }
}

impl Ingredient for NodeOperator {
    fn take(&mut self) -> NodeOperator {
        impl_ingredient_fn_mut!(self, take,)
    }
    fn ancestors(&self) -> Vec<NodeIndex> {
        impl_ingredient_fn_ref!(self, ancestors,)
    }
    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        impl_ingredient_fn_ref!(self, must_replay_among,)
    }
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, SuggestedIndex> {
        impl_ingredient_fn_ref!(self, suggest_indexes, you)
    }
    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        impl_ingredient_fn_ref!(self, column_source, cols)
    }
    fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        impl_ingredient_fn_mut!(self, handle_upquery, miss)
    }
    fn is_join(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_join,)
    }
    fn description(&self, detailed: bool) -> String {
        impl_ingredient_fn_ref!(self, description, detailed)
    }
    fn probe(&self) -> HashMap<String, String> {
        impl_ingredient_fn_ref!(self, probe,)
    }
    fn on_connected(&mut self, graph: &Graph) {
        impl_ingredient_fn_mut!(self, on_connected, graph)
    }
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        impl_ingredient_fn_mut!(self, on_commit, you, remap)
    }
    fn on_input(
        &mut self,
        ex: &mut dyn Executor,
        from: LocalNodeIndex,
        data: Records,
        replay_key_col: Option<&[usize]>,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        impl_ingredient_fn_mut!(
            self,
            on_input,
            ex,
            from,
            data,
            replay_key_col,
            domain,
            states
        )
    }
    fn on_input_raw(
        &mut self,
        ex: &mut dyn Executor,
        from: LocalNodeIndex,
        data: Records,
        replay: ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ReadySetResult<RawProcessingResult> {
        impl_ingredient_fn_mut!(self, on_input_raw, ex, from, data, replay, domain, states)
    }
    fn on_eviction(&mut self, from: LocalNodeIndex, tag: Tag, keys: &[KeyComparison]) {
        impl_ingredient_fn_mut!(self, on_eviction, from, tag, keys)
    }
    fn can_query_through(&self) -> bool {
        impl_ingredient_fn_ref!(self, can_query_through,)
    }
    #[allow(clippy::type_complexity)]
    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType,
        nodes: &DomainNodes,
        states: &'a StateMap,
        mode: LookupMode,
    ) -> Option<Option<Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a>>> {
        impl_ingredient_fn_ref!(self, query_through, columns, key, nodes, states, mode)
    }
    #[allow(clippy::type_complexity)]
    fn lookup<'a>(
        &self,
        parent: LocalNodeIndex,
        columns: &[usize],
        key: &KeyType,
        domain: &DomainNodes,
        states: &'a StateMap,
        mode: LookupMode,
    ) -> Option<Option<Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a>>> {
        impl_ingredient_fn_ref!(self, lookup, parent, columns, key, domain, states, mode)
    }
    fn is_selective(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_selective,)
    }
    fn requires_full_materialization(&self) -> bool {
        impl_ingredient_fn_ref!(self, requires_full_materialization,)
    }
}

#[cfg(test)]
pub mod test {
    use std::cell;
    use std::collections::HashMap;

    use crate::node;
    use crate::prelude::*;
    use crate::processing::SuggestedIndex;

    use petgraph::graph::NodeIndex;

    pub(super) struct MockGraph {
        graph: Graph,
        source: NodeIndex,
        nut: Option<IndexPair>, // node under test
        pub(super) states: StateMap,
        nodes: DomainNodes,
        remap: HashMap<NodeIndex, IndexPair>,
    }

    #[allow(clippy::new_without_default)]
    impl MockGraph {
        pub fn new() -> MockGraph {
            let mut graph = Graph::new();
            let source = graph.add_node(Node::new(
                "source",
                &["because-type-inference"],
                node::NodeType::Source,
            ));
            MockGraph {
                graph,
                source,
                nut: None,
                states: StateMap::new(),
                nodes: DomainNodes::default(),
                remap: HashMap::new(),
            }
        }

        pub fn add_base(&mut self, name: &str, fields: &[&str]) -> IndexPair {
            self.add_base_defaults(name, fields, vec![])
        }

        pub fn add_base_defaults(
            &mut self,
            name: &str,
            fields: &[&str],
            defaults: Vec<DataType>,
        ) -> IndexPair {
            use crate::node::special::Base;
            let i = Base::new(defaults);
            let global = self.graph.add_node(Node::new(name, fields, i));
            self.graph.add_edge(self.source, global, ());
            let mut remap = HashMap::new();
            let local = unsafe { LocalNodeIndex::make(self.remap.len() as u32) };
            let mut ip: IndexPair = global.into();
            ip.set_local(local);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .set_finalized_addr(ip);
            remap.insert(global, ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .on_commit(&remap);
            self.states.insert(local, Box::new(MemoryState::default()));
            self.remap.insert(global, ip);
            ip
        }

        pub fn set_op<I>(&mut self, name: &str, fields: &[&str], mut i: I, materialized: bool)
        where
            I: Ingredient + Into<NodeOperator>,
        {
            assert!(self.nut.is_none(), "only one node under test is supported");

            i.on_connected(&self.graph);
            let parents = i.ancestors();
            assert!(!parents.is_empty(), "node under test should have ancestors");

            let i: NodeOperator = i.into();
            let global = self.graph.add_node(Node::new(name, fields, i));
            let local = unsafe { LocalNodeIndex::make(self.remap.len() as u32) };
            if materialized {
                self.states.insert(local, Box::new(MemoryState::default()));
            }
            for parent in parents {
                self.graph.add_edge(parent, global, ());
            }
            let mut ip: IndexPair = global.into();
            ip.set_local(local);
            self.remap.insert(global, ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .set_finalized_addr(ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .on_commit(&self.remap);

            // we need to set the indices for all the base tables so they *actually* store things.
            let idx = self.graph[global].suggest_indexes(global);
            for (tbl, suggested_index) in idx {
                if let Some(ref mut s) = self.states.get_mut(self.graph[tbl].local_addr()) {
                    s.add_key(suggested_index.index(), None);
                    if suggested_index.is_weak() {
                        s.add_weak_key(suggested_index.index())
                    }
                }
            }
            // and get rid of states we don't need
            let unused: Vec<_> = self
                .remap
                .values()
                .filter_map(|ni| {
                    let ni = self.graph[ni.as_global()].local_addr();
                    self.states.get(ni).map(move |s| (ni, !s.is_useful()))
                })
                .filter(|&(_, x)| x)
                .collect();
            for (ni, _) in unused {
                self.states.remove(ni);
            }

            // we're now committing to testing this op
            // add all nodes to the same domain
            for node in self.graph.node_weights_mut() {
                if node.is_source() {
                    continue;
                }
                node.add_to(0.into());
            }
            // store the id
            self.nut = Some(ip);
            // and also set up the node list
            let mut nodes = vec![];
            let mut topo = petgraph::visit::Topo::new(&self.graph);
            while let Some(node) = topo.next(&self.graph) {
                if node == self.source {
                    continue;
                }
                let n = self.graph[node].take();
                let n = n.finalize(&self.graph);
                nodes.push((node, n));
            }

            self.nodes = nodes
                .into_iter()
                .map(|(_, n)| (n.local_addr(), cell::RefCell::new(n)))
                .collect();
        }

        pub fn seed(&mut self, base: IndexPair, data: Vec<DataType>) {
            assert!(self.nut.is_some(), "seed must happen after set_op");

            // base here is some identifier that was returned by Self::add_base.
            // which means it's a global address (and has to be so that it will correctly refer to
            // ancestors pre on_commit). we need to translate it into a local address.
            // since we set up the graph, we actually know that the NodeIndex is simply one greater
            // than the local index (since bases are added first, and assigned local + global
            // indices in order, but global ids are prefixed by the id of the source node).

            // no need to call on_input since base tables just forward anyway

            // if the base node has state, keep it
            if let Some(ref mut state) = self.states.get_mut(*base) {
                state.process_records(&mut vec![data].into(), None, None);
            } else {
                panic!(
                    "unnecessary seed value for {} (never used by any node)",
                    base.as_global().index()
                );
            }
        }

        #[allow(dead_code)]
        pub fn unseed(&mut self, base: IndexPair) {
            assert!(self.nut.is_some(), "unseed must happen after set_op");
            let global = self.nut.unwrap().as_global();
            let idx = self.graph[global].suggest_indexes(global);
            let mut state = MemoryState::default();
            for (tbl, suggested_index) in idx {
                if tbl == base.as_global() {
                    match suggested_index {
                        SuggestedIndex::Strict(index) => state.add_key(&index, None),
                        SuggestedIndex::Weak(index) => state.add_weak_key(&index),
                    }
                }
            }

            self.states.insert(*base, Box::new(state));
        }

        pub fn input_raw<U: Into<Records>>(
            &mut self,
            src: IndexPair,
            u: U,
            replay: ReplayContext,
            remember: bool,
        ) -> RawProcessingResult {
            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(*self.nut.unwrap()));

            struct Ex;

            impl Executor for Ex {
                fn send(&mut self, _: ReplicaAddr, _: Box<Packet>) {}
            }

            let tag = replay.tag();
            let mut res = {
                let id = self.nut.unwrap();
                let mut n = self.nodes[*id].borrow_mut();
                n.as_mut_internal()
                    .unwrap()
                    .on_input_raw(&mut Ex, *src, u.into(), replay, &self.nodes, &self.states)
                    .unwrap()
            };

            if !remember || !self.states.contains_key(*self.nut.unwrap()) {
                return res;
            }

            if let RawProcessingResult::Regular(ref mut res) = &mut res {
                node::materialize(
                    &mut res.results,
                    None,
                    tag,
                    self.states.get_mut(*self.nut.unwrap()),
                );
            }

            res
        }

        pub fn input<U: Into<Records>>(
            &mut self,
            src: IndexPair,
            u: U,
            remember: bool,
        ) -> ProcessingResult {
            match self.input_raw(src, u, ReplayContext::None, remember) {
                RawProcessingResult::Regular(res) => res,
                _ => unreachable!(),
            }
        }

        pub fn one<U: Into<Records>>(&mut self, src: IndexPair, u: U, remember: bool) -> Records {
            self.input(src, u, remember).results
        }

        pub fn one_row<R: Into<Record>>(
            &mut self,
            src: IndexPair,
            d: R,
            remember: bool,
        ) -> Records {
            self.one::<Record>(src, d.into(), remember)
        }

        pub fn narrow_one<U: Into<Records>>(&mut self, u: U, remember: bool) -> Records {
            let src = self.narrow_base_id();
            self.one::<Records>(src, u.into(), remember)
        }

        pub fn narrow_one_row<R: Into<Record>>(&mut self, d: R, remember: bool) -> Records {
            self.narrow_one::<Record>(d.into(), remember)
        }

        pub fn node(&self) -> cell::Ref<Node> {
            self.nodes[*self.nut.unwrap()].borrow()
        }

        pub fn narrow_base_id(&self) -> IndexPair {
            assert_eq!(self.remap.len(), 2 /* base + nut */);
            *self
                .remap
                .values()
                .skip_while(|&n| n.as_global() == self.nut.unwrap().as_global())
                .next()
                .unwrap()
        }
    }
}
