use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use ops;
use prelude::*;

// TODO: make a Key type that is an ArrayVec<DataType>

#[derive(PartialEq, Eq, Debug)]
crate struct Miss {
    /// The node we missed when looking up into.
    crate on: LocalNodeIndex,
    /// The columns of `on` we were looking up on.
    crate lookup_idx: Vec<usize>,
    /// The columns of `record` we were using for the lookup.
    crate lookup_cols: Vec<usize>,
    /// The columns of `record` that identify the replay key (if any).
    crate replay_cols: Option<Vec<usize>>,
    /// The record we were processing when we missed.
    crate record: Vec<DataType>,
}

impl Miss {
    crate fn replay_key<'a>(&'a self) -> Option<impl Iterator<Item = &DataType> + 'a> {
        self.replay_cols
            .as_ref()
            .map(move |rc| rc.iter().map(move |&rc| &self.record[rc]))
    }

    crate fn replay_key_vec(&self) -> Option<Vec<DataType>> {
        self.replay_cols
            .as_ref()
            .map(|rc| rc.iter().map(|&rc| &self.record[rc]).cloned().collect())
    }

    crate fn lookup_key<'a>(&'a self) -> impl Iterator<Item = &DataType> + 'a {
        self.lookup_cols.iter().map(move |&rc| &self.record[rc])
    }

    crate fn lookup_key_vec(&self) -> Vec<DataType> {
        self.lookup_cols
            .iter()
            .map(|&rc| &self.record[rc])
            .cloned()
            .collect()
    }
}

crate struct ProcessingResult {
    pub(crate) results: Records,
    pub(crate) misses: Vec<Miss>,
}

crate enum RawProcessingResult {
    Regular(ProcessingResult),
    FullReplay(Records, bool),
    CapturedFull,
    ReplayPiece {
        rows: Records,
        keys: HashSet<Vec<DataType>>,
        captured: HashSet<Vec<DataType>>,
    },
}

#[derive(Debug)]
crate enum ReplayContext {
    None,
    Partial {
        key_cols: Vec<usize>,
        keys: HashSet<Vec<DataType>>,
    },
    Full {
        last: bool,
    },
}

impl ReplayContext {
    fn key(&self) -> Option<&[usize]> {
        if let ReplayContext::Partial { ref key_cols, .. } = *self {
            Some(&key_cols[..])
        } else {
            None
        }
    }
}

crate trait Ingredient
where
    Self: Send,
{
    /// Construct a new node from this node that will be given to the domain running this node.
    /// Whatever is left behind in self is what remains observable in the graph.
    fn take(&mut self) -> ops::NodeOperator;

    fn ancestors(&self) -> Vec<NodeIndex>;

    /// May return a set of nodes such that *one* of the given ancestors *must* be the one to be
    /// replayed if this node's state is to be initialized.
    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        None
    }

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    ///
    /// Note that a vector of length > 1 for any one node means that that node should be given a
    /// *compound* key, *not* that multiple columns should be independently indexed. The bool in
    /// the return value specifies if the node wants to do *lookups* on that key; false would imply
    /// that this index will only be used for partial replay.
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>>;

    fn is_join(&self) -> bool {
        false
    }

    /// Produce a compact, human-readable description of this node for Graphviz.
    ///
    /// If `detailed` is true, emit more info.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    fn description(&self, detailed: bool) -> String;

    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet.
    fn on_connected(&mut self, graph: &Graph);

    /// Called when a domain is finalized and is about to be booted.
    ///
    /// The provided arguments give mappings from global to local addresses.
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>);

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    #[allow(clippy::too_many_arguments)]
    fn on_input(
        &mut self,
        executor: &mut Executor,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        replay_key_cols: Option<&[usize]>,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ProcessingResult;

    #[allow(clippy::too_many_arguments)]
    fn on_input_raw(
        &mut self,
        executor: &mut Executor,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        replay: &ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> RawProcessingResult {
        RawProcessingResult::Regular(self.on_input(
            executor,
            from,
            data,
            tracer,
            replay.key(),
            domain,
            states,
        ))
    }

    /// Triggered whenever a replay occurs, to allow the operator to react evict from any auxillary
    /// state other than what is stored in its materialization.
    fn on_eviction(
        &mut self,
        _from: LocalNodeIndex,
        _key_columns: &[usize],
        _keys: &mut Vec<Vec<DataType>>,
    ) {
    }

    fn can_query_through(&self) -> bool {
        false
    }

    #[allow(clippy::type_complexity)]
    #[allow(clippy::option_option)]
    fn query_through<'a>(
        &self,
        _columns: &[usize],
        _key: &KeyType,
        _nodes: &DomainNodes,
        _states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        None
    }

    /// Look up the given key in the given parent's state, falling back to query_through if
    /// necessary. The return values signifies:
    ///
    ///  - `None` => no materialization of the parent state exists
    ///  - `Some(None)` => materialization exists, but lookup got a miss
    ///  - `Some(Some(rs))` => materialization exists, and got results rs
    #[allow(clippy::type_complexity)]
    #[allow(clippy::option_option)]
    fn lookup<'a>(
        &self,
        parent: LocalNodeIndex,
        columns: &[usize],
        key: &KeyType,
        nodes: &DomainNodes,
        states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        states
            .get(parent)
            .and_then(move |state| match state.lookup(columns, key) {
                LookupResult::Some(rs) => Some(Some(Box::new(rs.into_iter()) as Box<_>)),
                LookupResult::Missing => Some(None),
            })
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                let parent = nodes[parent].borrow();
                if parent.is_internal() {
                    parent.query_through(columns, key, nodes, states)
                } else {
                    None
                }
            })
    }

    /// Translate a column in this ingredient into the corresponding column(s) in
    /// parent ingredients. None for the column means that the parent doesn't
    /// have an associated column. Similar to resolve, but does not depend on
    /// materialization, and returns results even for computed columns.
    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)>;

    /// Performance hint: should return true if this operator reduces the size of its input
    fn is_selective(&self) -> bool {
        false
    }

    /// Returns true if this operator requires a full materialization
    fn requires_full_materialization(&self) -> bool {
        false
    }
}
