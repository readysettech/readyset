use std::collections::{HashSet, HashMap};

use ops::base::Base;
use ops;
use flow::prelude;

#[derive(PartialEq, Eq, Debug)]
pub struct Miss {
    pub node: prelude::LocalNodeIndex,
    pub columns: Vec<usize>,
    pub replay_key: Option<Vec<prelude::DataType>>,
    pub key: Vec<prelude::DataType>,
}

pub struct ProcessingResult {
    pub results: prelude::Records,
    pub misses: Vec<Miss>,
}

pub enum RawProcessingResult {
    Regular(ProcessingResult),
    FullReplay(prelude::Records, bool),
    ReplayPiece(prelude::Records, HashSet<Vec<prelude::DataType>>),
    Captured,
}

pub enum ReplayContext {
    None,
    Partial {
        key_col: usize,
        keys: HashSet<Vec<prelude::DataType>>,
    },
    Full { last: bool },
}

impl ReplayContext {
    fn key(&self) -> Option<usize> {
        if let ReplayContext::Partial { key_col, .. } = *self {
            Some(key_col)
        } else {
            None
        }
    }
}

pub trait Ingredient
where
    Self: Send,
{
    /// Construct a new node from this node that will be given to the domain running this node.
    /// Whatever is left behind in self is what remains observable in the graph.
    fn take(&mut self) -> ops::NodeOperator;

    fn ancestors(&self) -> Vec<prelude::NodeIndex>;

    /// May return a set of nodes such that *one* of the given ancestors *must* be the one to be
    /// replayed if this node's state is to be initialized.
    fn must_replay_among(&self) -> Option<HashSet<prelude::NodeIndex>> {
        None
    }

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    ///
    /// Note that a vector of length > 1 for any one node means that that node should be given a
    /// *compound* key, *not* that multiple columns should be independently indexed. The bool in
    /// the return value specifies if the node wants to do *lookups* on that key; false would imply
    /// that this index will only be used for partial replay.
    fn suggest_indexes(
        &self,
        you: prelude::NodeIndex,
    ) -> HashMap<prelude::NodeIndex, (Vec<usize>, bool)>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, i: usize) -> Option<Vec<(prelude::NodeIndex, usize)>>;

    /// Returns a reference to the underlying Base node (if any)
    fn get_base(&self) -> Option<&Base> {
        None
    }

    /// Returns a mutable reference to the underlying Base node (if any)
    fn get_base_mut(&mut self) -> Option<&mut Base> {
        None
    }

    fn is_join(&self) -> bool {
        false
    }

    /// Produce a compact, human-readable description of this node.
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
    fn description(&self) -> String;

    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet.
    fn on_connected(&mut self, graph: &prelude::Graph);

    /// Called when a domain is finalized and is about to be booted.
    ///
    /// The provided arguments give mappings from global to local addresses.
    fn on_commit(
        &mut self,
        you: prelude::NodeIndex,
        remap: &HashMap<prelude::NodeIndex, prelude::IndexPair>,
    );

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    fn on_input(
        &mut self,
        from: prelude::LocalNodeIndex,
        data: prelude::Records,
        tracer: &mut prelude::Tracer,
        replay_key_col: Option<usize>,
        domain: &prelude::DomainNodes,
        states: &prelude::StateMap,
    ) -> ProcessingResult;

    fn on_input_raw(
        &mut self,
        from: prelude::LocalNodeIndex,
        data: prelude::Records,
        tracer: &mut prelude::Tracer,
        replay: &ReplayContext,
        domain: &prelude::DomainNodes,
        states: &prelude::StateMap,
    ) -> RawProcessingResult {
        RawProcessingResult::Regular(self.on_input(
            from,
            data,
            tracer,
            replay.key(),
            domain,
            states,
        ))
    }

    fn can_query_through(&self) -> bool {
        false
    }

    fn query_through<'a>(
        &self,
        _columns: &[usize],
        _key: &prelude::KeyType<prelude::DataType>,
        _states: &'a prelude::StateMap,
    ) -> Option<Option<Box<Iterator<Item = &'a [prelude::DataType]> + 'a>>> {
        None
    }

    /// Look up the given key in the given parent's state, falling back to query_through if
    /// necessary. The return values signifies:
    ///
    ///  - `None` => no materialization of the parent state exists
    ///  - `Some(None)` => materialization exists, but lookup got a miss
    ///  - `Some(Some(rs))` => materialization exists, and got results rs
    fn lookup<'a>(
        &self,
        parent: prelude::LocalNodeIndex,
        columns: &[usize],
        key: &prelude::KeyType<prelude::DataType>,
        domain: &prelude::DomainNodes,
        states: &'a prelude::StateMap,
    ) -> Option<Option<Box<Iterator<Item = &'a [prelude::DataType]> + 'a>>> {
        states
            .get(&parent)
            .and_then(move |state| match state.lookup(columns, key) {
                prelude::LookupResult::Some(rs) => Some(
                    Some(Box::new(rs.iter().map(|r| &r[..])) as Box<_>),
                ),
                prelude::LookupResult::Missing => Some(None),
            })
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                let parent = domain.get(&parent).unwrap().borrow();
                if parent.is_internal() {
                    parent.query_through(columns, key, states)
                } else {
                    None
                }
            })
    }

    // Translate a column in this ingredient into the corresponding column(s) in
    // parent ingredients. None for the column means that the parent doesn't
    // have an associated column. Similar to resolve, but does not depend on
    // materialization, and returns results even for computed columns.
    fn parent_columns(&self, column: usize) -> Vec<(prelude::NodeIndex, Option<usize>)>;

    /// Performance hint: should return true if this operator reduces the size of its input
    fn is_selective(&self) -> bool {
        false
    }
}
