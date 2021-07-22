use derive_more::From;
use noria::KeyComparison;
use slog::Logger;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::ops::Bound;
use vec1::Vec1;

use crate::ops;
use crate::prelude::*;
use noria::errors::ReadySetResult;

// TODO: make a Key type that is an ArrayVec<DataType>

#[derive(PartialEq, Eq, Debug, From)]
pub(crate) enum MissRecord {
    /// A miss on a point query
    Point(Vec1<DataType>),
    /// A miss on a range query
    Range((Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)),
}

impl MissRecord {
    /// Project the values in `columns` out of this `MissRecord` into a [`KeyComparison`]
    ///
    /// # Panics
    ///
    /// * Panics if any of the columns passed in exceed the length of the record.
    /// * Panics if columns is empty.
    pub fn project_key(&self, columns: &[usize]) -> KeyComparison {
        let project_rec = move |rec: &Vec1<DataType>| {
            #[allow(clippy::expect_used)] // Documented invariant.
            Vec1::try_from_vec(
                columns
                    .iter()
                    .map(|i| {
                        #[allow(clippy::indexing_slicing)] // Documented invariant.
                        rec[*i].clone()
                    })
                    .collect(),
            )
            .expect("Empty key columns")
        };
        match self {
            Self::Point(rec) => KeyComparison::Equal(project_rec(rec)),
            Self::Range((lower, upper)) => KeyComparison::Range((
                lower.as_ref().map(|r| project_rec(r)),
                upper.as_ref().map(|r| project_rec(r)),
            )),
        }
    }
}

impl TryFrom<Vec<DataType>> for MissRecord {
    type Error = vec1::Size0Error;

    fn try_from(value: Vec<DataType>) -> Result<Self, Self::Error> {
        Ok(Self::Point(value.try_into()?))
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Miss {
    /// The node we missed when looking up into.
    pub(crate) on: LocalNodeIndex,
    /// The columns of `on` we were looking up on.
    pub(crate) lookup_idx: Vec<usize>,
    /// The columns of `record` we were using for the lookup.
    /// Invariant: lookup_cols cannot contain a column index that exceeds record.len()
    pub(crate) lookup_cols: Vec<usize>,
    /// The columns of `record` that identify the replay key (if any).
    /// Invariant: replay_cols cannot contain a column index that exceeds record.len()
    pub(crate) replay_cols: Option<Vec<usize>>,
    /// The record we were processing when we missed.
    pub(crate) record: MissRecord,
}

impl Miss {
    pub(crate) fn replay_key(&self) -> Option<KeyComparison> {
        self.replay_cols
            .as_ref()
            .map(|cols| self.record.project_key(cols))
    }

    pub(crate) fn lookup_key(&self) -> KeyComparison {
        self.record.project_key(&self.lookup_cols[..])
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Lookup {
    /// The node we looked up into.
    pub(crate) on: LocalNodeIndex,
    /// The columns of `on` we were looking up on.
    pub(crate) cols: Vec<usize>,
    /// The key used for the lookup.
    pub(crate) key: KeyComparison,
}

#[derive(Default, Debug)]
pub(crate) struct ProcessingResult {
    pub(crate) results: Records,
    pub(crate) misses: Vec<Miss>,

    /// Lookups performed during processing.
    ///
    /// NOTE: Only populated if the processed update was an upquery response.
    pub(crate) lookups: Vec<Lookup>,
}

#[derive(Debug)]
pub(crate) enum RawProcessingResult {
    Regular(ProcessingResult),
    FullReplay(Records, bool),
    CapturedFull,
    ReplayPiece {
        rows: Records,
        keys: HashSet<KeyComparison>,
        captured: HashSet<KeyComparison>,
    },
}

#[derive(Debug)]
pub(crate) enum ReplayContext<'a> {
    None,
    Partial {
        key_cols: &'a [usize],
        keys: &'a HashSet<KeyComparison>,
        requesting_shard: usize,
        tag: Tag,
        unishard: bool,
    },
    Full {
        last: bool,
    },
}

impl<'a> ReplayContext<'a> {
    pub(crate) fn key(&self) -> Option<&'a [usize]> {
        if let ReplayContext::Partial { key_cols, .. } = *self {
            Some(key_cols)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub(crate) fn tag(&self) -> Option<Tag> {
        match self {
            Self::Partial { tag, .. } => Some(*tag),
            _ => None,
        }
    }
}

/// A reference to one or more of a node's columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnRef {
    /// The index of the referenced node.
    pub node: NodeIndex,
    /// The referenced column indices.
    pub columns: Vec1<usize>,
}

/// A miss on a node's column(s).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMiss {
    /// Which columns the miss occurred on.
    pub missed_columns: ColumnRef,
    /// The keys that we missed on.
    pub missed_key: Vec1<KeyComparison>,
}

/// A description of where some node columns come from, used by the materialization planner
/// to build replay paths for the columns.
///
/// This API operates on the *index* level, not the individual column level; indexing e.g. a join
/// by `[0, 1]` when columns 0 and 1 come from different parents is a very different thing to
/// indexing by `[0]` and `[1]` separately, because the replay logic has to change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnSource {
    /// These columns are an exact copy of those referenced in the [`ColumnRef`].
    /// A miss on these columns can therefore be 1:1 mapped to a miss on the referenced columns.
    ExactCopy(ColumnRef),
    /// These columns are part of a union node.
    /// A miss on these columns should trigger multiple replays through this node through all of
    /// the referenced columns.
    Union(Vec1<ColumnRef>),
    /// The node does some internal logic to generate these columns from one or more parents,
    /// referenced by [`ColumnRef`]s.
    ///
    /// Replay paths will be built for each reference provided here, and replay paths for the
    /// generated columns on this node will terminate at this node.
    ///
    /// **NOTE:** A miss on these columns will result in a call to [`Ingredient::handle_upquery`].
    /// You MUST read and understand the API docs for that function before returning this from a new
    /// ingredient implementation.
    GeneratedFromColumns(Vec1<ColumnRef>),
    /// This column has no clear mapping to a set of parent columns; misses on this column require
    /// a full replay from all base tables reachable through the given parents to resolve.
    ///
    /// NOTE: this always forces full materialization currently.
    RequiresFullReplay(Vec1<NodeIndex>),
}

impl ColumnSource {
    /// Helper function to make a [`ColumnSource::ExactCopy`] with the provided information.
    pub fn exact_copy(index: NodeIndex, cols: Vec1<usize>) -> Self {
        ColumnSource::ExactCopy(ColumnRef {
            node: index,
            columns: cols,
        })
    }
}

pub(crate) trait Ingredient
where
    Self: Send,
{
    /// Construct a new node from this node that will be given to the domain running this node.
    /// Whatever is left behind in self is what remains observable in the graph.
    fn take(&mut self) -> ops::NodeOperator;

    fn ancestors(&self) -> Vec<NodeIndex>;

    /// Dictate which parents are replayed through in the case of full materialization.
    ///
    /// The materialization planner will generate replay paths through all of the nodes supplied
    /// by this API when trying to fully materialize this node. If this API returns `None`, all
    /// of the node's parents are used.
    ///
    /// (The meaning of this API has changed slightly; we now replay through *all* parents returned
    /// here, instead of choosing one.)
    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        None
    }

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    ///
    /// Note that a vector of length > 1 for any one node means that that node should be given a
    /// *compound* key, *not* that multiple columns should be independently indexed.
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, Index>;

    /// Provide information about where the `cols` come from for the materialization planner
    /// (among other things) to make use of. (See the [`ColumnSource`] docs for more.)
    ///
    /// This is used to generate replay paths for [`Ingredient::handle_upquery`], so if you need
    /// paths to be present for that function to work, you must declare a correct source here.
    ///
    /// **NOTE:** You MUST ensure that the returned [`ColumnSource`], if it is a
    /// [`ColumnSource::ExactCopy`] or [`ColumnSource::Union`], does not change the length of `cols`
    /// -- i.e. if `column_source` was called with a slice of length 1, the `columns` field in any
    /// [`ColumnRef`]s returned by this functio must also have length 1, unless the columns are
    /// generated.
    ///
    /// (The above invariant is checked with an assertion in debug builds only.)
    ///
    /// (This replaces the old `parent_columns` and `resolve` APIs, which are now gone.)
    fn column_source(&self, cols: &[usize]) -> ColumnSource;

    /// Handle a miss on some columns that were marked as generated by the node's
    /// [`Ingredient::column_source`] implementation. Using this function is complicated, so read
    /// this doc comment if you plan on doing so!
    ///
    /// This function is called if and only if an upquery is made against a set of columns that
    /// returned [`ColumnSource::GeneratedFromColumns`] when passed to the [`column_source`] API.
    ///
    /// **NOTE:** The returned misses MUST only reference nodes and columns returned by the
    /// [`column_source`] API.
    ///
    /// ## Handling the responses to this API
    ///
    /// The responses to the upqueries generated through this API come through
    /// [`Ingredient::on_input`] as normal.
    ///
    /// Ingredients SHOULD buffer these responses internally, and MUST only release records from
    /// `on_input` when all of the upquery responses have been received (at which point the records
    /// will be materialized and sent as an upquery response from this ingredient, as usual).
    ///
    /// **NOTE:** Ingredients MUST NOT miss while processing responses to this API (doing so is
    /// a hard error). This restriction may be relaxed later.
    ///
    /// ## How this API works internally
    ///
    /// The response to the `column_source` API call is used to generate replay paths when the
    /// materialization planner is run, and also marks the column index as generated.
    ///
    /// Column indices marked as generated result in a call to this function when upqueries are
    /// received for the generated columns. The replay paths set up earlier are then filtered
    /// to only include those referenced by the returned [`ColumnMiss`] objects, and the key for
    /// the upqueries along these paths is changed to that specified in the [`ColumnMiss`].
    fn handle_upquery(&mut self, _miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        Ok(vec![])
    }

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
    ///    σ    |  Filter
    ///    π    |  Projection
    ///    ≡    |  Identity
    ///    T    |  Trigger
    fn description(&self, detailed: bool) -> String;

    /// Provide measurements of transient internal state that may be useful in debugging contexts.
    ///
    /// For example, a union might use this to report if it has captured any replays that it has
    /// not yet released.
    ///
    /// The default implementation returns `null`.
    fn probe(&self) -> HashMap<String, String> {
        Default::default()
    }

    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet.
    fn on_connected(&mut self, _graph: &Graph) {}

    /// Called when a domain is finalized and is about to be booted.
    ///
    /// The provided arguments give mappings from global to local addresses.
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>);

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    #[allow(clippy::too_many_arguments)]
    fn on_input(
        &mut self,
        executor: &mut dyn Executor,
        from: LocalNodeIndex,
        data: Records,
        replay_key_cols: Option<&[usize]>,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ReadySetResult<ProcessingResult>;

    #[allow(clippy::too_many_arguments)]
    fn on_input_raw(
        &mut self,
        executor: &mut dyn Executor,
        from: LocalNodeIndex,
        data: Records,
        replay: ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
        _: &Logger,
    ) -> ReadySetResult<RawProcessingResult> {
        Ok(RawProcessingResult::Regular(self.on_input(
            executor,
            from,
            data,
            replay.key(),
            domain,
            states,
        )?))
    }

    /// Triggered whenever a replay occurs, to allow the operator to react evict from any auxillary
    /// state other than what is stored in its materialization.
    fn on_eviction(&mut self, _from: LocalNodeIndex, _tag: Tag, _keys: &[KeyComparison]) {}

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
    ) -> Option<Option<Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a>>> {
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
    ) -> Option<Option<Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a>>> {
        match states.get(parent) {
            Some(state) => match state.lookup(columns, key) {
                LookupResult::Some(rs) => Some(Some(Box::new(rs.into_iter().map(Ok)) as Box<_>)),
                LookupResult::Missing => Some(None),
            },
            None => {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                #[allow(clippy::indexing_slicing)] // Node must exist to have gotten here.
                let parent = nodes[parent].borrow();

                if parent.is_internal() {
                    parent.query_through(columns, key, nodes, states)
                } else {
                    None
                }
            }
        }
    }

    /// Performance hint: should return true if this operator reduces the size of its input
    fn is_selective(&self) -> bool {
        false
    }

    /// Returns true if this operator requires a full materialization
    fn requires_full_materialization(&self) -> bool {
        false
    }
}
