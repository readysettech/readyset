use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};
use std::{iter, mem};

use derive_more::From;
use launchpad::Indices;
use noria::KeyComparison;
use noria_errors::ReadySetResult;
use serde::{Deserialize, Serialize};
use vec1::Vec1;

use crate::ops;
use crate::prelude::*;

// TODO: make a Key type that is an ArrayVec<DataType>

/// Indication, for a [`Miss`], for how to derive the replay key that was being processed during
/// that miss
#[derive(PartialEq, Eq, Debug, Clone, From)]
pub(crate) enum MissReplayKey {
    /// A point replay was being performed, meaning we can get the values for the key out of the
    /// miss record with these column indices
    ///
    /// # Invariants
    ///
    /// * The column indices here must be in-bounds for the miss's [`record`][Miss::record]
    /// * The column indices cannot be empty
    RecordColumns(Vec<usize>),

    /// A range replay was being performed, with the given range key
    ///
    /// # Invariants
    ///
    /// * The endpoints of the range must be the same length
    /// * The endpoints of the range may not be empty
    Range((Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)),
}

/// Indication, for a [`Miss`], for how to derive the key that was used for the lookup that resulted
/// in the miss.
#[derive(PartialEq, Eq, Debug, Clone, From)]
pub(crate) enum MissLookupKey {
    /// The columns of the miss's [`record`][] we were using for a point lookup
    ///
    /// # Invariants:
    ///
    /// * These column indices cannot contain a column index that exceeds record.len()
    /// * These column indices must be the same len as lookup_idx
    /// * These column indices cannot be empty
    ///
    /// [`record`]: Miss::record
    RecordColumns(Vec<usize>),

    /// A key was used for a lookup other than a direct point lookup of the cols in the miss's
    /// [`record`]
    ///
    /// [`record`]: Miss::record
    Key(KeyComparison),
}

/// A representation of a miss that occurs during processing of dataflow.
///
/// Misses are constructed, using [`Miss::builder`] by implementations of [`Ingredient`], and
/// returned as part of [`ProcessingResult`] from [`on_input`] or [`on_input_raw`], as a way of
/// recording that during forward-processing of records we attempted to perform a lookup into
/// *partial* state and encountered a hole. The [`Domain`] then uses misses differently depending on
/// the context:
///
/// * If a miss occurs during a replay, we pause that replay, issue a new "recursive" upquery to
///   fill the keys that we missed on, then re-process the replay.
/// * If a miss occurs during normal forward processing of writes, we normally drop the write (since
///   that means the write is to a key that has never been replayed), *unless*:
///   * If the node that missed is a join (eg [`Ingredient::is_join`] returns `true`), we use the
///     [`record`] stored in the miss to generate eviction messages for all replay paths downstream
///     of the join - in short, this is because joins do lookups into their parents using keys other
///     than the replay key and so might have a miss where a record wouldn't hit a hole downstream -
///     but see [note: downstream-join-evictions] for more information. Note that this downstream
///     eviction process only happens for normal writes, since during replays we can just upquery
///     for the keys we missed in.
///
/// [`on_input`]: Ingredient::on_input
/// [`on_input`]: Ingredient::on_input_raw
/// [`Domain`]: noria_dataflow::Domain
/// [`record`]: Miss::record
#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Miss {
    /// The node we missed when looking up into.
    pub(crate) on: LocalNodeIndex,
    /// The columns of `on` we were looking up on.
    pub(crate) lookup_idx: Vec<usize>,
    /// The key that we used to do the lookup that resulted in this miss
    pub(crate) lookup_key: MissLookupKey,
    /// The replay key that was being processed during the lookup (if any)
    pub(crate) replay_key: Option<MissReplayKey>,
    /// The record we were processing when we missed.
    pub(crate) record: Vec<DataType>,
}

/// A builder for [`Miss`]es.
///
/// Create a [`MissBuilder`] by calling [`Miss::builder`].
#[derive(Default)]
pub(crate) struct MissBuilder<'a> {
    on: Option<LocalNodeIndex>,
    lookup_idx: Option<Vec<usize>>,
    lookup_key: Option<MissLookupKey>,
    replay: Option<&'a ReplayContext<'a>>,
    replay_key_cols: Option<&'a [usize]>,
    record: Option<Vec<DataType>>,
}

impl<'a> MissBuilder<'a> {
    /// Set the value for [`Miss::on`]
    pub(crate) fn on(&mut self, on: LocalNodeIndex) -> &mut Self {
        self.on = Some(on);
        self
    }

    /// Set the value for [`Miss::lookup_idx`]
    pub(crate) fn lookup_idx(&mut self, lookup_idx: Vec<usize>) -> &mut Self {
        self.lookup_idx = Some(lookup_idx);
        self
    }

    /// Set the value for [`Miss::lookup_key`]
    pub(crate) fn lookup_key(&mut self, lookup_key: impl Into<MissLookupKey>) -> &mut Self {
        self.lookup_key = Some(lookup_key.into());
        self
    }

    /// Set the replay context for the miss.
    ///
    /// If the provided value is [`ReplayContext::Partial`], the miss's [`replay_key`] will be built
    /// using the `key_cols` and `keys` of that partial replay. Setting [`replay_key_cols`] will
    /// override the `key_cols` in the replay context.
    pub(crate) fn replay(&mut self, replay: &'a ReplayContext) -> &mut Self {
        self.replay = Some(replay);
        self
    }

    /// Override the replay key columns for this miss, for nodes (like joins) that generate columns
    /// at different indices than the records they receive.
    ///
    /// Ignored if [`replay`] is not set to a [`ReplayContext::Partial`].
    pub(crate) fn replay_key_cols(&mut self, key_cols: Option<&'a [usize]>) -> &mut Self {
        self.replay_key_cols = key_cols;
        self
    }

    /// Set the value for [`Miss::record`].
    pub(crate) fn record(&mut self, record: Vec<DataType>) -> &mut Self {
        self.record = Some(record);
        self
    }

    /// Build the [`Miss`].
    ///
    /// # Panics
    ///
    /// * Panics if any of [`Self::on`], [`Self::lookup_idx`], [`Self::lookup_key`],
    ///   [`Self::replay`], or [`Self::record`] have not been called.
    /// * Panics if the fields of the replay cols are out-of-bounds for the record.
    pub(crate) fn build(&mut self) -> Miss {
        let record = self.record.take().unwrap();
        let replay_key = match self.replay.take().unwrap() {
            ReplayContext::Partial { key_cols, keys, .. } => {
                let replay_key_cols = self.replay_key_cols.take().unwrap_or(*key_cols);
                // Does `keys` contain a range that covers `record`?
                // Since we unfortunately have to do some cloning to answer that question due to the
                // limiting type signature of `RangeBounds::contains`, avoid doing that cloning if
                // we don't find any ranges by inserting into a memo (`record_key_memo`).
                let mut record_key_memo = None;
                let range = keys
                    .iter()
                    .filter_map(|k| k.range())
                    // NOTE: Since overlapping range queries will be deduplicated by the domain, we
                    // can be assured that we will find at most one range that covers our record
                    // here.
                    .find(|(lower, upper)| {
                        (
                            lower.as_ref().map(|b| b.as_ref()),
                            upper.as_ref().map(|b| b.as_ref()),
                        )
                            .contains(record_key_memo.get_or_insert_with(
                                // TODO(grfn): This clone shouldn't be necessary, but comparing
                                // Vec<&DataType> with &Vec<DataType> is surprisingly difficult
                                || {
                                    record
                                        .cloned_indices(replay_key_cols.iter().copied())
                                        .unwrap()
                                },
                            ))
                    });
                Some(match range {
                    Some(k) => MissReplayKey::Range(k.clone()),
                    None => MissReplayKey::RecordColumns(replay_key_cols.to_vec()),
                })
            }
            _ => None,
        };

        Miss {
            on: self.on.take().unwrap(),
            lookup_idx: self.lookup_idx.take().unwrap(),
            lookup_key: self.lookup_key.take().unwrap(),
            replay_key,
            record,
        }
    }
}

impl Miss {
    /// Construct a new [`MissBuilder`] to create a new miss
    pub(crate) fn builder<'a>() -> MissBuilder<'a> {
        MissBuilder::default()
    }

    /// Return a reference to the keys for the replay that were being performed during the miss, if
    /// any
    #[allow(clippy::unwrap_used)] // invariants on the fields
    pub(crate) fn replay_key(&self) -> Option<KeyComparison> {
        self.replay_key.as_ref().map(|rk| match rk {
            MissReplayKey::RecordColumns(cols) => self
                .record
                .cloned_indices(cols.iter().copied())
                .unwrap()
                .try_into()
                .unwrap(),
            MissReplayKey::Range((lower, upper)) => {
                KeyComparison::Range((lower.clone(), upper.clone()))
            }
        })
    }

    /// Return a reference to the key used to perform the lookup that resulted in this miss
    #[allow(clippy::unwrap_used)] // invariants on the fields
    pub(crate) fn lookup_key(&self) -> Cow<KeyComparison> {
        match &self.lookup_key {
            MissLookupKey::Key(lk) => Cow::Borrowed(lk),
            MissLookupKey::RecordColumns(cols) => Cow::Owned(
                self.record
                    .cloned_indices(cols.iter().copied())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
        }
    }
}

/// Which kind of index to use when performing lookups as part of processing for an Ingredient.
///
/// The variants of this enum correspond to the variants of the [`LookupIndex`] enum - see the
/// documentation for that enum, and [the documentation for the State
/// trait](trait@crate::state::State) for more information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LookupMode {
    /// Lookup into a strict index, returning either *all* rows for the given key, or a miss.
    Strict,

    /// Lookup into a weak index, returning all rows for a given key that are materialized into
    /// other strict indices in the same node.
    ///
    /// This is not automatically enforced, but it is invalid to perform lookups with
    /// [`LookupMode::Weak`] when processing replays, since replays must represent a total set of
    /// rows for the replay key, and weak indices may be missing rows
    Weak,
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

    /// Returns `true` if the replay context is [`Partial`].
    ///
    /// [`Partial`]: ReplayContext::Partial
    pub(crate) fn is_partial(&self) -> bool {
        matches!(self, Self::Partial { .. })
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
    /// You MUST read and understand the API docs for that function before returning this from a
    /// new ingredient implementation.
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

/// A description of an index that can be used to do a lookup directly into a node.
///
/// For more information about the different kinds of indices, see [the documentation for the State
/// trait](trait@crate::state::State)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum LookupIndex {
    /// A regular (strict) index
    Strict(Index),

    /// A weak index
    ///
    /// Because lookups into weak indices during replays are forbidden, a request for a weak index
    /// will *also* create a [`Strict`] index with the same index type and columns.
    Weak(Index),
}

#[allow(clippy::len_without_is_empty)]
impl LookupIndex {
    /// Return a reference to the underlying [`Index`]
    pub fn index(&self) -> &Index {
        match self {
            LookupIndex::Strict(idx) => idx,
            LookupIndex::Weak(idx) => idx,
        }
    }

    /// Convert this LookupIndex into the underlying index, discarding information about whether
    /// it's weak or strict
    pub fn into_index(self) -> Index {
        match self {
            LookupIndex::Strict(idx) => idx,
            LookupIndex::Weak(idx) => idx,
        }
    }

    /// Return a reference to the set of columns in the underlying [`Index`]
    pub fn columns(&self) -> &[usize] {
        &self.index().columns
    }

    /// Return the length of the columns in the underlying [`Index`]
    pub fn len(&self) -> usize {
        self.index().len()
    }

    /// Returns `true` if the lookup index is [`Weak`].
    pub fn is_weak(&self) -> bool {
        matches!(self, Self::Weak(..))
    }
}

impl std::ops::Index<usize> for LookupIndex {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.index()[index]
    }
}

/// The result returned from a lookup operation performed by an ingredient during processing.
///
/// This is distinct from [`LookupResult`] because the iterator of records returned is fallible, to
/// allow for errors that occur in [`Ingredient::query_through`] (eg failure in evaluating a filter
/// expression)
pub(crate) enum IngredientLookupResult<'a> {
    /// Records returned from a successful lookup
    Records(Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a>),
    /// The lookup got a miss
    Miss,
}

impl<'a> From<LookupResult<'a>> for IngredientLookupResult<'a> {
    fn from(lookup_res: LookupResult<'a>) -> Self {
        match lookup_res {
            LookupResult::Some(rs) => Self::records(rs),
            LookupResult::Missing => Self::Miss,
        }
    }
}

impl<'a, I> From<Box<I>> for IngredientLookupResult<'a>
where
    I: Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a,
{
    fn from(rs: Box<I>) -> Self {
        Self::Records(rs as Box<_>)
    }
}

impl<'a> IngredientLookupResult<'a> {
    /// Construct a new [`IngredientLookupResult`] from the given iterator of records
    pub(crate) fn records<I, R>(rs: I) -> Self
    where
        I: IntoIterator<Item = R> + 'a,
        Cow<'a, [DataType]>: From<R>,
        R: 'a,
    {
        Box::new(rs.into_iter().map(|r| Ok(r.into()))).into()
    }

    /// Construct an [`IngredientLookupResult::Records`] that yields no results on iteration
    pub(crate) fn empty() -> Self {
        Box::new(iter::empty()).into()
    }

    /// Construct an [`IngredientLookupResult::Records`] that yields a single error on iteration
    pub(crate) fn err<E>(e: E) -> Self
    where
        ReadySetError: From<E>,
    {
        Box::new(iter::once(Err(e.into()))).into()
    }

    /// Transform into a new [`IngredientLookupResult`] of the same structure by calling `f` on all
    /// records in [`Self::Records`], or by returning [`Self::Miss`]
    pub(crate) fn map<F>(self, f: F) -> Self
    where
        F: 'a + FnMut(ReadySetResult<Cow<'a, [DataType]>>) -> ReadySetResult<Cow<'a, [DataType]>>,
    {
        match self {
            Self::Records(rs) => Self::Records(Box::new(rs.map(f)) as _),
            Self::Miss => Self::Miss,
        }
    }

    /// Returns `true` if self is [`Miss`].
    ///
    /// [`Miss`]: IngredientLookupResult::Miss
    pub(crate) fn is_miss(&self) -> bool {
        matches!(self, Self::Miss)
    }

    /// Returns the contained [`Records`] value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if `self` is [`IngredientLookupResult::Miss`]
    #[cfg(test)]
    pub(crate) fn unwrap(
        self,
    ) -> Box<dyn Iterator<Item = ReadySetResult<Cow<'a, [DataType]>>> + 'a> {
        match self {
            IngredientLookupResult::Records(rs) => rs,
            IngredientLookupResult::Miss => {
                #[allow(clippy::panic)] // documented
                {
                    panic!("unwrap() called on IngredientLookupResult::Miss")
                }
            }
        }
    }

    /// Consume this IngredientLookupResult, leaving a [`Miss`][] in its place
    ///
    /// [`Miss`]: IngredientLookupResult::Miss
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, Self::Miss)
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
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, LookupIndex>;

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
        from: LocalNodeIndex,
        data: Records,
        replay: &ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ReadySetResult<ProcessingResult>;

    #[allow(clippy::too_many_arguments)]
    fn on_input_raw(
        &mut self,
        from: LocalNodeIndex,
        data: Records,
        replay: ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ReadySetResult<RawProcessingResult> {
        Ok(RawProcessingResult::Regular(
            self.on_input(from, data, &replay, domain, states)?,
        ))
    }

    /// Triggered whenever a replay occurs, to allow the operator to react evict from any auxillary
    /// state other than what is stored in its materialization.
    fn on_eviction(&mut self, _from: LocalNodeIndex, _tag: Tag, _keys: &[KeyComparison]) {}

    fn can_query_through(&self) -> bool {
        false
    }

    fn query_through<'a>(
        &self,
        _columns: &[usize],
        _key: &KeyType,
        _nodes: &DomainNodes,
        _states: &'a StateMap,
        _mode: LookupMode,
    ) -> ReadySetResult<IngredientLookupResult<'a>> {
        internal!("Node does not support query_through")
    }

    /// Look up the given key in the given parent's state using the given `mode` to specify which
    /// kind of index to look up into, falling back to query_through if necessary.
    ///
    /// Will return [`ReadySetError::IndexNotFound`] if no materialization exists in `parent` with
    /// the given `columns` that can satisfy a lookup of `key` with `mode` (eg because
    /// [`suggest_indexes`] did not suggest one).
    ///
    /// # Invariants
    ///
    /// * `columns` and `key` must have the same length
    fn lookup<'a>(
        &self,
        parent_index: LocalNodeIndex,
        columns: &[usize],
        key: &KeyType,
        nodes: &DomainNodes,
        states: &'a StateMap,
        mode: LookupMode,
    ) -> ReadySetResult<IngredientLookupResult<'a>> {
        match states.get(parent_index) {
            Some(state) => match mode {
                LookupMode::Weak if state.is_partial() => match state.lookup_weak(columns, key) {
                    Some(rs) if !rs.is_empty() => Ok(IngredientLookupResult::records(rs)),
                    _ => Ok(IngredientLookupResult::Miss),
                },
                _ => Ok(state.lookup(columns, key).into()),
            },
            None => {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                #[allow(clippy::indexing_slicing)] // Node must exist to have gotten here.
                let parent = nodes[parent_index].borrow();

                if let Some(n) = parent.as_internal() {
                    n.query_through(columns, key, nodes, states, mode)
                } else {
                    Err(ReadySetError::IndexNotFound {
                        node: parent_index.id(),
                        columns: columns.to_vec(),
                    })
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
