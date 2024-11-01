mod key;
mod keyed_state;
mod memory_state;
mod mk_key;
mod persistent_state;
mod single_state;

use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;
use std::vec;

use ahash::RandomState;
use common::{Records, SizeOf, Tag};
use derive_more::From;
use hashbag::HashBag;
use itertools::Either;
pub use partial_map::PartialMap;
use readyset_client::debug::info::KeyCount;
use readyset_client::internal::Index;
use readyset_client::{KeyComparison, PersistencePoint};
use readyset_data::{Bound, DfValue};
use readyset_errors::ReadySetResult;
use readyset_util::iter::eq_by;
use replication_offset::ReplicationOffset;
use serde::{Deserialize, Serialize};

pub use crate::key::{PointKey, RangeKey};
pub use crate::memory_state::MemoryState;
pub use crate::persistent_state::{
    clean_working_dir, DurabilityMode, PersistenceParameters, PersistenceType, PersistentState,
    PersistentStateHandle, RocksDbOptions, SnapshotMode,
};

/// Information about state evicted via a call to [`State::evict_bytes`]
pub struct EvictBytesResult<'a> {
    /// The index that was evicted from
    pub index: &'a Index,
    /// The keys that were evicted
    pub keys_evicted: Vec<Vec<DfValue>>,
    /// The number of bytes removed from the state
    pub bytes_freed: u64,
}

/// Information about state evicted via a call to [`State::evict_keys`]
pub struct EvictKeysResult<'a> {
    /// The index that was evicted from
    pub index: &'a Index,
    /// The number of bytes removed from the state
    pub bytes_freed: u64,
}

/// Information about state evicted via a call to [`State::evict_random`]
pub struct EvictRandomResult<'a> {
    /// The index that was evicted from
    pub index: &'a Index,
    /// The evicted key
    pub key_evicted: Vec<DfValue>,
    /// The number of bytes removed from the state
    pub bytes_freed: u64,
}

/// The state of an individual, non-reader node in the graph
pub enum MaterializedNodeState {
    /// The state that stores all the materialized rows in-memory.
    Memory(MemoryState),
    /// The state that stores all the materialized rows in a persistent
    /// storage.
    Persistent(PersistentState),
    /// A read handle to a [`PersistentState`] owned by another node.
    PersistentReadHandle(PersistentStateHandle),
}

/// Enum representing whether a base table node was already initialized (and has a replication
/// offset assigned), or if it is still pending initialization. This type is used as a container
/// for responses from base tables that will only return data if the base table is initialized.
#[derive(Serialize, Deserialize)]
pub enum BaseTableState<T> {
    Initialized(T),
    Pending,
}

/// The [`State`] trait is the interface to the state of a non-reader node in the graph, containing
/// all rows that have been materialized from the output of that node. States have multiple
/// *indexes*, each of which provides efficient lookup of rows based on a subset of the columns in
/// those rows (a "key"). In the case of *partial* state, those indexes are identified by the
/// [`Tag`]s for the replay paths that can materialize to the index. For a given key, a partial
/// index always stores either all possible rows matching that value, or has a *hole*, meaning that
/// the rows have not been materialized yet. When [performing writes into state](process_records),
/// all records that match a hole in partial state will be ignored - to allow inserting new records
/// in the case of replays, the [`mark_filled`](State::mark_filled) method must be called to mark
/// the hole as filled prior to processing the records.
///
/// # Weak indexes
///
/// Partial state can additionally have a number of *weak* indexes, created by [`add_weak_index`][]
/// and queried by [`lookup_weak`][]. These indexes provide efficient lookup of rows that are
/// otherwise materialized into normal ("strict") indexes. Weak indexes do not have filled/unfilled
/// holes - they only index into rows that are stored in filled holes in strict indexes.
///
/// See [this design doc][weak-indexes-doc] for more information about the context in which weak
/// indexes were added
///
/// [`add_weak_index`]: State::add_weak_index
/// [`lookup_weak`]: State::lookup_weak
/// [weak-keys-doc]: https://docs.google.com/document/d/1JFyvA_3GhMaTewaR0Bsk4N8uhzOwMtB0uH7dD4gJvoQ
pub trait State: SizeOf + Send {
    /// Add an index of the given type, keyed by the given columns and replayed to by the given
    /// partial tags.
    fn add_index(&mut self, index: Index, tags: Option<Vec<Tag>>);

    /// Add a new weak index index to this state.
    ///
    /// See [the section about weak index](trait@State#weak-indexes) for more information
    fn add_weak_index(&mut self, index: Index);

    fn add_index_multi(&mut self, strict: Vec<(Index, Option<Vec<Tag>>)>, weak: Vec<Index>) {
        for (index, tags) in strict.into_iter() {
            self.add_index(index, tags);
        }
        for index in weak.into_iter() {
            self.add_weak_index(index);
        }
    }

    /// Returns whether this state is currently indexed on anything. If not, then it cannot store
    /// any information and is thus "not useful".
    fn is_useful(&self) -> bool;

    /// Returns true if this state is partially materialized
    fn is_partial(&self) -> bool;

    /// Returns `true` if this state is fully materialized and has received its full replay.
    ///
    /// If this state is partial, the return value of this method is unspecified
    fn replay_done(&self) -> bool;

    /// Inserts or removes each record into State. Records that miss all indices in partial state
    /// are removed from `records` (thus the mutable reference).
    ///
    /// `replication_offset`, which is ignored for all non-base-table state, can be used to specify
    /// an update to the replication offset of a base table. See [the documentation for
    /// PersistentState](::readyset_dataflow::state::persistent_state) for more information about
    /// replication offsets.
    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()>;

    /// Returns the current replication offset written to this state.
    ///
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
    fn replication_offset(&self) -> Option<&ReplicationOffset>;

    /// Returns the replication offset up to which data has been persisted.
    ///
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
    fn persisted_up_to(&self) -> ReadySetResult<PersistencePoint>;

    /// Mark the given `key` as a *filled hole* in the given partial `tag`, causing all lookups to
    /// that key to return an empty non-miss result, and all writes to that key to not be dropped.
    ///
    /// # Invariants
    ///
    /// The given `tag` must identify an index with the same length as the given `key` and whose
    /// [`IndexType`]  supports the given `key` type (eg it cannot be a [`HashMap`] index if the
    /// `key` is a range key)
    ///
    /// [`HashMap`]: IndexType::HashMap
    fn mark_filled(&mut self, key: KeyComparison, tag: Tag);

    /// Mark the given `key` as a *hole* in the given partial `tag`, deleting all records that were
    /// otherwise materialized into that `key`.
    ///
    /// # Invariants
    ///
    /// The given `tag` must identify an index with the same length as the given `key` and whose
    /// [`IndexType`]  supports the given `key` type (eg it cannot be a [`HashMap`] index if the
    /// `key` is a range key)
    ///
    /// [`HashMap`]: IndexType::HashMap
    fn mark_hole(&mut self, key: &KeyComparison, tag: Tag);

    /// Lookup all rows in this state where the values at the given `columns` match the given `key`.
    ///
    /// # Invariants
    ///
    /// * The length of `columns` must match the length of `key`
    /// * There must be a [`HashMap`] [`Index`] on the given `columns` that was previously created
    ///   via [`add_index`]
    ///
    /// [`HashMap`]: IndexType::HashMap
    /// [`add_index`]: State::add_index
    fn lookup<'a>(&'a self, columns: &[usize], key: &PointKey) -> LookupResult<'a>;

    /// Lookup all rows in this state where the values at the given `columns` are within the range
    /// specified by the given `key`
    ///
    /// # Invariants
    ///
    /// * The length of `columns` must match the length of `key`
    /// * There must be a [`BTreeMap`] [`Index`] on the given `columns` that was previously created
    ///   via [`add_index`]
    ///
    /// [`BTreeMap`]: IndexType::BTreeMap
    /// [`add_index`]: State::add_index
    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a>;

    /// Lookup all the rows matching the given `key` in the weak index for the given set of
    /// `columns`, and return them if any exist. Some(empty) should never be returned from this
    /// method.
    ///
    /// See [the section about weak indexes](trait@State#weak-indexes) for more information.
    ///
    /// # Invariants
    ///
    /// * This method should only be called with a set of `columns` that have been previously added
    ///   as a weak index with [`add_weak_index`](State::add_weak_index)
    /// * The length of `columns` must match the length of `key`
    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>>;

    /// If the internal type is a `PersistentState` return a reference to itself
    fn as_persistent(&self) -> Option<&PersistentState> {
        None
    }

    /// If the internal type is a `PersistentState` return a mutable reference to itself
    fn as_persistent_mut(&mut self) -> Option<&mut PersistentState> {
        None
    }

    /// Return (a potentially inaccurate estimate of) the number of keys stored in this state
    fn key_count(&self) -> KeyCount;

    /// Return (a potentially inaccurate estimate of) the number of rows materialized into this
    /// state
    fn row_count(&self) -> usize;

    /// Return a handle that allows streaming a consistent snapshot of all records within this
    /// state. Panics if the state is only partially materialized.
    fn all_records(&self) -> AllRecords;

    /// Evict up to `bytes` by randomly selected keys, returning a struct representing the index
    /// chosen to evict from along with the keys evicted and the number of bytes evicted.
    fn evict_bytes(&mut self, bytes: usize) -> Option<EvictBytesResult>;

    /// Evict the listed keys from the materialization targeted by `tag`, returning the index chosen
    /// to evict from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<EvictKeysResult>;

    /// Evict a random key from the materialization targeted by `tag`, returning the index chosen to
    /// evict from and the number of bytes evicted.
    fn evict_random<R: rand::Rng>(&mut self, tag: Tag, rng: &mut R) -> Option<EvictRandomResult>;

    /// Remove all rows from this state
    fn clear(&mut self);

    /// Cleanly shut down the state, so that it can be reopened in the future.
    /// This function is for standard restart operations.
    fn shut_down(&mut self) -> ReadySetResult<()>;

    /// Tear down the state, freeing any resources. This is a permanent operation.
    /// For those states that are backed by resources outside ReadySet, the implementation of this
    /// method should guarantee that those resources are permamently freed (delete data from disk,
    /// for example).
    fn tear_down(self) -> ReadySetResult<()>;
}

impl MaterializedNodeState {
    /// Set whether or not this state, which should be fully materialized, has received its full
    /// replay
    pub fn set_replay_done(&mut self, replay_done: bool) {
        debug_assert!(!self.is_partial());
        match self {
            MaterializedNodeState::Memory(ms) => ms.replay_done = replay_done,
            MaterializedNodeState::Persistent(ps) => ps.set_replay_done(replay_done),
            MaterializedNodeState::PersistentReadHandle(_rh) => (),
        }
    }
}

impl SizeOf for MaterializedNodeState {
    fn deep_size_of(&self) -> u64 {
        match self {
            MaterializedNodeState::Memory(ms) => ms.deep_size_of(),
            MaterializedNodeState::Persistent(ps) => ps.deep_size_of(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.deep_size_of(),
        }
    }

    fn size_of(&self) -> u64 {
        match self {
            MaterializedNodeState::Memory(ms) => ms.size_of(),
            MaterializedNodeState::Persistent(ps) => ps.size_of(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.size_of(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            MaterializedNodeState::Memory(ms) => ms.is_empty(),
            MaterializedNodeState::Persistent(ps) => ps.is_empty(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.is_empty(),
        }
    }
}

impl State for MaterializedNodeState {
    fn add_index(&mut self, index: Index, tags: Option<Vec<Tag>>) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.add_index(index, tags),
            MaterializedNodeState::Persistent(ps) => ps.add_index(index, tags),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.add_index(index, tags),
        }
    }

    fn add_weak_index(&mut self, index: Index) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.add_weak_index(index),
            MaterializedNodeState::Persistent(ps) => ps.add_weak_index(index),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.add_weak_index(index),
        }
    }

    fn add_index_multi(&mut self, strict: Vec<(Index, Option<Vec<Tag>>)>, weak: Vec<Index>) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.add_index_multi(strict, weak),
            MaterializedNodeState::Persistent(ps) => ps.add_index_multi(strict, weak),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.add_index_multi(strict, weak),
        }
    }

    fn is_useful(&self) -> bool {
        match self {
            MaterializedNodeState::Memory(ms) => ms.is_useful(),
            MaterializedNodeState::Persistent(ps) => ps.is_useful(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.is_useful(),
        }
    }

    fn is_partial(&self) -> bool {
        match self {
            MaterializedNodeState::Memory(ms) => ms.is_partial(),
            MaterializedNodeState::Persistent(ps) => ps.is_partial(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.is_partial(),
        }
    }

    fn replay_done(&self) -> bool {
        match self {
            MaterializedNodeState::Memory(ms) => ms.replay_done(),
            MaterializedNodeState::Persistent(ps) => ps.replay_done(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.replay_done(),
        }
    }

    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()> {
        match self {
            MaterializedNodeState::Memory(ms) => {
                ms.process_records(records, partial_tag, replication_offset)
            }
            MaterializedNodeState::Persistent(ps) => {
                ps.process_records(records, partial_tag, replication_offset)
            }
            MaterializedNodeState::PersistentReadHandle(rh) => {
                rh.process_records(records, partial_tag, replication_offset)
            }
        }
    }

    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.replication_offset(),
            MaterializedNodeState::Persistent(ps) => ps.replication_offset(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.replication_offset(),
        }
    }

    fn persisted_up_to(&self) -> ReadySetResult<PersistencePoint> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.persisted_up_to(),
            MaterializedNodeState::Persistent(ps) => ps.persisted_up_to(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.persisted_up_to(),
        }
    }

    fn mark_filled(&mut self, key: KeyComparison, tag: Tag) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.mark_filled(key, tag),
            MaterializedNodeState::Persistent(ps) => ps.mark_filled(key, tag),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.mark_filled(key, tag),
        }
    }

    fn mark_hole(&mut self, key: &KeyComparison, tag: Tag) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.mark_hole(key, tag),
            MaterializedNodeState::Persistent(ps) => ps.mark_hole(key, tag),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.mark_hole(key, tag),
        }
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &PointKey) -> LookupResult<'a> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.lookup(columns, key),
            MaterializedNodeState::Persistent(ps) => ps.lookup(columns, key),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.lookup(columns, key),
        }
    }

    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.lookup_range(columns, key),
            MaterializedNodeState::Persistent(ps) => ps.lookup_range(columns, key),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.lookup_range(columns, key),
        }
    }

    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &PointKey) -> Option<RecordResult<'a>> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.lookup_weak(columns, key),
            MaterializedNodeState::Persistent(ps) => ps.lookup_weak(columns, key),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.lookup_weak(columns, key),
        }
    }

    fn as_persistent(&self) -> Option<&PersistentState> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.as_persistent(),
            MaterializedNodeState::Persistent(ps) => ps.as_persistent(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.as_persistent(),
        }
    }

    fn as_persistent_mut(&mut self) -> Option<&mut PersistentState> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.as_persistent_mut(),
            MaterializedNodeState::Persistent(ps) => ps.as_persistent_mut(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.as_persistent_mut(),
        }
    }

    fn key_count(&self) -> KeyCount {
        match self {
            MaterializedNodeState::Memory(ms) => ms.key_count(),
            MaterializedNodeState::Persistent(ps) => ps.key_count(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.key_count(),
        }
    }

    fn row_count(&self) -> usize {
        match self {
            MaterializedNodeState::Memory(ms) => ms.row_count(),
            MaterializedNodeState::Persistent(ps) => ps.row_count(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.row_count(),
        }
    }

    fn all_records(&self) -> AllRecords {
        match self {
            MaterializedNodeState::Memory(ms) => ms.all_records(),
            MaterializedNodeState::Persistent(ps) => ps.all_records(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.all_records(),
        }
    }

    fn evict_bytes(&mut self, bytes: usize) -> Option<EvictBytesResult> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.evict_bytes(bytes),
            MaterializedNodeState::Persistent(ps) => ps.evict_bytes(bytes),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.evict_bytes(bytes),
        }
    }

    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<EvictKeysResult> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.evict_keys(tag, keys),
            MaterializedNodeState::Persistent(ps) => ps.evict_keys(tag, keys),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.evict_keys(tag, keys),
        }
    }

    fn evict_random<R: rand::Rng>(&mut self, tag: Tag, rng: &mut R) -> Option<EvictRandomResult> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.evict_random(tag, rng),
            MaterializedNodeState::Persistent(ps) => ps.evict_random(tag, rng),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.evict_random(tag, rng),
        }
    }

    fn clear(&mut self) {
        match self {
            MaterializedNodeState::Memory(ms) => ms.clear(),
            MaterializedNodeState::Persistent(ps) => ps.clear(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.clear(),
        }
    }

    fn shut_down(&mut self) -> ReadySetResult<()> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.shut_down(),
            MaterializedNodeState::Persistent(ps) => ps.shut_down(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.shut_down(),
        }
    }

    fn tear_down(self) -> ReadySetResult<()> {
        match self {
            MaterializedNodeState::Memory(ms) => ms.tear_down(),
            MaterializedNodeState::Persistent(ps) => ps.tear_down(),
            MaterializedNodeState::PersistentReadHandle(rh) => rh.tear_down(),
        }
    }
}

/// Handle to all the records in a state.
///
/// This type exists as distinct from [`AllRecordsGuard`] to allow it to be sent between threads.
pub enum AllRecords {
    /// Owned records taken from a [`MemoryState`]
    Owned(Vec<Vec<DfValue>>),
    /// Records streaming out of a [`PersistentState`]
    Persistent(persistent_state::AllRecords),
}

/// RAII guard providing the ability to stream all the records out of a state
pub enum AllRecordsGuard<'a> {
    /// Owned records taken from a [`MemoryState`]
    Owned(vec::IntoIter<Vec<DfValue>>),
    /// Records streaming out of a [`PersistentState`]
    Persistent(persistent_state::AllRecordsGuard<'a>),
}

impl AllRecords {
    /// Construct an RAII guard providing the ability to stream all the records out of a state
    pub fn read(&mut self) -> AllRecordsGuard<'_> {
        match self {
            AllRecords::Owned(i) => AllRecordsGuard::Owned(std::mem::take(i).into_iter()),
            AllRecords::Persistent(g) => AllRecordsGuard::Persistent(g.read()),
        }
    }
}

impl<'a> AllRecordsGuard<'a> {
    /// Construct an iterator over all the records in a state.
    ///
    /// Do not call this method multiple times on the same `guard` - doing so will yield an empty
    /// result set.
    pub fn iter<'b>(&'b mut self) -> impl Iterator<Item = Vec<DfValue>> + 'b
    where
        'a: 'b,
    {
        match self {
            AllRecordsGuard::Owned(v) => Either::Left(v),
            AllRecordsGuard::Persistent(g) => Either::Right(g.iter()),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Row(Rc<Vec<DfValue>>);

pub type Rows = HashBag<Row, RandomState>;

unsafe impl Send for Row {}

impl Row {
    /// This is very unsafe. Since `Row` unsafely implements `Send`, one can clone a row
    /// and have two `Row`s with an inner Rc being sent to two different threads leading
    /// to undefined behaviour. In the context of `State` it is only safe because all references
    /// to the same row always belong to the same state.
    pub(crate) unsafe fn clone(&self) -> Self {
        Row(Rc::clone(&self.0))
    }
}

impl From<Vec<DfValue>> for Row {
    fn from(r: Vec<DfValue>) -> Self {
        Self(Rc::new(r))
    }
}

impl From<Rc<Vec<DfValue>>> for Row {
    fn from(r: Rc<Vec<DfValue>>) -> Self {
        Self(r)
    }
}

impl AsRef<[DfValue]> for Row {
    fn as_ref(&self) -> &[DfValue] {
        &self.0
    }
}

impl std::borrow::Borrow<[DfValue]> for Row {
    fn borrow(&self) -> &[DfValue] {
        &self.0
    }
}

impl Deref for Row {
    type Target = Vec<DfValue>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SizeOf for Row {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;
        size_of::<Self>() as u64
    }
    fn deep_size_of(&self) -> u64 {
        (*self.0).deep_size_of()
    }
    fn is_empty(&self) -> bool {
        false
    }
}

/// An std::borrow::Cow-like wrapper around a collection of rows.
#[derive(From)]
pub enum RecordResult<'a> {
    Borrowed(&'a HashBag<Row, RandomState>),
    #[from(ignore)]
    References(Vec<&'a Row>),
    Owned(Vec<Vec<DfValue>>),
}

impl PartialEq for RecordResult<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Borrowed(s), Self::Borrowed(o)) => s == o,
            (Self::References(s), Self::References(o)) => s == o,
            (Self::Owned(s), Self::Owned(o)) => s == o,
            (Self::Borrowed(s), Self::References(o)) => eq_by(s.iter(), o.iter(), |x, y| x == *y),
            (Self::Borrowed(s), Self::Owned(o)) => eq_by(s.iter(), o.iter(), |x, y| **x == *y),
            (Self::References(s), Self::Owned(o)) => eq_by(s.iter(), o.iter(), |x, y| ***x == *y),
            (Self::Owned(s), Self::References(o)) => eq_by(s.iter(), o.iter(), |x, y| *x == ***y),
            (s, o) => o == s,
        }
    }
}

impl Eq for RecordResult<'_> {}

impl Debug for RecordResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Borrowed(rows) => f
                .debug_tuple("Borrowed")
                .field(&rows.into_iter().collect::<HashBag<_>>())
                .finish(),
            Self::Owned(rows) => f.debug_tuple("Owned").field(rows).finish(),
            Self::References(refs) => f.debug_tuple("Refs").field(refs).finish(),
        }
    }
}

impl Default for RecordResult<'_> {
    fn default() -> Self {
        Self::Owned(vec![])
    }
}

impl RecordResult<'_> {
    pub fn len(&self) -> usize {
        match *self {
            RecordResult::Borrowed(rs) => rs.len(),
            RecordResult::Owned(ref rs) => rs.len(),
            RecordResult::References(ref refs) => refs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            RecordResult::Borrowed(rs) => rs.is_empty(),
            RecordResult::Owned(ref rs) => rs.is_empty(),
            RecordResult::References(ref refs) => refs.is_empty(),
        }
    }

    pub fn retain<F>(&mut self, func: F)
    where
        F: Fn(&[DfValue]) -> bool,
    {
        match *self {
            RecordResult::Borrowed(rs) => {
                if !rs.is_empty() {
                    *self = RecordResult::References(rs.iter().filter(|x| func(x)).collect());
                }
            }
            RecordResult::References(ref mut rs) => rs.retain(|row| func(row)),
            RecordResult::Owned(ref mut rs) => rs.retain(|row| func(row)),
        }
    }
}

impl FromIterator<Vec<DfValue>> for RecordResult<'_> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Vec<DfValue>>,
    {
        Self::Owned(iter.into_iter().collect())
    }
}

impl<'a> IntoIterator for RecordResult<'a> {
    type Item = Cow<'a, [DfValue]>;
    type IntoIter = RecordResultIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            RecordResult::Borrowed(rs) => RecordResultIterator::Borrowed(rs.iter()),
            RecordResult::Owned(rs) => RecordResultIterator::Owned(rs.into_iter()),
            RecordResult::References(rs) => RecordResultIterator::References(rs.into_iter()),
        }
    }
}

pub enum RecordResultIterator<'a> {
    Owned(vec::IntoIter<Vec<DfValue>>),
    Borrowed(hashbag::Iter<'a, Row>),
    References(vec::IntoIter<&'a Row>),
}

impl<'a> Iterator for RecordResultIterator<'a> {
    type Item = Cow<'a, [DfValue]>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RecordResultIterator::Borrowed(iter) => iter.next().map(|r| Cow::from(&r[..])),
            RecordResultIterator::Owned(iter) => iter.next().map(Cow::from),
            RecordResultIterator::References(iter) => iter.next().map(|r| Cow::from(&r[..])),
        }
    }
}

#[derive(Debug)]
pub enum LookupResult<'a> {
    Some(RecordResult<'a>),
    Missing,
}

#[allow(dead_code)]
impl<'a> LookupResult<'a> {
    /// Returns true if this LookupResult is `LookupResult::Some`
    pub fn is_some(&self) -> bool {
        matches!(self, Self::Some(_))
    }

    /// Returns true if this LookupResult is `LookupResult::Missing`
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Converts from `LookupResult<'a>` into an [`Option<RecordResult<'a>>`]
    pub fn records(self) -> Option<RecordResult<'a>> {
        match self {
            Self::Some(res) => Some(res),
            Self::Missing => None,
        }
    }

    /// Returns the contained [`RecordResult<'a>`](RecordResult) value, panicking if the value is
    /// [`Missing`].
    pub fn unwrap(self) -> RecordResult<'a> {
        self.records().unwrap()
    }
}

pub type Misses = Vec<(Bound<Vec<DfValue>>, Bound<Vec<DfValue>>)>;

#[derive(Eq, PartialEq, Debug)]
pub enum RangeLookupResult<'a> {
    Some(RecordResult<'a>),
    /// We encountered a miss in some partial state
    Missing(Misses),
}

#[allow(dead_code)]
impl<'a> RangeLookupResult<'a> {
    /// Returns true if this RangeLookupResult is `RangeLookupResult::Some`
    pub fn is_some(&self) -> bool {
        matches!(self, Self::Some(_))
    }

    /// Returns true if this RangeLookupResult is `RangeLookupResult::Missing`
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }

    /// Converts from `RangeLookupResult<'a>` into an [`Option<RecordResult<'a>>`]
    pub fn records(self) -> Option<RecordResult<'a>> {
        match self {
            Self::Some(res) => Some(res),
            Self::Missing(_) => None,
        }
    }

    /// Returns the contained [`RecordResult<'a>`](RecordResult) value, panicking if the value is
    /// [`Missing`].
    pub fn unwrap(self) -> RecordResult<'a> {
        self.records().unwrap()
    }

    /// Convert this RangeLookupResult into a [`Result<RecordResult<'a>, Misses>`]
    pub fn into_result(self) -> Result<RecordResult<'a>, Misses> {
        match self {
            Self::Some(records) => Ok(records),
            Self::Missing(misses) => Err(misses),
        }
    }
}
