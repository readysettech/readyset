mod keyed_state;
mod memory_state;
mod mk_key;
mod persistent_state;
mod single_state;

#[cfg(feature = "bench")]
pub use persistent_state::bench;

use std::fmt::{self, Debug};
use std::ops::{Bound, Deref};
use std::rc::Rc;
use std::vec;
use std::{borrow::Cow, iter::FromIterator};

use crate::prelude::*;
use ahash::RandomState;
use common::SizeOf;
use derive_more::From;
use hashbag::HashBag;
use noria::replication::ReplicationOffset;
use noria::KeyComparison;

pub use partial_map::PartialMap;

pub(crate) use self::memory_state::MemoryState;
pub(crate) use self::persistent_state::PersistentState;

/// Information about state evicted via a call to [`State::evict_bytes`]
pub(crate) struct StateEvicted<'a> {
    /// The index that was evicted from
    pub(crate) index: &'a Index,
    /// The keys that were evicted
    pub(crate) keys_evicted: Vec<Vec<DataType>>,
    /// The number of bytes removed from the state
    pub(crate) bytes_freed: u64,
}

/// The state of an individual, non-reader node in the graph
///
/// The [`State`] trait is the interface to the state of a non-reader node in the graph, containing
/// all rows that have been materialized from the output of that node. States have multiple *keys*,
/// each of which is an index providing efficient lookup of the rows based on a subset of the
/// columns in those rows. In the case of *partial* state, those keys are identified by the [`Tag`]s
/// for the replay paths that can materialize to those keys. For a given key value, a partial key
/// always stores either all possible rows matching that value, or has a *hole*, meaning that the
/// rows have not been materialized yet. When [performing writes into state](process_records), all
/// records that match a hole in partial state will be ignored - to allow inserting new records in
/// the case of replays, the [`mark_filled`](State::mark_filled) method must be called to mark the
/// hole as filled prior to processing the records.
///
/// # Weak keys
///
/// Partial state can additionally have a number of *weak* keys, created by [`add_weak_key`][] and
/// queried by [`lookup_weak`][]. These keys provide an efficient lookup index into rows that are
/// otherwise materialized into normal ("strict") indices. Weak keys do not have filled/unfilled
/// holes - they only index into rows that are stored in filled holes in strict indices.
///
/// See [this design doc][weak-keys-doc] for more information about the context in which weak keys
/// were added
///
/// [`add_weak_key`]: State::add_weak_key
/// [`lookup_weak`]: State::lookup_weak
/// [weak-keys-doc]: https://docs.google.com/document/d/1JFyvA_3GhMaTewaR0Bsk4N8uhzOwMtB0uH7dD4gJvoQ
pub(crate) trait State: SizeOf + Send {
    /// Add an index of the given type, keyed by the given columns and replayed to by the given
    /// partial tags.
    fn add_key(&mut self, index: Index, tags: Option<Vec<Tag>>);

    /// Add a new weak key index to this state.
    ///
    /// See [the section about weak keys](trait@State#weak-keys) for more information
    fn add_weak_key(&mut self, index: Index);

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool;

    /// Returns true if this state is partially materialized
    fn is_partial(&self) -> bool;

    /// Inserts or removes each record into State. Records that miss all indices in partial state
    /// are removed from `records` (thus the mutable reference).
    ///
    /// `replication_offset`, which is ignored for all non-base-table state, can be used to specify
    /// an update to the replication offset of a base table. See [the documentation for
    /// PersistentState](::noria_dataflow::state::persistent_state) for more information about
    /// replication offsets.
    fn process_records(
        &mut self,
        records: &mut Records,
        partial_tag: Option<Tag>,
        replication_offset: Option<ReplicationOffset>,
    );

    /// Returns the current replication offset written to this state.
    ///
    ///  See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    fn replication_offset(&self) -> Option<&ReplicationOffset> {
        None
    }

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
    ///   via [`add_key`]
    ///
    /// [`HashMap`]: IndexType::HashMap
    /// [`add_key`]: State::add_key
    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a>;

    /// Lookup all rows in the index in this state specified by the given `tag` matching the given
    /// `key`.
    ///
    /// # Invariants
    ///
    /// * There must be an index in this state identified by the given `tag` that was previously
    ///   created via [`add_key`]
    /// * The index identified by the given `tag` must have the same length as the given `key`
    /// * The index identified by the given `tag` must support point lookups (currently this is true
    ///   of all index types).
    ///
    /// [`add_key`]: State::add_key
    fn lookup_in_tag<'a>(&'a self, tag: Tag, key: &KeyType) -> LookupResult<'a>;

    /// Lookup all rows in this state where the values at the given `columns` are within the range
    /// specified by the given `key`
    ///
    /// # Invariants
    ///
    /// * The length of `columns` must match the length of `key`
    /// * There must be a [`BTreeMap`] [`Index`] on the given `columns` that was previously created
    ///   via [`add_key`]
    ///
    /// [`BTreeMap`]: IndexType::BTreeMap
    /// [`add_key`]: State::add_key
    fn lookup_range<'a>(&'a self, columns: &[usize], key: &RangeKey) -> RangeLookupResult<'a>;

    /// Lookup all the rows matching the given `key` in the weak index for the given set of
    /// `columns`, and return them if any exist. Some(empty) should never be returned from this
    /// method.
    ///
    /// See [the section about weak keys](trait@State#weak-keys) for more information.
    ///
    /// # Invariants
    ///
    /// * This method should only be called with a set of `columns` that have been previously added
    ///   as a weak key with [`add_weak_key`](State::add_weak_key)
    /// * The length of `columns` must match the length of `key`
    fn lookup_weak<'a>(&'a self, columns: &[usize], key: &KeyType) -> Option<RecordResult<'a>>;

    /// If the internal type is a `PersistentState` return a reference to itself
    fn as_persistent(&self) -> Option<&PersistentState> {
        None
    }

    /// If the internal type is a `PersistentState` return a mutable reference to itself
    fn as_persistent_mut(&mut self) -> Option<&mut PersistentState> {
        None
    }

    /// Return (a potentially inaccurate estimate of) the number of rows materialized into this
    /// state
    fn rows(&self) -> usize;

    /// Return a copy of all records. Panics if the state is only partially materialized.
    fn cloned_records(&self) -> Vec<Vec<DataType>>;

    /// Evict up to `bytes` by randomly selected keys, returning a struct representing the index
    /// chosen to evict from along with the keys evicted and the number of bytes evicted.
    fn evict_bytes(&mut self, bytes: usize) -> Option<StateEvicted>;

    /// Evict the listed keys from the materialization targeted by `tag`, returning the index chosen
    /// to evict from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: Tag, keys: &[KeyComparison]) -> Option<(&Index, u64)>;

    /// Remove all rows from this state
    fn clear(&mut self);
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct Row(Rc<Vec<DataType>>);

pub(crate) type Rows = HashBag<Row, RandomState>;

unsafe impl Send for Row {}

impl Row {
    /// This is very unsafe. Since `Row` unsafely implements `Send`, one can clone a row
    /// and have two `Row`s with an inner Rc being sent to two different threads leading
    /// to undefined behaviour. In the context of `State` it is only safe because all references
    /// to the same row always belong to the same state.
    pub(in crate::state) unsafe fn clone(&self) -> Self {
        Row(Rc::clone(&self.0))
    }
}

impl From<Vec<DataType>> for Row {
    fn from(r: Vec<DataType>) -> Self {
        Self(Rc::new(r))
    }
}

impl From<Rc<Vec<DataType>>> for Row {
    fn from(r: Rc<Vec<DataType>>) -> Self {
        Self(r)
    }
}

impl AsRef<[DataType]> for Row {
    fn as_ref(&self) -> &[DataType] {
        &**self.0
    }
}

impl std::borrow::Borrow<[DataType]> for Row {
    fn borrow(&self) -> &[DataType] {
        &**self.0
    }
}

impl Deref for Row {
    type Target = Vec<DataType>;
    fn deref(&self) -> &Self::Target {
        &*self.0
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
pub(crate) enum RecordResult<'a> {
    Borrowed(&'a HashBag<Row, RandomState>),
    #[from(ignore)]
    References(Vec<&'a Row>),
    Owned(Vec<Vec<DataType>>),
}

impl<'a> PartialEq for RecordResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Borrowed(s), Self::Borrowed(o)) => s == o,
            (Self::References(s), Self::References(o)) => s == o,
            (Self::Owned(s), Self::Owned(o)) => s == o,
            (Self::Borrowed(s), Self::References(o)) => s.iter().eq_by(o.iter(), |x, y| x == *y),
            (Self::Borrowed(s), Self::Owned(o)) => s.iter().eq_by(o.iter(), |x, y| **x == *y),
            (Self::References(s), Self::Owned(o)) => s.iter().eq_by(o.iter(), |x, y| ***x == *y),
            (Self::Owned(s), Self::References(o)) => s.iter().eq_by(o.iter(), |x, y| *x == ***y),
            (s, o) => o == s,
        }
    }
}
impl<'a> Eq for RecordResult<'a> {}

impl<'a> Debug for RecordResult<'a> {
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

impl<'a> RecordResult<'a> {
    pub(crate) fn len(&self) -> usize {
        match *self {
            RecordResult::Borrowed(rs) => rs.len(),
            RecordResult::Owned(ref rs) => rs.len(),
            RecordResult::References(ref refs) => refs.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match *self {
            RecordResult::Borrowed(rs) => rs.is_empty(),
            RecordResult::Owned(ref rs) => rs.is_empty(),
            RecordResult::References(ref refs) => refs.is_empty(),
        }
    }

    pub(crate) fn retain<F>(&mut self, func: F)
    where
        F: Fn(&[DataType]) -> bool,
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

impl<'a> FromIterator<Vec<DataType>> for RecordResult<'a> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Vec<DataType>>,
    {
        Self::Owned(iter.into_iter().collect())
    }
}

impl<'a> IntoIterator for RecordResult<'a> {
    type Item = Cow<'a, [DataType]>;
    type IntoIter = RecordResultIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            RecordResult::Borrowed(rs) => RecordResultIterator::Borrowed(rs.iter()),
            RecordResult::Owned(rs) => RecordResultIterator::Owned(rs.into_iter()),
            RecordResult::References(rs) => RecordResultIterator::References(rs.into_iter()),
        }
    }
}

pub(crate) enum RecordResultIterator<'a> {
    Owned(vec::IntoIter<Vec<DataType>>),
    Borrowed(hashbag::Iter<'a, Row>),
    References(vec::IntoIter<&'a Row>),
}

impl<'a> Iterator for RecordResultIterator<'a> {
    type Item = Cow<'a, [DataType]>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RecordResultIterator::Borrowed(iter) => iter.next().map(|r| Cow::from(&r[..])),
            RecordResultIterator::Owned(iter) => iter.next().map(Cow::from),
            RecordResultIterator::References(iter) => iter.next().map(|r| Cow::from(&r[..])),
        }
    }
}

pub(crate) enum LookupResult<'a> {
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

    /// Returns the contained [`RecordResult<'a>`](RecordResult) value, panicing if the value is
    /// [`Missing`].
    pub fn unwrap(self) -> RecordResult<'a> {
        self.records().unwrap()
    }
}

pub(crate) type Misses = Vec<(Bound<Vec<DataType>>, Bound<Vec<DataType>>)>;

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum RangeLookupResult<'a> {
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

    /// Returns the contained [`RecordResult<'a>`](RecordResult) value, panicing if the value is
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
