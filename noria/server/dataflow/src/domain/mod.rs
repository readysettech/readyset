use std::borrow::Cow;
use std::cell;
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::mem;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time;

use ahash::RandomState;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use launchpad::Indices;
use metrics::{counter, gauge, histogram};
use noria::internal::Index;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use timekeeper::{RealTime, SimpleTracker, ThreadTime, Timer, TimerSet};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info_span;
use tracing::{debug, error, info, trace, warn};
use unbounded_interval_tree::IntervalTree;
use vec1::Vec1;

use failpoint_macros::failpoint;
pub use internal::DomainIndex;
use noria::channel;
use noria::metrics::recorded;
use noria::replication::ReplicationOffset;
use noria::{internal, KeyComparison, ReadySetError};
use noria_errors::{internal, internal_err, ReadySetResult};

use crate::node::NodeProcessingResult;
use crate::payload::{ReplayPieceContext, SourceSelection};
use crate::prelude::*;
use crate::processing::ColumnMiss;
use crate::state::RangeLookupResult;
use crate::{DomainRequest, Readers};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub concurrent_replays: usize,

    /// If set to `true`, the metric tracking the in-memory size of materialized state will be
    /// updated after every packet is handled, rather than only when requested by the eviction
    /// worker. This causes a (minor) runtime cost, with the upside being that the materialization
    /// state sizes will never be out-of-date.
    pub aggressively_update_state_sizes: bool,
}

const BATCH_SIZE: usize = 256;

#[derive(Debug)]
enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Box<Packet>>,
        passes: usize,
    },
}

impl PartialEq for DomainMode {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (&DomainMode::Forwarding, &DomainMode::Forwarding)
        )
    }
}

enum TriggerEndpoint {
    None,
    Start(Index),
    End {
        source: SourceSelection,
        options: Vec<Box<dyn channel::Sender<Item = Box<Packet>> + Send>>,
    },
    Local(Index),
}

pub(crate) struct ReplayPath {
    source: Option<LocalNodeIndex>,
    /// The nodes in the replay path.
    path: Vec1<ReplayPathSegment>,
    notify_done: bool,
    pub(crate) partial_unicast_sharder: Option<NodeIndex>,
    trigger: TriggerEndpoint,
}

impl ReplayPath {
    /// Return a reference to the last [`ReplayPathSegment`] of this replay path
    pub(crate) fn last_segment(&self) -> &ReplayPathSegment {
        self.path.last()
    }
}

type Hole = (Vec<usize>, KeyComparison);

/// The result of do_lookup, consists of the vector of the found records
/// the hashset of the fullfilled keys, and a hashset of the missed key/replay key tuples
struct StateLookupResult<'a> {
    /// Records returned by the lookup
    records: Vec<RecordResult<'a>>,
    /// Keys for which records were found
    found_keys: HashSet<KeyComparison>,
    /// Keys that were missed and need a replay
    replay_keys: HashSet<(KeyComparison, KeyComparison)>,
}

/// Describes a required replay
#[derive(Clone)]
struct ReplayDescriptor {
    idx: LocalNodeIndex,
    tag: Tag,
    replay_key: KeyComparison,
    lookup_key: KeyComparison,
    lookup_columns: Vec<usize>,
    unishard: bool,
    requesting_shard: usize,
}

impl ReplayDescriptor {
    fn from_miss(miss: &Miss, tag: Tag, unishard: bool, requesting_shard: usize) -> Self {
        #[allow(clippy::unwrap_used)]
        // We know this is a partial replay
        ReplayDescriptor {
            idx: miss.on,
            tag,
            replay_key: miss.replay_key().unwrap(),
            lookup_key: miss.lookup_key().into_owned(),
            lookup_columns: miss.lookup_idx.clone(),
            unishard,
            requesting_shard,
        }
    }

    // Returns true if the given `ReplayDescriptor` can be processed together with `self`, i.e. they
    // only differ in their miss and lookup keys, and have the same replay key type (range or
    // equal).
    fn can_combine(&self, other: &ReplayDescriptor) -> bool {
        self.tag == other.tag
            && self.idx == other.idx
            && self.lookup_columns == other.lookup_columns
            && self.unishard == other.unishard
            && self.requesting_shard == other.requesting_shard
            && self.replay_key.is_range() == other.replay_key.is_range()
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct Redo {
    tag: Tag,
    replay_key: KeyComparison,
    unishard: bool,
    requesting_shard: usize,
}

/// When a replay misses while being processed, it triggers a replay to backfill the hole that it
/// missed in. We need to ensure that when this happens, we re-run the original replay to fill the
/// hole we *originally* were trying to fill.
///
/// This comes with some complexity:
///
///  - If two replays both hit the *same* hole, we should only request a backfill of it once, but
///    need to re-run *both* replays when the hole is filled.
///  - If one replay hits two *different* holes, we should backfill both holes, but we must ensure
///    that we only re-run the replay once when both holes have been filled.
///
/// To keep track of this, we use the `Waiting` structure below. One is created for every node with
/// at least one outstanding backfill, and contains the necessary bookkeeping to ensure the two
/// behaviors outlined above.
///
/// Note that in the type aliases above, we have chosen to use Vec<usize> instead of Tag to
/// identify a hole. This is because there may be more than one Tag used to fill a given hole, and
/// the set of columns uniquely identifies the set of tags.
#[derive(Debug, Default)]
struct Waiting {
    /// For each eventual redo, how many holes are we waiting for?
    holes: HashMap<Redo, usize>,
    /// For each hole, which redos do we expect we'll have to do?
    redos: HashMap<Hole, HashSet<Redo>>,
}

/// Data structure representing the set of keys that have been requested by a reader.
///
/// As an optimization, this structure is backed by either a [`HashSet`] if the reader's index is a
/// [`HashMap`], or an [`IntervalTree`] if the reader's index is a [`BTreeMap`] - interval trees can
/// act as sets of points, but are much slower than hash sets for that purpose.
///
/// [`HashMap`]: IndexType::HashMap
/// [`BTreeMap`]: IndexType::BTreeMap
enum RequestedKeys {
    Points(HashSet<Vec1<DataType>, RandomState>),
    Ranges(IntervalTree<Vec1<DataType>>),
}

impl RequestedKeys {
    /// Create a new set of requested keys for storing requests to the given [`IndexType`].
    fn new(index_type: IndexType) -> Self {
        match index_type {
            IndexType::HashMap => Self::Points(Default::default()),
            IndexType::BTreeMap => Self::Ranges(Default::default()),
        }
    }

    /// Returns true if `self` contains no keys
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            RequestedKeys::Points(requested) => requested.is_empty(),
            RequestedKeys::Ranges(requested) => requested.is_empty(),
        }
    }

    /// Extend `self` with the given `keys`, mutating `keys` in-place such that it contains only
    /// those keys or subranges of keys that were not already in `self`.
    ///
    /// # Panics
    ///
    /// Panics if `self` is a set of keys for a [`HashMap`] index and any of the keys in `keys` are
    /// ranges.
    ///
    /// [`HashMap`]: IndexType::HashMap`
    fn extend(&mut self, keys: &mut Vec<KeyComparison>) {
        match self {
            RequestedKeys::Points(requested) => keys.retain(|key| {
                requested.insert(
                    key.equal()
                        .expect("RequestedKeys::Points received range key")
                        .clone(),
                )
            }),
            RequestedKeys::Ranges(requested) => {
                *keys = keys
                    .iter()
                    .flat_map(|key| {
                        let diff = requested
                            .get_interval_difference(key)
                            .into_iter()
                            .map(
                                |(lower, upper): (
                                    Bound<&Vec1<DataType>>,
                                    Bound<&Vec1<DataType>>,
                                )| {
                                    (lower.cloned(), upper.cloned())
                                },
                            )
                            .collect::<Vec<_>>();
                        requested.insert(key);
                        diff
                    })
                    .map(|r| KeyComparison::from_range(&r))
                    .collect()
            }
        }
    }

    /// Mutate `keys` in place such that it only contains keys or subranges of keys that are already
    /// in `self`
    ///
    /// # Panics
    ///
    /// Panics if `self` is a set of keys for a [`HashMap`] index and any of the keys in `keys` are
    /// ranges.
    ///
    /// [`HashMap`]: IndexType::HashMap`
    fn filter_keys(&self, keys: &mut HashSet<KeyComparison>) {
        match self {
            RequestedKeys::Points(requested) => keys.retain(|key| {
                requested.contains(
                    key.equal()
                        .expect("RequestedKeys::Points received range key"),
                )
            }),
            RequestedKeys::Ranges(requested) => {
                *keys = keys
                    .iter()
                    .flat_map(|key| {
                        requested
                            .get_interval_intersection(key)
                            .into_iter()
                            .map(|r| KeyComparison::from_range(&r))
                    })
                    .collect()
            }
        }
    }

    /// Remove `key` from `self`
    ///
    /// # Panics
    ///
    /// Panics if `self` is a set of keys for a [`HashMap`] index and `key` is a range
    pub(crate) fn remove(&mut self, key: &KeyComparison) {
        match self {
            RequestedKeys::Points(requested) => {
                requested.remove(
                    key.equal()
                        .expect("RequestedKeys::Points received range key"),
                );
            }
            RequestedKeys::Ranges(requested) => {
                requested.remove(key);
            }
        }
    }
}

/// Struct sent to a worker to start a domain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainBuilder {
    /// The domain's index.
    pub index: DomainIndex,
    /// The shard ID represented by this `DomainBuilder`.
    pub shard: Option<usize>,
    /// The number of shards in the domain.
    pub nshards: usize,
    /// The nodes in the domain.
    pub nodes: DomainNodes,
    /// The domain's persistence setting.
    pub persistence_parameters: PersistenceParameters,
    /// Configuration parameters for the domain.
    pub config: Config,
}

impl DomainBuilder {
    /// Starts up the domain represented by this `DomainBuilder`.
    pub fn build(
        self,
        readers: Readers,
        channel_coordinator: Arc<ChannelCoordinator>,
        state_size: Arc<AtomicUsize>,
    ) -> Domain {
        // initially, all nodes are not ready
        let not_ready = self
            .nodes
            .values()
            .map(|n| n.borrow().local_addr())
            .collect();

        Domain {
            index: self.index,
            shard: self.shard,
            _nshards: self.nshards,

            persistence_parameters: self.persistence_parameters,
            nodes: self.nodes,
            state: StateMap::default(),
            not_ready,
            mode: DomainMode::Forwarding,
            waiting: Default::default(),
            reader_triggered: Default::default(),
            replay_paths: Default::default(),
            replay_paths_by_dst: Default::default(),

            ingress_inject: Default::default(),

            readers,
            channel_coordinator,

            timed_purges: Default::default(),

            concurrent_replays: 0,
            max_concurrent_replays: self.config.concurrent_replays,
            replay_request_queue: Default::default(),
            delayed_for_self: Default::default(),

            state_size,
            total_time: Timer::new(),
            total_ptime: Timer::new(),
            wait_time: Timer::new(),
            process_times: TimerSet::new(),
            process_ptimes: TimerSet::new(),

            total_replay_time: Timer::new(),
            total_forward_time: Timer::new(),

            aggressively_update_state_sizes: self.config.aggressively_update_state_sizes,
            replay_completed: false,
            generated_columns: Default::default(),
            raw_replay_paths: Default::default(),
            active_remaps: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
struct TimedPurge {
    time: time::Instant,
    view: LocalNodeIndex,
    keys: HashSet<KeyComparison>,
}

pub struct Domain {
    index: DomainIndex,
    shard: Option<usize>,
    _nshards: usize,

    /// Map of nodes managed by this domain
    ///
    /// # Invariants
    ///
    /// * All nodes mentioned in `self.replay_paths`, `self.active_remaps`, `self.not_ready` must
    ///   exist in `self.nodes`
    /// * All keys of `self.state` must also be keys in `self.nodes`
    /// * `nodes` cannot be empty
    nodes: DomainNodes,

    /// State for all materialized nodes managed by this domain
    ///
    /// Invariant: All keys of `self.state` must also be keys in `self.nodes`
    state: StateMap,

    not_ready: HashSet<LocalNodeIndex>,

    ingress_inject: NodeMap<(usize, Vec<DataType>)>,

    persistence_parameters: PersistenceParameters,

    mode: DomainMode,
    waiting: NodeMap<Waiting>,
    /// Contains information about active upquery remaps (i.e. when the
    /// [`Ingredient::handle_upquery`] API is used). These result in lots of secondary upqueries to
    /// fill the hole of the original upquery that was remapped, and the state required is stored
    /// here.
    ///
    /// This is a map of holes in nodes (that we're trying to fill via `handle_upquery`) to a set
    /// of upqueries that are in-flight to fill that hole (identified by tag and keycomparison).
    /// When all of the upqueries in the set are done, the original hole is considered filled
    /// and the results to the original upquery can be sent.
    ///
    /// FIXME(eta): There are likely more efficient ways to access / store this data; currently,
    ///             dealing with remaps involves a fair deal of heavy looping / iteration.
    ///             See ENG-228.
    active_remaps: HashMap<(LocalNodeIndex, Hole), HashSet<(Tag, KeyComparison)>>,
    /// Map of replay paths by tag
    ///
    /// Invariant: All nodes mentioned in replay paths must exist in `self.nodes`
    ///
    /// TODO(grfn): Write a doc somewhere explaining what replay paths are, and link to that here
    replay_paths: HashMap<Tag, ReplayPath>,
    /// Raw replay paths, as returned by `replay_paths_for` in `keys.rs`. Used to inform processing
    /// for the `handle_upquery` API.
    ///
    /// These include the whole replay path, not just the parts relevant to this domain, and don't
    /// include the domain-specific information in `ReplayPath` and `ReplayPathSegment`.
    raw_replay_paths: HashMap<Tag, Vec<IndexRef>>,

    /// Map from node ID to an interval tree of the keys of all current pending upqueries to that
    /// node
    reader_triggered: NodeMap<RequestedKeys>,

    /// Queue of purge operations to be performed on reader nodes at some point in the future, used
    /// as part of the implementation of materialization frontiers
    ///
    /// # Invariants
    ///
    /// * Each node referenced by a `view` of a TimedPurge must be in `self.nodes`
    /// * Each node referenced by a `view` of a TimedPurge must be a reader node
    timed_purges: VecDeque<TimedPurge>,

    /// Map from terminating nodes of replay paths, to indexes, to tags for those replay paths
    replay_paths_by_dst: NodeMap<HashMap<Index, Vec<Tag>>>,

    /// Set of nodes and columns which are "generated" by that node, meaning those columns do not
    /// appear unchanged in exactly one of that node's parents. If a  If a (node, cols) pair appears
    /// in this set, then misses on those columns require the use of [`Ingredient::handle_upquery`]
    generated_columns: HashSet<(LocalNodeIndex, Vec<usize>)>,

    concurrent_replays: usize,
    max_concurrent_replays: usize,
    replay_request_queue: VecDeque<(Tag, Vec<KeyComparison>)>,

    readers: Readers,
    channel_coordinator: Arc<ChannelCoordinator>,

    delayed_for_self: VecDeque<Box<Packet>>,

    state_size: Arc<AtomicUsize>,
    total_time: Timer<SimpleTracker, RealTime>,
    total_ptime: Timer<SimpleTracker, ThreadTime>,
    wait_time: Timer<SimpleTracker, RealTime>,
    process_times: TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
    process_ptimes: TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,

    /// time spent processing replays
    total_replay_time: Timer<SimpleTracker, RealTime>,
    /// time spent processing ordinary, forward updates
    total_forward_time: Timer<SimpleTracker, RealTime>,

    /// If set to `true`, the metric tracking the in-memory size of materialized state will be
    /// updated after every packet is handled, rather than only when requested by the eviction
    /// worker. This causes a (minor) runtime cost, with the upside being that the materialization
    /// state sizes will never be out-of-date.
    pub aggressively_update_state_sizes: bool,

    replay_completed: bool,
}

impl Domain {
    /// Return the unique index for this domain
    pub fn index(&self) -> DomainIndex {
        self.index
    }

    /// Return this domain's shard
    pub fn shard(&self) -> usize {
        self.shard.unwrap_or(0)
    }

    fn snapshotting_base_nodes(&self) -> Vec<LocalNodeIndex> {
        self.state
            .iter()
            .filter_map(|(idx, s)| match s.as_persistent() {
                Some(p_state) if p_state.is_snapshotting() => Some(idx),
                _ => None,
            })
            .collect()
    }

    /// Initiate a replay for a miss represented by the given keys and column indices in the given
    /// node.
    ///
    /// # Invariants
    ///
    /// * `miss_columns` must not be empty
    /// * `miss_in` must be a node in the graph
    fn find_tags_and_replay(
        &mut self,
        miss_keys: Vec<KeyComparison>,
        miss_columns: &[usize],
        miss_in: LocalNodeIndex,
    ) -> Result<(), ReadySetError> {
        let miss_index = Index::new(IndexType::best_for_keys(&miss_keys), miss_columns.to_vec());
        // the cloned is a bit sad; self.request_partial_replay doesn't use
        // self.replay_paths_by_dst.
        let tags = self
            .replay_paths_by_dst
            .get(miss_in)
            .and_then(|indexes| indexes.get(&miss_index))
            .cloned()
            .unwrap_or_default();

        let remapped_upqueries = if self
            .generated_columns
            .contains(&(miss_in, miss_columns.to_owned()))
        {
            #[allow(clippy::indexing_slicing)] // Documented invariant
            let mut n = self.nodes[miss_in].borrow_mut();
            let miss_node = n.global_addr();
            #[allow(clippy::unwrap_used)] // Documented invariant
            let upqueries = n.handle_upquery(ColumnMiss {
                missed_columns: ColumnRef {
                    node: miss_node,
                    columns: Vec1::try_from(miss_columns).unwrap(),
                },
                missed_key: Vec1::try_from(miss_keys.clone()).unwrap(),
            })?;
            drop(n);
            if upqueries.is_empty() {
                internal!(
                    "columns {:?} in l{} are generated, but could not remap an upquery",
                    miss_index,
                    miss_in.id()
                );
            }
            upqueries
        } else {
            vec![]
        };
        let mut processed = 0;
        for &tag in &tags {
            // send a message to the source domain(s) responsible
            // for the chosen tag so they'll start replay.
            let keys = if remapped_upqueries.is_empty() {
                miss_keys.clone()
            } else {
                // If `handle_upquery` was used, we need to filter tags down to only include those
                // that are mentioned in the list of remapped upqueries.
                // We also need to twiddle the `keys` to be the key associated with the remapped
                // upquery for the given tag.
                #[allow(clippy::indexing_slicing)] // Tag must exist
                let replay_path = &self.raw_replay_paths[&tag];
                let mut new_keys = None;
                for IndexRef { node, index } in replay_path.iter() {
                    if let Some(index) = index {
                        for upquery in remapped_upqueries.iter() {
                            if *node == upquery.missed_columns.node
                                // TODO(grfn): Why is this matching on `len()` rather than the
                                // columns themselves?
                                && upquery.missed_columns.columns.len() == index.columns.len()
                                && upquery
                                    .missed_key
                                    .iter()
                                    // TODO(grfn): Btrees support point keys - what if we find a
                                    // btree first while a hash already exists?  I *think* this is
                                    // fine, but should be double checked later
                                    .all(|k| index.index_type.supports_key(k))
                            {
                                // This tag is one we want to replay along.
                                if new_keys.is_some() {
                                    // We should only match one remapped upquery per tag.
                                    internal!(
                                        "conflicting remapped upqueries while finding tags for {:?} in l{}: {:?}",
                                        miss_index,
                                        miss_in.id(),
                                        remapped_upqueries
                                    );
                                }
                                new_keys = Some(upquery.missed_key.clone().into_vec());
                            }
                        }
                    }
                }
                if let Some(nk) = new_keys {
                    for (orig_key, new_key) in
                        miss_keys.clone().into_iter().zip(nk.clone().into_iter())
                    {
                        self.active_remaps
                            .entry((miss_in, (miss_index.columns.to_owned(), orig_key)))
                            .or_default()
                            .insert((tag, new_key));
                    }
                    nk
                } else {
                    continue;
                }
            };
            processed += 1;
            #[allow(clippy::indexing_slicing)] // Tag must exist
            if let TriggerEndpoint::Local(..) = self.replay_paths[&tag].trigger {
                // *in theory* we could just call self.seed_all, and everything would be good.
                // however, then we start recursing, which could get us into sad situations where
                // we break invariants where some piece of code is assuming that it is the only
                // thing processing at the time (think, e.g., borrow_mut()).
                //
                // for example, consider the case where two misses occurred on the same key.
                // normally, those requests would be deduplicated so that we don't get two replay
                // responses for the same key later. however, the way we do that is by tracking
                // keys we have requested in self.waiting.redos (see `redundant` in
                // `on_replay_miss`). in particular, on_replay_miss is called while looping over
                // all the misses that need replays, and while the first miss of a given key will
                // trigger a replay, the second will not. if we call `seed_all` directly here,
                // that might immediately fill in this key and remove the entry. when the next miss
                // (for the same key) is then hit in the outer iteration, it will *also* request a
                // replay of that same key, which gets us into trouble with `State::mark_filled`.
                //
                // so instead, we simply keep track of the fact that we have a replay to handle,
                // and then get back to it after all processing has finished (at the bottom of
                // `Self::handle()`)
                self.delayed_for_self
                    .push_back(Box::new(Packet::RequestPartialReplay {
                        tag,
                        keys,
                        unishard: true, // local replays are necessarily single-shard
                        requesting_shard: self.shard.unwrap_or(0),
                    }));
                continue;
            }

            // NOTE: due to max_concurrent_replays, it may be that we only replay from *some* of
            // these ancestors now, and some later. this will cause more of the replay to be
            // buffered up at the union above us, but that's probably fine.
            self.request_partial_replay(tag, keys)?;
        }

        if tags.is_empty() {
            internal!(
                "no tag found to fill missing value {:?} in {}.{:?}; available tags: {:?}",
                miss_keys,
                miss_in,
                miss_index,
                self.replay_paths_by_dst.get(miss_in)
            );
        }
        if processed == 0 {
            internal!(
                "no tag found to fill missing value {:?} in {}.{:?} after remap; available tags: {:?}; remapped upqueries: {:?}",
                miss_keys,
                miss_in,
                miss_index,
                self.replay_paths_by_dst.get(miss_in),
                remapped_upqueries
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn on_replay_misses(
        &mut self,
        miss_in: LocalNodeIndex,
        miss_columns: &[usize],
        misses: HashSet<(KeyComparison, KeyComparison)>,
        was_single_shard: bool,
        requesting_shard: usize,
        needed_for: Tag,
    ) -> Result<(), ReadySetError> {
        use std::collections::hash_map::Entry;
        use std::ops::AddAssign;

        let mut miss_keys = Vec::with_capacity(misses.len());

        // when the replay eventually succeeds, we want to re-do the replay.
        let mut w = self.waiting.remove(miss_in).unwrap_or_default();

        counter!(
            recorded::DOMAIN_REPLAY_MISSES,
            misses.len() as u64,
            // HACK(eta): having to call `to_string()` here makes me sad,
            // but seems to be a limitation of the `metrics` crate
            "domain" => self.index.index().to_string(),
            "shard" => self.shard.unwrap_or(0).to_string(),
            "miss_in" => miss_in.id().to_string(),
            "needed_for" => needed_for.to_string()
        );

        for (replay_key, miss_key) in misses {
            let redo = Redo {
                tag: needed_for,
                replay_key,
                unishard: was_single_shard,
                requesting_shard,
            };
            match w.redos.entry((Vec::from(miss_columns), miss_key.clone())) {
                Entry::Occupied(e) => {
                    // we have already requested backfill of this key
                    // remember to notify this Redo when backfill completes
                    if e.into_mut().insert(redo.clone()) {
                        // this Redo should wait for this backfill to complete before redoing
                        w.holes.entry(redo).or_default().add_assign(1);
                    }
                    continue;
                }
                Entry::Vacant(e) => {
                    // we haven't already requested backfill of this key
                    let mut redos = HashSet::new();
                    // remember to notify this Redo when backfill completes
                    redos.insert(redo.clone());
                    e.insert(redos);
                    // this Redo should wait for this backfill to complete before redoing
                    w.holes.entry(redo).or_default().add_assign(1);
                }
            }
            miss_keys.push(miss_key);
        }

        self.waiting.insert(miss_in, w);

        if !miss_keys.is_empty() {
            self.find_tags_and_replay(miss_keys, miss_columns, miss_in)?
        };

        Ok(())
    }

    /// Send a partial replay request for keys to the replay path indicated by tag
    ///
    /// # Invariants
    ///
    /// * `tag` must be a tag for a valid replay path
    fn send_partial_replay_request(
        &mut self,
        tag: Tag,
        keys: Vec<KeyComparison>,
    ) -> ReadySetResult<()> {
        debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
        #[allow(clippy::unwrap_used)] // documented invariant
        if let TriggerEndpoint::End {
            source,
            ref mut options,
        } = self.replay_paths.get_mut(&tag).unwrap().trigger
        {
            let ask_shard_by_key_i = match source {
                SourceSelection::AllShards(_) => None,
                SourceSelection::SameShard => {
                    // note that we "ask all" here because we're not indexing the vector by the
                    // key's shard index. unipath will still be set to true though, since
                    // options.len() == 1.
                    None
                }
                SourceSelection::KeyShard { key_i_to_shard, .. } => Some(key_i_to_shard),
            };

            if ask_shard_by_key_i.is_none() && options.len() != 1 {
                // source is sharded by a different key than we are doing lookups for,
                // so we need to trigger on all the shards.
                self.concurrent_replays += 1;
                trace!(
                    ?tag,
                    ?keys,
                    buffered = self.replay_request_queue.len(),
                    concurrent = self.concurrent_replays,
                    "sending shuffled shard replay request"
                );

                for trigger in options {
                    if trigger
                        .send(Box::new(Packet::RequestPartialReplay {
                            tag,
                            unishard: false, // ask_all is true, so replay is sharded
                            keys: keys.clone(), // sad to clone here
                            requesting_shard: self.shard.unwrap_or(0),
                        }))
                        .is_err()
                    {
                        // we're shutting down -- it's fine.
                    }
                }
                return Ok(());
            }

            self.concurrent_replays += 1;
            trace!(
                tag = ?tag,
                keys = ?keys,
                buffered = %self.replay_request_queue.len(),
                concurrent = %self.concurrent_replays,
                "sending replay request",
            );

            if options.len() == 1 {
                #[allow(clippy::indexing_slicing)] // we just checked len() is 1
                if options[0]
                    .send(Box::new(Packet::RequestPartialReplay {
                        tag,
                        keys,
                        unishard: true, // only one option, so only one path
                        requesting_shard: self.shard.unwrap_or(0),
                    }))
                    .is_err()
                {
                    // we're shutting down -- it's fine.
                }
            } else if let Some(key_shard_i) = ask_shard_by_key_i {
                let mut shards = HashMap::new();
                for key in keys {
                    for shard in key.shard_keys_at(key_shard_i, options.len()) {
                        shards
                            .entry(shard)
                            .or_insert_with(Vec::new)
                            .push(key.clone());
                    }
                }
                for (shard, keys) in shards {
                    #[allow(clippy::indexing_slicing)] // we know len(options) is num_shards
                    if options[shard]
                        .send(Box::new(Packet::RequestPartialReplay {
                            tag,
                            keys,
                            unishard: true, // !ask_all, so only one path
                            requesting_shard: self.shard.unwrap_or(0),
                        }))
                        .is_err()
                    {
                        // we're shutting down -- it's fine.
                    }
                }
            } else {
                // would have hit the if further up
                internal!();
            };
        } else {
            internal!("asked to replay along non-existing path");
        }
        Ok(())
    }

    fn request_partial_replay(
        &mut self,
        tag: Tag,
        keys: Vec<KeyComparison>,
    ) -> Result<(), ReadySetError> {
        if self.concurrent_replays < self.max_concurrent_replays {
            assert_eq!(self.replay_request_queue.len(), 0);
            self.send_partial_replay_request(tag, keys)?;
            Ok(())
        } else {
            trace!(
                tag = ?tag,
                keys = ?keys,
                buffered = %self.replay_request_queue.len(),
                "buffering replay request"
            );
            self.replay_request_queue.push_back((tag, keys));
            Ok(())
        }
    }

    /// Called when a partial replay has been completed
    ///
    /// # Invariants
    ///
    /// * `tag` must be a valid replay tag
    fn finished_partial_replay(&mut self, tag: Tag, num: usize) -> Result<(), ReadySetError> {
        #[allow(clippy::indexing_slicing)] // documented invariant
        match self.replay_paths[&tag].trigger {
            TriggerEndpoint::End { .. } => {
                // A backfill request we made to another domain was just satisfied!
                // We can now issue another request from the concurrent replay queue.
                // However, since unions require multiple backfill requests, but produce only one
                // backfill reply, we need to check how many requests we're now free to issue. If
                // we just naively release one slot here, a union with two parents would mean that
                // `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
                // -1 when satisfied), which would lead to a deadlock!
                let mut requests_satisfied = 0;
                #[allow(clippy::unwrap_used)] // Replay paths can't be empty
                let last = self.replay_paths[&tag].last_segment();
                if let Some(cs) = self.replay_paths_by_dst.get(last.node) {
                    // We already know it's a partial replay path, so it must have a partial key
                    #[allow(clippy::unwrap_used)]
                    if let Some(tags) = cs.get(last.partial_index.as_ref().unwrap()) {
                        requests_satisfied = tags
                            .iter()
                            .filter(|tag| {
                                matches!(
                                    self.replay_paths[tag].trigger,
                                    TriggerEndpoint::End { .. }
                                )
                            })
                            .count();
                    }
                }

                // we also sent that many requests *per key*.
                requests_satisfied *= num;

                // TODO: figure out why this can underflow
                self.concurrent_replays =
                    self.concurrent_replays.saturating_sub(requests_satisfied);
                trace!(
                    num_done = requests_satisfied,
                    ongoing = self.concurrent_replays,
                    "notified of finished replay"
                );
                debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
                let mut per_tag = HashMap::new();
                while self.concurrent_replays < self.max_concurrent_replays {
                    if let Some((tag, mut keys)) = self.replay_request_queue.pop_front() {
                        per_tag
                            .entry(tag)
                            .or_insert_with(Vec::new)
                            .append(&mut keys);
                    } else {
                        break;
                    }
                }

                for (tag, keys) in per_tag {
                    trace!(
                        ?tag,
                        ?keys,
                        left = self.replay_request_queue.len(),
                        ongoing = self.concurrent_replays,
                        "releasing replay request"
                    );
                    self.send_partial_replay_request(tag, keys)?;
                }
                Ok(())
            }
            TriggerEndpoint::Local(..) => {
                // didn't count against our quote, so we're also not decementing
                Ok(())
            }
            TriggerEndpoint::Start(..) | TriggerEndpoint::None => {
                internal!();
            }
        }
    }

    fn dispatch(&mut self, m: Box<Packet>, executor: &mut dyn Executor) -> ReadySetResult<()> {
        let src = m.src();
        let me = m.dst();

        if !self.nodes.contains_key(me) {
            return Err(ReadySetError::NoSuchNode(me.id()));
        }

        match self.mode {
            DomainMode::Forwarding => (),
            DomainMode::Replaying {
                ref to,
                ref mut buffered,
                ..
            } if to == &me => {
                buffered.push_back(m);
                return Ok(());
            }
            DomainMode::Replaying { .. } => (),
        }

        if !self.not_ready.is_empty() && self.not_ready.contains(&me) {
            return Ok(());
        }

        let (mut m, evictions) = {
            #[allow(clippy::indexing_slicing)] // we checked the node exists already
            let mut n = self.nodes[me].borrow_mut();
            self.process_times.start(me);
            self.process_ptimes.start(me);
            let mut m = Some(m);
            let NodeProcessingResult {
                misses, captured, ..
            } = n.process(
                &mut m,
                None,
                &mut self.state,
                &self.nodes,
                self.shard,
                true,
                None,
                executor,
                false,
            )?;
            assert_eq!(captured.len(), 0);
            self.process_ptimes.stop();
            self.process_times.stop();

            if m.is_none() {
                // no need to deal with our children if we're not sending them anything
                return Ok(());
            }

            // normally, we ignore misses during regular forwarding.
            // however, we have to be a little careful in the case of joins.
            let evictions = if n.is_internal() && n.is_join()? && !misses.is_empty() {
                // [note: downstream-join-evictions]
                //
                // there are two possible cases here:
                //
                //  - this is a write that will hit a hole in every downstream materialization.
                //    dropping it is totally safe!
                //  - this is a write that will update an entry in some downstream materialization.
                //    this is *not* allowed! we *must* ensure that downstream remains up to date.
                //    but how can we? we missed in the other side of the join, so we can't produce
                //    the necessary output record... what we *can* do though is evict from any
                //    downstream, and then we guarantee that we're in case 1!
                //
                // if you're curious about how we may have ended up in case 2 above, here are two
                // ways:
                //
                //  - some downstream view is partial over the join key. some time in the past, it
                //    requested a replay of key k. that replay produced *no* rows from the side
                //    that was replayed. this in turn means that no lookup was performed on the
                //    other side of the join, and so k wasn't replayed to that other side (which
                //    then still has a hole!). in that case, any subsequent write with k in the
                //    join column from the replay side will miss in the other side.
                //  - some downstream view is partial over a column that is *not* the join key. in
                //    the past, it replayed some key k, which means that we aren't allowed to drop
                //    any write with k in that column. now, a write comes along with k in that
                //    replay column, but with some hitherto unseen key z in the join column. if the
                //    replay of k never caused a lookup of z in the other side of the join, then
                //    the other side will have a hole. thus, we end up in the situation where we
                //    need to forward a write through the join, but we miss.
                //
                // unfortunately, we can't easily distinguish between the case where we have to
                // evict and the case where we don't (at least not currently), so we *always* need
                // to evict when this happens. this shouldn't normally be *too* bad, because writes
                // are likely to be dropped before they even reach the join in most benign cases
                // (e.g., in an ingress). this can be remedied somewhat in the future by ensuring
                // that the first of the two causes outlined above can't happen (by always doing a
                // lookup on the replay key, even if there are now rows). then we know that the
                // *only* case where we have to evict is when the replay key != the join key.
                //
                // but, for now, here we go:
                // first, what partial replay paths go through this node?
                let from = self
                    .nodes
                    .get(src)
                    .ok_or_else(|| ReadySetError::NoSuchNode(src.id()))?
                    .borrow()
                    .global_addr();
                // TODO: this is a linear walk of replay paths -- we should make that not linear
                let deps: Vec<(Tag, Vec<usize>)> = self
                    .replay_paths
                    .iter()
                    .filter_map(|(&tag, rp)| {
                        rp.path
                            .iter()
                            .find(|rps| rps.node == me)
                            .and_then(|rps| rps.partial_index.as_ref())
                            .and_then(|index| {
                                // we need to find the *input* column that produces that output.
                                //
                                // if one of the columns for this replay path's keys does not
                                // resolve into the ancestor we got the update from, we don't need
                                // to issue an eviction for that path. this is because we *missed*
                                // on the join column in the other side, so we *know* it can't have
                                // forwarded anything related to the write we're now handling.
                                index
                                    .columns
                                    .iter()
                                    .map(|&k| {
                                        n.parent_columns(k)
                                            .into_iter()
                                            .find(|&(ni, _)| ni == from)
                                            .and_then(|k| k.1)
                                    })
                                    .collect::<Option<Vec<_>>>()
                            })
                            .map(move |k| (tag, k))
                    })
                    .collect();

                let mut evictions: HashMap<Tag, HashSet<KeyComparison>> = HashMap::new();
                for miss in misses {
                    for (tag, cols) in &deps {
                        evictions.entry(*tag).or_insert_with(HashSet::new).insert(
                            miss.record
                                .cloned_indices(cols.iter().copied())
                                .unwrap()
                                .try_into()
                                .unwrap(),
                        );
                    }
                }

                Some(evictions)
            } else {
                None
            };

            (m, evictions)
        };

        if let Some(evictions) = evictions {
            // now send evictions for all the (tag, [key]) things in evictions
            for (tag, keys) in evictions {
                self.handle_eviction(
                    Packet::EvictKeys {
                        keys: keys.into_iter().collect(),
                        link: Link::new(src, me),
                        tag,
                    },
                    executor,
                )?;
            }
        }

        // We checked it's Some above, it's only an Option so we can take()
        #[allow(clippy::unwrap_used)]
        match &**m.as_ref().unwrap() {
            m @ &Packet::Message { .. } if m.is_empty() => {
                // no need to deal with our children if we're not sending them anything
                return Ok(());
            }
            &Packet::Message { .. } => {}
            &Packet::ReplayPiece { .. } => {
                internal!("Replay should never go through dispatch.");
            }
            m => {
                internal!("dispatch process got {:?}", m);
            }
        }

        // NOTE: we can't directly iterate over .children due to self.dispatch in the loop
        #[allow(clippy::indexing_slicing)] // we checked the node exists above
        let nchildren = self.nodes[me].borrow().children().len();
        for i in 0..nchildren {
            // We checked it's Some above, it's only an Option so we can take()
            #[allow(clippy::unwrap_used)]
            // avoid cloning if we can
            let mut m = if i == nchildren - 1 {
                m.take().unwrap()
            } else {
                m.as_ref().map(|m| Box::new(m.clone_data())).unwrap()
            };

            #[allow(clippy::indexing_slicing)] // see NOTE above
            let childi = self.nodes[me].borrow().children()[i];
            let child_is_merger = {
                // XXX: shouldn't NLL make this unnecessary?
                #[allow(clippy::indexing_slicing)]
                // we got the node from the children of the other node
                let c = self.nodes[childi].borrow();
                c.is_shard_merger()
            };

            if child_is_merger {
                // we need to preserve the egress src (which includes shard identifier)
            } else {
                m.link_mut().src = me;
            }
            m.link_mut().dst = childi;

            self.dispatch(m, executor)?;
        }
        Ok(())
    }

    fn handle_timestamp(
        &mut self,
        message: Packet,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let me = message.dst();

        let message = {
            let mut n = self
                .nodes
                .get(me)
                .ok_or_else(|| ReadySetError::NoSuchNode(me.id()))?
                .borrow_mut();
            n.process_timestamp(message, executor)?
        };

        let message = if let Some(m) = message {
            m
        } else {
            // no message to send, so no need to run through children
            return Ok(());
        };

        #[allow(clippy::indexing_slicing)] // Already checked the node exists
        let nchildren = self.nodes[me].borrow().children().len();
        for i in 0..nchildren {
            let mut p = Box::new(message.as_ref().clone_data());

            #[allow(clippy::indexing_slicing)]
            // Already checked the node exists, and we're iterating through nchildren so we know i
            // is in-bounds
            let childi = self.nodes[me].borrow().children()[i];
            let child_is_merger = {
                // XXX: shouldn't NLL make this unnecessary?
                #[allow(clippy::indexing_slicing)]
                // we know the child exists since we got it from the node
                let c = self.nodes[childi].borrow();
                c.is_shard_merger()
            };

            // The packet `m` must have a link by this point as `link_mut` calls
            // unwrap on the option.
            if child_is_merger {
                // we need to preserve the egress src (which includes shard identifier)
            } else {
                p.link_mut().src = me;
            }
            p.link_mut().dst = childi;

            self.handle_packet(p, executor)?;
        }
        Ok(())
    }

    pub fn domain_request(
        &mut self,
        req: DomainRequest,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let span = info_span!("domain", index = %self.index);
        let _guard = span.enter();

        let ret = match req {
            DomainRequest::AddNode { node, parents } => {
                let addr = node.local_addr();
                self.not_ready.insert(addr);

                for p in parents {
                    self.nodes
                        .get_mut(p)
                        .unwrap()
                        .borrow_mut()
                        .add_child(node.local_addr());
                }
                self.nodes.insert(addr, cell::RefCell::new(node));
                trace!(local = addr.id(), "new node incorporated");
                Ok(None)
            }
            DomainRequest::RemoveNodes { nodes } => {
                for &node in &nodes {
                    self.nodes
                        .get(node)
                        .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                        .borrow_mut()
                        .remove();
                    self.state.remove(node);
                    gauge!(
                        "domain.node_state_size_bytes",
                        0.0,
                        "domain" => self.index.index().to_string(),
                        "shard" => self.shard.unwrap_or(0).to_string(),
                        "node" => node.id().to_string()
                    );
                    trace!(local = node.id(), "node removed");
                }

                for node in nodes {
                    for cn in self.nodes.iter_mut() {
                        cn.1.borrow_mut().try_remove_child(node);
                        // NOTE: since nodes are always removed leaves-first, it's not
                        // important to update parent pointers here
                    }
                }
                Ok(None)
            }
            DomainRequest::AddBaseColumn {
                node,
                field,
                default,
            } => {
                let mut n = self
                    .nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                    .borrow_mut();
                n.add_column(&field);
                if let Some(b) = n.get_base_mut() {
                    b.add_column(default)?;
                } else if n.is_ingress() {
                    self.ingress_inject
                        .entry(node)
                        .or_insert_with(|| (n.fields().len(), Vec::new()))
                        .1
                        .push(default);
                } else {
                    internal!("node unrelated to base got AddBaseColumn");
                }
                Ok(None)
            }
            DomainRequest::DropBaseColumn { node, column } => {
                let mut n = self
                    .nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                    .borrow_mut();
                n.get_base_mut()
                    .ok_or_else(|| internal_err("told to drop base column from non-base node"))?
                    .drop_column(column)?;
                Ok(None)
            }
            DomainRequest::UpdateEgress {
                node,
                new_tx,
                new_tag,
            } => {
                let mut n = self
                    .nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                    .borrow_mut();

                let e = n.as_mut_egress().ok_or(ReadySetError::InvalidNodeType {
                    node_index: node.id(),
                    expected_type: NodeType::Egress,
                })?;

                if let Some((node, local, addr)) = new_tx {
                    e.add_tx(node, local, addr);
                }

                if let Some(new_tag) = new_tag {
                    e.add_tag(new_tag.0, new_tag.1);
                }
                Ok(None)
            }
            DomainRequest::AddEgressFilter {
                egress_node,
                target_node,
            } => {
                let mut n = self
                    .nodes
                    .get(egress_node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(egress_node.id()))?
                    .borrow_mut();

                n.as_mut_egress()
                    .ok_or(ReadySetError::InvalidNodeType {
                        node_index: egress_node.id(),
                        expected_type: NodeType::Egress,
                    })?
                    .add_for_filtering(target_node);
                Ok(None)
            }
            DomainRequest::UpdateSharder { node, new_txs } => {
                self.nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                    .borrow_mut()
                    .as_mut_sharder()
                    .ok_or(ReadySetError::InvalidNodeType {
                        node_index: node.id(),
                        expected_type: NodeType::Sharder,
                    })?
                    .add_sharded_child(new_txs.0, new_txs.1);
                Ok(None)
            }
            DomainRequest::StateSizeProbe { node } => {
                let row_count = self.state.get(node).map(|r| r.rows()).unwrap_or(0);
                let mem_size = self.state.get(node).map(|s| s.deep_size_of()).unwrap_or(0);
                let ret = (row_count, mem_size);
                Ok(Some(bincode::serialize(&ret)?))
            }
            DomainRequest::PrepareState { node, state } => {
                use crate::payload::InitialState;
                match state {
                    InitialState::PartialLocal { strict, weak } => {
                        if !self.state.contains_key(node) {
                            self.state.insert(node, Box::new(MemoryState::default()));
                        }
                        let state = self.state.get_mut(node).unwrap();
                        for (index, tags) in strict {
                            debug!(
                                index = ?index,
                                tags = ?tags,
                                "told to prepare partial state"
                            );
                            state.add_key(index, Some(tags));
                        }

                        for index in weak {
                            state.add_weak_key(index);
                        }
                    }
                    InitialState::IndexedLocal { strict, weak } => {
                        if !self.state.contains_key(node) {
                            self.state.insert(node, Box::new(MemoryState::default()));
                        }
                        let state = self.state.get_mut(node).unwrap();
                        for index in strict {
                            debug!(
                                key = ?index,
                                "told to prepare full state"
                            );
                            state.add_key(index, None);
                        }

                        for index in weak {
                            state.add_weak_key(index);
                        }
                    }
                    InitialState::PartialGlobal {
                        gid,
                        cols,
                        key,
                        trigger_domain: (trigger_domain, shards),
                    } => {
                        use crate::backlog;

                        if !self
                            .nodes
                            .get(node)
                            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                            .borrow()
                            .is_reader()
                        {
                            return Err(ReadySetError::InvalidNodeType {
                                node_index: node.id(),
                                expected_type: NodeType::Reader,
                            });
                        }

                        let k = key.clone(); // ugh
                        let txs = (0..shards)
                            .map(|shard| -> ReadySetResult<_> {
                                let key = key.clone();
                                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                                let sender = self
                                    .channel_coordinator
                                    .builder_for(&(trigger_domain, shard))?
                                    .build_async()?;

                                tokio::spawn(
                                    UnboundedReceiverStream::new(rx)
                                        .map(move |misses| {
                                            Box::new(Packet::RequestReaderReplay {
                                                keys: misses,
                                                cols: key.clone(),
                                                node,
                                            })
                                        })
                                        .map(Ok)
                                        .forward(sender)
                                        .map(|r| {
                                            if let Err(e) = r {
                                                // domain went away?
                                                error!(error = %e, "replay source went away");
                                            }
                                        }),
                                );
                                Ok(tx)
                            })
                            .collect::<ReadySetResult<Vec<_>>>()?;
                        let (mut r_part, w_part) = backlog::new_partial(
                            cols,
                            &k[..],
                            move |misses: &mut dyn Iterator<Item = &KeyComparison>| {
                                let n = txs.len();
                                if n == 1 {
                                    let misses = misses.cloned().collect::<Vec<_>>();
                                    if misses.is_empty() {
                                        return true;
                                    }
                                    #[allow(clippy::indexing_slicing)] // just checked len is 1
                                    txs[0].send(misses).is_ok()
                                } else {
                                    // TODO: compound reader
                                    let mut per_shard = HashMap::new();
                                    for miss in misses {
                                        assert!(matches!(miss.len(), Some(1) | None));
                                        for shard in miss.shard_keys(n) {
                                            per_shard
                                                .entry(shard)
                                                .or_insert_with(Vec::new)
                                                .push(miss);
                                        }
                                    }
                                    if per_shard.is_empty() {
                                        return true;
                                    }
                                    per_shard.into_iter().all(|(shard, keys)| {
                                        #[allow(clippy::indexing_slicing)]
                                        // we know txs.len() is equal to num_shards
                                        txs[shard].send(keys.into_iter().cloned().collect()).is_ok()
                                    })
                                }
                            },
                        );

                        #[allow(clippy::indexing_slicing)] // checked node exists above
                        let mut n = self.nodes[node].borrow_mut();
                        let name = n.name().to_owned();
                        #[allow(clippy::unwrap_used)] // checked it was a reader above
                        let r = n.as_mut_reader().unwrap();
                        r_part.post_lookup = r.post_lookup().clone();

                        let shard = *self.shard.as_ref().unwrap_or(&0);
                        // TODO(ENG-838): Don't recreate every single node on leader failure.
                        // This requires us to overwrite the existing reader.
                        #[allow(clippy::unwrap_used)] // lock poisoning is unrecoverable
                        if self
                            .readers
                            .lock()
                            .unwrap()
                            .insert((gid, name.clone(), shard), r_part)
                            .is_some()
                        {
                            warn!(?gid, %name, %shard, "Overwrote existing reader at worker");
                        }

                        // make sure Reader is actually prepared to receive state
                        r.set_write_handle(w_part)
                    }
                    InitialState::Global { gid, cols, key } => {
                        use crate::backlog;
                        let (mut r_part, w_part) = backlog::new(cols, &key[..]);

                        if !self
                            .nodes
                            .get(node)
                            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                            .borrow()
                            .is_reader()
                        {
                            return Err(ReadySetError::InvalidNodeType {
                                node_index: node.id(),
                                expected_type: NodeType::Reader,
                            });
                        }

                        let mut n = self
                            .nodes
                            .get(node)
                            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                            .borrow_mut();
                        let name = n.name().to_owned();

                        #[allow(clippy::unwrap_used)] // checked it was a reader above
                        let r = n.as_mut_reader().unwrap();
                        r_part.post_lookup = r.post_lookup().clone();

                        let shard = *self.shard.as_ref().unwrap_or(&0);
                        // TODO(ENG-838): Don't recreate every single node on leader failure.
                        // This requires us to overwrite the existing reader.
                        #[allow(clippy::unwrap_used)] // lock poisoning is unrecoverable
                        if self
                            .readers
                            .lock()
                            .unwrap()
                            .insert((gid, name, shard), r_part)
                            .is_some()
                        {
                            warn!(?gid, ?shard, "Overwrote existing reader at worker");
                        }

                        // make sure Reader is actually prepared to receive state
                        r.set_write_handle(w_part)
                    }
                }
                Ok(None)
            }
            DomainRequest::SetupReplayPath {
                tag,
                source,
                path,
                notify_done,
                partial_unicast_sharder,
                trigger,
                raw_path,
            } => {
                if notify_done {
                    debug!(?path, ?tag, "told about terminating replay path",);
                    // NOTE: we set self.replaying_to when we first receive a replay with
                    // this tag
                } else {
                    debug!(?path, ?tag, "told about replay path");
                }

                use crate::payload;
                let trigger = match trigger {
                    payload::TriggerEndpoint::None => TriggerEndpoint::None,
                    payload::TriggerEndpoint::Start(index) => TriggerEndpoint::Start(index),
                    payload::TriggerEndpoint::Local(index) => TriggerEndpoint::Local(index),
                    payload::TriggerEndpoint::End(selection, domain) => {
                        let shard = |shardi| -> ReadySetResult<_> {
                            // TODO: make async
                            Ok(self
                                .channel_coordinator
                                .builder_for(&(domain, shardi))?
                                .build_sync()?)
                        };

                        let options = match selection {
                            SourceSelection::AllShards(nshards)
                            | SourceSelection::KeyShard { nshards, .. } => {
                                // we may need to send to any of these shards
                                (0..nshards).map(shard).collect::<Result<Vec<_>, _>>()
                            }
                            SourceSelection::SameShard => {
                                Ok(vec![shard(self.shard.ok_or_else(|| {
                                    internal_err(
                                        "Cannot use SourceSelection::SameShard for a replay path\
                                             through an unsharded domain",
                                    )
                                })?)?])
                            }
                        }?;

                        TriggerEndpoint::End {
                            source: selection,
                            options,
                        }
                    }
                };

                if let TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) = trigger {
                    let last = path.last();
                    #[allow(clippy::unwrap_used)] // Replay path must have a partial key
                    self.replay_paths_by_dst
                        .entry(last.node)
                        .or_insert_with(HashMap::new)
                        .entry(last.partial_index.clone().unwrap())
                        .or_insert_with(Vec::new)
                        .push(tag);
                }

                self.replay_paths.insert(
                    tag,
                    ReplayPath {
                        source,
                        path,
                        notify_done,
                        partial_unicast_sharder,
                        trigger,
                    },
                );
                self.raw_replay_paths.insert(tag, raw_path);
                Ok(None)
            }
            DomainRequest::StartReplay { tag, from } => {
                use std::thread;
                invariant_eq!(
                    self.replay_paths
                        .get(&tag)
                        .ok_or_else(|| ReadySetError::NoSuchReplayPath(tag.into()))?
                        .source,
                    Some(from)
                );

                let start = time::Instant::now();
                self.total_replay_time.start();
                info!("starting replay");

                // we know that the node is materialized, as the migration coordinator
                // picks path that originate with materialized nodes. if this weren't the
                // case, we wouldn't be able to do the replay, and the entire migration
                // would fail.
                //
                // we clone the entire state so that we can continue to occasionally
                // process incoming updates to the domain without disturbing the state that
                // is being replayed.
                let state = self
                    .state
                    .get(from)
                    .expect("migration replay path started with non-materialized node")
                    .cloned_records();

                debug!(
                    μs = %start.elapsed().as_micros(),
                    "current state cloned for replay"
                );

                #[allow(clippy::indexing_slicing)]
                // we checked the replay path exists above, and replay paths cannot be empty
                let link = Link::new(from, self.replay_paths[&tag].path[0].node);

                // we're been given an entire state snapshot, but we need to digest it
                // piece by piece spawn off a thread to do that chunking. however, before
                // we spin off that thread, we need to send a single Replay message to tell
                // the target domain to start buffering everything that follows. we can't
                // do that inside the thread, because by the time that thread is scheduled,
                // we may already have processed some other messages that are not yet a
                // part of state.
                let p = Box::new(Packet::ReplayPiece {
                    tag,
                    link,
                    context: ReplayPieceContext::Regular {
                        last: state.is_empty(),
                    },
                    data: Vec::<Record>::new().into(),
                });

                if !state.is_empty() {
                    let added_cols = self.ingress_inject.get(from).cloned();
                    let default = {
                        let n = self
                            .nodes
                            .get(from)
                            .ok_or_else(|| ReadySetError::NoSuchNode(from.id()))?
                            .borrow();
                        let mut default = None;
                        if let Some(b) = n.get_base() {
                            let mut row = Vec::new();
                            b.fix(&mut row);
                            default = Some(row);
                        }
                        default
                    };
                    let fix = move |mut r: Vec<DataType>| -> Vec<DataType> {
                        if let Some((start, ref added)) = added_cols {
                            let rlen = r.len();
                            r.extend(added.iter().skip(rlen - start).cloned());
                        } else if let Some(ref defaults) = default {
                            let rlen = r.len();
                            r.extend(defaults.iter().skip(rlen).cloned());
                        }
                        r
                    };

                    let replay_tx_desc = self
                        .channel_coordinator
                        .builder_for(&(self.index, self.shard.unwrap_or(0)))?;

                    let domain_str = self.index.index().to_string();
                    let shard_str = self.shard.unwrap_or(0).to_string();

                    thread::Builder::new()
                        .name(format!(
                            "replay{}.{}",
                            #[allow(clippy::unwrap_used)] // self.nodes can't be empty
                            self.nodes
                                .values()
                                .next()
                                .unwrap()
                                .borrow()
                                .domain()
                                .index(),
                            link.src
                        ))
                        .spawn(move || {
                            use itertools::Itertools;

                            // TODO: make async
                            let mut chunked_replay_tx = match replay_tx_desc.build_sync() {
                                Ok(r) => r,
                                Err(error) => {
                                    error!(%error, "Error building channel for chunked replay");
                                    return;
                                }
                            };

                            let start = time::Instant::now();
                            debug!(node = %link.dst, "starting state chunker");

                            let iter = state.into_iter().chunks(BATCH_SIZE);
                            let mut iter = iter.into_iter().enumerate().peekable();

                            // process all records in state to completion within domain
                            // and then forward on tx (if there is one)
                            while let Some((i, chunk)) = iter.next() {
                                use std::iter::FromIterator;
                                let chunk = Records::from_iter(chunk.map(&fix));
                                let len = chunk.len();
                                let last = iter.peek().is_none();
                                let p = Box::new(Packet::ReplayPiece {
                                    tag,
                                    link, // to is overwritten by receiver
                                    context: ReplayPieceContext::Regular { last },
                                    data: chunk,
                                });

                                trace!(num = i, len, "sending batch");
                                if chunked_replay_tx.send(p).is_err() {
                                    warn!("replayer noticed domain shutdown");
                                    break;
                                }
                            }

                            debug!(
                               node = %link.dst,
                               μs = %start.elapsed().as_micros(),
                               "state chunker finished"
                            );

                            histogram!(
                                recorded::DOMAIN_CHUNKED_REPLAY_TIME,
                                // HACK(eta): scary cast
                                start.elapsed().as_micros() as f64,
                                "domain" => domain_str.clone(),
                                "shard" => shard_str.clone(),
                                "from_node" => link.dst.id().to_string()
                            );

                            counter!(
                                recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_TIME,
                                // HACK(eta): scary cast
                                start.elapsed().as_micros() as u64,
                                "domain" => domain_str,
                                "shard" => shard_str,
                                "from_node" => link.dst.id().to_string()
                            );
                        })?;
                }
                self.handle_replay(*p, executor)?;

                self.total_replay_time.stop();
                histogram!(
                    recorded::DOMAIN_CHUNKED_REPLAY_START_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );

                counter!(
                    recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME,
                    start.elapsed().as_micros() as u64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );
                Ok(None)
            }
            DomainRequest::Ready { node, purge, index } => {
                invariant_eq!(self.mode, DomainMode::Forwarding);

                let node_ref = self
                    .nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?;

                node_ref.borrow_mut().purge = purge;

                if !index.is_empty() {
                    let mut s: Box<dyn State> = {
                        match (
                            node_ref.borrow().get_base(),
                            &self.persistence_parameters.mode,
                        ) {
                            (Some(base), &DurabilityMode::DeleteOnExit)
                            | (Some(base), &DurabilityMode::Permanent) => {
                                let base_name = format!(
                                    "{}-{}-{}",
                                    &self
                                        .persistence_parameters
                                        .db_filename_prefix
                                        .replace('-', "_"),
                                    node_ref.borrow().name(),
                                    self.shard.unwrap_or(0),
                                );

                                Box::new(PersistentState::new(
                                    base_name,
                                    base.all_unique_keys(),
                                    &self.persistence_parameters,
                                ))
                            }
                            _ => Box::new(MemoryState::default()),
                        }
                    };
                    for idx in index {
                        s.add_key(idx, None);
                    }
                    assert!(self.state.insert(node, s).is_none());
                } else {
                    // NOTE: just because index_on is None does *not* mean we're not
                    // materialized
                }

                if self.not_ready.remove(&node) {
                    trace!(local = node.id(), "readying empty node");
                }

                // swap replayed reader nodes to expose new state
                if let Some(r) = node_ref.borrow_mut().as_mut_reader() {
                    if let Some(ref mut state) = r.writer_mut() {
                        trace!(local = node.id(), "swapping state");
                        state.swap();
                        trace!(local = node.id(), "state swapped");
                    }
                }

                Ok(None)
            }
            DomainRequest::GetStatistics => {
                let domain_stats = noria::debug::stats::DomainStats {
                    total_time: self.total_time.num_nanoseconds(),
                    total_ptime: self.total_ptime.num_nanoseconds(),
                    total_replay_time: self.total_replay_time.num_nanoseconds(),
                    total_forward_time: self.total_forward_time.num_nanoseconds(),
                    wait_time: self.wait_time.num_nanoseconds(),
                };

                let node_stats: HashMap<
                    petgraph::graph::NodeIndex,
                    noria::debug::stats::NodeStats,
                > = self
                    .nodes
                    .values()
                    .filter_map(|nd| {
                        let n = &*nd.borrow();
                        let local_index = n.local_addr();
                        let node_index: NodeIndex = n.global_addr();

                        let time = self.process_times.num_nanoseconds(local_index);
                        let ptime = self.process_ptimes.num_nanoseconds(local_index);
                        let mem_size = n
                            .as_reader()
                            .map(|r| r.state_size().unwrap_or(0))
                            .unwrap_or_else(|| {
                                self.state
                                    .get(local_index)
                                    .map(|s| s.deep_size_of())
                                    .unwrap_or(0)
                            });

                        let mat_state = n
                            .as_reader()
                            .map(|r| {
                                if r.is_partial() {
                                    MaterializationStatus::Partial {
                                        beyond_materialization_frontier: n.purge,
                                    }
                                } else {
                                    MaterializationStatus::Full
                                }
                            })
                            .unwrap_or_else(|| match self.state.get(local_index) {
                                Some(s) => {
                                    if s.is_partial() {
                                        MaterializationStatus::Partial {
                                            beyond_materialization_frontier: n.purge,
                                        }
                                    } else {
                                        MaterializationStatus::Full
                                    }
                                }
                                None => MaterializationStatus::Not,
                            });

                        let probe_result = if let Some(n) = n.as_internal() {
                            n.probe()
                        } else {
                            Default::default()
                        };

                        if let (Some(time), Some(ptime)) = (time, ptime) {
                            Some((
                                node_index,
                                noria::debug::stats::NodeStats {
                                    desc: format!("{:?}", n),
                                    process_time: time,
                                    process_ptime: ptime,
                                    mem_size,
                                    materialized: mat_state,
                                    probe_result,
                                },
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();

                let ret = (domain_stats, node_stats);
                Ok(Some(bincode::serialize(&ret)?))
            }
            DomainRequest::UpdateStateSize => {
                self.update_state_sizes();
                Ok(None)
            }
            DomainRequest::RequestReplicationOffsets => {
                Ok(Some(bincode::serialize(&self.replication_offsets())?))
            }
            DomainRequest::RequestSnapshottingTables => {
                Ok(Some(bincode::serialize(&self.snapshotting_base_nodes())?))
            }
            DomainRequest::Packet(pkt) => {
                self.handle_packet(Box::new(pkt), executor)?;
                Ok(None)
            }
            DomainRequest::QueryReplayDone => {
                let ret = self.replay_completed;
                self.replay_completed = false;
                Ok(Some(bincode::serialize(&ret)?))
            }
            DomainRequest::GeneratedColumns { node, cols } => {
                self.generated_columns.insert((node, cols));
                Ok(None)
            }
        };
        // What we just did might have done things like insert into `self.delayed_for_self`, so
        // run the event loop before returning to make sure that gets processed.
        //
        // Not doing this leads to complete insanity, as things just don't replay sometimes and
        // you aren't sure why.
        self.handle_packet(Box::new(Packet::Spin), executor)?;
        ret
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle(&mut self, m: Box<Packet>, executor: &mut dyn Executor) -> Result<(), ReadySetError> {
        // TODO(eta): better error handling here.
        // In particular one dodgy packet can kill the whole domain, which is probably not what we
        // want.

        // Count of each packet sent from a domain.
        counter!(
            recorded::DOMAIN_PACKET_SENT,
            1,
            "domain" => self.index.index().to_string(),
            "packet_type" => m.to_string()
        );

        match *m {
            Packet::Message { .. } | Packet::Input { .. } => {
                // WO for https://github.com/rust-lang/rfcs/issues/1403
                let start = time::Instant::now();
                let src = m.src().id();
                let dst = m.dst().id();
                self.total_forward_time.start();
                self.dispatch(m, executor)?;
                self.total_forward_time.stop();
                counter!(
                    recorded::DOMAIN_TOTAL_FORWARD_TIME,
                    start.elapsed().as_micros() as u64,
                    // HACK(eta): having to call `to_string()` here makes me sad,
                    // but seems to be a limitation of the `metrics` crate
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "from_node" => src.to_string(),
                    "to_node" => dst.to_string(),
                );

                histogram!(
                    recorded::DOMAIN_FORWARD_TIME,
                    start.elapsed().as_micros() as f64,
                    // HACK(eta): having to call `to_string()` here makes me sad,
                    // but seems to be a limitation of the `metrics` crate
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "from_node" => src.to_string(),
                    "to_node" => dst.to_string(),
                );
            }
            Packet::ReplayPiece { tag, .. } => {
                let start = time::Instant::now();
                self.total_replay_time.start();
                self.handle_replay(*m, executor)?;
                self.total_replay_time.stop();
                counter!(
                    recorded::DOMAIN_TOTAL_REPLAY_TIME,
                    start.elapsed().as_micros() as u64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );

                histogram!(
                    recorded::DOMAIN_REPLAY_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );
            }
            Packet::Evict { .. } | Packet::EvictKeys { .. } => {
                self.handle_eviction(*m, executor)?;
            }
            Packet::Timestamp { .. } => {
                // TODO(justinmiron): Handle timestamp packets at data flow nodes. The
                // ack should be moved to the base table node's handling of the packet.
                // As the packet is not propagated or mutated before reaching the
                // domain, we still have a source channel identifier that we can use
                // to ack the packet.
                self.handle_timestamp(*m, executor)?;
            }
            Packet::RequestReaderReplay {
                mut keys,
                cols,
                node,
            } => {
                let start = time::Instant::now();
                self.total_replay_time.start();

                let mut n = self
                    .nodes
                    .get(node)
                    .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                    .borrow_mut();

                let r = n.as_mut_reader().ok_or(ReadySetError::InvalidNodeType {
                    node_index: node.id(),
                    expected_type: NodeType::Reader,
                })?;

                // the reader could have raced with us filling in the key after some
                // *other* reader requested it, so let's double check that it indeed still
                // misses!
                let w = r.writer_mut().ok_or_else(|| {
                    internal_err("reader replay requested for non-materialized reader")
                })?;
                // ensure that all writes have been applied
                w.swap();

                // don't request keys that have been filled since the request was sent
                let mut whoopsed = false;
                keys.retain(|key| {
                    !w.contains(key).unwrap_or_else(|| {
                        whoopsed = true;
                        true
                    })
                });
                if whoopsed {
                    internal!("reader replay requested for non-ready reader")
                }

                let reader_index_type = r.index_type().ok_or_else(|| {
                    internal_err("reader replay requested for non-indexed reader")
                })?;
                drop(n); // NLL needs a little help. don't we all, sometimes?

                // ensure that we haven't already requested a replay of this key
                let already_requested = self
                    .reader_triggered
                    .entry(node)
                    .or_insert_with(|| RequestedKeys::new(reader_index_type));
                already_requested.extend(&mut keys);
                if !keys.is_empty() {
                    self.find_tags_and_replay(keys, &cols[..], node)?;
                }

                self.total_replay_time.stop();
                histogram!(
                    recorded::DOMAIN_READER_REPLAY_REQUEST_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "node" => node.id().to_string()
                );

                counter!(
                    recorded::DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME,
                    start.elapsed().as_micros() as u64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "node" => node.id().to_string()
                );
            }
            Packet::RequestPartialReplay {
                tag,
                keys,
                unishard,
                requesting_shard,
            } => {
                trace!(%tag, ?keys, "got replay request");
                let start = time::Instant::now();
                self.total_replay_time.start();
                self.seed_all(
                    tag,
                    requesting_shard,
                    keys.into_iter().collect(),
                    unishard,
                    executor,
                )?;
                self.total_replay_time.stop();
                histogram!(
                    recorded::DOMAIN_SEED_REPLAY_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );

                counter!(
                    recorded::DOMAIN_TOTAL_SEED_REPLAY_TIME,
                    start.elapsed().as_micros() as u64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()

                );
            }
            Packet::Finish(tag, ni) => {
                let start = time::Instant::now();
                self.total_replay_time.start();
                self.finish_replay(tag, ni, executor)?;
                self.total_replay_time.stop();
                histogram!(
                    recorded::DOMAIN_FINISH_REPLAY_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );

                counter!(
                    recorded::DOMAIN_TOTAL_FINISH_REPLAY_TIME,
                    start.elapsed().as_micros() as u64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string(),
                    "tag" => tag.to_string()
                );
            }
            Packet::Spin => {
                // spinning as instructed
            }
        }

        Ok(())
    }

    /// Timed purges happen when [`FrontierStrategy`] is not None, in which case all keys
    /// are purged from the node after a given amount of time
    fn handle_timed_purges(&mut self) {
        let mut swap = HashSet::new();
        while let Some(tp) = self.timed_purges.front() {
            let now = time::Instant::now();
            if tp.time <= now {
                #[allow(clippy::unwrap_used)]
                // we know it's Some because we check at the head of the while
                let tp = self.timed_purges.pop_front().unwrap();
                #[allow(clippy::indexing_slicing)]
                // nodes in tp.view must reference nodes in self
                let mut node = self.nodes[tp.view].borrow_mut();
                trace!(
                    node = node.global_addr().index(),
                    "eagerly purging state from reader"
                );
                #[allow(clippy::unwrap_used)] // nodes in tp.view must reference readers
                let r = node.as_mut_reader().unwrap();
                if let Some(wh) = r.writer_mut() {
                    for key in tp.keys {
                        wh.mark_hole(&key);
                    }
                    swap.insert(tp.view);
                }
            } else {
                break;
            }
        }

        for n in swap {
            #[allow(clippy::indexing_slicing)]
            // nodes in tp.view must reference nodes in self
            let mut n = self.nodes[n].borrow_mut();
            #[allow(clippy::unwrap_used)] // nodes in tp.view must reference readers
            let r = n.as_mut_reader().unwrap();
            if let Some(wh) = r.writer_mut() {
                wh.swap();
            }
        }
    }

    fn seed_row(&self, source: LocalNodeIndex, row: Cow<[DataType]>) -> ReadySetResult<Record> {
        if let Some(&(start, ref defaults)) = self.ingress_inject.get(source) {
            let mut v = Vec::with_capacity(start + defaults.len());
            v.extend(row.iter().cloned());
            v.extend(defaults.iter().cloned());
            return Ok((v, true).into());
        }

        let n = self
            .nodes
            .get(source)
            .ok_or_else(|| ReadySetError::NoSuchNode(source.id()))?
            .borrow();
        if let Some(b) = n.get_base() {
            let mut row = row.into_owned();
            b.fix(&mut row);
            return Ok(Record::Positive(row));
        }

        Ok(row.into_owned().into())
    }

    // Records state lookup metrics for specific dataflow nodes.
    fn record_dataflow_lookup_metrics(&self, node_idx: LocalNodeIndex) {
        if let Some(node) = self.nodes.get(node_idx) {
            if node.borrow().is_base() {
                counter!(
                    recorded::BASE_TABLE_LOOKUP_REQUESTS,
                    1,
                    "domain" => self.index.index().to_string(),
                    "node" => node_idx.to_string(),
                );
            }
        }
    }

    /// Lookup the provided keys, returns a vector of results
    /// Assuming that this is a persistent state, we can do an efficient multi-key
    /// lookup for equal keys
    fn do_lookup_multi<'a>(
        &self,
        state: &'a PersistentState,
        cols: &[usize],
        keys: &HashSet<KeyComparison>,
    ) -> Vec<RecordResult<'a>> {
        let mut range_records = Vec::new();
        let equal_keys = keys
            .iter()
            .filter_map(|k| match k {
                KeyComparison::Equal(equal) => Some(KeyType::from(equal)),
                KeyComparison::Range(range) => {
                    // TODO: aggregate ranges to optimize range lookups too?
                    match state.lookup_range(cols, &RangeKey::from(range)) {
                        RangeLookupResult::Some(res) => range_records.push(res),
                        #[allow(clippy::unreachable)]
                        // Can't miss in persistent state
                        RangeLookupResult::Missing(_) => unreachable!("Persistent state"),
                    }
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut records = state.lookup_multi(cols, &equal_keys);
        records.append(&mut range_records);
        records
    }

    /// Lookup the provided keys one by one, returns a vector of results, the set of the hit keys
    /// and a set of the misses
    fn do_lookup_iter<'a>(
        &self,
        state: &'a dyn State,
        cols: &[usize],
        mut keys: HashSet<KeyComparison>,
    ) -> ReadySetResult<StateLookupResult<'a>> {
        let mut records = Vec::new();
        let mut replay_keys = HashSet::new();
        // Drain misses, and keep the hits
        keys.drain_filter(|key| match key {
            KeyComparison::Equal(equal) => match state.lookup(cols, &KeyType::from(equal)) {
                LookupResult::Some(record) => {
                    records.push(record);
                    false
                }
                LookupResult::Missing => {
                    replay_keys.insert((key.clone(), key.clone()));
                    true
                }
            },
            KeyComparison::Range(range) => match state.lookup_range(cols, &RangeKey::from(range)) {
                RangeLookupResult::Some(record) => {
                    records.push(record);
                    false
                }
                RangeLookupResult::Missing(ms) => {
                    // FIXME(eta): error handling impl here adds overhead
                    let ms = ms.into_iter().map(|m| {
                        // This is the only point where the replay_key and miss_key are different.
                        #[allow(clippy::unwrap_used)]
                        // keys can't be empty coming from misses
                        (key.clone(), KeyComparison::try_from(m).unwrap())
                    });
                    replay_keys.extend(ms);
                    true
                }
            },
        });

        Ok(StateLookupResult {
            records,
            found_keys: keys,
            replay_keys,
        })
    }

    fn do_lookup<'a>(
        &self,
        state: &'a dyn State,
        cols: &[usize],
        keys: HashSet<KeyComparison>,
    ) -> ReadySetResult<StateLookupResult<'a>> {
        if let Some(state) = state.as_persistent() {
            Ok(StateLookupResult {
                records: self.do_lookup_multi(state, cols, &keys),
                found_keys: keys, // PersistentState can't miss
                replay_keys: HashSet::new(),
            })
        } else {
            self.do_lookup_iter(state, cols, keys)
        }
    }

    fn seed_all(
        &mut self,
        tag: Tag,
        requesting_shard: usize,
        keys: HashSet<KeyComparison>,
        single_shard: bool,
        ex: &mut dyn Executor,
    ) -> Result<(), ReadySetError> {
        #[allow(clippy::indexing_slicing)]
        // tag came from an internal data structure that guarantees it's present
        let (source, index, path) = match &self.replay_paths[&tag] {
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(index),
                path,
                ..
            }
            | ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Local(index),
                path,
                ..
            } => (source, index, path),
            _ => internal!(),
        };

        let state = self.state.get(*source).ok_or_else(|| {
            internal_err(format!(
                "migration replay path (tag {:?}) started with non-materialized node",
                tag
            ))
        })?;

        self.record_dataflow_lookup_metrics(*source);

        let StateLookupResult {
            records,
            found_keys,
            replay_keys,
        } = self.do_lookup(state.as_ref(), &index.columns, keys)?;

        let records = records
            .into_iter()
            .flat_map(|rr| rr.into_iter().map(|r| self.seed_row(*source, r)))
            .collect::<ReadySetResult<Vec<Record>>>()?;

        let dst = path[0].node;
        let index = index.clone(); // Clone to free the immutable reference at the top
        let src = *source;

        if !replay_keys.is_empty() {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            trace!(
                ?tag,
                miss_keys = ?replay_keys,
                "missed during replay request"
            );

            self.on_replay_misses(
                src,
                &index.columns,
                // NOTE:
                // `replay_keys` are tuples of (replay_key, miss_key), where `replay_key` is the key we're trying
                // to replay and `miss_key` is the part of it we missed on.
                // This is only relevant for range queries; for non-range queries the two are the same.
                // Assuming they were the same for range queries was a whole bug that eta had to spend like 2
                // hours tracking down, only to find it was as simple as this.
                replay_keys,
                single_shard,
                requesting_shard,
                tag,
            )?;
        }

        if !found_keys.is_empty() {
            trace!(
                %tag,
                keys = ?found_keys,
                ?records,
                "satisfied replay request"
            );

            self.handle_replay(
                Packet::ReplayPiece {
                    link: Link::new(src, dst),
                    tag,
                    context: ReplayPieceContext::Partial {
                        for_keys: found_keys,
                        unishard: single_shard, // if we are the only source, only one path
                        requesting_shard,
                    },
                    data: records.into(),
                },
                ex,
            )?;
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_replay(&mut self, m: Packet, ex: &mut dyn Executor) -> ReadySetResult<()> {
        let tag = m
            .tag()
            .ok_or_else(|| internal_err("handle_replay called on an invalid message"))?;
        #[allow(clippy::indexing_slicing)]
        // tag came from an internal data structure that guarantees it exists
        if self.nodes[self.replay_paths[&tag].last_segment().node]
            .borrow()
            .is_dropped()
        {
            return Ok(());
        }

        let mut finished = None;
        let mut need_replay = Vec::new();
        let mut finished_partial = 0;

        // this loop is just here so we have a way of giving up the borrow of self.replay_paths
        #[allow(clippy::never_loop)]
        'outer: loop {
            #[allow(clippy::indexing_slicing)]
            // tag came from an internal data structure that guarantees it exists
            let rp = &self.replay_paths[&tag];
            let &ReplayPath {
                ref path,
                ref source,
                notify_done,
                ..
            } = rp;

            match self.mode {
                DomainMode::Forwarding if notify_done => {
                    // this is the first message we receive for this tagged replay path. only at
                    // this point should we start buffering messages for the target node. since the
                    // node is not yet marked ready, all previous messages for this node will
                    // automatically be discarded by dispatch(). the reason we should ignore all
                    // messages preceeding the first replay message is that those have already been
                    // accounted for in the state we are being replayed. if we buffered them and
                    // applied them after all the state has been replayed, we would double-apply
                    // those changes, which is bad.
                    self.mode = DomainMode::Replaying {
                        to: rp.last_segment().node,
                        buffered: VecDeque::new(),
                        passes: 0,
                    };
                }
                DomainMode::Forwarding => {
                    // we're replaying to forward to another domain
                }
                DomainMode::Replaying { .. } => {
                    // another packet the local state we are constructing
                }
            }

            // will look somewhat nicer with https://github.com/rust-lang/rust/issues/15287
            match m {
                Packet::ReplayPiece {
                    tag,
                    link,
                    mut data,
                    mut context,
                } => {
                    if let ReplayPieceContext::Partial { ref for_keys, .. } = context {
                        trace!(
                            num = data.len(),
                            %tag,
                            keys = ?for_keys,
                            "replaying batch"
                        );
                    } else {
                        debug!(num = data.len(), "replaying batch");
                    }

                    // let's collect some information about the destination of this replay
                    let dst = path.last().node;
                    #[allow(clippy::indexing_slicing)] // dst came from a replay path
                    let dst_is_reader = self.nodes[dst]
                        .borrow()
                        .as_reader()
                        .map(|r| r.is_materialized())
                        .unwrap_or(false);
                    let dst_is_target = !self
                        .nodes
                        .get(dst)
                        .ok_or_else(|| ReadySetError::NoSuchNode(dst.id()))?
                        .borrow()
                        .is_sender();
                    let mut holes_for_remap = vec![];
                    let mut remap_holes_mark_filled = vec![];

                    if dst_is_target {
                        // prune keys and data for keys we're not waiting for
                        if let ReplayPieceContext::Partial {
                            ref mut for_keys, ..
                        } = context
                        {
                            let had = for_keys.len();
                            let partial_index = path.last().partial_index.as_ref().unwrap();
                            if let Some(w) = self.waiting.get(dst) {
                                let mut remapped_keys_to_holes = vec![];
                                // Scan the list of active upquery maps to see if this replay is
                                // going to fill part of a remapped hole.
                                for ((_, hole), tag_keys) in self
                                    .active_remaps
                                    .iter()
                                    .filter(|(&(miss_in, ..), _)| miss_in == dst)
                                {
                                    // Find keys that are relevant to the tag we're replaying along.
                                    let keys_for_this_tag = tag_keys
                                        .iter()
                                        .filter(|&(t, _)| tag == *t)
                                        .map(|(_, keyc)| keyc.clone())
                                        .collect::<HashSet<_>>();
                                    // Get the total number of keys required to fill the remapped
                                    // hole, including the `keys_for_this_tag`. This becomes
                                    // relevant later.
                                    let total_keys = tag_keys.len();
                                    if !keys_for_this_tag.is_empty() {
                                        // If we get here, the `hole` requires the `keys_for_this_tag`
                                        // to be filled before it gets filled.
                                        remapped_keys_to_holes.push((
                                            keys_for_this_tag,
                                            hole.clone(),
                                            total_keys,
                                        ));
                                    }
                                }
                                // If we didn't find any remaps for this tag, it's probably just a
                                // regular replay.
                                if remapped_keys_to_holes.is_empty() {
                                    // discard all the keys that we aren't waiting for
                                    for_keys.retain(|k| {
                                        w.redos.contains_key(&(
                                            partial_index.columns.to_owned(),
                                            k.clone(),
                                        ))
                                    });
                                } else {
                                    // discard keys that won't fill any remapped holes
                                    for_keys.retain(|k| {
                                        remapped_keys_to_holes
                                            .iter()
                                            .any(|(keys, ..)| keys.contains(k))
                                    });
                                    for (keys_for_hole, hole, total_keys) in remapped_keys_to_holes
                                    {
                                        let filled_keys_count = for_keys
                                            .iter()
                                            .filter(|k| keys_for_hole.contains(k))
                                            .count();
                                        if filled_keys_count == 0 {
                                            // we won't fill any of this hole with `for_keys`, so
                                            // ignore it
                                            continue;
                                        }
                                        holes_for_remap.push(hole.clone());
                                        // if the keys we'll fill with this replay fully fill the
                                        // `hole`...
                                        if total_keys - filled_keys_count == 0 {
                                            // ...we need to remember to mark the hole as filled
                                            // before doing the replay, because the ingredient will
                                            // give us some records to materialize
                                            remap_holes_mark_filled.push(hole);
                                        }
                                    }
                                }
                            } else if let Some(prev) = self.reader_triggered.get(dst) {
                                // discard all the keys or subranges of keys that we aren't waiting
                                // for
                                if !prev.is_empty() {
                                    prev.filter_keys(for_keys);
                                }
                            } else {
                                // this packet contained no keys that we're waiting for, so it's
                                // useless to us.
                                return Ok(());
                            }

                            if for_keys.is_empty() {
                                return Ok(());
                            } else if for_keys.len() != had {
                                // discard records in data associated with the keys we weren't
                                // waiting for
                                // note that we need to use the partial_keys column IDs from the
                                // *start* of the path here, as the records haven't been processed
                                // yet
                                // We already know it's a partial replay path, so it must have a partial key
                                #[allow(clippy::unwrap_used)]
                                let partial_keys = path.first().partial_index.as_ref().unwrap();
                                data.retain(|r| {
                                    for_keys.iter().any(|k| {
                                        k.contains(
                                            &partial_keys
                                                .columns
                                                .iter()
                                                .map(|c| {
                                                    #[allow(clippy::indexing_slicing)]
                                                    // record came from processing, which means it
                                                    // must have the right number of columns
                                                    r[*c].clone()
                                                })
                                                .collect::<Vec<_>>(),
                                        )
                                    })
                                });
                            }
                        }
                    }

                    // forward the current message through all local nodes.
                    let m = Box::new(Packet::ReplayPiece {
                        link,
                        tag,
                        data,
                        context,
                    });
                    let mut m = Some(m);

                    macro_rules! replay_context {
                        ($m:ident, $field:ident) => {
                            if let Some(&mut Packet::ReplayPiece {
                                ref mut context, ..
                            }) = $m.as_deref_mut()
                            {
                                if let ReplayPieceContext::Partial { ref mut $field, .. } = *context
                                {
                                    Some($field)
                                } else {
                                    None
                                }
                            } else {
                                internal!("asked to fetch replay field on non-replay packet")
                            }
                        };
                    }

                    for (i, segment) in path.iter().enumerate() {
                        if let Some(force_tag) = segment.force_tag_to {
                            #[allow(clippy::unwrap_used)]
                            // We would have bailed in a previous iteration (break 'outer, below) if
                            // it wasn't Some
                            if let Packet::ReplayPiece { ref mut tag, .. } =
                                m.as_deref_mut().unwrap()
                            {
                                *tag = force_tag;
                            }
                        }

                        #[allow(clippy::indexing_slicing)]
                        // we know replay paths only contain real nodes
                        let mut n = self.nodes[segment.node].borrow_mut();
                        let is_reader = n.as_reader().map(|r| r.is_materialized()).unwrap_or(false);

                        // keep track of whether we're filling any partial holes
                        let partial_key_cols = segment.partial_index.as_ref();
                        // keep a copy of the partial keys from before we process
                        // we need this because n.process may choose to reduce the set of keys
                        // (e.g., because some of them missed), in which case we need to know what
                        // keys to _undo_.
                        let mut backfill_keys = if let Some(for_keys) = replay_context!(m, for_keys)
                        {
                            debug_assert!(partial_key_cols.is_some());
                            Some(for_keys.clone())
                        } else {
                            None
                        };

                        // figure out if we're the target of a partial replay.
                        // this is the case either if the current node is waiting for a replay,
                        // *or* if the target is a reader. the last case is special in that when a
                        // client requests a replay, the Reader isn't marked as "waiting".
                        let target = backfill_keys.is_some()
                            && i == path.len() - 1
                            && (is_reader || !n.is_sender());

                        // targets better be last
                        assert!(!target || i == path.len() - 1);

                        // are we about to fill a hole?
                        if target && (holes_for_remap.is_empty() || segment.node != dst) {
                            if let Some(backfill_keys) = &backfill_keys {
                                // mark the state for the key being replayed as *not* a hole otherwise
                                // we'll just end up with the same "need replay" response that
                                // triggered this replay initially.
                                if let Some(state) = self.state.get_mut(segment.node) {
                                    for key in backfill_keys.iter() {
                                        trace!(?key, ?tag, local = %segment.node, "Marking filled");
                                        state.mark_filled(key.clone(), tag);
                                    }
                                } else {
                                    let r = n.as_mut_reader().ok_or_else(|| {
                                        internal_err("Trying to fill hole in non-materialized node")
                                    })?;
                                    // we must be filling a hole in a Reader. we need to ensure
                                    // that the hole for the key we're replaying ends up being
                                    // filled, even if that hole is empty!
                                    if let Some(wh) = r.writer_mut() {
                                        for key in backfill_keys.iter() {
                                            trace!(?key, local = %segment.node, "Marking filled in reader");
                                            wh.mark_filled(key.clone())?;
                                        }
                                    }
                                }
                            }
                        }
                        let mut materialize_into_all = false;
                        if segment.node == dst && !holes_for_remap.is_empty() {
                            // remap case: mark the previously noted holes as filled
                            materialize_into_all = true;
                            if let Some(state) = self.state.get_mut(segment.node) {
                                for (_, keys) in remap_holes_mark_filled.clone() {
                                    // using the current tag is fine, because state impls don't
                                    // have multiple indices for the same set of columns, so marking
                                    // this tag as filled will mark other remapped replay paths'
                                    // tags as filled, too
                                    trace!(?keys, ?tag, local = %segment.node, "Marking filled");
                                    state.mark_filled(keys, tag);
                                }
                            }
                        }
                        // process the current message in this node
                        let process_result = n.process(
                            &mut m,
                            segment.partial_index.as_ref().map(|idx| &idx.columns),
                            &mut self.state,
                            &self.nodes,
                            self.shard,
                            false,
                            Some(rp),
                            ex,
                            materialize_into_all,
                        )?;

                        let misses = process_result.unique_misses();

                        let missed_on = if backfill_keys.is_some() {
                            let mut missed_on = HashSet::with_capacity(misses.len());
                            for miss in &misses {
                                #[allow(clippy::unwrap_used)]
                                // this is a partial miss, so it must have a partial key
                                missed_on.insert(miss.replay_key().unwrap());
                            }
                            missed_on
                        } else {
                            HashSet::new()
                        };

                        if target {
                            if !misses.is_empty() {
                                if !holes_for_remap.is_empty() {
                                    // dealing with misses during remaps is possible but hard.
                                    // no current use-cases for remaps require this to work, so we
                                    // don't implement it yet -- fail obviously in order to prevent
                                    // subtle bugs from happening
                                    internal!("Ingredients are currently not allowed to miss during a remapped replay");
                                }
                                // we missed while processing
                                // it's important that we clear out any partially-filled holes.
                                if let Some(state) = self.state.get_mut(segment.node) {
                                    for miss in &missed_on {
                                        state.mark_hole(miss, tag);
                                    }
                                } else if let Some(r) = n.as_mut_reader() {
                                    if let Some(wh) = r.writer_mut() {
                                        for miss in &missed_on {
                                            wh.mark_hole(miss);
                                        }
                                    }
                                }
                            } else if let Some(r) = n.as_mut_reader() {
                                // we filled a hole! swap the reader.
                                if let Some(wh) = r.writer_mut() {
                                    wh.swap();
                                }

                                // and also unmark the replay request
                                if let Some(ref mut prev) =
                                    self.reader_triggered.get_mut(segment.node)
                                {
                                    if let Some(backfill_keys) = &backfill_keys {
                                        for key in backfill_keys {
                                            prev.remove(key);
                                        }
                                    }
                                }
                            }
                        }

                        if target && !process_result.captured.is_empty() {
                            // materialized union ate some of our keys,
                            // so we didn't *actually* fill those keys after all!
                            if let Some(state) = self.state.get_mut(segment.node) {
                                for key in &process_result.captured {
                                    state.mark_hole(key, tag);
                                }
                            } else if let Some(r) = n.as_mut_reader() {
                                if let Some(wh) = r.writer_mut() {
                                    for key in &process_result.captured {
                                        wh.mark_hole(key);
                                    }
                                }
                            }
                        }

                        // we're done with the node
                        drop(n);

                        if m.is_none() {
                            // eaten full replay
                            assert_eq!(misses.len(), 0);

                            // it's been captured, so we need to *not* consider the replay finished
                            // (which the logic below matching on context would do)
                            break 'outer;
                        }

                        // we need to track how many replays we completed, and we need to do so
                        // *before* we prune keys that missed. these conditions are all important,
                        // so let's walk through them
                        //
                        //  1. this applies only to partial backfills
                        //  2. we should only set finished_partial if it hasn't already been set.
                        //     this is important, as misses will cause backfill_keys to be pruned
                        //     over time, which would cause finished_partial to hold the wrong
                        //     value!
                        //  3. if the last node on this path is a reader, or is a ::End (so we
                        //     triggered the replay) then we need to decrement the concurrent
                        //     replay count! note that it's *not* sufficient to check if the
                        //     *current* node is a target/reader, because we could miss during a
                        //     join along the path.
                        if let Some(backfill_keys) = &backfill_keys {
                            if finished_partial == 0 && (dst_is_reader || dst_is_target) {
                                finished_partial = backfill_keys.len();
                            }
                        }

                        // only continue with the keys that weren't captured
                        #[allow(clippy::unwrap_used)]
                        // We would have bailed earlier (break 'outer, above) if m wasn't Some
                        if let Packet::ReplayPiece {
                            context:
                                ReplayPieceContext::Partial {
                                    ref mut for_keys, ..
                                },
                            ..
                        } = **m.as_mut().unwrap()
                        {
                            for backfill_keys in &mut backfill_keys {
                                backfill_keys.retain(|k| for_keys.contains(k));
                            }
                        }

                        // if we missed during replay, we need to do another replay
                        if backfill_keys.is_some() && !misses.is_empty() {
                            // so, in theory, unishard can be changed by n.process. however, it
                            // will only ever be changed by a union, which can't cause misses.
                            // since we only enter this branch in the cases where we have a miss,
                            // it is okay to assume that unishard _hasn't_ changed, and therefore
                            // we can use the value that's in m.
                            #[allow(clippy::unwrap_used)]
                            // We would have bailed earlier (break 'outer, above) if m wasn't Some
                            let (unishard, requesting_shard) = if let Packet::ReplayPiece {
                                context:
                                    ReplayPieceContext::Partial {
                                        unishard,
                                        requesting_shard,
                                        ..
                                    },
                                ..
                            } = **m.as_mut().unwrap()
                            {
                                (unishard, requesting_shard)
                            } else {
                                internal!("backfill_keys.is_some() implies Context::Partial");
                            };

                            need_replay.extend(misses.iter().map(|m| {
                                ReplayDescriptor::from_miss(m, tag, unishard, requesting_shard)
                            }));

                            // we should only finish the replays for keys that *didn't* miss
                            #[allow(clippy::unwrap_used)] // We already checked it's Some
                            backfill_keys
                                .as_mut()
                                .unwrap()
                                .retain(|k| !missed_on.contains(k));

                            // prune all replayed records for keys where any replayed record for
                            // that key missed.
                            #[allow(clippy::unwrap_used)]
                            // We know this is a partial replay
                            let partial_index = partial_key_cols.as_ref().unwrap();
                            #[allow(clippy::unwrap_used)]
                            // We would have bailed earlier (break 'outer, above) if m wasn't Some
                            m.as_mut().unwrap().map_data(|rs| {
                                rs.retain(|r| {
                                    // XXX: don't we technically need to translate the columns a
                                    // bunch here? what if two key columns are reordered?
                                    // XXX: this clone and collect here is *really* sad
                                    let r = r.rec();
                                    !missed_on.iter().any(|miss| {
                                        miss.contains(
                                            &partial_index
                                                .columns
                                                .iter()
                                                .map(|&c| {
                                                    #[allow(clippy::indexing_slicing)]
                                                    // record came from processing, which means it
                                                    // must have the right number of columns
                                                    r[c].clone()
                                                })
                                                .collect::<Vec<_>>(),
                                        )
                                    })
                                })
                            });
                        }

                        // no more keys to replay, so we might as well terminate early
                        if backfill_keys
                            .as_ref()
                            .map(|b| b.is_empty())
                            .unwrap_or(false)
                        {
                            break 'outer;
                        }

                        // we successfully processed some upquery responses!
                        //
                        // at this point, we can discard the state that the replay used in n's
                        // ancestors if they are beyond the materialization frontier (and thus
                        // should not be allowed to amass significant state).
                        //
                        // we want to make sure we only remove state once it will no longer be
                        // looked up into though. consider this dataflow graph:
                        //
                        //  (a)     (b)
                        //   |       |
                        //   |       |
                        //  (q)      |
                        //   |       |
                        //   `--(j)--`
                        //       |
                        //
                        // where j is a join, a and b are materialized, q is query-through. if we
                        // removed state the moment a replay has passed through the next operator,
                        // then the following could happen: a replay then comes from a, passes
                        // through q, q then discards state from a and forwards to j. j misses in
                        // b. replay happens to b, and re-triggers replay from a. however, state in
                        // a is discarded, so replay to a needs to happen a second time. that's not
                        // _wrong_, and we will eventually make progress, but it is pretty
                        // inefficient.
                        //
                        // insted, we probably want the join to do the eviction. we achieve this by
                        // only evicting from a after the replay has passed the join (or, more
                        // generally, the operator that might perform lookups into a)
                        if let Some(ref backfill_keys) = backfill_keys {
                            // first and foremost -- evict the source of the replay (if we own it).
                            // we only do this when the replay has reached its target, or if it's
                            // about to leave the domain, otherwise we might evict state that a
                            // later operator (like a join) will still do lookups into.
                            if i == path.len() - 1 {
                                // only evict if we own the state where the replay originated
                                if let Some(src) = source {
                                    #[allow(clippy::indexing_slicing)]
                                    // src came from a replay path
                                    let n = self.nodes[*src].borrow();
                                    if n.beyond_mat_frontier() {
                                        let state = self.state.get_mut(*src).ok_or_else(|| {
                                            internal_err("replay sourced at non-materialized node")
                                        })?;
                                        trace!(
                                            node = n.global_addr().index(),
                                            keys = ?backfill_keys,
                                            "clearing keys from purgeable replay source after replay"
                                        );
                                        for key in backfill_keys {
                                            state.mark_hole(key, tag);
                                        }
                                    }
                                }
                            }

                            // next, evict any state that we had to look up to process this replay.
                            let mut evict_tags = Vec::new();
                            let mut pns_for = None;
                            let mut pns = Vec::new();
                            let mut tmp = Vec::new();
                            for lookup in process_result.lookups {
                                // don't evict from our own state
                                if lookup.on == segment.node {
                                    continue;
                                }

                                // resolve any lookups through query-through nodes
                                if pns_for != Some(lookup.on) {
                                    pns.clear();
                                    assert!(tmp.is_empty());
                                    tmp.push(lookup.on);

                                    while let Some(pn) = tmp.pop() {
                                        if self.state.contains_key(pn) {
                                            #[allow(clippy::indexing_slicing)]
                                            // we know the lookup was into a real node
                                            if self.nodes[pn].borrow().beyond_mat_frontier() {
                                                // we should evict from this!
                                                pns.push(pn);
                                            } else {
                                                // we should _not_ evict from this
                                            }
                                            continue;
                                        }

                                        // this parent needs to be resolved further
                                        #[allow(clippy::indexing_slicing)]
                                        // we know the lookup was into a real node
                                        let pn = self.nodes[pn].borrow();
                                        if !pn.can_query_through() {
                                            internal!("lookup into non-materialized, non-query-through node.");
                                        }

                                        for &ppn in pn.parents() {
                                            tmp.push(ppn);
                                        }
                                    }
                                    pns_for = Some(lookup.on);
                                }

                                #[allow(clippy::unwrap_used)]
                                // we know this is a partial replay path
                                let tag_match = |rp: &ReplayPath, pn| {
                                    let path_index =
                                        rp.last_segment().partial_index.as_ref().unwrap();
                                    rp.last_segment().node == pn
                                        && path_index.columns == lookup.cols
                                        && path_index.index_type.supports_key(&lookup.key)
                                };

                                for &pn in &pns {
                                    // this is a node that we were doing lookups into as part of
                                    // the replay -- make sure we evict any state we may have added
                                    // there.
                                    evict_tags.retain(|tag| {
                                        #[allow(clippy::indexing_slicing)]
                                        // tag came from an internal data structure that guarantees it exists
                                        tag_match(&self.replay_paths[tag], pn)
                                    });

                                    let state = self.state.get_mut(pn).unwrap();
                                    assert!(state.is_partial());

                                    if evict_tags.is_empty() {
                                        if let Some(cs) = self.replay_paths_by_dst.get(pn) {
                                            #[allow(clippy::indexing_slicing)]
                                            // we check len is 1 first
                                            for index_type in IndexType::all_for_key(&lookup.key) {
                                                if let Some(tags) = cs.get(&Index::new(
                                                    *index_type,
                                                    lookup.cols.clone(),
                                                )) {
                                                    // this is the tag we would have used to fill a
                                                    // lookup hole in this ancestor, so this is the
                                                    // tag we need to evict from.

                                                    // TODO: could there have been multiple
                                                    invariant_eq!(tags.len(), 1);
                                                    evict_tags.push(tags[0]);
                                                }
                                            }
                                        }
                                    }

                                    if evict_tags.is_empty() {
                                        internal!(
                                            "no tag found for lookup target {:?}({:?}) (really {:?})",
                                            self.nodes[lookup.on].borrow().global_addr(),
                                            lookup.cols,
                                            self.nodes[pn].borrow().global_addr());
                                    }

                                    #[allow(clippy::indexing_slicing)] // came from self.nodes
                                    for tag in &evict_tags {
                                        // NOTE: this assumes that the key order is the same
                                        trace!(
                                            node = self.nodes[pn].borrow().global_addr().index(),
                                            key = ?&lookup.key,
                                            "clearing keys from purgeable materialization after replay"
                                        );
                                        state.mark_hole(&lookup.key, *tag);
                                    }
                                }
                            }
                        }

                        // we're all good -- continue propagating
                        #[allow(clippy::unwrap_used)]
                        // We would have bailed earlier (break 'outer, above) if m wasn't Some
                        if m.as_ref().unwrap().is_empty() {
                            if let Packet::ReplayPiece {
                                context: ReplayPieceContext::Regular { last: false },
                                ..
                            } = m.as_deref().unwrap()
                            {
                                trace!("dropping empty non-terminal full replay packet");
                                // don't continue processing empty updates, *except* if this is the
                                // last replay batch. in that case we need to send it so that the
                                // next domain knows that we're done
                                // TODO: we *could* skip ahead to path.last() here
                                break;
                            }
                        }

                        #[allow(clippy::unwrap_used)]
                        // We would have bailed earlier (break 'outer, above) if m wasn't Some
                        if i + 1 < path.len() {
                            // update link for next iteration
                            #[allow(clippy::indexing_slicing)] // nodes in replay paths must exist
                            if self.nodes[path[i + 1].node].borrow().is_shard_merger() {
                                // we need to preserve the egress src for shard mergers
                                // (which includes shard identifier)
                            } else {
                                m.as_mut().unwrap().link_mut().src = segment.node;
                            }
                            m.as_mut().unwrap().link_mut().dst = {
                                #[allow(clippy::indexing_slicing)]
                                // we already checked i + 1 isn't out-of-bounds
                                path[i + 1].node
                            };
                        }

                        // feed forward the updated backfill_keys
                        #[allow(clippy::unwrap_used)]
                        // We would have bailed earlier (break 'outer, above) if m wasn't Some
                        if let Packet::ReplayPiece {
                            context:
                                ReplayPieceContext::Partial {
                                    ref mut for_keys, ..
                                },
                            ..
                        } = m.as_deref_mut().unwrap()
                        {
                            *for_keys = backfill_keys.unwrap();
                        }
                    }

                    #[allow(clippy::unwrap_used)]
                    // We would have bailed earlier (break 'outer, above) if m wasn't Some
                    let context = if let Packet::ReplayPiece { context, .. } = *m.unwrap() {
                        context
                    } else {
                        internal!("started as a replay, now not a replay?");
                    };

                    match context {
                        ReplayPieceContext::Regular { last } if last => {
                            debug!(terminal = notify_done, "last batch processed");
                            if notify_done {
                                debug!(local = dst.id(), "last batch received");
                                finished = Some((tag, dst, None));
                            }
                        }
                        ReplayPieceContext::Regular { .. } => {
                            debug!("batch processed");
                        }
                        ReplayPieceContext::Partial {
                            for_keys,
                            unishard: _,
                            requesting_shard: _,
                        } => {
                            if dst_is_reader {
                                if self
                                    .nodes
                                    .get(dst)
                                    .ok_or_else(|| ReadySetError::NoSuchNode(dst.id()))?
                                    .borrow()
                                    .beyond_mat_frontier()
                                {
                                    // make sure we eventually evict these from here
                                    self.timed_purges.push_back(TimedPurge {
                                        time: time::Instant::now()
                                            + time::Duration::from_millis(50),
                                        keys: for_keys,
                                        view: dst,
                                    });
                                }
                                assert_ne!(finished_partial, 0);
                            } else if dst_is_target {
                                trace!(local = dst.id(), "partial replay completed");
                                if finished_partial == 0 {
                                    assert!(for_keys.is_empty());
                                }
                                let mut actually_finished = holes_for_remap.is_empty();
                                let mut new_for_keys = HashSet::new();
                                for hole in holes_for_remap {
                                    let done = {
                                        let waiting_for = self
                                            .active_remaps
                                            .get_mut(&(dst, hole.clone()))
                                            .unwrap();
                                        for for_key in for_keys.clone() {
                                            waiting_for.remove(&(tag, for_key));
                                        }
                                        waiting_for.is_empty()
                                    };
                                    if done {
                                        self.active_remaps.remove(&(dst, hole.clone()));
                                        new_for_keys.insert(hole.1);
                                        actually_finished = true;
                                    }
                                }
                                if actually_finished {
                                    let keys = if new_for_keys.is_empty() {
                                        for_keys
                                    } else {
                                        new_for_keys
                                    };
                                    finished = Some((tag, dst, Some(keys)));
                                }
                            } else {
                                // we're just on the replay path
                            }
                        }
                    }
                }
                _ => {
                    internal!();
                }
            }
            break;
        }

        if finished_partial != 0 {
            self.finished_partial_replay(tag, finished_partial)?;
        }

        // While the are still misses, we iterate over the array, each time draining it from elements that
        // can be batched into a single call to `on_replay_misses`
        while let Some(next_replay) = need_replay.get(0).cloned() {
            let misses: HashSet<_> = need_replay
                .drain_filter(|rep| next_replay.can_combine(rep))
                .map(
                    |ReplayDescriptor {
                         lookup_key,
                         replay_key,
                         ..
                     }| (replay_key, lookup_key),
                )
                .collect();

            trace!(
                %tag,
                ?misses,
                on = %next_replay.idx,
                "missed during replay processing"
            );

            self.on_replay_misses(
                next_replay.idx,
                &next_replay.lookup_columns,
                misses,
                next_replay.unishard,
                next_replay.requesting_shard,
                next_replay.tag,
            )?;
        }

        if let Some((tag, ni, for_keys)) = finished {
            trace!(
                node = ?ni,
                keys = ?for_keys,
                "partial replay finished"
            );
            if let Some(mut waiting) = self.waiting.remove(ni) {
                trace!(
                    keys = ?for_keys,
                    ?waiting,
                    "partial replay finished to node with waiting backfills"
                );

                #[allow(clippy::indexing_slicing)]
                // tag came from an internal data structure that guarantees it exists
                #[allow(clippy::unwrap_used)]
                // We already know this is a partial replay path
                let key_index = self.replay_paths[&tag]
                    .last_segment()
                    .partial_index
                    .clone()
                    .unwrap();

                // We try to batch as many redos together, so they can be later issued in a single
                // call to `RequestPartialReplay`
                let mut replay_sets = HashMap::new();

                // we got a partial replay result that we were waiting for. it's time we let any
                // downstream nodes that missed in us on that key know that they can (probably)
                // continue with their replays.
                #[allow(clippy::unwrap_used)]
                // this is a partial replay (since it's in waiting), so it must have keys
                for key in for_keys.unwrap() {
                    let hole = (key_index.columns.clone(), key);
                    let replay = match waiting.redos.remove(&hole) {
                        Some(x) => x,
                        None => internal!(
                            "got backfill for unnecessary key {:?} via tag {:?}",
                            hole.1,
                            tag
                        ),
                    };

                    // we may need more holes to fill before some replays should be re-attempted
                    let replay: Vec<_> = replay
                        .into_iter()
                        .filter(|tagged_replay_key| {
                            let left = waiting.holes.get_mut(tagged_replay_key).unwrap();
                            *left -= 1;

                            if *left == 0 {
                                trace!(k = ?tagged_replay_key, "filled last hole for key, triggering replay");

                                // we've filled all holes that prevented the replay previously!
                                waiting.holes.remove(tagged_replay_key);
                                true
                            } else {
                                trace!(
                                    k = ?tagged_replay_key,
                                    left = *left,
                                    "filled hole for key, not triggering replay"
                                );
                                false
                            }
                        })
                        .collect();

                    for Redo {
                        tag,
                        replay_key,
                        unishard,
                        requesting_shard,
                    } in replay
                    {
                        replay_sets
                            .entry((tag, unishard, requesting_shard))
                            .or_insert_with(|| Vec::new())
                            .push(replay_key);
                    }
                }

                // After we actually finished sorting the Redos into batches, issue each batch
                for ((tag, unishard, requesting_shard), keys) in replay_sets.drain() {
                    self.delayed_for_self
                        .push_back(Box::new(Packet::RequestPartialReplay {
                            tag,
                            unishard,
                            keys,
                            requesting_shard,
                        }));
                }

                if !waiting.holes.is_empty() {
                    // there are still holes, so there must still be pending redos
                    assert!(!waiting.redos.is_empty());

                    // restore Waiting in case seeding triggers more replays
                    self.waiting.insert(ni, waiting);
                } else {
                    // there are no more holes that are filling, so there can't be more redos
                    assert!(waiting.redos.is_empty());
                }
                return Ok(());
            } else if for_keys.is_some() {
                internal!("got unexpected replay of {:?} for {:?}", for_keys, ni);
            } else {
                // must be a full replay
                // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
                // replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
                // this allows finish_replay to dispatch into the node by overriding replaying_to.
                self.not_ready.remove(&ni);
                self.delayed_for_self
                    .push_back(Box::new(Packet::Finish(tag, ni)));
            }
        }
        Ok(())
    }

    fn finish_replay(
        &mut self,
        tag: Tag,
        node: LocalNodeIndex,
        ex: &mut dyn Executor,
    ) -> Result<(), ReadySetError> {
        let mut was = mem::replace(&mut self.mode, DomainMode::Forwarding);
        let finished = if let DomainMode::Replaying {
            ref to,
            ref mut buffered,
            ref mut passes,
        } = was
        {
            if *to != node {
                // we're told to continue replay for node a, but not b is being replayed
                internal!(
                    "told to continue replay for {:?}, but {:?} is being replayed",
                    node,
                    to
                );
            }
            // log that we did another pass
            *passes += 1;

            let mut handle = buffered.len();
            if handle > 100 {
                handle /= 2;
            }

            let mut handled = 0;
            while let Some(m) = buffered.pop_front() {
                // some updates were propagated to this node during the migration. we need to
                // replay them before we take even newer updates. however, we don't want to
                // completely block the domain data channel, so we only process a few backlogged
                // updates before yielding to the main loop (which might buffer more things).

                if let Packet::Message { .. } = *m {
                    // NOTE: we specifically need to override the buffering behavior that our
                    // self.replaying_to = Some above would initiate.
                    self.mode = DomainMode::Forwarding;
                    self.dispatch(m, ex)?;
                } else {
                    internal!();
                }

                handled += 1;
                if handled == handle {
                    // we want to make sure we actually drain the backlog we've accumulated
                    // but at the same time we don't want to completely stall the system
                    // therefore we only handle half the backlog at a time
                    break;
                }
            }

            buffered.is_empty()
        } else {
            // we're told to continue replay, but nothing is being replayed
            internal!(
                "told to continue replay to {:?}, but nothing is being replayed",
                node
            );
        };
        self.mode = was;

        if finished {
            // node is now ready, and should start accepting "real" updates
            if let DomainMode::Replaying { passes, .. } =
                mem::replace(&mut self.mode, DomainMode::Forwarding)
            {
                debug!(local = node.id(), passes, "node is fully up-to-date");
            } else {
                internal!();
            }

            #[allow(clippy::indexing_slicing)]
            // tag came from an internal data structure that guarantees it exists
            if self.replay_paths[&tag].notify_done {
                // NOTE: this will only be Some for non-partial replays
                info!(node = node.id(), "noting replay completed");
                self.replay_completed = true;
                Ok(())
            } else {
                internal!();
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            self.delayed_for_self
                .push_back(Box::new(Packet::Finish(tag, node)));
            Ok(())
        }
    }

    pub fn handle_eviction(
        &mut self,
        m: Packet,
        ex: &mut dyn Executor,
    ) -> Result<(), ReadySetError> {
        #[allow(clippy::too_many_arguments)]
        fn trigger_downstream_evictions(
            index: &Index,
            keys: &[KeyComparison],
            node: LocalNodeIndex,
            ex: &mut dyn Executor,
            not_ready: &HashSet<LocalNodeIndex>,
            replay_paths: &HashMap<Tag, ReplayPath>,
            shard: Option<usize>,
            state: &mut StateMap,
            nodes: &DomainNodes,
        ) -> Result<(), ReadySetError> {
            // TODO: this is a linear walk of replay paths -- we should make that not linear
            for (tag, path) in replay_paths {
                if path.source == Some(node) {
                    // Check whether this replay path is for the same key.
                    match &path.trigger {
                        TriggerEndpoint::Local(ref key) | TriggerEndpoint::Start(ref key) => {
                            // what if just key order changed?
                            if key != index {
                                continue;
                            }
                        }
                        _ => {
                            internal!();
                        }
                    };

                    let mut keys = Vec::from(keys);
                    walk_path(&path.path[..], &mut keys, *tag, shard, nodes, ex)?;

                    if let TriggerEndpoint::Local(_) = path.trigger {
                        #[allow(clippy::indexing_slicing)] // tag came from replay_paths
                        let target = replay_paths[tag].last_segment();
                        #[allow(clippy::indexing_slicing)] // nodes in replay paths must exist
                        if nodes[target.node].borrow().is_reader() {
                            // already evicted from in walk_path
                            continue;
                        }
                        if !state.contains_key(target.node) {
                            // this is probably because
                            if !not_ready.contains(&target.node) {
                                debug!(
                                    node = target.node.id(),
                                    "got eviction for ready but stateless node"
                                )
                            }
                            continue;
                        }

                        #[allow(clippy::indexing_slicing)] // nodes in replay paths must exist
                        state[target.node].evict_keys(*tag, &keys[..]);
                        #[allow(clippy::unwrap_used)]
                        // we can only evict from partial replay paths, so we must have a partial key
                        trigger_downstream_evictions(
                            target.partial_index.as_ref().unwrap(),
                            &keys[..],
                            target.node,
                            ex,
                            not_ready,
                            replay_paths,
                            shard,
                            state,
                            nodes,
                        )?;
                    }
                }
            }
            Ok(())
        }

        fn walk_path(
            path: &[ReplayPathSegment],
            keys: &mut Vec<KeyComparison>,
            tag: Tag,
            shard: Option<usize>,
            nodes: &DomainNodes,
            executor: &mut dyn Executor,
        ) -> ReadySetResult<()> {
            #[allow(clippy::indexing_slicing)] // replay paths can't be empty
            let mut from = path[0].node;
            for segment in path {
                #[allow(clippy::indexing_slicing)] // nodes in replay paths must exist
                #[allow(clippy::unwrap_used)]
                // partial_key must be Some for partial replay paths
                nodes[segment.node].borrow_mut().process_eviction(
                    from,
                    &segment.partial_index.as_ref().unwrap().columns,
                    keys,
                    tag,
                    shard,
                    executor,
                )?;
                from = segment.node;
            }
            Ok(())
        }

        match m {
            Packet::Evict {
                node,
                mut num_bytes,
            } => {
                let start = std::time::Instant::now();
                counter!(
                    recorded::DOMAIN_EVICTION_REQUESTS,
                    1,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string()
                );

                let mut total_freed = 0;
                let nodes = if let Some(node) = node {
                    vec![(node, num_bytes)]
                } else {
                    let mut candidates: Vec<_> = self
                        .nodes
                        .values()
                        .filter_map(|nd| {
                            let n = &*nd.borrow();
                            let local_index = n.local_addr();

                            if let Some(r) = n.as_reader() {
                                if r.is_partial() {
                                    r.state_size()
                                } else {
                                    None
                                }
                            } else {
                                self.state
                                    .get(local_index)
                                    .filter(|state| state.is_partial())
                                    .map(|state| state.deep_size_of())
                            }
                            .map(|s| (local_index, s))
                        })
                        .filter(|&(_, s)| s > 0)
                        .map(|(x, s)| (x, s as usize))
                        .collect();

                    // we want to spread the eviction across the nodes,
                    // rather than emptying out one node completely.
                    // -1* so we sort in descending order
                    // TODO: be smarter than 3 here
                    candidates.sort_unstable_by_key(|&(_, s)| -(s as i64));
                    candidates.truncate(3);

                    // don't evict from tiny things (< 10% of max)
                    #[allow(clippy::indexing_slicing)]
                    // candidates must be at least nodes.len(), and nodes can't be empty
                    if let Some(too_small_i) = candidates
                        .iter()
                        .position(|&(_, s)| s < candidates[0].1 / 10)
                    {
                        // everything beyond this is smaller, so also too small
                        candidates.truncate(too_small_i);
                    }

                    let mut n = candidates.len();
                    // rev to start with the smallest of the n domains
                    for (_, size) in candidates.iter_mut().rev() {
                        // TODO: should this be evenly divided, or weighted by the size of the domains?
                        let share = (num_bytes + n - 1) / n;
                        // we're only willing to evict at most half the state in each node
                        // unless this is the only node left to evict from
                        *size = if n > 1 {
                            cmp::min(*size / 2, share)
                        } else {
                            assert_eq!(share, num_bytes);
                            share
                        };
                        num_bytes -= *size;
                        trace!(bytes = *size, node = ?n, "chose to evict from node");
                        n -= 1;
                    }

                    candidates
                };

                for (node, num_bytes) in nodes {
                    let mut freed = 0u64;
                    #[allow(clippy::indexing_slicing)] // we got the node from self.nodes
                    let mut n = self.nodes[node].borrow_mut();

                    if n.is_dropped() {
                        continue; // Node was dropped. Skip.
                    } else if let Some(r) = n.as_mut_reader() {
                        freed += r.evict_bytes(num_bytes as usize);
                    } else if let Some(evicted) = self.state[node].evict_bytes(num_bytes as usize) {
                        let keys = evicted
                            .keys_evicted
                            .into_iter()
                            .map(|k| {
                                KeyComparison::try_from(k)
                                    .map_err(|_| internal_err("Empty key evicted"))
                            })
                            .collect::<ReadySetResult<Vec<_>>>()?;

                        freed += evicted.bytes_freed;
                        let index = evicted.index.clone();

                        if !keys.is_empty() {
                            trigger_downstream_evictions(
                                &index,
                                &keys[..],
                                node,
                                ex,
                                &self.not_ready,
                                &self.replay_paths,
                                self.shard,
                                &mut self.state,
                                &self.nodes,
                            )?;
                        }
                    } else {
                        // This node was unable to evict any keys
                        continue;
                    }

                    debug!(%freed, node = ?n, "evicted from node");
                    self.state_size.fetch_sub(freed as usize, Ordering::AcqRel);
                    total_freed += freed;
                }

                histogram!(
                    recorded::DOMAIN_EVICTION_FREED_MEMORY,
                    total_freed as f64,
                    "domain" => self.index.index().to_string(),
                    "shard" => self.shard.unwrap_or(0).to_string()
                );

                histogram!(
                    recorded::DOMAIN_EVICTION_TIME,
                    start.elapsed().as_micros() as f64,
                    "domain" => self.index.index().to_string(),
                );
            }
            Packet::EvictKeys {
                link: Link { dst, .. },
                mut keys,
                tag,
            } => {
                let (trigger, path) = if let Some(rp) = self.replay_paths.get(&tag) {
                    (&rp.trigger, &rp.path)
                } else {
                    debug!(?tag, "got eviction for tag that has not yet been finalized");
                    return Ok(());
                };

                let i = path
                    .iter()
                    .position(|ps| ps.node == dst)
                    .ok_or_else(|| ReadySetError::NoSuchNode(dst.id()))?;
                #[allow(clippy::indexing_slicing)]
                // i is definitely in bounds, since it came from a call to position
                walk_path(&path[i..], &mut keys, tag, self.shard, &self.nodes, ex)?;

                match trigger {
                    TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) => {
                        // This path terminates inside the domain. Find the target node, evict
                        // from it, and then propagate the eviction further downstream.
                        let target = path.last().node;
                        // We've already evicted from readers in walk_path
                        #[allow(clippy::indexing_slicing)] // came from replay paths
                        if self.nodes[target].borrow().is_reader() {
                            return Ok(());
                        }
                        // No need to continue if node was dropped.
                        #[allow(clippy::indexing_slicing)] // came from replay paths
                        if self.nodes[target].borrow().is_dropped() {
                            return Ok(());
                        }
                        #[allow(clippy::indexing_slicing)] // came from replay paths
                        if let Some((index, _evicted_bytes)) =
                            self.state[target].evict_keys(tag, &keys)
                        {
                            let index = index.clone();
                            trigger_downstream_evictions(
                                &index,
                                &keys[..],
                                target,
                                ex,
                                &self.not_ready,
                                &self.replay_paths,
                                self.shard,
                                &mut self.state,
                                &self.nodes,
                            )?;
                        }
                    }
                    TriggerEndpoint::None | TriggerEndpoint::Start(..) => {}
                }
            }
            _ => {
                internal!();
            }
        };

        Ok(())
    }

    pub fn id(&self) -> (DomainIndex, usize) {
        (self.index, self.shard.unwrap_or(0))
    }

    pub fn update_state_sizes(&mut self) {
        let mut reader_size: u64 = 0;
        let total: u64 = self
            .nodes
            .values()
            .map(|nd| {
                let n = &*nd.borrow();
                let local_index = n.local_addr();

                if let Some(r) = n.as_reader() {
                    // We are a reader, which has its own kind of state
                    let mut size = 0;
                    if r.is_partial() {
                        size = r.state_size().unwrap_or(0);
                        reader_size += size;
                    }
                    size
                } else {
                    // Not a reader, state is with domain
                    self.state
                        .get(local_index)
                        .filter(|state| state.is_partial())
                        .map(|s| s.deep_size_of())
                        .unwrap_or(0)
                }
            })
            .sum();

        let domain = self.index.index().to_string();
        let shard = self.shard.unwrap_or(0).to_string();

        let total_node_state: u64 = self
            .state
            .iter()
            .map(|(ni, state)| {
                let ret = state.deep_size_of();
                gauge!(
                    recorded::DOMAIN_NODE_STATE_SIZE_BYTES,
                    ret as f64,
                    "domain" => domain.clone(),
                    "shard" => shard.clone(),
                    "node" => ni.id().to_string()
                );
                ret
            })
            .sum();

        gauge!(
            recorded::DOMAIN_PARTIAL_STATE_SIZE_BYTES,
            total as f64,
            "domain" => domain.clone(),
            "shard" => shard.clone(),
        );
        gauge!(
            recorded::DOMAIN_READER_STATE_SIZE_BYTES,
            reader_size as f64,
            "domain" => domain.clone(),
            "shard" => shard.clone(),
        );
        gauge!(
            recorded::DOMAIN_ESTIMATED_BASE_TABLE_SIZE_BYTES,
            self.estimated_base_tables_size() as f64,
            "domain" => domain.clone(),
            "shard" => shard.clone(),
        );
        gauge!(
            recorded::DOMAIN_TOTAL_NODE_STATE_SIZE_BYTES,
            (total_node_state + reader_size) as f64,
            "domain" => domain,
            "shard" => shard,
        );
        self.state_size.store(total as usize, Ordering::Release);
        // no response sent, as worker will read the atomic
    }

    pub fn estimated_base_tables_size(&self) -> u64 {
        self.state
            .values()
            .filter_map(|state| state.as_persistent().map(|s| s.deep_size_of()))
            .sum()
    }

    pub fn replication_offsets(&self) -> NodeMap<Option<ReplicationOffset>> {
        self.state
            .iter()
            .filter_map(|(ni, state)| {
                Some((ni, state.as_persistent()?.replication_offset().cloned()))
            })
            .collect()
    }

    /// If there is a pending timed purge, return the duration until it needs
    /// to happen
    pub fn next_poll_duration(&mut self) -> Option<time::Duration> {
        // when do we need to be woken up again?
        let now = time::Instant::now();
        self.timed_purges.front().map(|tp| {
            if tp.time > now {
                tp.time - now
            } else {
                time::Duration::from_millis(0)
            }
        })
    }

    /// Handle a single message for this domain
    #[failpoint("handle-packet")]
    pub fn handle_packet(
        &mut self,
        packet: Box<Packet>,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let span = info_span!("domain", index = %self.index);
        let _g = span.enter();

        if self.wait_time.is_running() {
            self.wait_time.stop();
        }

        self.handle(packet, executor)?;
        // After we handle an external packet, the domain may have accumulated a bunch of packets to itself
        // we need to process them all next;
        while let Some(message) = self.delayed_for_self.pop_front() {
            trace!("handling local transmission");
            self.handle(message, executor)?;
        }

        if self.aggressively_update_state_sizes {
            self.update_state_sizes();
        }

        if !self.wait_time.is_running() {
            self.wait_time.start();
        }

        Ok(())
    }

    /// Handle an expired timeout from `next_poll_duration`
    pub fn handle_timeout(&mut self) -> ReadySetResult<()> {
        if self.wait_time.is_running() {
            self.wait_time.stop();
        }

        if !self.timed_purges.is_empty() {
            self.handle_timed_purges();
        }

        if self.aggressively_update_state_sizes {
            self.update_state_sizes();
        }

        if !self.wait_time.is_running() {
            self.wait_time.start();
        }

        Ok(())
    }
}
