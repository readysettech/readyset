pub(crate) mod channel;
mod domain_metrics;
mod replay_paths;

use std::borrow::Cow;
use std::cell::RefMut;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{cell, cmp, mem, process, time};

use ahash::RandomState;
use common::{Len, LenMetric};
use dataflow_state::{
    BaseTableState, EvictBytesResult, EvictKeysResult, EvictRandomResult, MaterializedNodeState,
    PersistenceType, PointKey, RangeKey, RangeLookupResult,
};
use exponential_backoff::Backoff;
use failpoint_macros::set_failpoint;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use futures_util::TryFutureExt;
pub use internal::{DomainIndex, ReplicaAddress};
use itertools::Itertools;
use merging_interval_tree::IntervalTreeSet;
use metrics::{counter, histogram};
use nom_sql::Relation;
use petgraph::graph::NodeIndex;
use readyset_alloc::StdThreadBuildWrapper;
use readyset_client::debug::info::KeyCount;
use readyset_client::internal::{self, Index};
use readyset_client::metrics::recorded;
use readyset_client::{KeyComparison, PersistencePoint, ReaderAddress};
use readyset_data::DfType;
use readyset_errors::{internal, internal_err, ReadySetError, ReadySetResult};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::futures::abort_on_panic;
use readyset_util::progress::report_progress_with;
use readyset_util::ranges::RangeBounds;
use readyset_util::redacted::Sensitive;
use readyset_util::{time_scope, Indices};
use replication_offset::ReplicationOffset;
use serde::{Deserialize, Serialize};
use timekeeper::{RealTime, SimpleTracker, ThreadTime, Timer, TimerSet};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use url::Url;
use vec1::Vec1;

pub(crate) use self::replay_paths::ReplayPath;
use self::replay_paths::{Destination, ReplayPathSpec, ReplayPaths, Target};
use crate::domain::channel::{ChannelCoordinator, DomainReceiver, DomainSender};
use crate::node::special::EgressTx;
use crate::node::{Column, NodeProcessingResult, ProcessEnv};
use crate::payload::{
    self, Eviction, MaterializedState, PacketDiscriminants, PrepareStateKind, PrettyReplayPath,
    ReplayPieceContext, SenderReplication, SourceSelection,
};
use crate::prelude::*;
use crate::processing::ColumnMiss;
use crate::{backlog, DomainRequest, Readers};

const SLOW_LOOP_THRESHOLD: Duration = Duration::from_secs(1);

/// A stub for the cache name used for domain metrics that are emitted during a migration.
const MIGRATION_CACHE_NAME_STUB: &str = "migration";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// If set to `true`, the metric tracking the in-memory size of materialized state will be
    /// updated after every packet is handled, rather than only when requested by the eviction
    /// worker. This causes a (minor) runtime cost, with the upside being that the materialization
    /// state sizes will never be out-of-date.
    pub aggressively_update_state_sizes: bool,

    /// The amount of time to wait before timing out a view request to the domain.
    pub view_request_timeout: time::Duration,

    /// The amount of time to wait before timing out a table request to the domain.
    pub table_request_timeout: time::Duration,

    #[serde(default)]
    pub eviction_kind: crate::EvictionKind,

    /// Whether to emit verbose metrics for the domain.
    #[serde(default)]
    pub verbose_metrics: bool,

    /// Allow fully materialized nodes to be persisted to disk.
    #[serde(default)]
    pub materialization_persistence: bool,
}

const BATCH_SIZE: usize = 256;

#[derive(Debug)]
enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Packet>,
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
        options: Vec<ReplicaAddress>,
    },
    Local(Index),
}

impl TriggerEndpoint {
    /// Returns `true` if the trigger endpoint is [`Local`].
    ///
    /// [`Local`]: TriggerEndpoint::Local
    fn is_local(&self) -> bool {
        matches!(self, Self::Local(..))
    }

    /// Returns `true` if the trigger endpoint is [`End`].
    ///
    /// [`End`]: TriggerEndpoint::End
    fn is_end(&self) -> bool {
        matches!(self, Self::End { .. })
    }
}

impl Debug for TriggerEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Start(index) => f.debug_tuple("Start").field(index).finish(),
            Self::End { source, .. } => f
                .debug_struct("End")
                .field("source", source)
                .finish_non_exhaustive(),
            Self::Local(index) => f.debug_tuple("Local").field(index).finish(),
        }
    }
}

/// The result of do_lookup, consists of the vector of the found records
/// the hashset of the fulfilled keys, and a hashset of the missed key/replay key tuples
struct StateLookupResult<'a> {
    /// Records returned by the lookup
    records: Vec<RecordResult<'a>>,
    /// Keys for which records were found
    found_keys: HashSet<KeyComparison>,
    /// Tuples of (replay_key, miss_key) where `replay_key` is the key we're trying to replay
    /// and `miss_key` is the part of it we missed on.  For non-range queries, they are the same.
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
    requesting_replica: usize,
}

impl ReplayDescriptor {
    fn from_miss(
        miss: &Miss,
        tag: Tag,
        unishard: bool,
        requesting_shard: usize,
        requesting_replica: usize,
    ) -> Self {
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
            requesting_replica,
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
    requesting_replica: usize,
}

/// Struct indicating a single hole in a partial materialization that needs to be filled to satisfy
/// some downstream replay. Used in [`Waiting`].
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct Hole {
    node: LocalNodeIndex,
    column_indices: Vec<usize>,
    key: KeyComparison,
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
    ///
    /// Note that for extended replay paths, the `Hole` is in the *target* of the path, *not* the
    /// destination
    redos: HashMap<Hole, HashSet<Redo>>,
}

/// Data structure representing the set of keys that have been requested by a reader.
///
/// As an optimization, this structure is backed by either a [`HashSet`] if the reader's index is a
/// [`HashMap`], or an [`IntervalTreeSet`] if the reader's index is a [`BTreeMap`] - interval trees
/// can act as sets of points, but are much slower than hash sets for that purpose.
///
/// [`HashMap`]: IndexType::HashMap
/// [`BTreeMap`]: IndexType::BTreeMap
enum RequestedKeys {
    Points(HashSet<Vec1<DfValue>, RandomState>),
    Ranges(IntervalTreeSet<Vec1<DfValue>>),
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
                            .get_interval_difference(&key.as_std_range())
                            .map(
                                |(lower, upper): (Bound<&Vec1<DfValue>>, Bound<&Vec1<DfValue>>)| {
                                    (lower.cloned(), upper.cloned())
                                },
                            )
                            .collect::<Vec<_>>();
                        requested.insert_interval::<Vec1<_>, _>(key.as_std_range());
                        diff
                    })
                    .map(|(lower, upper)| {
                        // It is safe to unwrap here because we know we will never get a
                        // `std::ops::Bound::Unbounded` back from the interval tree, since we're
                        // never passing in unbounded bounds
                        let expect_message =
                            "we should never get unbounded bounds back from the interval tree";

                        KeyComparison::Range((
                            lower.try_into().expect(expect_message),
                            upper.try_into().expect(expect_message),
                        ))
                    })
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
                        requested.get_interval_overlaps(key).map(|(lower, upper)| {
                            // It is safe to unwrap here because we know we will never get a
                            // `std::ops::Bound::Unbounded` back from the interval tree, since we're
                            // never passing in unbounded bounds
                            let expect_message =
                                "we should never get unbounded bounds back from the interval tree";

                            KeyComparison::Range((
                                lower
                                    .cloned()
                                    .map(|l| Vec1::try_from(l).unwrap())
                                    .try_into()
                                    .expect(expect_message),
                                upper
                                    .cloned()
                                    .map(|u| Vec1::try_from(u).unwrap())
                                    .try_into()
                                    .expect(expect_message),
                            ))
                        })
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
                requested.remove_interval::<Vec1<_>, _>(&key.as_std_range());
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
    /// The replica index of the domain to run
    pub replica: usize,
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
    pub fn shard(&self) -> usize {
        self.shard.unwrap_or(0)
    }

    pub fn address(&self) -> ReplicaAddress {
        ReplicaAddress {
            domain_index: self.index,
            shard: self.shard(),
            replica: self.replica,
        }
    }

    /// Starts up the domain represented by this `DomainBuilder`.
    pub fn build(
        self,
        readers: Readers,
        channel_coordinator: Arc<ChannelCoordinator>,
        state_size: Arc<AtomicUsize>,
        init_state_tx: Sender<MaterializedState>,
    ) -> Domain {
        // initially, all nodes are not ready
        let not_ready = self
            .nodes
            .values()
            .map(|n| n.borrow().local_addr())
            .collect::<HashSet<_>>();

        let meta = vec![
            ("index", self.index.to_string()),
            ("shard", self.shard.unwrap_or_default().to_string()),
            ("replica", self.replica.to_string()),
        ];

        Domain {
            index: self.index,
            shard: self.shard,
            replica: self.replica,
            _nshards: self.nshards,

            persistence_parameters: self.persistence_parameters,
            state: LenMetric::new_meta("state", &meta),
            auxiliary_node_states: LenMetric::new_from(
                self.nodes
                    .iter()
                    .filter_map(|(n, node)| {
                        node.borrow()
                            .initial_auxiliary_state()
                            .map(|state| (n, state))
                    })
                    .collect(),
                "auxillary_node_states",
                &meta,
            ),
            nodes: LenMetric::new_from(self.nodes, "nodes", &meta),

            reader_write_handles: LenMetric::new_meta("reader_write_handles", &meta),
            not_ready: LenMetric::new_from(not_ready, "not_ready", &meta),
            mode: DomainMode::Forwarding,
            waiting: LenMetric::new_meta("waiting", &meta),
            reader_triggered: LenMetric::new_meta("reader_triggered", &meta),
            replay_paths: Default::default(),

            ingress_inject: LenMetric::new_meta("ingress_inject", &meta),

            readers,
            channel_coordinator,

            timed_purges: LenMetric::new_meta("timed_purges", &meta),

            delayed_for_self: LenMetric::new_meta("delayed_for_self", &meta),

            state_size,
            total_time: Timer::new(),
            total_ptime: Timer::new(),
            wait_time: Timer::new(),
            process_times: TimerSet::new(),
            process_ptimes: TimerSet::new(),

            total_replay_time: Timer::new(),
            total_forward_time: Timer::new(),

            aggressively_update_state_sizes: self.config.aggressively_update_state_sizes,

            metrics: domain_metrics::DomainMetrics::new(self.config.verbose_metrics),

            eviction_kind: self.config.eviction_kind,
            remapped_keys: LenMetric::new_meta("remapped_keys", &meta),

            init_state_tx,
            materialization_persistence: self.config.materialization_persistence,
        }
    }
}

#[derive(Clone, Debug)]
struct TimedPurge {
    time: time::Instant,
    view: LocalNodeIndex,
    keys: HashSet<KeyComparison>,
}

/// Mapping, for nodes which [generate columns][], from *upstream* keys, to downstream keys which
/// have remapped to those upstream keys.
///
/// Used when we receive an eviction for those upstream keys, to rewrite that eviction into an
/// eviction for the downstream keys.
///
/// # Internals
///
/// The internals of this type are reasonably complicated, and best illustrated by an example.
///
/// Consider we have some join, `n3`, with two parents `n1` and `n2`, and which is indexed on
/// columns `[1, 2]`, where column `1` maps to column `1` in `n1`, and column `2` maps to column `1`
/// in `n2` (a [straddled join][]). Now let's say we get some upquery on `[1, 2] = ["a", "b"]` for
/// that join.  That upquery would get remapped to a pair of upqueries on the parents, `[1] = ["a"]`
/// on `n1` and `[1] = ["b"]` on `n2`. We then perform both of those upqueries, and then once both
/// have finished we perform the join and return the results.
///
/// Now let's say `n3` gets an eviction for `[1] = ["a"]` from `n1`. We don't have an index on only
/// column `[1]` in `n3`, so we need to find a way of *mapping* that eviction into an eviction on
/// `[1, 2] = ["a", "b"]`. In that case, this data structure would look something like:
///
/// ```notrust
/// {
///     n3: { // The node we're processing through
///         n1: { // The node we got an eviction from
///             [1]: { // The column indices in the eviction
///                 ["a"]: { // The upstream key for the eviction
///                     { Tag(some tag): [["a", "b"]] } // The downstream eviction(s) to rewrite to
///                 }
///             }
///         }
///     }
/// }
/// ```
///
/// Which is sufficient information to remap our upstream eviction to a downstream one
///
/// [generate columns]: http://docs/dataflow/replay_paths.html#generated-columns
/// [straddled join]: http://docs/dataflow/replay_paths.html#straddled-joins
#[derive(Debug, Clone, Default)]
#[allow(clippy::type_complexity)]
struct RemappedKeys {
    // map from nodes which generate columns...
    map: NodeMap<
        // ...to nodes which are the source of those columns (which might be the same node)...
        NodeMap<
            // ...to the column indices within the source node which those columns are generated
            // from...
            HashMap<
                Vec<usize>,
                // ...to the upstream keys which have been remapped to...
                HashMap<
                    Vec<KeyComparison>,
                    // ...to the downstream tags and key comparisons which have remapped to those
                    // keys.
                    HashMap<Tag, Vec<KeyComparison>>,
                >,
            >,
        >,
    >,
    len: usize,
}

impl RemappedKeys {
    /// Record that some downstream key was rewritten by `miss_in` into an `upstream_miss`.
    fn insert(
        &mut self,
        miss_in: LocalNodeIndex,
        upstream_miss: ColumnMiss,
        downstream_key: KeyComparison,
        downstream_tag: Tag,
    ) {
        self.len += 1;
        self.map
            .entry(miss_in)
            .or_default()
            .entry(upstream_miss.node)
            .or_default()
            .entry(upstream_miss.column_indices)
            .or_default()
            .entry(upstream_miss.missed_keys.into_vec())
            .or_default()
            .entry(downstream_tag)
            .or_default()
            .push(downstream_key);
    }

    /// If `node` rewrites some of its downstream keys into upstream upqueries to `column` in a
    /// `target` node, look up the set of downstream tags and keys which have been rewritten to
    /// `keys`.
    fn remove(
        &mut self,
        node: LocalNodeIndex,
        target: LocalNodeIndex,
        columns: &[usize],
        keys: &[KeyComparison],
    ) -> Option<impl ExactSizeIterator<Item = (Tag, Vec<KeyComparison>)>> {
        // NOTE: we consciously don't remove nested maps here; the idea is that the only thing
        // that's actually a large state-space is the keys, which are the leaves, so we can leave
        // empty maps in up to the keys if we want (and that greatly simplifies this method)
        let iter = self
            .map
            .get_mut(node)
            .and_then(|m| m.get_mut(target))
            .and_then(|m| m.get_mut(columns))
            .and_then(|m| m.remove(keys))
            .map(|m| m.into_iter());
        iter.inspect(|i| self.len -= i.len())
    }
}

impl Len for RemappedKeys {
    fn len(&self) -> usize {
        self.len
    }
}

/// A [`Domain`] is a well-connected sub-graph of the overall dataflow graph, used as the unit
/// of execution of the dataflow engine. The dataflow graph is split up into domains using
/// [heuristics](../src/readyset_server/controller/migrate/assignment.rs.html)
/// Domains are [scheduled](../src/readyset_server/controller/migrate/scheduling.rs.html)
/// onto workers.
pub struct Domain {
    index: DomainIndex,
    shard: Option<usize>,
    replica: usize,
    _nshards: usize,

    /// Map of nodes managed by this domain
    ///
    /// # Invariants
    ///
    /// * All nodes mentioned in `self.replay_paths` and `self.not_ready` must exist in
    ///   `self.nodes`
    /// * All keys of `self.state` and `self.auxiliary_node_states` must also be keys in
    ///   `self.nodes` * `nodes` cannot be empty
    nodes: LenMetric<DomainNodes>,

    /// State for all materialized non-reader nodes managed by this domain
    ///
    /// Invariant: All keys of `self.state` must also be keys in `self.nodes`
    state: LenMetric<StateMap>,

    /// State for internal nodes managed by this domain
    ///
    /// Invariant: All keys of `self.auxiliary_node_states` must also be keys in
    /// `self.nodes`
    auxiliary_node_states: LenMetric<AuxiliaryNodeStateMap>,

    /// State for all reader nodes managed by this domain
    ///
    /// Invariant: All keys of `self.reader_write_handles` must also be keys in `self.nodes`
    reader_write_handles: LenMetric<NodeMap<backlog::WriteHandle>>,

    not_ready: LenMetric<HashSet<LocalNodeIndex>>,

    ingress_inject: LenMetric<NodeMap<(usize, Vec<DfValue>)>>,

    persistence_parameters: PersistenceParameters,

    mode: DomainMode,
    waiting: LenMetric<NodeMap<Waiting>>,

    remapped_keys: LenMetric<RemappedKeys>,

    /// Replay paths that go through this domain
    replay_paths: ReplayPaths,

    /// Map from node ID to an interval tree of the keys of all current pending upqueries to that
    /// node
    reader_triggered: LenMetric<NodeMap<RequestedKeys>>,

    /// Queue of purge operations to be performed on reader nodes at some point in the future, used
    /// as part of the implementation of materialization frontiers
    ///
    /// # Invariants
    ///
    /// * Each node referenced by a `view` of a TimedPurge must be in `self.nodes`
    /// * Each node referenced by a `view` of a TimedPurge must be a reader node
    timed_purges: LenMetric<VecDeque<TimedPurge>>,

    readers: Readers,
    channel_coordinator: Arc<ChannelCoordinator>,

    delayed_for_self: LenMetric<VecDeque<Packet>>,

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

    metrics: domain_metrics::DomainMetrics,
    eviction_kind: crate::EvictionKind,

    /// This channel is used to notify the replica that a base node has its persistent state
    /// initialized.
    /// This allow us to asynchronously run that process, and avoid any bottlenecks on the
    /// initialization of their state.
    init_state_tx: tokio::sync::mpsc::Sender<MaterializedState>,

    materialization_persistence: bool,
}

/// Creates the materialized node state for the given node.
/// This is used to deferred the creation of the persistent state to a separate task, as we know
/// it takes a lot of time for large tables.
/// Upon completion, this method will send the node index and the pointer to the new
/// `PersistentState` through the `sender`.
async fn initialize_state(
    node_idx: LocalNodeIndex,
    indices: HashSet<Index>,
    base_name: String,
    unique_keys: Vec<Box<[usize]>>,
    persistence_params: PersistenceParameters,
    sender: Sender<MaterializedState>,
) -> ReadySetResult<()> {
    trace!("running separate task to initialize base node persistent state");
    let reported_once = Arc::new(AtomicBool::new(false));
    let mut s = MaterializedNodeState::Persistent(
        report_progress_with(
            Backoff::new(
                u32::MAX,
                Duration::from_secs(10),
                Some(Duration::from_secs(300)),
            ),
            {
                let base_name = base_name.clone();
                let reported_once = Arc::clone(&reported_once);
                move || {
                    reported_once.store(true, Ordering::Relaxed);
                    let base_name = base_name.clone();
                    async move {
                        info!(%base_name, %node_idx, "Still initializing state");
                    }
                }
            },
            {
                let base_name = base_name.clone();
                tokio::task::spawn_blocking(move || {
                    PersistentState::new(
                        base_name,
                        unique_keys,
                        &persistence_params,
                        PersistenceType::BaseTable,
                    )
                    .map_err(|e| ReadySetError::from(e))
                })
            }
            .fuse(),
        )
        .await
        .unwrap()?,
    );
    for idx in indices {
        s.add_index(idx, None);
    }
    if reported_once.load(Ordering::Relaxed) {
        info!(%base_name, %node_idx, "Finished initializing state");
    } else {
        trace!("done initializing persistent state! notifying replica");
    }
    sender
        .send(MaterializedState {
            node: node_idx,
            state: Box::new(s),
        })
        .await
        .map_err(|_| {
            internal_err!(
                "an error occurred while sending materialized state to replica for node {node_idx}"
            )
        })
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

    /// Return this domain's replica
    pub fn replica(&self) -> usize {
        self.replica
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

    fn find_tags(
        &self,
        keys: &[KeyComparison],
        cols: &[usize],
        dst: Destination,
        target: Target,
    ) -> ReadySetResult<HashSet<Tag>> {
        let index = Index::new(IndexType::best_for_keys(keys), cols.to_vec());
        let tags = self
            .replay_paths
            .tags_for_index(dst, target, &index)
            .cloned()
            .unwrap_or_default();

        invariant!(
            !tags.is_empty(),
            "no tag found for value {:?} in {}.{:?}{}",
            Sensitive(&keys),
            dst.0,
            index,
            if dst.0 == target.0 {
                "".to_owned()
            } else {
                format!(" (targeting {})", target.0)
            }
        );

        Ok(tags)
    }

    /// Initiate a replay for a miss represented by the given keys and column indices in the given
    /// node.
    ///
    /// Passed both the *destination* node (the node which must be the final node in the replay
    /// path) in addition to the *target* node (the node containing the index which must be filled).
    /// Usually, these will be the same node, except in  the case of *extended* replay paths. See
    /// [the docs section on straddled joins][straddled-joins] for more information about extended
    /// replay paths
    ///
    /// [straddled-joins]: http://docs/dataflow/replay_paths.html#straddled-joins
    ///
    /// # Invariants
    ///
    /// * `miss_columns` must not be empty
    /// * `dst` and `target` must both be nodes in this domain
    fn find_tags_and_replay(
        &mut self,
        ex: &mut dyn Executor,
        miss_keys: Vec<KeyComparison>,
        miss_columns: &[usize],
        dst: Destination,
        target: Target,
        cache_name: Relation,
    ) -> ReadySetResult<()> {
        let tags = self.find_tags(&miss_keys, miss_columns, dst, target)?;

        for &tag in &tags {
            let (miss_keys, cache_name) = (miss_keys.clone(), cache_name.clone());
            if self.replay_paths[tag].trigger.is_local() {
                self.request_partial_replay_local(tag, miss_keys, cache_name);
            } else {
                self.request_partial_replay(ex, tag, miss_keys, cache_name)?;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn on_replay_misses(
        &mut self,
        ex: &mut dyn Executor,
        miss_in: LocalNodeIndex,
        miss_columns: &[usize],
        missed_keys: HashSet<(KeyComparison, KeyComparison)>,
        was_single_shard: bool,
        requesting_shard: usize,
        requesting_replica: usize,
        needed_for: Tag,
        cache_name: Relation,
    ) -> ReadySetResult<()> {
        // when the replay eventually succeeds, we want to re-do the replay.
        let mut w = self.waiting.remove(miss_in).unwrap_or_default();

        self.metrics
            .inc_replay_misses(&cache_name, missed_keys.len());

        let is_generated = self
            .replay_paths
            .columns_are_generated(miss_in, miss_columns);

        // Map of replays we need to do, grouped by the set of columns at the *target* of the replay
        // (which in the case of remapped upqueries might be different than the columns we missed
        // on!)
        let mut needed_replays: HashMap<_, Vec<KeyComparison>> = Default::default();

        for (replay_key, miss_key) in missed_keys {
            let miss = ColumnMiss {
                node: miss_in,
                column_indices: miss_columns.to_vec(),
                missed_keys: vec1![miss_key],
            };
            let misses = if is_generated {
                // If these columns were generated, ask the node to remap them for us
                let misses = self.nodes[miss_in].borrow_mut().handle_upquery(miss)?;
                trace!(?misses, "Remapped misses on generated columns");

                // Record that we remapped these keys, so that any evictions on the upstream keys
                // can be translated into the original keys.  If the node we missed in is fully
                // materialized, we don't need to record the remaps, since we're guaranteed not
                // to get any evictions on the upstream keys
                for upstream_miss in &misses {
                    let Some(state) = self.state.get(upstream_miss.node) else {
                        continue;
                    };
                    if state.is_partial() {
                        self.remapped_keys.insert(
                            miss_in,
                            upstream_miss.clone(),
                            replay_key.clone(),
                            needed_for,
                        )
                    }
                }

                invariant!(
                    !misses.is_empty(),
                    "columns {:?} in {} are generated, but could not remap an upquery",
                    miss_columns,
                    miss_in
                );
                misses
            } else {
                vec![miss]
            };

            let redo = Redo {
                tag: needed_for,
                replay_key,
                unishard: was_single_shard,
                requesting_shard,
                requesting_replica,
            };
            for ColumnMiss {
                node,
                column_indices,
                missed_keys,
            } in misses
            {
                // redo should wait for backfill to complete before redoing
                let replays = needed_replays
                    .entry((Target(node), column_indices.clone()))
                    .or_default();
                for miss_key in missed_keys {
                    match w.redos.entry(Hole {
                        node,
                        column_indices: column_indices.clone(),
                        key: miss_key.clone(),
                    }) {
                        Entry::Occupied(e) => {
                            if e.into_mut().insert(redo.clone()) {
                                *w.holes.entry(redo.clone()).or_default() += 1;
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(HashSet::from([redo.clone()]));
                            *w.holes.entry(redo.clone()).or_default() += 1;

                            replays.push(miss_key);
                        }
                    }
                }
            }
        }

        self.waiting.insert(miss_in, w);

        for ((target, columns), keys) in needed_replays {
            self.find_tags_and_replay(
                ex,
                keys,
                &columns,
                Destination(miss_in),
                target,
                cache_name.clone(),
            )?
        }

        Ok(())
    }

    /// Send a partial replay request to oneself.
    ///
    /// In theory we could just call self.seed_all, and everything would be good.  However,
    /// then we start recursing, which could get us into sad situations where we break
    /// invariants where some piece of code is assuming that it is the only thing processing
    /// at the time (think, e.g., borrow_mut()).
    ///
    /// For example, consider the case where two misses occurred on the same key.  Normally,
    /// those requests would be deduplicated so that we don't get two replay responses for the
    /// same key later.  However, the way we do that is by tracking keys we have requested in
    /// self.waiting.redos (see `redundant` in `on_replay_miss`).  In particular, on_replay_miss
    /// is called while looping over all the misses that need replays, and while the first miss
    /// of a given key will trigger a replay, the second will not.  If we call `seed_all`
    /// directly here, that might immediately fill in this key and remove the entry. when the
    /// next miss (for the same key) is then hit in the outer iteration, it will *also* request
    /// a replay of that same key, which gets us into trouble with `State::mark_filled`.
    ///
    /// So instead, we simply keep track of the fact that we have a replay to handle, and then
    /// get back to it after all processing has finished (at the bottom of `Self::handle()`).
    fn request_partial_replay_local(
        &mut self,
        tag: Tag,
        keys: Vec<KeyComparison>,
        cache_name: Relation,
    ) {
        trace!(?tag, ?keys, "sending replay request to self");
        let (shard, replica) = (self.shard(), self.replica());
        self.delayed_for_self
            .push_back(Packet::RequestPartialReplay(RequestPartialReplay {
                tag,
                keys,
                unishard: true, // local replays are necessarily single-shard
                requesting_shard: shard,
                requesting_replica: replica,
                cache_name,
            }));
    }

    /// Send a partial replay request for keys to the replay path indicated by tag
    ///
    /// # Invariants
    ///
    /// * `tag` must be a tag for a valid replay path
    fn request_partial_replay(
        &mut self,
        ex: &mut dyn Executor,
        tag: Tag,
        keys: Vec<KeyComparison>,
        cache_name: Relation,
    ) -> ReadySetResult<()> {
        let requesting_shard = self.shard();
        let requesting_replica = self.replica();

        let pkt = |unishard, keys| {
            Packet::RequestPartialReplay(RequestPartialReplay {
                tag,
                unishard,
                keys,
                requesting_shard,
                requesting_replica,
                cache_name: cache_name.clone(),
            })
        };

        self.send_to_tag(ex, tag, keys, pkt)
    }

    fn send_to_tag<P>(
        &mut self,
        ex: &mut dyn Executor,
        tag: Tag,
        keys: Vec<KeyComparison>,
        pkt: P,
    ) -> ReadySetResult<()>
    where
        P: Fn(bool, Vec<KeyComparison>) -> Packet,
    {
        #[allow(clippy::unwrap_used)] // documented invariant
        let TriggerEndpoint::End {
            source,
            ref mut options,
        } = self.replay_paths.get_mut(tag).unwrap().trigger
        else {
            internal!("asked to replay along non-existing path");
        };

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
            trace!(?tag, ?keys, "sending broadcast shard replay request");

            for addr in options {
                ex.send(*addr, pkt(false, keys.clone())); // ignore error on shutdown
            }
        } else if options.len() == 1 {
            trace!(?tag, ?keys, "sending single replay request");
            ex.send(options[0], pkt(true, keys));
        } else if let Some(key_shard_i) = ask_shard_by_key_i {
            trace!(?tag, ?keys, "sending sharded replay request");
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
                ex.send(options[shard], pkt(true, keys));
            }
        } else {
            // would have hit the if further up
            internal!();
        }

        Ok(())
    }

    /// Called when a partial replay has been completed
    ///
    /// # Invariants
    ///
    /// * `tag` must be a valid replay tag
    fn finished_partial_replay(&mut self, tag: Tag, num: usize) -> ReadySetResult<()> {
        match self.replay_paths[tag].trigger {
            TriggerEndpoint::End { .. } => {
                // A backfill request we made to another domain was just satisfied!
                let mut requests_satisfied = 0;
                #[allow(clippy::unwrap_used)] // Replay paths can't be empty
                let last = self.replay_paths[tag].last_segment();
                if let Some(target) = self.replay_paths[tag].target_node() {
                    if let Some(tags) = self.replay_paths.tags_for_index(
                        Destination(last.node),
                        Target(target),
                        #[allow(clippy::unwrap_used)]
                        // We already know it's a partial replay path, so it must have a partial
                        // key
                        last.partial_index.as_ref().unwrap(),
                    ) {
                        requests_satisfied = tags
                            .iter()
                            .filter(|tag| self.replay_paths[**tag].trigger.is_end())
                            .count();
                    }
                } else {
                    internal!(
                        "Finished replay to a domain that does not contain the target of the replay path (tag: {:?})",
                        tag
                    );
                }

                // we also sent that many requests *per key*.
                requests_satisfied *= num;
                trace!(num_done = requests_satisfied, "notified of finished replay");
                Ok(())
            }
            TriggerEndpoint::Local(..) => {
                // didn't count against our quote, so we're also not decrementing
                Ok(())
            }
            TriggerEndpoint::Start(..) | TriggerEndpoint::None => {
                internal!();
            }
        }
    }

    fn dispatch(&mut self, m: Packet, executor: &mut dyn Executor) -> ReadySetResult<()> {
        let src = m.src();
        let me = m.dst();

        if !self.nodes.contains_key(me) {
            error!(%src, %me, "Packet destined for node that does not exist");
            return Ok(());
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
            let mut n = self.nodes[me].borrow_mut();
            self.process_times.start(me);
            self.process_ptimes.start(me);
            let mut m = Some(m);
            let NodeProcessingResult {
                misses, captured, ..
            } = n.process(
                &mut m,
                None,
                None,
                true,
                ProcessEnv {
                    state: &mut self.state,
                    reader_write_handles: &mut self.reader_write_handles,
                    nodes: &self.nodes,
                    executor,
                    shard: self.shard,
                    replica: self.replica,
                    auxiliary_node_states: &mut self.auxiliary_node_states,
                },
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
                //    requested a replay of key k. that replay produced *no* rows from the side that
                //    was replayed. this in turn means that no lookup was performed on the other
                //    side of the join, and so k wasn't replayed to that other side (which then
                //    still has a hole!). in that case, any subsequent write with k in the join
                //    column from the replay side will miss in the other side.
                //  - some downstream view is partial over a column that is *not* the join key. in
                //    the past, it replayed some key k, which means that we aren't allowed to drop
                //    any write with k in that column. now, a write comes along with k in that
                //    replay column, but with some hitherto unseen key z in the join column. if the
                //    replay of k never caused a lookup of z in the other side of the join, then the
                //    other side will have a hole. thus, we end up in the situation where we need to
                //    forward a write through the join, but we miss.
                //
                // unfortunately, we can't easily distinguish between the case where we have to
                // evict and the case where we don't (at least not currently), so we *always* need
                // to evict when this happens. this shouldn't normally be *too* bad, because writes
                // are likely to be dropped before they even reach the join in most benign cases
                // (e.g., in an ingress). this can be remedied somewhat in the future by ensuring
                // that the first of the two causes outlined above can't happen (by always doing a
                // lookup on the replay key, even if there are now rows). then we know that the
                // *only* case where we have to evict is when the replay key != the join key.
                let from = self
                    .nodes
                    .get(src)
                    .ok_or_else(|| ReadySetError::NoSuchNode(src.id()))?
                    .borrow()
                    .global_addr();
                // first, what partial replay paths go through this node, from the parent we
                // originally received the update from?
                let deps: Vec<(Tag, Vec<usize>)> = self.replay_paths.paths_through(&n, from);
                let mut evictions: HashMap<Tag, HashSet<KeyComparison>> = HashMap::new();
                for miss in misses {
                    for (tag, cols) in &deps {
                        evictions.entry(*tag).or_default().insert(
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
                    Eviction::Keys {
                        keys: keys.into_iter().collect(),
                        link: Link::new(src, me),
                        tag,
                    },
                    executor,
                    None,
                    0,
                    0,
                )?;
            }
        }

        // We checked it's Some above, it's only an Option so we can take()
        #[allow(clippy::unwrap_used)]
        match m.as_ref().unwrap() {
            Packet::Update(x) if x.is_empty() => {
                // no need to deal with our children if we're not sending them anything
                return Ok(());
            }
            Packet::Update(_) => {}
            Packet::ReplayPiece { .. } => {
                internal!("Replay should never go through dispatch.");
            }
            m => {
                internal!("dispatch process got {:?}", m);
            }
        }

        // NOTE: we can't directly iterate over .children due to self.dispatch in the loop
        let nchildren = self.nodes[me].borrow().children().len();
        for i in 0..nchildren {
            // We checked it's Some above, it's only an Option so we can take()
            #[allow(clippy::unwrap_used)]
            // avoid cloning if we can
            let mut m = if i == nchildren - 1 {
                m.take().unwrap()
            } else {
                m.clone().unwrap()
            };

            let childi = self.nodes[me].borrow().children()[i];

            // we got the node from the children of the other node
            let child_is_merger = self.nodes[childi].borrow().is_shard_merger();

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
        message: Timestamp,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let me = message.dst();

        let message = {
            let mut n = self
                .nodes
                .get(me)
                .ok_or_else(|| ReadySetError::NoSuchNode(me.id()))?
                .borrow_mut();
            n.process_timestamp(
                message,
                self.shard,
                self.replica,
                &mut self.reader_write_handles,
                executor,
            )?
        };

        let message = if let Some(m) = message {
            m
        } else {
            // no message to send, so no need to run through children
            return Ok(());
        };

        let nchildren = self.nodes[me].borrow().children().len();
        for i in 0..nchildren {
            let mut p = message.clone();

            let childi = self.nodes[me].borrow().children()[i];

            // we know the child exists since we got it from the node
            let child_is_merger = self.nodes[childi].borrow().is_shard_merger();

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

    #[inline(always)]
    fn handle_add_node(
        &mut self,
        node: Node,
        parents: Vec<LocalNodeIndex>,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let addr = node.local_addr();
        let aux_state = node.initial_auxiliary_state();
        self.not_ready.insert(addr);

        for p in parents {
            self.nodes
                .get_mut(p)
                .unwrap()
                .borrow_mut()
                .add_child(node.local_addr());
        }
        self.nodes.insert(addr, cell::RefCell::new(node));
        if let Some(aux_state) = aux_state {
            self.auxiliary_node_states.insert(addr, aux_state);
        }
        trace!(local = addr.id(), "new node incorporated");
        Ok(None)
    }

    #[inline(always)]
    fn handle_remove_nodes(
        &mut self,
        nodes: Vec<LocalNodeIndex>,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        for &node in &nodes {
            self.nodes
                .get(node)
                .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
                .borrow_mut()
                .remove();
            if let Some(state) = self.state.remove(node) {
                state.tear_down()?;
            };
            self.auxiliary_node_states.remove(node);
            self.reader_write_handles.remove(node);
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

    #[inline(always)]
    fn handle_add_base_column(
        &mut self,
        node: LocalNodeIndex,
        column: Column,
        default: DfValue,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let mut n = self
            .nodes
            .get(node)
            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
            .borrow_mut();
        n.add_column(column);
        if let Some(b) = n.get_base_mut() {
            b.add_column(default)?;
        } else if n.is_ingress() {
            self.ingress_inject
                .entry(node)
                .or_insert_with(|| (n.columns().len(), Vec::new()))
                .1
                .push(default);
        } else {
            internal!("node unrelated to base got AddBaseColumn");
        }
        Ok(None)
    }

    #[inline(always)]
    fn handle_drop_base_column(
        &mut self,
        node: LocalNodeIndex,
        column: usize,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let mut n = self
            .nodes
            .get(node)
            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
            .borrow_mut();
        n.get_base_mut()
            .ok_or_else(|| internal_err!("told to drop base column from non-base node"))?
            .drop_column(column)?;
        Ok(None)
    }

    #[inline(always)]
    fn handle_set_column_type(
        &mut self,
        node: LocalNodeIndex,
        column: usize,
        new_type: DfType,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        trace!(%node, %column, %new_type, "Setting column type");
        self.nodes
            .get(node)
            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
            .borrow_mut()
            .set_column_type(column, new_type)?;
        Ok(None)
    }

    #[inline(always)]
    fn handle_add_egress_tx(
        &mut self,
        egress_node: LocalNodeIndex,
        ingress_node_global: NodeIndex,
        ingress_node_local: LocalNodeIndex,
        target_domain: DomainIndex,
        target_shard: usize,
        replication: SenderReplication,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let mut n = self
            .nodes
            .get(egress_node)
            .ok_or_else(|| ReadySetError::NoSuchNode(egress_node.id()))?
            .borrow_mut();

        let e = n.as_mut_egress().ok_or(ReadySetError::InvalidNodeType {
            node_index: egress_node.id(),
            expected_type: NodeType::Egress,
        })?;

        e.add_tx(EgressTx::new(
            ingress_node_global,
            ingress_node_local,
            target_domain,
            target_shard,
            replication,
        ));

        Ok(None)
    }

    #[inline(always)]
    fn handle_add_egress_tag(
        &mut self,
        egress_node: LocalNodeIndex,
        tag: Tag,
        ingress_node: NodeIndex,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let mut n = self
            .nodes
            .get(egress_node)
            .ok_or_else(|| ReadySetError::NoSuchNode(egress_node.id()))?
            .borrow_mut();

        let e = n.as_mut_egress().ok_or(ReadySetError::InvalidNodeType {
            node_index: egress_node.id(),
            expected_type: NodeType::Egress,
        })?;

        e.add_tag(tag, ingress_node);
        Ok(None)
    }

    #[inline(always)]
    fn handle_add_egress_filter(
        &mut self,
        egress_node: LocalNodeIndex,
        target_node: NodeIndex,
    ) -> ReadySetResult<Option<Vec<u8>>> {
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

    #[inline(always)]
    fn handle_add_sharder_tx(
        &mut self,
        sharder_node: LocalNodeIndex,
        ingress_node: LocalNodeIndex,
        target_domain: DomainIndex,
        num_shards: usize,
        replication: SenderReplication,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        self.nodes
            .get(sharder_node)
            .ok_or_else(|| ReadySetError::NoSuchNode(sharder_node.id()))?
            .borrow_mut()
            .as_mut_sharder()
            .ok_or(ReadySetError::InvalidNodeType {
                node_index: sharder_node.id(),
                expected_type: NodeType::Sharder,
            })?
            .add_sharded_child(target_domain, ingress_node, num_shards, replication);
        Ok(None)
    }

    fn send_shards(miss: &KeyComparison, num_shards: usize) -> Vec<usize> {
        if num_shards == 1 {
            vec![0]
        } else {
            miss.shard_keys(num_shards)
        }
    }

    fn send_up<P>(
        txs: &[UnboundedSender<Result<Packet, Box<bincode::ErrorKind>>>],
        keys: &mut dyn Iterator<Item = KeyComparison>,
        pkt: P,
    ) -> bool
    where
        P: Fn(Vec<KeyComparison>) -> Packet,
    {
        let mut all = Vec::new();
        let mut sharded = vec![Vec::new(); txs.len()];
        for (i, m) in keys.enumerate() {
            assert!(txs.len() == 1 || m.len() == 1);
            for s in Self::send_shards(&m, txs.len()) {
                sharded[s].push(i);
            }
            all.push(m);
        }

        sharded.into_iter().enumerate().all(|(shard, keys)| {
            if keys.is_empty() {
                true
            } else {
                let pkt = pkt(keys.into_iter().map(|i| all[i].clone()).collect());
                txs[shard].send(Ok(pkt)).is_ok()
            }
        })
    }

    fn prepare_partial(
        &mut self,
        node: LocalNodeIndex,
        strict_indices: Vec<(Index, Vec<Tag>)>,
        weak_indices: HashSet<Index>,
    ) {
        if !self.state.contains_key(node) {
            self.state
                .insert(node, MaterializedNodeState::Memory(MemoryState::default()));
        }
        let state = self.state.get_mut(node).unwrap();
        let strict = strict_indices
            .into_iter()
            .map(|(i, t)| (i, Some(t)))
            .collect();
        let weak = weak_indices.into_iter().collect();
        state.add_index_multi(strict, weak);
    }

    fn prepare_full(
        &mut self,
        node: LocalNodeIndex,
        strict_indices: HashSet<Index>,
        weak_indices: HashSet<Index>,
    ) -> ReadySetResult<()> {
        if !self.state.contains_key(node) {
            if self.materialization_persistence {
                let name = format!("full_mat-{}-{}", self.index(), node.id());
                // we'll add indices a little further down, so empty keys here is fine.
                let keys: Vec<Box<[usize]>> = vec![];
                self.state.insert(
                    node,
                    MaterializedNodeState::Persistent(PersistentState::new(
                        name,
                        keys,
                        &self.persistence_parameters,
                        PersistenceType::FullMaterialization,
                    )?),
                );
            } else {
                self.state
                    .insert(node, MaterializedNodeState::Memory(MemoryState::default()));
            }
        }
        let state = self.state.get_mut(node).unwrap();
        let strict = strict_indices.into_iter().map(|x| (x, None)).collect();
        let weak = weak_indices.into_iter().collect();
        state.add_index_multi(strict, weak);
        Ok(())
    }

    fn upquery(
        txs: &[UnboundedSender<Result<Packet, Box<bincode::ErrorKind>>>],
        node: LocalNodeIndex,
        cache_name: &Relation,
        cols: &[usize],
        misses: &mut dyn Iterator<Item = KeyComparison>,
    ) -> bool {
        Self::send_up(txs, misses, |keys| {
            Packet::RequestReaderReplay(RequestReaderReplay {
                node,
                cols: cols.to_vec(),
                keys,
                cache_name: cache_name.clone(),
            })
        })
    }

    fn prepare_partial_reader(
        &mut self,
        node: LocalNodeIndex,
        node_index: petgraph::graph::NodeIndex,
        num_columns: usize,
        num_shards: usize,
        index: Index,
        trigger_domain: DomainIndex,
    ) -> ReadySetResult<()> {
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

        let cols = index.columns.clone();
        let txs = (0..num_shards)
            .map(|shard| {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let sender = self
                    .channel_coordinator
                    .builder_for(&ReplicaAddress {
                        domain_index: trigger_domain,
                        shard,
                        replica: self.replica(),
                    })?
                    .build_async()?;

                tokio::spawn(UnboundedReceiverStream::new(rx).forward(sender).map(|r| {
                    if let Err(e) = r {
                        // domain went away?
                        error!(error = %e, "replay source went away");
                    }
                }));
                Ok(tx)
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        let mut n = self.nodes[node].borrow_mut();
        let name = n.name().clone();
        #[allow(clippy::unwrap_used)] // checked it was a reader above
        let r = n.as_mut_reader().unwrap();

        let name2 = name.clone();
        let (read, write) = backlog::new_partial(
            num_columns,
            index,
            move |misses| Self::upquery(&txs, node, &name2, &cols, misses),
            self.eviction_kind,
            r.reader_processing().clone(),
            node_index,
        );

        let shard = *self.shard.as_ref().unwrap_or(&0);
        // TODO(ENG-838): Don't recreate every single node on leader failure.
        // This requires us to overwrite the existing reader.
        let mut readers = self.readers.lock().unwrap();
        let addr = ReaderAddress {
            node: node_index,
            name: name.clone(),
            shard,
        };
        if readers.insert(addr, read).is_some() {
            warn!(
                ?node_index,
                name = %name.display_unquoted(),
                %shard,
                "Overwrote existing reader at worker"
            );
        }

        self.reader_write_handles.insert(node, write);
        Ok(())
    }

    fn prepare_full_reader(
        &mut self,
        node: LocalNodeIndex,
        node_index: petgraph::graph::NodeIndex,
        num_columns: usize,
        index: Index,
    ) -> ReadySetResult<()> {
        let mut n = self
            .nodes
            .get(node)
            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
            .borrow_mut();
        let name = n.name().clone();

        let r = n
            .as_mut_reader()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: node.id(),
                expected_type: NodeType::Reader,
            })?;

        let (read, write) = backlog::new(
            num_columns,
            index,
            r.reader_processing().clone(),
            node_index,
        );

        let shard = *self.shard.as_ref().unwrap_or(&0);
        // TODO(ENG-838): Don't recreate every single node on leader failure.
        // This requires us to overwrite the existing reader.
        let mut readers = self.readers.lock().unwrap();
        let addr = ReaderAddress {
            node: node_index,
            name,
            shard,
        };
        if readers.insert(addr, read).is_some() {
            warn!(?node_index, ?shard, "Overwrote existing reader at worker");
        }

        // make sure Reader is actually prepared to receive state
        self.reader_write_handles.insert(node, write);
        Ok(())
    }

    #[inline(always)]
    fn handle_prepare_state(
        &mut self,
        node: LocalNodeIndex,
        state: PrepareStateKind,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        match state {
            PrepareStateKind::Partial {
                strict_indices,
                weak_indices,
            } => self.prepare_partial(node, strict_indices, weak_indices),
            PrepareStateKind::Full {
                strict_indices,
                weak_indices,
            } => self.prepare_full(node, strict_indices, weak_indices)?,
            PrepareStateKind::PartialReader {
                node_index,
                num_columns,
                num_shards,
                index,
                trigger_domain,
            } => self.prepare_partial_reader(
                node,
                node_index,
                num_columns,
                num_shards,
                index,
                trigger_domain,
            )?,
            PrepareStateKind::FullReader {
                node_index,
                num_columns,
                index,
            } => self.prepare_full_reader(node, node_index, num_columns, index)?,
        }
        Ok(None)
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    fn handle_setup_replay_path(
        &mut self,
        tag: Tag,
        source: Option<LocalNodeIndex>,
        source_index: Option<Index>,
        path: Vec1<ReplayPathSegment>,
        partial_unicast_sharder: Option<NodeIndex>,
        notify_done: bool,
        trigger: crate::payload::TriggerEndpoint,
        replica_fanout: bool,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        if notify_done {
            debug!(
                ?tag,
                ?source,
                ?source_index,
                path = %PrettyReplayPath(&path),
                "told about terminating replay path",
            );
            // NOTE: we set self.replaying_to when we first receive a replay with
            // this tag
        } else {
            debug!(
                ?tag,
                ?source,
                ?source_index,
                path = %PrettyReplayPath(&path),
                "told about replay path"
            );
        }

        let trigger = match trigger {
            payload::TriggerEndpoint::None => TriggerEndpoint::None,
            payload::TriggerEndpoint::Start(index) => TriggerEndpoint::Start(index),
            payload::TriggerEndpoint::Local(index) => TriggerEndpoint::Local(index),
            payload::TriggerEndpoint::End(selection, domain_index) => {
                // See the documentation for DomainRequest::SetupReplayPath::replica_fanout
                let replica = if replica_fanout { 0 } else { self.replica() };
                let addr = |shard| ReplicaAddress {
                    domain_index,
                    shard,
                    replica,
                };

                let options = match selection {
                    SourceSelection::AllShards(nshards)
                    | SourceSelection::KeyShard { nshards, .. } => {
                        // we may need to send to any of these shards
                        (0..nshards).map(addr).collect::<Vec<_>>()
                    }
                    SourceSelection::SameShard => {
                        vec![addr(self.shard.ok_or_else(|| {
                            internal_err!(
                                "Cannot use SourceSelection::SameShard for a replay path\
                                             through an unsharded domain",
                            )
                        })?)]
                    }
                };

                TriggerEndpoint::End {
                    source: selection,
                    options,
                }
            }
        };

        self.replay_paths.insert(ReplayPathSpec {
            tag,
            source,
            source_index,
            path,
            partial_unicast_sharder,
            notify_done,
            trigger,
        })?;
        Ok(None)
    }

    #[inline(always)]
    fn handle_start_replay(
        &mut self,
        executor: &mut dyn Executor,
        tag: Tag,
        from: LocalNodeIndex,
        replicas: Option<Vec<usize>>,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        // if the node's state was not initialized yet, then just return and do nothing.
        // we should only hit this for base nodes which are in the process of having their
        // persistent state initialized.
        // it is ok to do nothing as this will cause the reader to keep on retrying later
        // on, so that will act as a polling mechanism.
        // this should also rarely happen, since the persistent state is initialized almost
        // instantly (when opening a table for the first time, likely during runtime), or
        // might take a huge amount of time but during a period of recovery (where we are
        // still in a deployment stage).
        if self.not_ready.contains(&from) {
            debug!(%from, "attempted to start a replay, but node is not ready yet");
            return Ok(None);
        }
        invariant_eq!(
            self.replay_paths
                .get(tag)
                .ok_or_else(|| ReadySetError::NoSuchReplayPath(tag.into()))?
                .source,
            Some(from)
        );

        let start = time::Instant::now();
        self.total_replay_time.start();
        debug!(%from, "starting replay");

        if self
            .nodes
            .get(from)
            .filter(|n| n.borrow().is_dropped())
            .is_some()
        {
            warn!(node = ?from, domain = ?self.index, "replay path started with removed node; ignoring...");
            return Ok(None);
        }

        // we know that the node is materialized, as the migration coordinator
        // picks path that originate with materialized nodes. if this weren't the
        // case, we wouldn't be able to do the replay, and the entire migration
        // would fail.
        //
        // In the case of memory state, we clone the entire state so that we can continue to
        // occasionally process incoming updates to the domain without disturbing the state
        // that is being replayed. For persistent state, we can stream records from a
        // consistent snapshot and avoid the allocations
        let state = self
            .state
            .get(from)
            .expect("migration replay path started with non-materialized node");
        let is_empty = state.is_empty();
        let mut all_records = state.all_records();

        debug!(
            μs = %start.elapsed().as_micros(),
            "current state cloned for replay"
        );

        let link = Link::new(from, self.replay_paths[tag].path[0].node);

        // we're been given an entire state snapshot, but we need to digest it
        // piece by piece spawn off a thread to do that chunking. however, before
        // we spin off that thread, we need to send a single Replay message to tell
        // the target domain to start buffering everything that follows. we can't
        // do that inside the thread, because by the time that thread is scheduled,
        // we may already have processed some other messages that are not yet a
        // part of state.
        let data = Default::default();
        let cache_name = MIGRATION_CACHE_NAME_STUB.into();
        let context = ReplayPieceContext::Full {
            // NOTE: If we're replaying from persistent state this might be wrong, since
            // it's backed by an *estimate* of the number of keys in the state
            last: is_empty,
            replicas: replicas.clone(),
        };

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
        let fix = move |mut r: Vec<DfValue>| -> Vec<DfValue> {
            if let Some((start, ref added)) = added_cols {
                let rlen = r.len();
                r.extend(added.iter().skip(rlen - start).cloned());
            } else if let Some(ref defaults) = default {
                let rlen = r.len();
                r.extend(defaults.iter().skip(rlen).cloned());
            }
            r
        };

        let replay_tx_desc = self.channel_coordinator.builder_for(&self.address())?;

        let address = self.address();
        std::thread::Builder::new()
            .name(format!("replay{}.{}", self.index(), link.src))
            .spawn_wrapper(move || {
                let span = info_span!("full_replay", %address, src = %link.src);
                let _guard = span.enter();

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

                let mut guard = all_records.read();
                let iter = guard.iter().chunks(BATCH_SIZE);
                let mut iter = iter
                    .into_iter()
                    .map(|chunk| Records::from_iter(chunk.map(&fix)))
                    .enumerate()
                    .peekable();

                // process all records in state to completion within domain and then
                // forward on tx (if there is one)
                let mut sent_last = is_empty;
                while let Some((i, chunk)) = iter.next() {
                    let len = chunk.len();
                    let last = iter.peek().is_none();
                    sent_last = last;
                    let p = Packet::ReplayPiece(ReplayPiece {
                        tag,
                        link, // to is overwritten by receiver
                        context: ReplayPieceContext::Full {
                            last,
                            replicas: replicas.clone(),
                        },
                        data: chunk,
                        cache_name: MIGRATION_CACHE_NAME_STUB.into(),
                    });

                    trace!(num = i, len, "sending batch");
                    if let Err(error) = chunked_replay_tx.send(p) {
                        warn!(%error, "replayer noticed domain shutdown");
                        break;
                    }
                }

                // Since we're using `is_empty` above to send a `last: true` packet
                // before launching the thread, and that's based on a potentially
                // inaccurate estimate, it might be the case that we started this thread
                // with no records - if so, we need to send a `last: true` packet to
                // tell the target domain we're done
                if !sent_last {
                    trace!("Sending empty last batch");
                    if let Err(error) = chunked_replay_tx.send(Packet::ReplayPiece(ReplayPiece {
                        tag,
                        link,
                        context: ReplayPieceContext::Full {
                            last: true,
                            replicas: replicas.clone(),
                        },
                        data: Default::default(),
                        cache_name: MIGRATION_CACHE_NAME_STUB.into(),
                    })) {
                        warn!(%error, "replayer noticed domain shutdown");
                    }
                }

                debug!(
                   node = %link.dst,
                   μs = %start.elapsed().as_micros(),
                   "state chunker finished"
                );

                let time = start.elapsed();
                counter!(recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_TIME)
                    .increment(time.as_micros() as u64);
                histogram!(recorded::DOMAIN_CHUNKED_REPLAY_TIME).record(time.as_micros() as f64);
            })?;
        self.handle_replay(link, tag, data, context, cache_name, executor)?;

        self.total_replay_time.stop();
        self.metrics.rec_chunked_replay_start_time(start.elapsed());
        Ok(None)
    }

    #[inline(always)]
    fn handle_ready(
        &mut self,
        node_idx: LocalNodeIndex,
        purge: bool,
        index: HashSet<Index>,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        invariant_eq!(self.mode, DomainMode::Forwarding);

        let node_ref = self
            .nodes
            .get(node_idx)
            .ok_or_else(|| ReadySetError::NoSuchNode(node_idx.id()))?;

        node_ref.borrow_mut().purge = purge;

        let is_ready = if !index.is_empty() {
            match (
                node_ref.borrow().get_base(),
                &self.persistence_parameters.mode,
            ) {
                (Some(base), &DurabilityMode::DeleteOnExit)
                | (Some(base), &DurabilityMode::Permanent) => {
                    let node = node_ref.borrow();
                    let node_name = node.name();
                    let base_name = format!(
                        "{}-{}{}-{}",
                        &self
                            .persistence_parameters
                            .db_filename_prefix
                            .replace('-', "_"),
                        match &node_name.schema {
                            Some(schema) => format!("{schema}-"),
                            _ => "".into(),
                        },
                        node_name.name,
                        self.shard.unwrap_or(0),
                    );

                    let persistence_params = self.persistence_parameters.clone();
                    let init_state_tx = self.init_state_tx.clone();
                    let unique_keys = base.all_unique_keys();

                    // run the base table initialization in a separate task, as we know
                    // this might take a lot of time for large
                    // tables. upon completion, we'll notify the
                    // domain and set the materialized state for
                    // this node.
                    // TODO(fran): Avoid panicking the whole process and instead just fail
                    //  the domain thread (ENG-2752).
                    tokio::spawn(abort_on_panic(
                        initialize_state(
                            node_idx,
                            index,
                            base_name.clone(),
                            unique_keys,
                            persistence_params,
                            init_state_tx,
                        )
                        .instrument(tracing::trace_span!(
                            "initialize_state",
                            name = %base_name,
                            node = %node_idx,
                        ))
                        .map_err(move |e| {
                            error!(error = %e, "Domain failed while initializing base table");
                            process::abort();
                        }),
                    ));
                    false
                }
                _ => {
                    let mut s = MaterializedNodeState::Memory(MemoryState::default());
                    for idx in index {
                        s.add_index(idx, None);
                    }
                    assert!(self.state.insert(node_idx, s).is_none());
                    true
                }
            }
        } else {
            // NOTE: just because index_on is None does *not* mean we're not
            // materialized
            true
        };

        if is_ready && self.not_ready.remove(&node_idx) {
            trace!(local = node_idx.id(), "readying empty node");
        }

        // publish replayed reader nodes to expose new state
        if let Some(state) = self.reader_write_handles.get_mut(node_idx) {
            trace!(local = %node_idx, "publishing state");
            state.publish();
            trace!(local = %node_idx, "state published");
        }

        Ok(Some(bincode::serialize(&is_ready)?))
    }

    #[inline(always)]
    fn handle_get_statistics(&mut self) -> ReadySetResult<Option<Vec<u8>>> {
        let domain_stats = readyset_client::debug::stats::DomainStats {
            total_time: self.total_time.num_nanoseconds(),
            total_ptime: self.total_ptime.num_nanoseconds(),
            total_replay_time: self.total_replay_time.num_nanoseconds(),
            total_forward_time: self.total_forward_time.num_nanoseconds(),
            wait_time: self.wait_time.num_nanoseconds(),
        };

        let node_stats: HashMap<
            petgraph::graph::NodeIndex,
            readyset_client::debug::stats::NodeStats,
        > = self
            .nodes
            .values()
            .filter_map(|nd| {
                let n = &*nd.borrow();
                let local_index = n.local_addr();
                let node_index: NodeIndex = n.global_addr();

                let time = self.process_times.num_nanoseconds(local_index);
                let ptime = self.process_ptimes.num_nanoseconds(local_index);
                let mem_size = self
                    .reader_write_handles
                    .get(local_index)
                    .map(|wh| wh.deep_size_of())
                    .unwrap_or_else(|| {
                        self.state
                            .get(local_index)
                            .map(|s| s.deep_size_of())
                            .unwrap_or(0)
                    });

                let mat_state = self
                    .reader_write_handles
                    .get(local_index)
                    .map(|wh| {
                        if wh.is_partial() {
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
                        readyset_client::debug::stats::NodeStats {
                            desc: format!("{:?}", n),
                            process_time: time,
                            process_ptime: ptime,
                            mem_size: mem_size as _,
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

    #[inline(always)]
    fn handle_request_node_sizes(&mut self) -> ReadySetResult<Option<Vec<u8>>> {
        let mut res = Vec::new();
        for (local_index, node_ref) in self.nodes.iter() {
            let node = node_ref.borrow();
            if node.is_reader() {
                if let Some(wh) = self.reader_write_handles.get(local_index) {
                    res.push((
                        node.global_addr(),
                        KeyCount::ExactKeyCount(wh.len()),
                        wh.deep_size_of(),
                    ));
                }
            } else if let Some(state) = self.state.get(local_index) {
                // non-reader node with state
                res.push((node.global_addr(), state.key_count(), state.deep_size_of()))
            }
        }
        Ok(Some(bincode::serialize(&res)?))
    }

    #[inline(always)]
    fn handle_query_replay_done(
        &mut self,
        node: LocalNodeIndex,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        let ret = self
            .state
            .get(node)
            .map(|s| s.replay_done())
            .or_else(|| {
                self.reader_write_handles
                    .get(node)
                    .map(|rwh| rwh.replay_done())
            })
            .unwrap_or_else(|| {
                error!(%node, "Received QueryReplayDone for non-materialized node");
                true
            });
        Ok(Some(bincode::serialize(&ret)?))
    }

    #[inline(always)]
    fn handle_generated_columns(
        &mut self,
        node: LocalNodeIndex,
        index: Index,
        tag: Tag,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        // Record that these columns are generated...
        self.replay_paths
            .insert_generated_columns(node, index.columns.clone(), tag);
        // ...and also make sure we use that tag to index those columns in this node, so we
        // know what hole to fill when we've satisfied replays to those columns
        self.state
            .entry(node)
            .or_insert_with(|| MaterializedNodeState::Memory(MemoryState::default()))
            .add_index(index, Some(vec![tag]));
        Ok(None)
    }

    #[inline(always)]
    fn handle_all_tables_compacted(&mut self) -> ReadySetResult<Option<Vec<u8>>> {
        let finished = self
            .state
            .values_mut()
            .filter_map(|state| state.as_persistent_mut())
            .all(|state| state.compaction_finished());
        Ok(Some(bincode::serialize(&finished)?))
    }

    #[inline(always)]
    fn handle_external_eviction(
        &mut self,
        executor: &mut dyn Executor,
        req: Eviction,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        // Handle an external request for an eviction. Returns the evicted key unless no
        // eviction occurred.
        let key = self.handle_eviction(req, executor, None, 0, 0)?;
        Ok(Some(bincode::serialize(&key)?))
    }

    #[inline(always)]
    fn handle_shutdown(&mut self) -> ReadySetResult<Option<Vec<u8>>> {
        self.state.values_mut().for_each(|s| {
            let err = s.shut_down();
            if let Err(e) = err {
                warn!(error = %e, "error on shutting down domains");
            }
        });
        Ok(None)
    }

    pub fn domain_request(
        &mut self,
        req: DomainRequest,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        trace!(?req, "processing domain request");
        let ret = match req {
            DomainRequest::AddNode { node, parents } => self.handle_add_node(node, parents),
            DomainRequest::RemoveNodes { nodes } => self.handle_remove_nodes(nodes),
            DomainRequest::AddBaseColumn {
                node,
                column,
                default,
            } => self.handle_add_base_column(node, column, default),
            DomainRequest::DropBaseColumn { node, column } => {
                self.handle_drop_base_column(node, column)
            }
            DomainRequest::SetColumnType {
                node,
                column,
                new_type,
            } => self.handle_set_column_type(node, column, new_type),
            DomainRequest::AddEgressTx {
                egress_node,
                ingress_node: (ingress_node_global, ingress_node_local),
                target_domain,
                target_shard,
                replication,
            } => self.handle_add_egress_tx(
                egress_node,
                ingress_node_global,
                ingress_node_local,
                target_domain,
                target_shard,
                replication,
            ),
            DomainRequest::AddEgressTag {
                egress_node,
                tag,
                ingress_node,
            } => self.handle_add_egress_tag(egress_node, tag, ingress_node),
            DomainRequest::AddEgressFilter {
                egress_node,
                target_node,
            } => self.handle_add_egress_filter(egress_node, target_node),
            DomainRequest::AddSharderTx {
                sharder_node,
                ingress_node,
                target_domain,
                num_shards,
                replication,
            } => self.handle_add_sharder_tx(
                sharder_node,
                ingress_node,
                target_domain,
                num_shards,
                replication,
            ),
            DomainRequest::PrepareState { node, state } => self.handle_prepare_state(node, state),
            DomainRequest::SetupReplayPath {
                tag,
                source,
                source_index,
                path,
                partial_unicast_sharder,
                notify_done,
                trigger,
                replica_fanout,
            } => self.handle_setup_replay_path(
                tag,
                source,
                source_index,
                path,
                partial_unicast_sharder,
                notify_done,
                trigger,
                replica_fanout,
            ),
            DomainRequest::StartReplay {
                tag,
                from,
                replicas,
                targeting_domain: _,
            } => self.handle_start_replay(executor, tag, from, replicas),
            DomainRequest::Ready {
                node: node_idx,
                purge,
                index,
            } => self.handle_ready(node_idx, purge, index),
            DomainRequest::GetStatistics => self.handle_get_statistics(),
            DomainRequest::RequestMinPersistedReplicationOffset => Ok(Some(bincode::serialize(
                &self.min_persisted_replication_offset()?,
            )?)),
            DomainRequest::RequestReplicationOffsets => {
                Ok(Some(bincode::serialize(&self.replication_offsets())?))
            }
            DomainRequest::RequestSnapshottingTables => {
                Ok(Some(bincode::serialize(&self.snapshotting_base_nodes())?))
            }
            DomainRequest::RequestNodeSizes => self.handle_request_node_sizes(),
            DomainRequest::Packet(pkt) => {
                self.handle_packet(pkt, executor)?;
                Ok(None)
            }
            DomainRequest::QueryReplayDone { node } => self.handle_query_replay_done(node),
            DomainRequest::GeneratedColumns { node, index, tag } => {
                self.handle_generated_columns(node, index, tag)
            }
            DomainRequest::IsReady { node } => {
                Ok(Some(bincode::serialize(&!self.not_ready.contains(&node))?))
            }
            DomainRequest::AllTablesCompacted => self.handle_all_tables_compacted(),
            DomainRequest::Evict { req } => self.handle_external_eviction(executor, req),
            DomainRequest::Shutdown => self.handle_shutdown(),
        };

        // What we just did might have done things like insert into `self.delayed_for_self`, so
        // run the event loop before returning to make sure that gets processed.
        //
        // Not doing this leads to complete insanity, as things just don't replay sometimes and
        // you aren't sure why.
        self.handle_packet(Packet::Spin, executor)?;
        ret
    }

    fn get_node(
        nodes: &DomainNodes,
        node: LocalNodeIndex,
    ) -> ReadySetResult<Option<RefMut<'_, Node>>> {
        let n = nodes
            .get(node)
            .ok_or_else(|| ReadySetError::NoSuchNode(node.id()))?
            .borrow_mut();
        if n.is_dropped() {
            return Ok(None);
        }
        Ok(Some(n))
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle(&mut self, m: Packet, executor: &mut dyn Executor) -> ReadySetResult<()> {
        // TODO(eta): better error handling here.
        // In particular one dodgy packet can kill the whole domain, which is probably not what we
        // want.

        self.metrics.inc_packets_sent(&m);

        match m {
            Packet::Update(_) | Packet::Input(_) => {
                // WO for https://github.com/rust-lang/rfcs/issues/1403
                let start = time::Instant::now();
                let d: PacketDiscriminants = (&m).into();
                self.total_forward_time.start();
                self.dispatch(m, executor)?;
                self.total_forward_time.stop();

                if matches!(d, PacketDiscriminants::Update) {
                    self.metrics.rec_forward_time_message(start.elapsed());
                } else {
                    self.metrics.rec_forward_time_input(start.elapsed());
                }
            }
            Packet::ReplayPiece(x) => {
                let start = time::Instant::now();
                let name = x.cache_name.clone();
                self.total_replay_time.start();
                self.handle_replay(x.link, x.tag, x.data, x.context, x.cache_name, executor)?;
                self.total_replay_time.stop();
                self.metrics.rec_replay_time(&name, start.elapsed());
            }
            Packet::Evict(e) => {
                debug!(
                    "{} evicting in barrier {:x}, credits: {:x}",
                    self.address(),
                    e.barrier,
                    e.credits
                );
                self.handle_eviction(e.req, executor, e.done, e.barrier, e.credits)?;
            }
            Packet::Timestamp(m) => {
                // TODO(justinmiron): Handle timestamp packets at data flow nodes. The
                // ack should be moved to the base table node's handling of the packet.
                // As the packet is not propagated or mutated before reaching the
                // domain, we still have a source channel identifier that we can use
                // to ack the packet.
                self.handle_timestamp(m, executor)?;
            }
            Packet::RequestReaderReplay(RequestReaderReplay {
                mut keys,
                cols,
                node,
                cache_name,
            }) => {
                let start = time::Instant::now();
                self.total_replay_time.start();
                set_failpoint!(failpoints::UPQUERY_START);

                let Some(mut n) = Self::get_node(&self.nodes, node)? else {
                    return Ok(());
                };

                let r = n.as_mut_reader().ok_or(ReadySetError::InvalidNodeType {
                    node_index: node.id(),
                    expected_type: NodeType::Reader,
                })?;

                // the reader could have raced with us filling in the key after some
                // *other* reader requested it, so let's double check that it indeed still
                // misses!
                let w = self.reader_write_handles.get_mut(node).ok_or_else(|| {
                    internal_err!("reader replay requested for non-materialized reader")
                })?;
                // ensure that all writes have been applied
                w.publish();

                // don't request keys that have been filled since the request was sent
                let mut keys = keys
                    .drain(..)
                    .filter_map(|k| match k {
                        key @ KeyComparison::Equal(_) if w.contains(&key) == Ok(true) => None,
                        key @ KeyComparison::Equal(_) => Some(vec![key]),
                        key @ KeyComparison::Range(_) => w.interval_difference(key),
                    })
                    .flatten()
                    .collect();

                let reader_index_type = r.index_type().ok_or_else(|| {
                    internal_err!("reader replay requested for non-indexed reader")
                })?;
                drop(n);

                // ensure that we haven't already requested a replay of this key
                let already_requested = self
                    .reader_triggered
                    .entry(node)
                    .or_insert_with(|| RequestedKeys::new(reader_index_type));
                already_requested.extend(&mut keys);
                if !keys.is_empty() {
                    self.find_tags_and_replay(
                        executor,
                        keys,
                        &cols,
                        // Destination and target are the same since readers can't generate columns
                        Destination(node),
                        Target(node),
                        cache_name.clone(),
                    )?;
                }

                self.total_replay_time.stop();
                self.metrics
                    .rec_reader_replay_time(&cache_name, start.elapsed());
            }
            Packet::RequestPartialReplay(pkt) => {
                trace!(%pkt.tag, ?pkt.keys, "got replay request");
                let start = time::Instant::now();
                let cache_name = pkt.cache_name.clone();
                self.total_replay_time.start();
                self.seed_all(executor, pkt)?;
                self.total_replay_time.stop();
                self.metrics
                    .rec_seed_replay_time(&cache_name, start.elapsed());
            }
            Packet::Finish(pkt) => {
                let start = time::Instant::now();
                self.total_replay_time.start();
                self.finish_replay(pkt.tag, pkt.node, &pkt.cache_name, executor)?;
                self.total_replay_time.stop();
                self.metrics
                    .rec_finish_replay_time(&pkt.cache_name, start.elapsed());
            }
            Packet::Spin => {}
        }

        Ok(())
    }

    /// Timed purges happen when [`FrontierStrategy`] is not None, in which case all keys
    /// are purged from the node after a given amount of time
    fn handle_timed_purges(&mut self) -> ReadySetResult<()> {
        let mut publish = HashSet::new();
        while let Some(tp) = self.timed_purges.front() {
            let now = time::Instant::now();
            if tp.time <= now {
                #[allow(clippy::unwrap_used)]
                // we know it's Some because we check at the head of the while
                let tp = self.timed_purges.pop_front().unwrap();
                // nodes in tp.view must reference nodes in self
                let node = self.nodes[tp.view].borrow_mut();
                trace!(
                    node = node.global_addr().index(),
                    "eagerly purging state from reader"
                );
                if let Some(wh) = self.reader_write_handles.get_mut(tp.view) {
                    for key in tp.keys {
                        wh.mark_hole(&key)?;
                    }
                    publish.insert(tp.view);
                }
            } else {
                break;
            }
        }

        for node in publish {
            if let Some(wh) = self.reader_write_handles.get_mut(node) {
                wh.publish();
            }
        }

        Ok(())
    }

    fn seed_row(&self, source: LocalNodeIndex, row: Cow<[DfValue]>) -> ReadySetResult<Record> {
        if let Some(&(start, ref defaults)) = self.ingress_inject.get(source) {
            let mut v = Vec::with_capacity(start + defaults.len());
            v.extend(row.iter().cloned());
            v.extend(defaults.iter().cloned());
            return Ok(v.into());
        }

        let n = self
            .nodes
            .get(source)
            .ok_or_else(|| ReadySetError::NoSuchNode(source.id()))?
            .borrow();
        if let Some(b) = n.get_base() {
            let mut row = row.into_owned();
            b.fix(&mut row);
            return Ok(row.into());
        }

        Ok(row.into_owned().into())
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
                KeyComparison::Equal(equal) => Some(PointKey::from(equal.clone())),
                KeyComparison::Range(range) => {
                    // TODO: aggregate ranges to optimize range lookups too?
                    match state.lookup_range(cols, &RangeKey::from(range)) {
                        RangeLookupResult::Some(res) => range_records.push(res),
                        RangeLookupResult::Missing(_) => {
                            unreachable!("Can't miss in persistent state")
                        }
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
        state: &'a MaterializedNodeState,
        cols: &[usize],
        mut keys: HashSet<KeyComparison>,
    ) -> ReadySetResult<StateLookupResult<'a>> {
        let mut records = Vec::new();
        let mut replay_keys = HashSet::new();
        // Drain misses, and keep the hits
        keys.retain(|key| match key {
            KeyComparison::Equal(equal) => {
                match state.lookup(cols, &PointKey::from(equal.clone())) {
                    LookupResult::Some(record) => {
                        records.push(record);
                        true
                    }
                    LookupResult::Missing => {
                        replay_keys.insert((key.clone(), key.clone()));
                        false
                    }
                }
            }
            KeyComparison::Range(range) => {
                match state.lookup_range(cols, &RangeKey::from(range)) {
                    RangeLookupResult::Some(record) => {
                        records.push(record);
                        true
                    }
                    RangeLookupResult::Missing(ms) => {
                        // FIXME(eta): error handling impl here adds overhead
                        let ms = ms.into_iter().map(|m| {
                            // This is the only point where the replay_key and miss_key are
                            // different.
                            #[allow(clippy::unwrap_used)]
                            // keys can't be empty coming from misses
                            (key.clone(), KeyComparison::try_from(m).unwrap())
                        });
                        replay_keys.extend(ms);
                        false
                    }
                }
            }
        });

        Ok(StateLookupResult {
            records,
            found_keys: keys,
            replay_keys,
        })
    }

    #[allow(clippy::type_complexity)]
    fn do_lookup(
        &self,
        state: &MaterializedNodeState,
        source: LocalNodeIndex,
        cols: &[usize],
        keys: HashSet<KeyComparison>,
    ) -> ReadySetResult<(
        Vec<Record>,
        HashSet<KeyComparison>,
        HashSet<(KeyComparison, KeyComparison)>,
    )> {
        let (records, found_keys, replay_keys) = if let Some(state) = state.as_persistent() {
            let records = self.do_lookup_multi(state, cols, &keys);
            (records, keys, HashSet::new()) // can't miss
        } else {
            let StateLookupResult {
                records,
                found_keys,
                replay_keys,
            } = self.do_lookup_iter(state, cols, keys)?;
            (records, found_keys, replay_keys)
        };

        let records = records
            .into_iter()
            .flat_map(|rr| rr.into_iter().map(|r| self.seed_row(source, r)))
            .collect::<ReadySetResult<Vec<Record>>>()?;
        Ok((records, found_keys, replay_keys))
    }

    // returns (src, index, dst)
    fn lookup_replay(&self, tag: Tag) -> ReadySetResult<(LocalNodeIndex, Index, LocalNodeIndex)> {
        match &self.replay_paths[tag] {
            ReplayPath {
                source: Some(src),
                trigger: TriggerEndpoint::Start(index) | TriggerEndpoint::Local(index),
                path,
                ..
            } => Ok((*src, index.clone(), path[0].node)),
            _ => internal!(),
        }
    }

    fn seed_all(&mut self, ex: &mut dyn Executor, pkt: RequestPartialReplay) -> ReadySetResult<()> {
        let (src, index, dst) = self.lookup_replay(pkt.tag)?;
        let Some(node) = Self::get_node(&self.nodes, src)? else {
            return Ok(());
        };

        let Some(state) = self.state.get(src) else {
            internal!(
                "replay requested ({:?}) from non-materialized node",
                pkt.tag
            );
        };

        if node.is_base() {
            self.metrics
                .inc_base_table_lookups(&pkt.cache_name, node.name());
        }
        drop(node);

        let keys = pkt.keys.into_iter().collect();
        let (records, found_keys, replay_keys) =
            self.do_lookup(state, src, &index.columns, keys)?;

        if !replay_keys.is_empty() {
            trace!(%pkt.tag, ?replay_keys, "replay request miss");

            self.on_replay_misses(
                ex,
                src,
                &index.columns,
                replay_keys,
                pkt.unishard,
                pkt.requesting_shard,
                pkt.requesting_replica,
                pkt.tag,
                pkt.cache_name.clone(),
            )?;
        }

        if !found_keys.is_empty() {
            trace!(%pkt.tag, ?found_keys, ?records, "replay request hit");

            self.handle_replay(
                Link::new(src, dst),
                pkt.tag,
                records.into(),
                ReplayPieceContext::Partial {
                    for_keys: found_keys,
                    unishard: pkt.unishard, // if we are the only source, only one path
                    requesting_shard: pkt.requesting_shard,
                    requesting_replica: pkt.requesting_replica,
                },
                pkt.cache_name,
                ex,
            )?;
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_replay(
        &mut self,
        link: Link,
        tag: Tag,
        mut data: Records,
        mut context: ReplayPieceContext,
        cache_name: Relation,
        ex: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let path = self
            .replay_paths
            .get(tag)
            .ok_or_else(|| internal_err!("Replay path not found with tag {tag:?}"))?;

        if self.nodes[path.last_segment().node].borrow().is_dropped() {
            return Ok(());
        }

        let mut finished = None;
        let mut need_replay = Vec::new();
        let mut finished_partial = 0;

        // this loop is just here so we have a way of giving up the borrow of self.replay_paths
        #[allow(clippy::never_loop)]
        'outer: loop {
            let rp = &self.replay_paths[tag];
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
                    // messages preceding the first replay message is that those have already been
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
            let target = path
                .iter()
                .find(|s| s.is_target)
                .map(|s| s.node)
                .or(*source);
            let dst_is_reader = self.nodes[dst]
                .borrow()
                .as_reader()
                .map(|r| r.is_materialized())
                .unwrap_or(false);
            // Is the destination of this replay path within this domain just going to
            // forward packets on to another node (is it an egress or sharder)?
            let dst_is_sender = self
                .nodes
                .get(dst)
                .ok_or_else(|| ReadySetError::NoSuchNode(dst.id()))?
                .borrow()
                .is_sender();
            // Is the target of this replay path inside this domain?
            let target_segment = path.iter().find(|n| n.is_target);

            if let Some(target_segment) = target_segment {
                // If this replay path is bound for us, prune keys and data for keys we're
                // not waiting for
                if let ReplayPieceContext::Partial {
                    ref mut for_keys, ..
                } = context
                {
                    let had = for_keys.len();
                    let partial_index = target_segment.partial_index.as_ref().unwrap();
                    if let Some(w) = self.waiting.get(dst) {
                        // discard all the keys that we aren't waiting for
                        for_keys.retain(|k| {
                            w.redos.contains_key(&Hole {
                                node: target.expect("already checked target_in_self"),
                                column_indices: partial_index.columns.to_owned(),
                                key: k.clone(),
                            })
                        });
                    } else if !self.reader_triggered.contains_key(dst) {
                        internal!(
                            "Received replay targeted at node {dst} that is not waiting for any \
                             replays"
                        );
                    }

                    if let Some(prev) = self.reader_triggered.get(dst) {
                        // discard all the keys or subranges of keys that we aren't waiting
                        // for
                        if !prev.is_empty() {
                            prev.filter_keys(for_keys);
                        }
                    }

                    if for_keys.is_empty() {
                        debug!(
                            src = %link.src,
                            dst = %link.dst,
                            ?tag,
                            "Received packet with no keys that we were waiting for",
                        );
                        return Ok(());
                    } else if for_keys.len() != had {
                        // discard records in data associated with the keys we weren't
                        // waiting for
                        // note that we need to use the partial_keys column IDs from the
                        // *start* of the path here, as the records haven't been processed
                        // yet
                        // We already know it's a partial replay path, so it must have a
                        // partial key
                        #[allow(clippy::unwrap_used)]
                        let partial_keys = path.first().partial_index.as_ref().unwrap();
                        data.retain(|r| {
                            for_keys.iter().any(|k| {
                                k.contains(partial_keys.columns.iter().map(|c| {
                                    // record came from processing, which means it
                                    // must have the right number of columns
                                    &r[*c]
                                }))
                            })
                        });
                    }
                }
            }

            // forward the current message through all local nodes.
            let mut m = ReplayPiece {
                link,
                tag,
                data,
                context,
                cache_name: cache_name.clone(),
            };

            for (i, segment) in path.iter().enumerate() {
                if let Some(force_tag) = segment.force_tag_to {
                    trace!(
                        %force_tag,
                        original_tag = %m.tag,
                        node = %segment.node,
                        "Forcing tag",
                    );
                    m.tag = force_tag;
                }

                // we know replay paths only contain real nodes
                let mut n = self.nodes[segment.node].borrow_mut();

                // keep track of whether we're filling any partial holes
                let partial_key_cols = segment.partial_index.as_ref();
                // keep a copy of the partial keys from before we process
                // we need this because n.process may choose to reduce the set of keys
                // (e.g., because some of them missed), in which case we need to know what
                // keys to _undo_.
                let mut backfill_keys = if let ReplayPiece {
                    context:
                        ReplayPieceContext::Partial {
                            ref mut for_keys, ..
                        },
                    ..
                } = m
                {
                    debug_assert!(partial_key_cols.is_some());
                    Some(for_keys.clone())
                } else {
                    None
                };

                // Is this segment the target of the replay path?
                let is_target = backfill_keys.is_some() && segment.is_target;
                let cols = segment.partial_index.as_ref().map(|idx| &idx.columns);
                // If this replay path is targeting a set of generated columns, figure out
                // what the tags are for those generated columns so we can mark them as
                // filled later
                let tags_for_generated = cols.and_then(|cols| {
                    self.replay_paths
                        .tags_for_generated_columns(segment.node, cols)
                });

                // are we about to fill a hole?
                if let Some(backfill_keys) = &backfill_keys {
                    if is_target {
                        // mark the state for the key being replayed as *not* a hole
                        // otherwise we'll just end up with
                        // the same "need replay" response that
                        // triggered this replay initially.
                        if let Some(state) = self.state.get_mut(segment.node) {
                            for key in backfill_keys.iter() {
                                trace!(?key, ?tag, local = %segment.node, "Marking filled");
                                state.mark_filled(key.clone(), tag);
                            }
                        } else {
                            // we must be filling a hole in a Reader. we need to ensure
                            // that the hole for the key we're replaying ends up being
                            // filled, even if that hole is empty!
                            if let Some(wh) = self.reader_write_handles.get_mut(segment.node) {
                                for key in backfill_keys.iter() {
                                    trace!(?key, local = %segment.node, "Marking filled in reader");
                                    wh.mark_filled(key.clone())?;
                                }
                            }
                        }
                    } else if tags_for_generated.is_some() {
                        // If we're processing a replay that ends at a set of generated
                        // columns, and there's some downstream replay that's waiting on us,
                        // we need to mark that downstream replay's key as filled before
                        // processing the records, so that when we materialize the result
                        // we don't miss when processing the redo
                        //
                        // TODO(aspen): there's an opportunity for an optimization here -
                        // since we're ostensibly querying for considerably more data than
                        // we actually need (think eg paginate where we query for all the
                        // rows in a group in order to satisfy a lookup of an individual
                        // page) we could instead mark all keys taken from the column
                        // indices of the redo in *all* rows returned from the node as
                        // filled (since because of the semantics of
                        // ColumnSource::GeneratedFromColumns we know we've just loaded all
                        // the rows we'd need to mark those holes as filled!).
                        // Unfortunately, we don't actually know what those rows are going
                        // to be at this point (since we haven't processed through the node
                        // yet), not to mention that optimization doesn't make sense for
                        // range keys (since we can't just take the key out of the rows
                        // themselves). So for now we just mark the original key as filled,
                        // and any subsequent queries that remap to the same upstream key
                        // have to replay the same set of rows over again (sad!)
                        if let Some(state) = self.state.get_mut(segment.node) {
                            if let Some(waiting) = self.waiting.get(segment.node) {
                                for key in backfill_keys.clone() {
                                    let hole = Hole {
                                        node: target.unwrap(),
                                        column_indices: self.replay_paths[tag]
                                            .target_index
                                            .as_ref()
                                            .unwrap()
                                            .columns
                                            .clone(),
                                        key,
                                    };
                                    if let Some(redos) = waiting.redos.get(&hole) {
                                        for redo in redos {
                                            // Are we about to satisfy the last hole this
                                            // redo was waiting for?
                                            if waiting.holes.get(redo) == Some(&1) {
                                                trace!(
                                                    key = ?redo.replay_key,
                                                    tag = ?redo.tag,
                                                    local = %segment.node,
                                                    "Marking remapped hole filled"
                                                );
                                                state.mark_filled(redo.replay_key.clone(), redo.tag)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // process the current message in this node
                let mut pkt = Some(Packet::ReplayPiece(m));
                let process_result = n.process(
                    &mut pkt,
                    cols,
                    Some(rp),
                    false,
                    ProcessEnv {
                        state: &mut self.state,
                        reader_write_handles: &mut self.reader_write_handles,
                        nodes: &self.nodes,
                        executor: ex,
                        shard: self.shard,
                        replica: self.replica,
                        auxiliary_node_states: &mut self.auxiliary_node_states,
                    },
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

                if is_target {
                    if !misses.is_empty() {
                        // we missed while processing
                        // it's important that we clear out any partially-filled holes.
                        if let Some(state) = self.state.get_mut(segment.node) {
                            for miss in &missed_on {
                                state.mark_hole(miss, tag);
                            }
                        } else if let Some(wh) = self.reader_write_handles.get_mut(segment.node) {
                            for miss in &missed_on {
                                wh.mark_hole(miss)?;
                            }
                        }
                    } else if n.is_reader() {
                        // we filled a hole! publish the reader.
                        if let Some(wh) = self.reader_write_handles.get_mut(segment.node) {
                            wh.publish();
                            wh.notify_readers()?;
                        }

                        // and also unmark the replay request
                        if let Some(ref mut prev) = self.reader_triggered.get_mut(segment.node) {
                            if let Some(backfill_keys) = &backfill_keys {
                                for key in backfill_keys {
                                    prev.remove(key);
                                }
                            }
                        }
                    }
                }

                if is_target && !process_result.captured.is_empty() {
                    // materialized union ate some of our keys,
                    // so we didn't *actually* fill those keys after all!
                    if let Some(state) = self.state.get_mut(segment.node) {
                        for key in &process_result.captured {
                            state.mark_hole(key, tag);
                        }
                    } else if n.is_reader() {
                        if let Some(wh) = self.reader_write_handles.get_mut(segment.node) {
                            for key in &process_result.captured {
                                wh.mark_hole(key)?;
                            }
                        }
                    }
                }

                // we're done with the node
                drop(n);

                if pkt.is_none() {
                    // eaten full replay
                    assert_eq!(misses.len(), 0);

                    // it's been captured, so we need to *not* consider the replay finished
                    // (which the logic below matching on context would do)
                    break 'outer;
                } else if let Some(Packet::ReplayPiece(left)) = pkt {
                    m = left;
                } else {
                    unreachable!("invalid packet, expecting ReplayPiece: {:?}", pkt);
                }

                // we need to track how many replays we completed, and we need to do so
                // *before* we prune keys that missed. these conditions are all important,
                // so let's walk through them
                //
                //  1. this applies only to partial backfills
                //  2. we should only set finished_partial if it hasn't already been set. this is
                //     important, as misses will cause backfill_keys to be pruned over time, which
                //     would cause finished_partial to hold the wrong value!
                if let Some(backfill_keys) = &backfill_keys {
                    if finished_partial == 0 && (dst_is_reader || !dst_is_sender) {
                        finished_partial = backfill_keys.len();
                    }
                }

                // only continue with the keys that weren't captured
                if let ReplayPiece {
                    context:
                        ReplayPieceContext::Partial {
                            ref mut for_keys, ..
                        },
                    ..
                } = m
                {
                    if let Some(backfill_keys) = &mut backfill_keys {
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
                    let (unishard, requesting_shard, requesting_replica) = if let ReplayPiece {
                        context:
                            ReplayPieceContext::Partial {
                                unishard,
                                requesting_shard,
                                requesting_replica,
                                ..
                            },
                        ..
                    } = m
                    {
                        (unishard, requesting_shard, requesting_replica)
                    } else {
                        internal!("backfill_keys.is_some() implies Context::Partial");
                    };

                    need_replay.extend(misses.iter().map(|m| {
                        ReplayDescriptor::from_miss(
                            m,
                            tag,
                            unishard,
                            requesting_shard,
                            requesting_replica,
                        )
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
                    m.data_mut().retain(|r| {
                        // XXX: don't we technically need to translate the columns a
                        // bunch here? what if two key columns are reordered?
                        // XXX: this clone and collect here is *really* sad
                        let r = r.rec();
                        !missed_on.iter().any(|miss| {
                            miss.contains(partial_index.columns.iter().map(|&c| {
                                // record came from processing, which means it
                                // must have the right number of columns
                                &r[c]
                            }))
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
                // instead, we probably want the join to do the eviction. we achieve this by
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
                            // src came from a replay path
                            let n = self.nodes[*src].borrow();
                            if n.beyond_mat_frontier() {
                                let state = self.state.get_mut(*src).ok_or_else(|| {
                                    internal_err!("replay sourced at non-materialized node")
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
                                // we know the lookup was into a real node
                                let pn = self.nodes[pn].borrow();
                                if !pn.can_query_through() {
                                    internal!(
                                        "lookup into non-materialized, non-query-through node."
                                    );
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
                            let path_index = rp.target_index.as_ref().unwrap();
                            rp.target_node() == Some(pn)
                                && path_index.columns == lookup.cols
                                && path_index.index_type.supports_key(&lookup.key)
                        };

                        for &pn in &pns {
                            // this is a node that we were doing lookups into as part of
                            // the replay -- make sure we evict any state we may have added
                            // there.
                            evict_tags.retain(|tag| tag_match(&self.replay_paths[*tag], pn));

                            let state = self.state.get_mut(pn).unwrap();
                            assert!(state.is_partial());

                            if evict_tags.is_empty() {
                                for index_type in IndexType::all_for_key(&lookup.key) {
                                    if let Some(tags) = self.replay_paths.tags_for_index(
                                        Destination(pn),
                                        Target(pn),
                                        &Index::new(*index_type, lookup.cols.clone()),
                                    ) {
                                        // this is the tag we would have used to fill a
                                        // lookup hole in this ancestor, so this is the
                                        // tag we need to evict from.

                                        // TODO: could there have been multiple
                                        invariant_eq!(tags.len(), 1);
                                        #[allow(clippy::unwrap_used)] // we check len is 1 first
                                        evict_tags.push(*tags.iter().next().unwrap());
                                    }
                                }
                            }

                            if evict_tags.is_empty() {
                                internal!(
                                    "no tag found for lookup target {:?}({:?}) (really {:?})",
                                    self.nodes[lookup.on].borrow().global_addr(),
                                    lookup.cols,
                                    self.nodes[pn].borrow().global_addr()
                                );
                            }

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
                if m.is_empty() {
                    if let ReplayPiece {
                        context: ReplayPieceContext::Full { last: false, .. },
                        ..
                    } = m
                    {
                        trace!("dropping empty non-terminal full replay packet");
                        // don't continue processing empty updates, *except* if this is the
                        // last replay batch. in that case we need to send it so that the
                        // next domain knows that we're done
                        // TODO: we *could* skip ahead to path.last() here
                        break;
                    }
                }

                if i + 1 < path.len() {
                    // update link for next iteration
                    if self.nodes[path[i + 1].node].borrow().is_shard_merger() {
                        // we need to preserve the egress src for shard mergers
                        // (which includes shard identifier)
                    } else {
                        m.link_mut().src = segment.node;
                    }
                    m.link_mut().dst = path[i + 1].node;
                }

                // feed forward the updated backfill_keys
                if let ReplayPiece {
                    context:
                        ReplayPieceContext::Partial {
                            ref mut for_keys, ..
                        },
                    ..
                } = m
                {
                    *for_keys = backfill_keys.unwrap();
                }
            }

            match m.context {
                ReplayPieceContext::Full { last, .. } if last => {
                    debug!(terminal = notify_done, "last batch processed");
                    if notify_done {
                        debug!(local = dst.id(), "last batch received");
                        finished = Some((tag, dst, target.unwrap(), None));
                    }
                }
                ReplayPieceContext::Full { .. } => {
                    debug!("batch processed");
                }
                ReplayPieceContext::Partial { for_keys, .. } => {
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
                                time: time::Instant::now() + time::Duration::from_millis(50),
                                keys: for_keys,
                                view: dst,
                            });
                        }
                        assert_ne!(finished_partial, 0);
                    } else if !dst_is_sender {
                        trace!(local = dst.id(), "partial replay completed");
                        if finished_partial == 0 {
                            assert!(for_keys.is_empty());
                        }
                        finished = Some((tag, dst, target.unwrap(), Some(for_keys)));
                    } else {
                        // we're just on the replay path
                    }
                }
            }

            break;
        }

        if finished_partial != 0 {
            self.finished_partial_replay(tag, finished_partial)?;
        }

        // While there are still misses, we iterate over the array, each time draining it from
        // elements that can be batched into a single call to `on_replay_misses`
        while let Some(next_replay) = need_replay.first().cloned() {
            let mut misses = HashSet::new();
            need_replay.retain(|rep| {
                if next_replay.can_combine(rep) {
                    misses.insert((rep.replay_key.clone(), rep.lookup_key.clone()));
                    false
                } else {
                    true
                }
            });

            trace!(%tag, ?misses, on = %next_replay.idx, "missed during replay processing");

            self.on_replay_misses(
                ex,
                next_replay.idx,
                &next_replay.lookup_columns,
                misses,
                next_replay.unishard,
                next_replay.requesting_shard,
                next_replay.requesting_replica,
                next_replay.tag,
                cache_name.clone(),
            )?;
        }

        if let Some((tag, dst, target, for_keys)) = finished {
            trace!(%dst, %target, keys = ?for_keys, "partial replay finished");
            if let Some(mut waiting) = self.waiting.remove(dst) {
                trace!(
                    keys = ?for_keys,
                    ?waiting,
                    "partial replay finished to node with waiting backfills"
                );

                #[allow(clippy::unwrap_used)]
                // We already know this is a partial replay path
                let key_index = self.replay_paths[tag].target_index.clone().unwrap();

                // We try to batch as many redos together, so they can be later issued in a single
                // call to `RequestPartialReplay`
                let mut replay_sets = HashMap::new();

                // we got a partial replay result that we were waiting for. it's time we let any
                // downstream nodes that missed in us on that key know that they can (probably)
                // continue with their replays.
                #[allow(clippy::unwrap_used)]
                // this is a partial replay (since it's in waiting), so it must have keys
                for key in for_keys.unwrap() {
                    let hole = Hole {
                        node: target,
                        column_indices: key_index.columns.clone(),
                        key,
                    };
                    let replay = match waiting.redos.remove(&hole) {
                        Some(x) => x,
                        None => {
                            internal!(
                                "backfill for unnecessary hole {:?}, tag {:?} (for node {})",
                                Sensitive(&hole),
                                tag,
                                dst
                            );
                        }
                    };

                    // we may need more holes to fill before some replays should be re-attempted
                    let replay = replay.into_iter().filter(|tagged_replay_key| {
                        let left = waiting.holes.get_mut(tagged_replay_key).unwrap();
                        *left -= 1;

                        if *left == 0 {
                            trace!(k = ?tagged_replay_key, "filled last hole, replaying");

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
                    });

                    for Redo {
                        tag,
                        replay_key,
                        unishard,
                        requesting_shard,
                        requesting_replica,
                    } in replay
                    {
                        replay_sets
                            .entry((tag, unishard, requesting_shard, requesting_replica))
                            .or_insert_with(|| Vec::new())
                            .push(replay_key);
                    }
                }

                // After we actually finished sorting the Redos into batches, issue each batch
                for ((tag, unishard, requesting_shard, requesting_replica), keys) in
                    replay_sets.drain()
                {
                    self.delayed_for_self
                        .push_back(Packet::RequestPartialReplay(RequestPartialReplay {
                            tag,
                            unishard,
                            keys,
                            requesting_shard,
                            requesting_replica,
                            cache_name: cache_name.clone(),
                        }));
                }

                if !waiting.holes.is_empty() {
                    // there are still holes, so there must still be pending redos
                    assert!(!waiting.redos.is_empty());

                    // restore Waiting in case seeding triggers more replays
                    self.waiting.insert(dst, waiting);
                } else {
                    // there are no more holes that are filling, so there can't be more redos
                    assert!(waiting.redos.is_empty());
                }
                return Ok(());
            } else if for_keys.is_some() {
                internal!(
                    "got unexpected replay of {:?} for {:?}",
                    Sensitive(&for_keys),
                    dst
                );
            } else {
                // must be a full replay
                // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
                // replaying_to is still set, "normal" dispatch calls will continue to be buffered,
                // but this allows finish_replay to dispatch into the node by
                // overriding replaying_to.
                self.not_ready.remove(&dst);
                self.delayed_for_self.push_back(Packet::Finish(Finish {
                    tag,
                    node: dst,
                    cache_name: cache_name.clone(),
                }));
            }
        }
        Ok(())
    }

    fn finish_replay(
        &mut self,
        tag: Tag,
        node: LocalNodeIndex,
        cache_name: &Relation,
        ex: &mut dyn Executor,
    ) -> ReadySetResult<()> {
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

                if let Packet::Update(_) = m {
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

            if self.replay_paths[tag].notify_done {
                // NOTE: this will only be Some for non-partial replays
                debug!(node = node.id(), "noting replay completed");
                self.state
                    .get_mut(node)
                    .map(|n| n.set_replay_done(true))
                    .or_else(|| {
                        self.reader_write_handles
                            .get_mut(node)
                            .map(|rwh| rwh.set_replay_done(true))
                    })
                    .ok_or_else(|| {
                        internal_err!("Replayed to non-materialized, non-reader node {node}")
                    })?;
                Ok(())
            } else {
                internal!();
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            self.delayed_for_self.push_back(Packet::Finish(Finish {
                tag,
                node,
                cache_name: cache_name.clone(),
            }));
            Ok(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn trigger_downstream_evictions(
        index: &Index,
        keys: &[KeyComparison],
        node: LocalNodeIndex,
        ex: &mut dyn Executor,
        not_ready: &HashSet<LocalNodeIndex>,
        replay_paths: &ReplayPaths,
        shard: Option<usize>,
        replica: usize,
        state: &mut StateMap,
        reader_write_handles: &mut NodeMap<backlog::WriteHandle>,
        nodes: &DomainNodes,
        remapped_keys: &mut RemappedKeys,
    ) -> ReadySetResult<usize> {
        let mut bytes_freed = 0;

        for (tag, path, keys) in
            replay_paths.downstream_dependent_paths(node, index, keys, remapped_keys)
        {
            Self::walk_path(
                &path.path[..],
                &keys,
                tag,
                shard,
                replica,
                nodes,
                reader_write_handles,
                ex,
            )?;
            match path.trigger {
                TriggerEndpoint::Local(_) => {
                    let replay_path = &replay_paths[tag];

                    let dest = replay_path.last_segment();

                    if nodes[dest.node].borrow().is_reader() {
                        // already evicted from in walk_path
                        continue;
                    }
                    if !state.contains_key(dest.node) {
                        if !not_ready.contains(&dest.node) {
                            debug!(
                                node = dest.node.id(),
                                "got eviction for ready but stateless node"
                            )
                        }
                        continue;
                    }

                    trace!(
                        local = %dest.node,
                        ?keys,
                        ?tag,
                        target = ?(replay_path.target_node(), &replay_path.target_index),
                        "Evicting keys"
                    );
                    if let Some(result) = state[dest.node].evict_keys(tag, &keys) {
                        bytes_freed += result.bytes_freed;
                        // we can only evict from partial replay paths, so we must have a
                        // partial key
                        bytes_freed += Self::trigger_downstream_evictions(
                            #[allow(clippy::unwrap_used)]
                            dest.partial_index.as_ref().unwrap(),
                            &keys,
                            dest.node,
                            ex,
                            not_ready,
                            replay_paths,
                            shard,
                            replica,
                            state,
                            reader_write_handles,
                            nodes,
                            remapped_keys,
                        )?;
                    }
                }
                TriggerEndpoint::Start(_) => {
                    if let Some(result) = state[path.source.unwrap()].evict_keys(tag, &keys) {
                        bytes_freed += result.bytes_freed;
                    }
                }
                _ => (),
            }
        }
        Ok(bytes_freed)
    }

    #[allow(clippy::too_many_arguments)]
    fn walk_path(
        path: &[ReplayPathSegment],
        keys: &[KeyComparison],
        tag: Tag,
        shard: Option<usize>,
        replica: usize,
        nodes: &DomainNodes,
        reader_write_handles: &mut NodeMap<backlog::WriteHandle>,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let mut from = path[0].node;
        for segment in path {
            #[allow(clippy::unwrap_used)]
            // partial_key must be Some for partial replay paths
            nodes[segment.node].borrow_mut().process_eviction(
                from,
                &segment.partial_index.as_ref().unwrap().columns,
                keys,
                tag,
                shard,
                replica,
                reader_write_handles,
                executor,
            )?;
            from = segment.node;
        }
        Ok(())
    }

    fn eviction_candidates(
        nodes: &DomainNodes,
        num_bytes: &mut usize,
        state: &StateMap,
        reader_write_handles: &NodeMap<backlog::WriteHandle>,
    ) -> Vec<(LocalNodeIndex, usize)> {
        let mut candidates: Vec<_> = nodes
            .values()
            .filter_map(|nd| {
                let n = &*nd.borrow();
                let local_index = n.local_addr();

                if let Some(wh) = reader_write_handles.get(local_index) {
                    if wh.is_partial() {
                        Some(wh.deep_size_of())
                    } else {
                        None
                    }
                } else {
                    state
                        .get(local_index)
                        .filter(|state| state.is_partial())
                        .map(|state| state.deep_size_of())
                }
                .map(|s| (local_index, s))
            })
            .filter(|&(_, s)| s > 0)
            .collect();

        // we want to spread the eviction across the nodes,
        // rather than emptying out one node completely.
        // -1* so we sort in descending order
        // TODO: be smarter than 3 here
        candidates.sort_unstable_by_key(|&(_, s)| -(s as i64));
        candidates.truncate(3);

        // don't evict from tiny things (< 10% of max)
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
            // TODO: should this be evenly divided, or weighted by the size of the
            // domains?
            let share = (*num_bytes).div_ceil(n);
            // we're only willing to evict at most half the state in each node
            // unless this is the only node left to evict from
            *size = if n > 1 {
                cmp::min(*size / 2, share)
            } else {
                assert_eq!(share, *num_bytes);
                share
            };
            *num_bytes -= *size;
            trace!(bytes = *size, node = ?n, "chose to evict from node");
            n -= 1;
        }

        candidates
    }

    fn handle_eviction_bytes(
        &mut self,
        ex: &mut dyn Executor,
        node: Option<LocalNodeIndex>,
        mut num_bytes: usize,
    ) -> ReadySetResult<Option<Vec<DfValue>>> {
        let start = std::time::Instant::now();
        self.metrics.inc_eviction_requests();

        let mut total_freed = 0;
        let nodes = if let Some(node) = node {
            vec![(node, num_bytes)]
        } else {
            Self::eviction_candidates(
                &self.nodes,
                &mut num_bytes,
                &self.state,
                &self.reader_write_handles,
            )
        };

        for (node, num_bytes) in nodes {
            let mut freed = 0;
            let n = self.nodes[node].borrow_mut();

            if n.is_dropped() {
                continue; // Node was dropped. Skip.
            } else if let Some(state) = self.reader_write_handles.get_mut(node) {
                freed += state.evict_bytes(num_bytes);
                state.publish();
                state.notify_readers_of_eviction()?;
            } else if let Some(EvictBytesResult {
                index,
                keys_evicted,
                bytes_freed,
                ..
            }) = self.state[node].evict_bytes(num_bytes)
            {
                let keys = keys_evicted
                    .into_iter()
                    .map(|k| {
                        KeyComparison::try_from(k).map_err(|_| internal_err!("Empty key evicted"))
                    })
                    .collect::<ReadySetResult<Vec<_>>>()?;

                freed += bytes_freed;
                if !keys.is_empty() {
                    let index = index.clone();
                    freed += Self::trigger_downstream_evictions(
                        &index,
                        &keys[..],
                        node,
                        ex,
                        &self.not_ready,
                        &self.replay_paths,
                        self.shard,
                        self.replica,
                        &mut self.state,
                        &mut self.reader_write_handles,
                        &self.nodes,
                        &mut self.remapped_keys,
                    )?;
                }
            } else {
                // This node was unable to evict any keys
                continue;
            }

            debug!(%freed, node = ?n, "evicted from node");
            self.state_size.fetch_sub(freed, Ordering::AcqRel);
            total_freed += freed;
        }

        self.metrics.rec_eviction_time(start.elapsed(), total_freed);
        Ok(None)
    }

    fn handle_eviction_keys(
        &mut self,
        ex: &mut dyn Executor,
        dst: LocalNodeIndex,
        tag: Tag,
        keys: Vec<KeyComparison>,
    ) -> ReadySetResult<Option<Vec<DfValue>>> {
        let (trigger, path) = if let Some(rp) = self.replay_paths.get(tag) {
            (&rp.trigger, &rp.path)
        } else {
            debug!(?tag, "got eviction for tag that has not yet been finalized");
            return Ok(None);
        };

        let i = path
            .iter()
            .position(|ps| ps.node == dst)
            .ok_or_else(|| ReadySetError::NoSuchNode(dst.id()))?;
        Self::walk_path(
            &path[i..],
            &keys,
            tag,
            self.shard,
            self.replica,
            &self.nodes,
            &mut self.reader_write_handles,
            ex,
        )?;

        match trigger {
            TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) => {
                // This path terminates inside the domain. Find the destination node, evict
                // from it, and then propagate the eviction further downstream.
                let destination = path.last().node;
                // We've already evicted from readers in walk_path
                if self.nodes[destination].borrow().is_reader() {
                    return Ok(None);
                }
                // No need to continue if node was dropped.
                if self.nodes[destination].borrow().is_dropped() {
                    return Ok(None);
                }

                let index = path.last().partial_index.clone().ok_or_else(|| {
                    internal_err!("Received eviction for non-partial replay path")
                })?;

                trace!(local = %destination, ?keys, ?tag, "Evicting keys");
                if let Some(result) = self.state[destination].evict_keys(tag, &keys) {
                    let mut freed = result.bytes_freed;
                    freed += Self::trigger_downstream_evictions(
                        &index,
                        &keys[..],
                        destination,
                        ex,
                        &self.not_ready,
                        &self.replay_paths,
                        self.shard,
                        self.replica,
                        &mut self.state,
                        &mut self.reader_write_handles,
                        &self.nodes,
                        &mut self.remapped_keys,
                    )?;
                    self.state_size.fetch_sub(freed, Ordering::AcqRel);
                }
            }
            TriggerEndpoint::None | TriggerEndpoint::Start(_) => {}
        }
        Ok(None)
    }

    fn handle_eviction_single_key(
        &mut self,
        ex: &mut dyn Executor,
        tag: Tag,
        key: Option<Vec<DfValue>>,
    ) -> ReadySetResult<Option<Vec<DfValue>>> {
        let (trigger, path) = if let Some(rp) = self.replay_paths.get(tag) {
            (&rp.trigger, &rp.path)
        } else {
            debug!(?tag, "got eviction for tag that has not yet been finalized");
            return Ok(None);
        };

        match trigger {
            TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) => {
                // This path terminates inside the domain. Find the destination node, evict
                // from it, and then propagate the eviction further downstream.
                let destination = path.last().node;
                let n = self.nodes[destination].borrow_mut();

                let (freed, eviction) = if n.is_dropped() {
                    // Node was dropped. Skip.
                    (0, None)
                } else if let Some(state) = self.reader_write_handles.get_mut(destination) {
                    let (bytes_freed, eviction) = match key {
                        Some(key) => {
                            let key_comparison = {
                                let key_comparison = KeyComparison::try_from(key.clone())?;
                                // We must pass a KeyComparison::Range to a BTreeMap reader
                                // or it will not update its intervals
                                match state.index_type() {
                                    IndexType::HashMap => key_comparison,
                                    IndexType::BTreeMap => key_comparison.into_range(),
                                }
                            };
                            let freed = state.mark_hole(&key_comparison)?;
                            // TODO(REA-2682): Should reader evictions be accounted for in
                            // self.state_size anyways? They're not when we evict during
                            // walk_path().
                            //
                            // Don't return an evicted key if there was no eviction
                            (freed, if freed > 0 { Some(key) } else { None })
                        }
                        None => state.evict_random(),
                    };
                    state.publish();
                    state.notify_readers_of_eviction()?;
                    (bytes_freed, eviction)
                } else {
                    let eviction = match key {
                        Some(key) => {
                            // A BTree MemoryState will still update its intervals if passed
                            // a KeyComparison::Equal.
                            if let Some(EvictKeysResult { bytes_freed, index }) = self.state
                                [destination]
                                .evict_keys(tag, &[KeyComparison::try_from(key.clone())?])
                            {
                                if bytes_freed > 0 {
                                    Some((index, bytes_freed, key))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        None => {
                            if let Some(EvictRandomResult {
                                index,
                                key_evicted,
                                bytes_freed,
                            }) = {
                                let mut rng = rand::thread_rng();
                                self.state[destination].evict_random(tag, &mut rng)
                            } {
                                Some((index, bytes_freed, key_evicted))
                            } else {
                                None
                            }
                        }
                    };
                    let Some((index, bytes_freed, key_evicted)) = eviction else {
                        return Ok(None);
                    };
                    let key = KeyComparison::try_from(key_evicted.clone())
                        .map_err(|_| internal_err!("Empty key evicted"))?;

                    let index = index.clone();
                    let freed = Self::trigger_downstream_evictions(
                        &index,
                        &[key],
                        destination,
                        ex,
                        &self.not_ready,
                        &self.replay_paths,
                        self.shard,
                        self.replica,
                        &mut self.state,
                        &mut self.reader_write_handles,
                        &self.nodes,
                        &mut self.remapped_keys,
                    )?;
                    (bytes_freed + freed, Some(key_evicted))
                };

                debug!(%freed, node = ?n, "evicted from node");
                self.state_size.fetch_sub(freed, Ordering::AcqRel);
                Ok(eviction)
            }
            TriggerEndpoint::None | TriggerEndpoint::Start { .. } => {
                // Node not in domain.
                Ok(None)
            }
        }
    }

    /// Handles an [`Eviction`], triggering downstream evictions in this Domain or others as
    /// necessary.
    pub fn handle_eviction(
        &mut self,
        request: Eviction,
        ex: &mut dyn Executor,
        done: Option<Url>,
        barrier: u128,
        credits: u128,
    ) -> ReadySetResult<Option<Vec<DfValue>>> {
        let barrier = done.map(|done| Barrier::new(ex, done, barrier, credits));

        let res = match request {
            Eviction::Bytes { node, num_bytes } => self.handle_eviction_bytes(ex, node, num_bytes),
            Eviction::Keys {
                link: Link { dst, .. },
                tag,
                keys,
            } => self.handle_eviction_keys(ex, dst, tag, keys),
            Eviction::SingleKey { tag, key } => self.handle_eviction_single_key(ex, tag, key),
        };

        barrier.map(|b| b.flush(ex));
        res
    }

    pub fn address(&self) -> ReplicaAddress {
        ReplicaAddress {
            domain_index: self.index(),
            shard: self.shard(),
            replica: self.replica(),
        }
    }

    pub fn update_state_sizes(&mut self) {
        let mut reader_size = 0;
        let total = self
            .nodes
            .values()
            .map(|nd| {
                let n = &*nd.borrow();
                let local_index = n.local_addr();

                if n.is_reader() {
                    // We are a reader, which has its own kind of state
                    let mut size = 0;
                    if let Some(wh) = self.reader_write_handles.get(local_index) {
                        if wh.is_partial() {
                            size = wh.deep_size_of();
                            reader_size += size;
                        }
                    }

                    self.metrics.set_reader_state_size(n.name(), size);
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

        self.state_size.store(total, Ordering::Release);
        // no response sent, as worker will read the atomic
    }

    pub fn estimated_base_tables_size(&self) -> usize {
        self.state
            .iter()
            .filter_map(|(ni, state)| {
                state.as_persistent().map(|s| {
                    let n = &*self.nodes.get(ni).unwrap().borrow();
                    let size = s.deep_size_of();

                    self.metrics.set_base_table_size(n.name(), size);

                    size
                })
            })
            .sum()
    }

    pub fn min_persisted_replication_offset(
        &self,
    ) -> ReadySetResult<BaseTableState<PersistencePoint>> {
        let mut cur_min = PersistencePoint::Persisted;

        for (idx, n) in self.nodes.iter() {
            let node = n.borrow();
            if node.is_base() && !node.is_dropped() {
                if let Some(state) = self.state.get(idx) {
                    match (&mut cur_min, state.persisted_up_to()?) {
                        (_, PersistencePoint::Persisted) => continue,
                        (PersistencePoint::Persisted, PersistencePoint::UpTo(offset)) => {
                            cur_min = PersistencePoint::UpTo(offset);
                        }
                        (PersistencePoint::UpTo(min), PersistencePoint::UpTo(offset)) => {
                            if offset.try_partial_cmp(min)?.is_lt() {
                                cur_min = PersistencePoint::UpTo(offset);
                            }
                        }
                    }
                } else {
                    return Ok(BaseTableState::Pending);
                }
            }
        }

        Ok(BaseTableState::Initialized(cur_min))
    }

    pub fn replication_offsets(&self) -> NodeMap<BaseTableState<Option<ReplicationOffset>>> {
        self.nodes
            .iter()
            .filter_map(|(idx, n)| {
                let node = n.borrow();
                if !node.is_base() || node.is_dropped() {
                    None
                } else {
                    Some((
                        idx,
                        self.state
                            .get(idx)
                            .map(|s| BaseTableState::Initialized(s.replication_offset().cloned()))
                            .unwrap_or(BaseTableState::Pending),
                    ))
                }
            })
            .collect()
    }

    /// If there is a pending timed purge, return the duration until it needs
    /// to happen
    pub fn next_poll_duration(&self) -> Option<time::Duration> {
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
    pub fn handle_packet(
        &mut self,
        packet: Packet,
        executor: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        #[cfg(feature = "failure_injection")]
        if self.check_failpoint() {
            return Err(ReadySetError::Internal("failpoint".to_string()));
        }

        let span = info_span!(
            target: "readyset_dataflow::domain",
            "domain_handle_packet",
            op = ?packet,
        );
        let _time = time_scope(span, SLOW_LOOP_THRESHOLD);

        if self.wait_time.is_running() {
            self.wait_time.stop();
        }

        self.handle(packet, executor)?;
        // After we handle an external packet, the domain may have accumulated a bunch of packets to
        // itself we need to process them all next;
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
            self.handle_timed_purges()?;
        }

        if self.aggressively_update_state_sizes {
            self.update_state_sizes();
        }

        if !self.wait_time.is_running() {
            self.wait_time.start();
        }

        Ok(())
    }

    /// Sets the [`MaterializedNodeState`] for the given node, and
    /// makes sure to:
    /// 1. Remove the node from the `not_ready` set.
    /// 2. Set the state for the node
    /// 3. Process any message that was meant for the node but couldn't be processed since the its
    ///    state was not ready yet.
    ///
    /// NOTE: If the node was removed while we were initializing its persistent state, then
    /// we make sure to just tear it down here.
    pub fn process_state_for_node(
        &mut self,
        local_idx: LocalNodeIndex,
        state: MaterializedNodeState,
    ) -> ReadySetResult<()> {
        if let Some(node) = self.nodes.get(local_idx) {
            if node.borrow().is_dropped() {
                warn!(
                    local = local_idx.id(),
                    "tried to set state for node, but node does not exist anymore"
                );
                state.tear_down()?;
                return Ok(());
            }
            if self.not_ready.remove(&local_idx) {
                trace!(local = local_idx.id(), "readying empty node");
            }
            assert!(self.state.insert(local_idx, state).is_none());
        } else {
            warn!(
                local = local_idx.id(),
                "tried to set state for non-existent node"
            );
        }

        Ok(())
    }

    pub fn channel(&self) -> (DomainSender, DomainReceiver) {
        channel::domain_channel()
    }

    #[cfg(feature = "failure_injection")]
    fn check_failpoint(&self) -> bool {
        use fail::fail_point;

        fail_point!(
            failpoints::HANDLE_PACKET,
            |domain| if let Some(s) = domain {
                if let Ok(index) = s.parse() {
                    DomainIndex::new(index) == self.index
                } else {
                    false
                }
            } else {
                false
            }
        );
        false
    }
}

struct Barrier {
    done: Url,
    id: u128,
    credits: u128,
}

impl Barrier {
    fn new(ex: &mut dyn Executor, done: Url, id: u128, credits: u128) -> Self {
        ex.cork();
        Self { done, id, credits }
    }

    fn flush(self, ex: &mut dyn Executor) -> ReadySetResult<()> {
        let mut msgs = ex.uncork().into_iter();
        let n = msgs.len() as u128;

        if n == 0 {
            debug!(
                "flushing barrier {:x} to worker, credits {:x}",
                self.id, self.credits
            );
            self.rpc(ex, self.credits);
        } else {
            let each = self.credits / n;
            let extra = self.credits % n;
            debug!(
                "flushing barrier {:x}, split {}, each {:x} + extra {:x}",
                self.id, n, each, extra
            );
            invariant!(each > 0, "barrier split too many times");

            let (rep, msg) = msgs.next().unwrap(); // n > 0
            self.send(ex, rep, msg, each + extra);

            for (rep, msg) in msgs {
                self.send(ex, rep, msg, each)
            }
        }

        Ok(())
    }

    fn rpc(&self, ex: &mut dyn Executor, credits: u128) {
        ex.rpc(
            self.done.clone(),
            Upcall::BarrierCredit {
                id: self.id,
                credits,
            },
        );
    }

    fn send(&self, ex: &mut dyn Executor, rep: ReplicaAddress, mut msg: Packet, credits: u128) {
        match msg {
            Packet::Evict(ref mut e) => {
                e.done = Some(self.done.clone());
                e.barrier = self.id;
                e.credits = credits;
            }
            ref pkt => {
                warn!("unexpected packet type during eviction: {:?}", pkt);
                self.rpc(ex, credits);
            }
        }
        ex.send(rep, msg);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const BID: u128 = 666;
    const CREDITS: u128 = 0x40000000000000000000000000000000;

    const fn fake_addr() -> ReplicaAddress {
        ReplicaAddress {
            domain_index: DomainIndex::new(0),
            shard: 0,
            replica: 0,
        }
    }

    const fn fake_packet() -> Packet {
        Packet::Evict(Evict {
            req: Eviction::Bytes {
                node: None,
                num_bytes: 0,
            },
            done: None,
            barrier: 0,
            credits: 0,
        })
    }

    #[test]
    fn barrier_upcall() {
        let mut out = Outboxes::new();
        let url = Url::parse("http://example.org/foo").unwrap();
        let b = Barrier::new(&mut out, url.clone(), BID, CREDITS);

        b.flush(&mut out).unwrap();

        let pkts = out.take_messages();
        assert_eq!(pkts.len(), 0);

        let rpcs = out.take_rpcs();
        assert_eq!(rpcs.len(), 1);
        match &rpcs[0] {
            (up, Upcall::BarrierCredit { id, credits }) => {
                assert_eq!(*up, url);
                assert_eq!(*id, BID);
                assert_eq!(*credits, CREDITS);
            }
        }
    }

    fn barrier_test(send: fn(&mut dyn Executor)) {
        let mut out = Outboxes::new();
        let url = Url::parse("http://example.org/foo").unwrap();
        let b = Barrier::new(&mut out, url.clone(), BID, CREDITS);

        send(&mut out);
        b.flush(&mut out).unwrap();

        let mut total = 0;
        for pkt in out.take_messages().pop().unwrap().1 {
            if let Packet::Evict(e) = pkt {
                assert_eq!(e.done, Some(url.clone()));
                assert_eq!(e.barrier, BID);
                total += e.credits;
            }
        }

        for rpc in out.take_rpcs() {
            match rpc {
                (up, Upcall::BarrierCredit { id, credits }) => {
                    assert_eq!(up, url);
                    assert_eq!(id, BID);
                    total += credits;
                }
            }
        }

        assert_eq!(total, CREDITS);
    }

    #[test]
    fn barrier_total() {
        barrier_test(|out| {
            for _ in 0..3 {
                out.send(fake_addr(), fake_packet());
            }
        });
    }

    #[test]
    fn barrier_unexpected_packet() {
        barrier_test(|out| {
            for _ in 0..3 {
                out.send(fake_addr(), fake_packet());
            }
            out.send(fake_addr(), Packet::Spin);
        });
    }
}
