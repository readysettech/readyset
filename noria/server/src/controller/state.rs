//! Through me you pass into the city of woe:
//! Through me you pass into eternal pain:
//! Through me among the people lost for aye.
//! Justice the founder of my fabric moved:
//! To rear me was the task of Power divine,
//! Supremest Wisdom, and primeval Love.
//! Before me things create were none, save things
//! Eternal, and eternal I endure.
//! All hope abandon, ye who enter here.
//!    - The Divine Comedy, Dante Alighieri
//!
//! This module provides the structures to store the state of the Noria dataflow graph, and
//! to manipulate it in a thread-safe way.
// Allow this until the whole state logic is in place.
// This is an intermediate stage to get ENG-805 into main.
#![allow(unused)]
use crate::controller::domain_handle::DomainHandle;
use crate::controller::migrate::materialization::Materializations;
use crate::controller::migrate::Migration;
use crate::controller::recipe::{Recipe, Schema};
use crate::controller::{
    schema, ControllerState, DomainPlacementRestriction, NodeRestrictionKey, Worker,
    WorkerIdentifier,
};
use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::internal::LocalNodeIndex;
use crate::worker::WorkerRequestKind;
use crate::RecipeSpec;
use common::IndexPair;
use dataflow::node::Node;
use dataflow::prelude::{ChannelCoordinator, DomainIndex, DomainNodes, Graph, NodeIndex};
use dataflow::{
    node, DomainBuilder, DomainConfig, DomainRequest, NodeMap, Packet, PersistenceParameters,
    Sharding,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::FutureExt;
use lazy_static::lazy_static;
use metrics::{gauge, histogram};
use noria::builders::{ReplicaShard, TableBuilder, ViewBuilder, ViewReplica};
use noria::consensus::{Authority, AuthorityControl};
use noria::debug::info::{DomainKey, GraphInfo};
use noria::debug::stats::{DomainStats, GraphStats, NodeStats};
use noria::internal::MaterializationStatus;
use noria::replication::{ReplicationOffset, ReplicationOffsets};
use noria::{
    metrics::recorded, ActivationResult, ReaderReplicationResult, ReaderReplicationSpec,
    ReadySetError, ReadySetResult, ViewFilter, ViewRequest, ViewSchema,
};
use noria_errors::{bad_request_err, internal, internal_err, invariant, invariant_eq, NodeType};
use petgraph::visit::Bfs;
use regex::Regex;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use std::{cell, mem, time};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, instrument, trace, warn};
use vec1::Vec1;

/// Number of concurrent requests to make when making multiple simultaneous requests to domains (eg
/// for replication offsets)
const CONCURRENT_REQUESTS: usize = 16;

/// This structure holds all the dataflow state.
/// It's meant to be handled exclusively by the [`DataflowStateHandle`], which is the structure
/// that guarantees thread-safe access to it.
#[derive(Clone)]
pub struct DataflowState {
    pub(super) ingredients: Graph,

    /// ID for the root node in the graph. This is used to retrieve a list of base tables.
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    domain_config: DomainConfig,

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is
    ///     deleted once the `Controller` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    ///
    /// Must be called before any domains have been created.
    persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    pub(super) recipe: Recipe,
    /// Latest replication position for the schema if from replica or binlog
    schema_replication_offset: Option<ReplicationOffset>,
    /// Placement restrictions for nodes and the domains they are placed into.
    pub(super) node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,

    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    pub(super) domain_nodes: HashMap<DomainIndex, NodeMap<NodeIndex>>,
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,

    /// Map from worker URI to the address the worker is listening on for reads.
    pub(super) read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,

    /// State between migrations
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,
}

impl DataflowState {
    /// Creates a new instance of [`DataflowState`].
    pub(super) fn new(
        ingredients: Graph,
        source: NodeIndex,
        ndomains: usize,
        sharding: Option<usize>,
        domain_config: DomainConfig,
        persistence: PersistenceParameters,
        materializations: Materializations,
        recipe: Recipe,
        schema_replication_offset: Option<ReplicationOffset>,
        node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> Self {
        Self {
            ingredients,
            source,
            ndomains,
            sharding,
            domain_config,
            persistence,
            materializations,
            recipe,
            schema_replication_offset,
            node_restrictions,
            domains: Default::default(),
            domain_nodes: Default::default(),
            channel_coordinator,
            read_addrs: Default::default(),
            workers: Default::default(),
            remap: Default::default(),
        }
    }
}

/// This structure acts as a wrapper for a [`DataflowStateReader`] in order to guarantee
/// thread-safe access (read and writes) to Noria's dataflow state.
///
/// # Overview
/// Two operations can be performed by this structure.
///
/// ## Reads
/// Reads are performed by taking a read lock on the underlying [`DataflowStateReader`].
/// This allows any thread to freely get a read-only view of the dataflow state without having
/// to worry about other threads attemting to modify it.
///
/// The choice of using [`tokio::sync::RwLock`] instead of [`std::sync::RwLock`] or [`parking_lot::RwLock`]
/// was made in order to ensure that:
/// 1. The read lock does not starve writers attempting to modify the dataflow state, as the lock is
/// fair and will block reader threads if there are writers waiting for the lock.
/// 2. To ensure that the read lock can be used by multiple threads, since the lock is `Send` and `Sync`.
///
/// ## Writes
/// Writes are performed by following a couple of steps:
/// 1. A mutex is acquired to ensure that only one write is in progress at any time.
/// 2. A copy of the current [`DataflowState`] (being held by the [`DataflowStateReader`] is made.
/// 3. A [`DataflowStateWriter`] is created from the copy and the mutex guard, having the lifetime
/// of the latter.
/// 4. All the computations/modifications are performed on the [`DataflowStateWriter`] (aka, on the
/// underlying [`DataflowState`] copy).
/// 5. The [`DataflowStateWriter`] is then committed to the [`DataflowState`] by calling
/// [`DataflowStateWriter::commit`], which replaces the old state by the new one in the
/// [`DataflowStateReader`].
///
/// As previously mentioned for reads, the choice of using [`tokio::sync::RwLock`] ensures writers
/// fairness and the ability to use the lock by multiple threads.
///
/// Following the three steps to perform a write guarantees that:
/// 1. Writes don't starve readers: when we start a write operation, we take a read lock
/// for the [`DataflowStateReader`] in order to make a copy of a state. In doing so, we ensure that
/// readers can continue to read the state: no modification has been made yet.
/// Writers can also perform all their computing/modifications (which can be pretty expensive time-wise),
/// and only then the changes can be committed by swapping the old state for the new one, which
/// is the only time readers are forced to wait.
/// 2. If a write operation fails, the state is not modified.
/// TODO(fran): Even though the state is not modified here, we might have sent messages to other workers/domains.
///   It is worth looking into a better way of handling that (if it's even necessary).
///   Such a mechanism was never in place to begin with.
/// 3. Writes are transactional: if there is an instance of [`DataflowStateWriter`] in
/// existence, then all the other writes must wait. This guarantees that the operations performed
/// to the dataflow state are executed transactionally.
///
/// # How to use
/// ## Reading the state
/// To get read-only access to the dataflow state, the [`DataflowState::read`] method must be used.
/// This method returns a read guard to another wrapper structure, [`DataflowStateReader`],
/// which only allows reference access to the dataflow state.
///
/// ## Modifying the state
/// To get write and read access to the dataflow state, the [`DataflowState::write`] method must be used.
/// This method returns a write guard to another wrapper structure, [`DataflowStateWriter`] which will
/// allow to get a reference or mutable reference to a [`DataflowState`], which starts off as a copy
/// of the actual dataflow state.
///
/// Once all the computations/modifications are done, the [`DataflowStateWriter`] must be passed on
/// to the [`DataflowStateWriter::commit`] to be committed and destroyed.
pub(super) struct DataflowStateHandle {
    /// A read/write lock protecting the [`DataflowStateWriter`] from
    /// being accessed directly and in a non-thread-safe way.
    reader: RwLock<DataflowStateReader>,
    /// A mutex used to ensure that writes are transactional (there's
    /// only one writer at a time holding an instance of [`DataflowStateWriter`]).
    write_guard: Mutex<()>,
}

impl DataflowStateHandle {
    /// Creates a new instance of [`DataflowStateHandle`].
    pub(super) fn new(
        ingredients: Graph,
        source: NodeIndex,
        ndomains: usize,
        sharding: Option<usize>,
        domain_config: DomainConfig,
        persistence: PersistenceParameters,
        materializations: Materializations,
        recipe: Recipe,
        replication_offset: Option<ReplicationOffset>,
        node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> Self {
        let dataflow_state = DataflowState::new(
            ingredients,
            source,
            ndomains,
            sharding,
            domain_config,
            persistence,
            materializations,
            recipe,
            replication_offset,
            node_restrictions,
            channel_coordinator,
        );
        Self {
            reader: RwLock::new(DataflowStateReader {
                state: dataflow_state,
            }),
            write_guard: Mutex::new(()),
        }
    }

    /// Acquires a read lock over the dataflow state, and returns the
    /// read guard.
    /// This method will block if there's a [`DataflowStateHandle::commit`] operation
    /// taking place.
    pub(super) async fn read(&self) -> RwLockReadGuard<'_, DataflowStateReader> {
        self.reader.read().await
    }

    /// Creates a new instance of a [`DataflowStateWriter`].
    /// This method will block if there's a [`DataflowStateHandle::commit`] operation
    /// taking place, or if there exists a thread that owns
    /// an instance of [`DataflowStateWriter`].
    pub(super) async fn write(&self) -> DataflowStateWriter<'_> {
        let write_guard = self.write_guard.lock().await;
        let read_guard = self.reader.read().await;
        let start = Instant::now();
        let state_copy = read_guard.state.clone();
        let elapsed = start.elapsed();
        histogram!(
            noria::metrics::recorded::DATAFLOW_STATE_CLONE_TIME,
            elapsed.as_micros() as f64,
        );
        DataflowStateWriter {
            state: state_copy,
            _guard: write_guard,
        }
    }

    /// Commits the changes made to the dataflow state.
    /// This method will block if there are threads reading the dataflow state.
    pub(super) async fn commit(&self, writer: DataflowStateWriter<'_>) {
        let mut state_guard = self.reader.write().await;
        state_guard.replace(writer.state);
    }
}

/// A read-only wrapper around the dataflow state.
/// This struct implements [`Deref`] in order to provide read-only access to the inner [`DataflowState`].
/// No implementation of [`DerefMut`] is provided, nor any other way of accessing the inner
/// [`DataflowState`] in a mutable way.
pub(super) struct DataflowStateReader {
    state: DataflowState,
}

impl DataflowStateReader {
    /// Replaces the dataflow state with a new one.
    /// This method is meant to be used by the [`DataflowStateHandle`] only, in order
    /// to atomically swap the dataflow state view exposed to the users.
    // This method MUST NEVER become public, as this guarantees
    // that only the [`DataflowStateHandle`] can modify it.
    fn replace(&mut self, state: DataflowState) {
        self.state = state;
    }
}

impl Deref for DataflowStateReader {
    type Target = DataflowState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// A read and write wrapper around the dataflow state.
/// This struct implements [`Deref`] to provide read-only access to the inner [`DataflowState`],
/// as well as [`DerefMut`] to allow mutability.
/// To commit the modifications made to the [`DataflowState`], use the [`DataflowStateHandle::commit`] method.
/// Dropping this struct without calling [`DataflowStateHandle::commit`] will cause the modifications
/// to be discarded, and the lock preventing other writer threads to acquiring an instance to be released.
pub(super) struct DataflowStateWriter<'handle> {
    state: DataflowState,
    _guard: MutexGuard<'handle, ()>,
}

impl<'handle> AsRef<DataflowState> for DataflowStateWriter<'handle> {
    fn as_ref(&self) -> &DataflowState {
        &self.state
    }
}

impl<'handle> AsMut<DataflowState> for DataflowStateWriter<'handle> {
    fn as_mut(&mut self) -> &mut DataflowState {
        &mut self.state
    }
}

// There is a chain of not thread-safe (not [`Send`] structures at play here:
// [`Graph`] is not [`Send`] (as it might contain a [`reader_map::WriteHandle`]), which
// makes [`DataflowState`] not [`Send`].
// Because [`DataflowStateReader`] holds a [`DataflowState`] instance, the compiler does not
// automatically implement [`Send`] for it. But here is what the compiler does not know:
// 1. Only the [`DataflowStateHandle`] can instantiate and hold an instance of [`DataflowStateReader`].
// 2. Only the [`DataflowStateHandle`] is able to get a mutable reference of the [`DataflowStateReader`].
// 2. The [`DataflowStateReader`] held by the [`DataflowStateHandle`] is behind a
// [`tokio::sync::RwLock`], which is only acquired as write in the [`DataflowStateHandle::commit`]
// method.
//
// Those three conditions guarantee that there are no concurrent modifications to the underlying
// dataflow state.
// So, we explicitly tell the compiler that the [`DataflowStateReader`] is safe to be moved
// between threads.
unsafe impl Sync for DataflowStateReader {}

pub(super) fn graphviz(
    graph: &Graph,
    detailed: bool,
    materializations: &Materializations,
) -> String {
    let mut s = String::new();

    let indentln = |s: &mut String| s.push_str("    ");

    #[allow(clippy::unwrap_used)] // regex is hardcoded and valid
    fn sanitize(s: &str) -> Cow<str> {
        lazy_static! {
            static ref SANITIZE_RE: Regex = Regex::new("([<>])").unwrap();
        };
        SANITIZE_RE.replace_all(s, "\\$1")
    }

    // header.
    s.push_str("digraph {{\n");

    // global formatting.
    indentln(&mut s);
    if detailed {
        s.push_str("node [shape=record, fontsize=10]\n");
    } else {
        s.push_str("graph [ fontsize=24 fontcolor=\"#0C6fA9\", outputorder=edgesfirst ]\n");
        s.push_str("edge [ color=\"#0C6fA9\", style=bold ]\n");
        s.push_str("node [ color=\"#0C6fA9\", shape=box, style=\"rounded,bold\" ]\n");
    }

    // node descriptions.
    for index in graph.node_indices() {
        #[allow(clippy::indexing_slicing)] // just got this out of the graph
        let node = &graph[index];
        let materialization_status = materializations.get_status(index, node);
        indentln(&mut s);
        s.push_str(&format!("n{}", index.index()));
        s.push_str(sanitize(&node.describe(index, detailed, materialization_status)).as_ref());
    }

    // edges.
    for (_, edge) in graph.raw_edges().iter().enumerate() {
        indentln(&mut s);
        s.push_str(&format!(
            "n{} -> n{} [ {} ]",
            edge.source().index(),
            edge.target().index(),
            #[allow(clippy::indexing_slicing)] // just got it out of the graph
            if graph[edge.source()].is_egress() {
                "color=\"#CCCCCC\""
            } else if graph[edge.source()].is_source() {
                "style=invis"
            } else {
                ""
            }
        ));
        s.push('\n');
    }

    // footer.
    s.push_str("}}");

    s
}
