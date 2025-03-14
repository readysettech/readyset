//! Logic for incorporating changes to a ReadySet graph into an already running graph.
//!
//! Performing a migration involves a number of steps:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Timestamp ingress nodes for existing domains must be connected to new base nodes
//!  - Timestamp ingress nodes must be added to all new domains
//!  - New nodes for existing domains must be sent to those domains
//!  - New domains must be booted up
//!  - Input channels must be set up for new base nodes
//!  - The graph must be analyzed for new materializations. These materializations must be
//!    *initialized* before data starts to flow to the new nodes. This may require two domains to
//!    communicate directly, and may delay migration completion.
//!  - Index requirements must be resolved, and checked for conflicts.
//!
//! Furthermore, these must be performed in the correct *order* so as to prevent dead- or
//! livelocks. This module defines methods for performing each step in relative isolation, as well
//! as a function for performing them in the right order.
//!
//! This is split into two stages: the planning stage, where parts of the [`Leader`] are
//! cloned, and the list of new domains to spin up and messages to send to new and existing domains
//! is built (via the [`DomainMigrationPlan`]). If this completes without failing, a
//! [`MigrationPlan`] is created and then applied to the running [`Leader`].
//!
//! A failure during the planning stage is inconsequential, as no part of the running controller
//! is mutated. A failure during the apply stage currently might leave the cluster in an
//! inconsistent state. However, it is also likely that a failure in this stage is symptomatic
//! of a much larger problem (such as nodes being partitioned), seeing as the only things that
//! happen during application are domains being spun up and messages being sent.
//!
//! Beware, Here be slightly smaller dragons™

use std::collections::{hash_map, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use array2::Array2;
use dataflow::node::Column;
use dataflow::prelude::*;
use dataflow::{node, DomainRequest, ReaderProcessing};
use metrics::{counter, histogram};
use readyset_client::metrics::recorded;
use readyset_client::{KeyColumnIdx, ViewPlaceholder};
use readyset_data::{DfType, Dialect};
use readyset_sql::ast::Relation;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{debug, debug_span, error, info, info_span, trace};

use crate::controller::migrate::materialization::InvalidEdge;
use crate::controller::migrate::node_changes::{MigrationNodeChanges, NodeChanges};
use crate::controller::migrate::scheduling::Scheduler;
use crate::controller::state::DfState;
use crate::controller::WorkerIdentifier;

pub(crate) mod assignment;
mod augmentation;
pub(crate) mod materialization;
pub(in crate::controller) mod node_changes;
pub(in crate::controller) mod routing;
pub(in crate::controller) mod scheduling;
mod sharding;

/// The base delay used when sending follow up requests to a domain, for the exponential backoff
/// strategy
const DOMAIN_REQUEST_DELAY_BASE_BACKOFF_MS: u64 = 2;
/// The multiplication factor used when sending follow up requests to a domain, for the exponential
/// backoff strategy
const DOMAIN_REQUEST_DELAY_BACKOFF_FACTOR: u64 = 2;
/// The max possible delay used when sending follow up requests to a domain, for the exponential
/// backoff strategy
const DOMAIN_REQUEST_MAX_DELAY_IN_MS: u64 = 1000 * 60; // 1 min

/// A [`DomainRequest`] with associated domain/shard information describing which domain it's for.
///
/// Used as part of [`DomainMigrationPlan`].
#[derive(Debug)]
pub struct StoredDomainRequest {
    /// The index of the destination domain.
    pub domain: DomainIndex,
    /// A specific shard to send the request to. If `None`, sends to all shards.
    pub shard: Option<usize>,
    /// The request to send.
    pub req: DomainRequest,
}

impl StoredDomainRequest {
    /// Sends the request to the domain.
    ///
    /// Optionally returns another request to be sent afterwards as a follow up (up to the caller to
    /// decide when to send it).
    pub async fn apply(
        mut self,
        mainline: &DfState,
        just_placed_shard_replicas: &HashMap<DomainIndex, Array2<bool>>,
    ) -> ReadySetResult<Option<StoredDomainRequest>> {
        trace!(req=?self, "Applying domain request");
        let dom =
            mainline
                .domains
                .get(&self.domain)
                .ok_or_else(|| ReadySetError::UnknownDomain {
                    domain_index: self.domain.index(),
                })?;

        let placed_replicas = |domain: DomainIndex| -> ReadySetResult<Option<Vec<usize>>> {
            just_placed_shard_replicas
                .get(&domain)
                .map(|placed| match self.shard {
                    Some(shard) => Ok(placed
                        .get_column(shard)
                        .ok_or_else(|| ReadySetError::ShardIndexOutOfBounds {
                            shard,
                            domain_index: domain.into(),
                            num_shards: placed.row_size(),
                        })?
                        .enumerate()
                        .filter_map(|(replica, placed)| (*placed).then_some(replica))
                        .collect()),
                    None => Ok(placed
                        .columns()
                        .enumerate()
                        .filter_map(|(replica, mut shards)| {
                            shards.all(|placed| *placed).then_some(replica)
                        })
                        .collect()),
                })
                .transpose()
        };

        match self.req {
            DomainRequest::QueryReplayDone { node } => {
                debug!("waiting for a done message");

                invariant!(self.shard.is_none()); // QueryReplayDone isn't ever sent to just one shard

                let mut spins = 0;
                let mut non_completed_replicas: BTreeSet<_> = match placed_replicas(self.domain)? {
                    Some(replicas) => replicas.into_iter().collect(),
                    None => {
                        let dh = mainline.domains.get(&self.domain).ok_or_else(|| {
                            ReadySetError::UnknownDomain {
                                domain_index: self.domain.into(),
                            }
                        })?;
                        (0..dh.num_replicas()).collect()
                    }
                };

                // FIXME(eta): this is a bit of a hack... (also, timeouts?)
                loop {
                    if non_completed_replicas.is_empty() {
                        break;
                    }

                    let res = dom
                        .send_to_healthy_replicas::<bool, _>(
                            DomainRequest::QueryReplayDone { node },
                            non_completed_replicas.iter().copied(),
                            &mainline.workers,
                        )
                        .await?;

                    for replicas_done in res.rows() {
                        for (replica, done) in non_completed_replicas
                            .clone()
                            .into_iter()
                            .zip(replicas_done)
                        {
                            if done.unwrap_or(true) {
                                non_completed_replicas.remove(&replica);
                            }
                        }
                    }

                    if res.into_cells().into_iter().all(|done| {
                        done.unwrap_or(
                            true, /* If the domain isn't running, we don't care if it's done */
                        )
                    }) {
                        break;
                    }

                    spins += 1;
                    if spins == 10 {
                        info!(
                            domain = %self.domain,
                            "waiting for full replay to complete"
                        );
                        spins = 0;
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
            DomainRequest::RemoveNodes { .. } => {
                match dom.send_to_healthy::<()>(self.req, &mainline.workers).await {
                    // The worker failing is an even more efficient way to remove nodes.
                    Ok(_) | Err(ReadySetError::WorkerFailed { .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            // If IsReady returns `false`, we need to send it again later
            DomainRequest::IsReady { node } => {
                trace!(request = ?self.req.clone(), node = node.id(), "sending domain ready/is_ready request");
                let req = self.req.clone();
                let is_ready = if let Some(shard) = self.shard {
                    dom.send_to_healthy_shard::<bool>(shard, req, &mainline.workers)
                        .await?
                        .into_iter()
                        .all(|t| {
                            t.unwrap_or(
                                true, /* If the domain isn't running, we don't care if it's ready */
                            )
                        })
                } else {
                    dom.send_to_healthy::<bool>(req, &mainline.workers)
                        .await?
                        .into_cells()
                        .into_iter()
                        .all(|t| {
                            t.unwrap_or(
                                true, /* If the domain isn't running, we don't care if it's ready */
                            )
                        })
                };
                trace!(
                    request = ?self.req,
                    node = node.id(),
                    ready = is_ready,
                    "received response from domain"
                );
                if !is_ready {
                    trace!(node = node.id(), "node is not ready yet");
                    return Ok(Some(StoredDomainRequest {
                        domain: self.domain,
                        shard: self.shard,
                        req: DomainRequest::IsReady { node },
                    }));
                }
            }
            DomainRequest::StartReplay {
                ref mut replicas,
                targeting_domain,
                ..
            } => {
                let target_domain = just_placed_shard_replicas
                    .get(&targeting_domain)
                    .ok_or_else(|| ReadySetError::UnknownDomain {
                        domain_index: targeting_domain.into(),
                    })?;

                if !target_domain.cells().iter().all(|x| *x) {
                    *replicas = placed_replicas(targeting_domain)?;
                }

                if let Some(shard) = self.shard {
                    dom.send_to_healthy_shard::<()>(shard, self.req, &mainline.workers)
                        .await?;
                } else {
                    dom.send_to_healthy::<()>(self.req, &mainline.workers)
                        .await?;
                }
            }
            _ => {
                if let Some(shard) = self.shard {
                    dom.send_to_healthy_shard::<()>(shard, self.req, &mainline.workers)
                        .await?;
                } else {
                    dom.send_to_healthy::<()>(self.req, &mainline.workers)
                        .await?;
                }
            }
        }
        Ok(None)
    }
}

/// A request to place a new domain, corresponding to the arguments passed to
/// [`Leader::place_domain`].
///
/// Used as part of [`DomainMigrationPlan`].
#[derive(Debug)]
pub struct PlaceRequest {
    /// The index the new domain will have.
    idx: DomainIndex,
    /// A map from domain shard, to replica index, to the worker to schedule the domain shard onto.
    shard_replica_workers: Array2<Option<WorkerIdentifier>>,
    /// Indices of new nodes to add.
    nodes: Vec<NodeIndex>,
}

/// Runtime configuration for a domain
#[derive(Debug, Clone, Copy)]
pub struct DomainSettings {
    /// The number of times the domain is sharded
    pub num_shards: usize,
    /// The number of times each shard of the domain is replicated
    pub num_replicas: usize,
}

/// Mode for constructing and executing a [`DomainMigrationPlan`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainMigrationMode {
    /// This plan is adding new nodes to an existing graph
    Extend,
    /// This plan is constructing domains, recovering the replay paths and performing full replays
    /// for running (replicas of) domains already within the graph
    Recover,
}

impl DomainMigrationMode {
    /// Returns `true` if the domain migration mode is [`Recover`].
    ///
    /// [`Recover`]: DomainMigrationMode::Recover
    #[must_use]
    pub fn is_recover(&self) -> bool {
        matches!(self, Self::Recover)
    }
}

/// A store for planned migration operations (spawning domains and sending messages).
///
/// This behaves a bit like a map of `DomainHandle`s.
#[derive(Debug)]
#[must_use]
pub struct DomainMigrationPlan {
    mode: DomainMigrationMode,
    /// An (ordered!) list of domain requests to send on application.
    stored: VecDeque<StoredDomainRequest>,
    /// A list of domains to instantiate on application.
    place: Vec<PlaceRequest>,
    /// A list of replicas which could not be placed, because no worker was available for them to
    /// run on
    failed_placement: Vec<ReplicaAddress>,
    /// A map of valid domain indices to the settings for that domain.
    domains: HashMap<DomainIndex, DomainSettings>,
}

/// A set of stored data sufficient to apply a migration.
pub struct MigrationPlan<'df> {
    dataflow_state: &'df mut DfState,
    dmp: DomainMigrationPlan,
}

impl MigrationPlan<'_> {
    /// Apply the migration plan to the provided `Leader`.
    ///
    /// If the plan fails, the `Leader`'s state is left unchanged; however, no attempt
    /// is made to roll back any destructive changes that may have occurred before the plan failed
    /// to apply.
    pub async fn apply(self) -> ReadySetResult<()> {
        let MigrationPlan {
            dataflow_state,
            dmp,
        } = self;

        debug!(
            new_domains = dmp.place.len(),
            messages = dmp.stored.len(),
            "applying migration plan",
        );

        let start = Instant::now();

        match dmp.apply(dataflow_state).await {
            Ok(_) => {
                debug!(ms = %start.elapsed().as_millis(), "migration plan applied");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "migration plan apply failed");
                Err(ReadySetError::MigrationApplyFailed {
                    source: Box::new(e),
                })
            }
        }
    }
}

impl DomainMigrationPlan {
    /// Make a new `DomainMigrationPlan` with the given mode and domains
    pub fn new(mode: DomainMigrationMode, domains: HashMap<DomainIndex, DomainSettings>) -> Self {
        Self {
            stored: VecDeque::new(),
            place: vec![],
            mode,
            domains,
            failed_placement: vec![],
        }
    }

    pub fn set_domain_settings(&mut self, idx: DomainIndex, settings: DomainSettings) {
        self.domains.insert(idx, settings);
    }

    /// Enqueues a request to add a new domain `idx` with `nodes`, running on a worker per-shard
    /// given by `shard_replica_workers`
    ///
    /// Arguments are passed to [`Leader::place_domain`] when the plan is applied.
    pub fn place_domain(
        &mut self,
        idx: DomainIndex,
        shard_replica_workers: Array2<Option<WorkerIdentifier>>,
        nodes: Vec<NodeIndex>,
    ) {
        self.place.push(PlaceRequest {
            idx,
            shard_replica_workers,
            nodes,
        });
    }

    /// Mark that a given replica could not be placed because because no worker was available for it
    /// to run on
    pub fn replica_failed_placement(&mut self, replica: ReplicaAddress) {
        self.failed_placement.push(replica);
    }

    /// Return the number of shards a given domain has.
    pub fn num_shards(&self, domain: DomainIndex) -> ReadySetResult<usize> {
        Ok(self
            .domains
            .get(&domain)
            .copied()
            .ok_or_else(|| ReadySetError::UnknownDomain {
                domain_index: domain.index(),
            })?
            .num_shards)
    }

    /// Returns the number of times each shard of a given domain is replicated
    pub fn num_replicas(&self, domain: DomainIndex) -> ReadySetResult<usize> {
        Ok(self
            .domains
            .get(&domain)
            .ok_or_else(|| ReadySetError::UnknownDomain {
                domain_index: domain.index(),
            })?
            .num_replicas)
    }

    /// Apply all stored changes using the given controller object, placing new domains and sending
    /// messages added since the last time this method was called.
    pub async fn apply(self, mainline: &mut DfState) -> ReadySetResult<()> {
        // First, tell all the workers to run the domains
        //
        // While we're doing this, we also maintain a map of all the domains' shard replicas which
        // have *just* been placed, so that we know what (if not all) replicas to send messages to
        let mut just_placed_shard_replicas = mainline
            .domains
            .iter()
            .map(|(di, dh)| (*di, dh.placed_shard_replicas()))
            .collect::<HashMap<_, _>>();
        for place in self.place {
            match just_placed_shard_replicas.entry(place.idx) {
                hash_map::Entry::Occupied(mut e) => {
                    for (pos, addr) in place.shard_replica_workers.entries() {
                        // Only true if the replica was *not* placed before, and is now
                        e.get_mut()[pos] ^= addr.is_some();
                    }
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(place.shard_replica_workers.map(|addr| addr.is_some()));
                }
            }

            let handle = mainline
                .place_domain(place.idx, place.shard_replica_workers, place.nodes)
                .await?;

            match mainline.domains.entry(place.idx) {
                hash_map::Entry::Occupied(mut e) => e.get_mut().merge(handle),
                hash_map::Entry::Vacant(e) => {
                    e.insert(handle);
                }
            }
        }

        // Send all the stored requests to the domain.
        // If a request returned a follow-up request, then we prioritize sending that one,
        // and we wait a bit, since follow-up requests are meant to poll domains for base table
        // nodes that might not be ready yet.
        // TODO(fran): This feels yucky, we should revisit this in the future and think of a better
        //  abstraction.
        let mut stored = self.stored;
        let create_exponential_backoff = || {
            ExponentialBackoff::from_millis(DOMAIN_REQUEST_DELAY_BASE_BACKOFF_MS)
                .factor(DOMAIN_REQUEST_DELAY_BACKOFF_FACTOR)
                .max_delay(Duration::from_millis(DOMAIN_REQUEST_MAX_DELAY_IN_MS))
        };
        let mut retry_strategy = create_exponential_backoff();
        while let Some(req) = stored.pop_front() {
            if let Some(req) = req.apply(mainline, &just_placed_shard_replicas).await? {
                // Initializing base table nodes might take a lot of time, so we try to wait using
                // an exponential backoff strategy.
                stored.push_front(req);
                if let Some(wait) = retry_strategy.next() {
                    trace!(retry = ?wait, "Got follow-up domain request. Waiting before sending...");
                    sleep(wait).await;
                }
            } else {
                retry_strategy = create_exponential_backoff();
            }
        }

        debug!("successfully sent all domain messages for this migration!");
        Ok(())
    }

    /// Enqueue a message to be sent to all replicas of a specific shard of a domain on plan
    /// application.
    ///
    /// Like [`DomainHandle::send_to_healthy_shard_blocking`], but includes the `domain` to which
    /// the command should apply.
    pub fn add_message_for_shard(
        &mut self,
        domain: DomainIndex,
        shard: usize,
        req: DomainRequest,
    ) -> ReadySetResult<()> {
        let domain_settings =
            self.domains
                .get(&domain)
                .ok_or_else(|| ReadySetError::UnknownDomain {
                    domain_index: domain.index(),
                })?;

        if shard > domain_settings.num_shards {
            return Err(ReadySetError::ShardIndexOutOfBounds {
                shard,
                domain_index: domain.index(),
                num_shards: domain_settings.num_shards,
            });
        }

        self.stored.push_back(StoredDomainRequest {
            domain,
            shard: Some(shard),
            req,
        });
        Ok(())
    }

    /// Enqueue a message to be sent to all replicas of all shards of a domain on plan application.
    ///
    /// Like [`DomainHandle::send_to_healthy_blocking`], but includes the `domain` to which the
    /// command should apply.
    pub fn add_message(&mut self, domain: DomainIndex, req: DomainRequest) -> ReadySetResult<()> {
        if self.domains.contains_key(&domain) {
            self.stored.push_back(StoredDomainRequest {
                domain,
                shard: None,
                req,
            });
            Ok(())
        } else {
            Err(ReadySetError::UnknownDomain {
                domain_index: domain.index(),
            })
        }
    }

    /// Extend the [`DomainMigrationPlan`] with all the valid domains and messages enqueued in
    /// `other`.
    pub fn extend(&mut self, other: DomainMigrationPlan) {
        self.place.extend(other.place);
        self.stored.extend(other.stored);
        self.domains.extend(other.domains);
    }

    /// Returns list of domains which could not be placed because no worker was available for them
    /// to run on
    pub fn failed_placement(&self) -> &[ReplicaAddress] {
        &self.failed_placement
    }

    /// Return the [`DomainMigrationMode`] for this plan
    pub fn mode(&self) -> DomainMigrationMode {
        self.mode
    }

    /// Returns true if this plan is recovering the replay paths and performing replays for existing
    /// nodes, or `false` if we adding new nodes to an existing graph
    pub fn is_recovery(&self) -> bool {
        self.mode().is_recover()
    }
}

fn topo_order(dataflow_state: &DfState, nodes: &HashSet<NodeIndex>) -> Vec<NodeIndex> {
    let mut topo_list = Vec::with_capacity(nodes.len());
    let mut topo = petgraph::visit::Topo::new(&dataflow_state.ingredients);
    while let Some(node) = topo.next(&dataflow_state.ingredients) {
        if node == dataflow_state.source {
            continue;
        }
        if dataflow_state.ingredients[node].is_dropped() || !nodes.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }
    topo_list
}

#[derive(Clone)]
pub(super) enum ColumnChange {
    Add(Column, DfValue),
    Drop(usize),
    SetType(usize, DfType),
}

/// Add messages to the dmp to inform nodes that columns have been added or removed
fn inform_col_changes(
    dmp: &mut DomainMigrationPlan,
    columns: &[(NodeIndex, ColumnChange)],
    ingredients: &Graph,
) -> ReadySetResult<()> {
    // Tell all base nodes and base ingress children about newly added columns
    for (ni, change) in columns {
        let mut inform = if let ColumnChange::Add(..) = change {
            // we need to inform all of the base's children too,
            // so that they know to add columns to existing records when replaying
            ingredients
                .neighbors_directed(*ni, petgraph::EdgeDirection::Outgoing)
                .filter(|&eni| ingredients[eni].is_egress())
                .flat_map(|eni| {
                    // find ingresses under this egress
                    ingredients.neighbors_directed(eni, petgraph::EdgeDirection::Outgoing)
                })
                .collect()
        } else {
            // ingress nodes don't need to know about deleted columns, because those are only
            // relevant when new writes enter the graph.
            Vec::new()
        };
        inform.push(*ni);

        for ni in inform {
            let n = &ingredients[ni];
            let m = match change.clone() {
                ColumnChange::Add(column, default) => DomainRequest::AddBaseColumn {
                    node: n.local_addr(),
                    column,
                    default,
                },
                ColumnChange::Drop(column) => DomainRequest::DropBaseColumn {
                    node: n.local_addr(),
                    column,
                },
                ColumnChange::SetType(column, new_type) => DomainRequest::SetColumnType {
                    node: n.local_addr(),
                    column,
                    new_type,
                },
            };

            dmp.add_message(n.domain(), m)?;
        }
    }
    Ok(())
}

/// A `Migration` encapsulates a number of changes to the dataflow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'df> {
    pub(super) dataflow_state: &'df mut DfState,
    pub(super) changes: MigrationNodeChanges,
    pub(super) columns: Vec<(NodeIndex, ColumnChange)>,
    pub(super) readers: HashMap<NodeIndex, NodeIndex>,
    pub(super) worker: Option<WorkerIdentifier>,
    pub(super) dialect: Dialect,
    pub(super) start: Instant,
}

impl<'df> Migration<'df> {
    pub(super) fn new(dataflow_state: &'df mut DfState, dialect: Dialect) -> Self {
        Self {
            dataflow_state,
            changes: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            worker: None,
            dialect,
            start: Instant::now(),
        }
    }

    /// Add the given `Ingredient` to the dataflow graph.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<N, S2, CS, I>(&mut self, name: N, columns: CS, i: I) -> NodeIndex
    where
        N: Into<Relation>,
        S2: Into<Column>,
        CS: IntoIterator<Item = S2>,
        I: Into<NodeOperator>,
    {
        let mut i = node::Node::new::<N, S2, CS, _>(name, columns, i.into());
        i.on_connected(&self.dataflow_state.ingredients);
        // ancestors() will not return an error since i is an internal node
        #[allow(clippy::unwrap_used)]
        let parents = i.ancestors().unwrap();
        assert!(!parents.is_empty());

        // add to the graph
        let ni = self.dataflow_state.ingredients.add_node(i);

        debug!(
            node = ni.index(),
            node_type = ?self.dataflow_state.ingredients[ni],
            "adding new node"
        );

        // keep track of the fact that it's new
        self.changes.add_node(ni);
        // insert it into the graph
        for parent in parents {
            self.dataflow_state.ingredients.add_edge(parent, ni, ());
        }
        // and tell the caller its id
        ni
    }

    /// Add the given `Base` to the dataflow graph.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    pub fn add_base<N, C, CS>(&mut self, name: N, columns: CS, b: node::special::Base) -> NodeIndex
    where
        N: Into<Relation>,
        C: Into<Column>,
        CS: IntoIterator<Item = C>,
    {
        // add to the graph
        let ni = self
            .dataflow_state
            .ingredients
            .add_node(node::Node::new(name, columns, b));
        debug!(node = ni.index(), "adding new base");

        // keep track of the fact that it's new
        self.changes.add_node(ni);
        // insert it into the graph
        self.dataflow_state
            .ingredients
            .add_edge(self.dataflow_state.source, ni, ());
        // and tell the caller its id
        ni
    }

    /// Mark the given node as being beyond the materialization frontier.
    ///
    /// When a node is marked as such, it will quickly evict state after it is no longer
    /// immediately useful, making such nodes generally mostly empty. This reduces read
    /// performance (since most reads now need to do replays), but can significantly reduce memory
    /// overhead and improve write performance.
    ///
    /// Note that if a node is marked this way, all of its children transitively _also_ have to be
    /// marked.
    #[cfg(test)]
    pub(crate) fn mark_shallow(&mut self, ni: NodeIndex) {
        debug!(
            node = ni.index(),
            "marking node as beyond materialization frontier"
        );
        self.dataflow_state
            .ingredients
            .node_weight_mut(ni)
            .unwrap()
            .purge = true;

        // This unimplemented is only allowed because we don't deal with the materialization
        // frontier in production code
        #[allow(clippy::unimplemented)]
        if !self.changes.contains_new(&ni) {
            unimplemented!("marking existing node as beyond materialization frontier");
        }
    }

    /// Add a new column to a base node.
    pub fn add_column(
        &mut self,
        node: NodeIndex,
        column: Column,
        default: DfValue,
    ) -> ReadySetResult<usize> {
        // not allowed to add columns to new nodes
        invariant!(!self.changes.contains_new(&node));

        let base = &mut self.dataflow_state.ingredients[node];
        invariant!(base.is_base());

        // we need to tell the base about its new column and its default, so that old writes that
        // do not have it get the additional value added to them.
        let col_i1 = base.add_column(column.clone());
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        {
            #[allow(clippy::unwrap_used)] // previously called invariant!(base.is_base())
            let col_i2 = base.get_base_mut().unwrap().add_column(default.clone())?;
            invariant_eq!(col_i1, col_i2);
        }

        // also eventually propagate to domain clone
        self.columns
            .push((node, ColumnChange::Add(column, default)));

        Ok(col_i1)
    }

    /// Drop a column from a base node.
    pub fn drop_column(&mut self, node: NodeIndex, column: usize) -> ReadySetResult<()> {
        // not allowed to drop columns from new nodes
        invariant!(!self.changes.contains_new(&node));

        let base = &mut self.dataflow_state.ingredients[node];
        invariant!(base.is_base());

        // we need to tell the base about the dropped column, so that old writes that contain that
        // column will have it filled in with default values (this is done in Mutator).
        // we can't rely on DerefMut, since it disallows mutating Taken nodes

        #[allow(clippy::unwrap_used)] // previously called invariant!(base.is_base())
        base.get_base_mut().unwrap().drop_column(column)?;

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Drop(column)));
        Ok(())
    }

    /// Set the column type within a base node
    pub fn set_column_type(
        &mut self,
        node_index: NodeIndex,
        column: usize,
        new_type: DfType,
    ) -> ReadySetResult<()> {
        self.dataflow_state
            .ingredients
            .node_weight_mut(node_index)
            .ok_or_else(|| ReadySetError::NoSuchNode(node_index.index()))?
            .set_column_type(column, new_type.clone())?;
        self.columns
            .push((node_index, ColumnChange::SetType(column, new_type)));
        Ok(())
    }

    /// Ensure that a reader node exists as a child of `n`, optionally with the given name and set
    /// of post-lookup operations, returning the index of that reader.
    fn ensure_reader_for(
        &mut self,
        n: NodeIndex,
        name: Option<Relation>,
        reader_processing: ReaderProcessing,
    ) -> NodeIndex {
        use std::collections::hash_map::Entry;
        match self.readers.entry(n) {
            Entry::Occupied(ni) => {
                let ni = *ni.into_mut();
                debug_assert!(
                    {
                        let node = &self.dataflow_state.ingredients[ni];
                        let name_matches = name.is_none_or(|n| *node.name() == n);
                        name_matches
                            && *node
                                .as_reader()
                                .expect("non-reader in readers")
                                .reader_processing()
                                == reader_processing
                    },
                    "existing reader state doesn't meet requirements"
                );
                ni
            }
            Entry::Vacant(e) => {
                // make a reader
                let r = node::special::Reader::new(n, reader_processing);

                let mut r = if let Some(name) = name {
                    self.dataflow_state.ingredients[n].named_mirror(r, name)
                } else {
                    self.dataflow_state.ingredients[n].mirror(r)
                };
                if r.name().name.starts_with("SHALLOW_") {
                    r.purge = true;
                }
                let r = self.dataflow_state.ingredients.add_node(r);
                self.dataflow_state.ingredients.add_edge(n, r, ());
                self.changes.add_node(r);
                *e.insert(r)
            }
        }
    }

    /// Set up the given node such that its output can be efficiently queried.
    #[cfg(test)]
    pub fn maintain_anonymous(&mut self, n: NodeIndex, index: &Index) -> NodeIndex {
        let ri = self.ensure_reader_for(n, None, Default::default());

        self.dataflow_state.ingredients[ri]
            .as_mut_reader()
            .unwrap()
            .set_index(index);

        ri
    }

    /// Set up the given node such that its output can be efficiently queried, with the given
    /// [`ReaderProcessing`] operations to be performed on the results of all lookups
    #[cfg(test)]
    pub fn maintain_anonymous_with_reader_processing(
        &mut self,
        n: NodeIndex,
        index: &Index,
        reader_processing: ReaderProcessing,
    ) -> NodeIndex {
        let ri = self.ensure_reader_for(n, None, reader_processing);

        self.dataflow_state.ingredients[ri]
            .as_mut_reader()
            .unwrap()
            .set_index(index);

        ri
    }

    /// Set up the given node such that its output can be efficiently queried.
    pub fn maintain(
        &mut self,
        name: Relation,
        n: NodeIndex,
        index: &Index,
        reader_processing: ReaderProcessing,
        placeholder_map: Vec<(ViewPlaceholder, KeyColumnIdx)>,
    ) {
        let ri = self.ensure_reader_for(n, Some(name), reader_processing);

        let r = self.dataflow_state.ingredients[ri].as_mut_reader().unwrap();

        r.set_index(index);
        r.set_mapping(placeholder_map);
    }

    /// Build a `MigrationPlan` for this migration, and apply it if the planning stage succeeds.
    pub(super) async fn commit(self, dry_run: bool) -> ReadySetResult<()> {
        let start = self.start;

        let plan = self
            .plan()
            .map_err(|e| ReadySetError::MigrationPlanFailed {
                source: Box::new(e),
            })?;
        // We skip the actual migration when we run in dry-run mode.
        if dry_run {
            return Ok(());
        }
        plan.apply().await?;

        debug!(
            ms = ?start.elapsed().as_millis(),
            "migration planning completed"
        );

        histogram!(recorded::CONTROLLER_MIGRATION_TIME,).record(start.elapsed().as_micros() as f64);

        Ok(())
    }

    /// Build a `MigrationPlan` for this migration, computing all necessary changes to the
    /// controller to make it happen.
    ///
    /// See the module-level docs for more information on what a migration entails.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn plan(self) -> ReadySetResult<MigrationPlan<'df>> {
        let span = info_span!("plan");
        let _g = span.enter();

        let start = self.start;
        let dataflow_state = self.dataflow_state;
        let mut dmp = DomainMigrationPlan::new(
            DomainMigrationMode::Extend,
            dataflow_state.domain_settings(),
        );

        let mut added = 0;
        let mut dropped = 0;
        let columns = self.columns;
        let worker = self.worker;
        for change in self.changes.into_iter() {
            match change {
                NodeChanges::Add(new_nodes) => {
                    added += new_nodes.len();
                    dmp.extend(plan_add_nodes(dataflow_state, new_nodes, &worker)?)
                }
                NodeChanges::Drop(drop_nodes) => {
                    dropped += drop_nodes.len();
                    dmp.extend(plan_drop_nodes(dataflow_state, drop_nodes)?)
                }
            }
        }

        // We have successfully made a valid graph! Now we can inform the dmp of all the
        // changes
        inform_col_changes(&mut dmp, &columns, &dataflow_state.ingredients)?;

        debug!(
            added_nodes = added,
            dropped_nodes = dropped,
            "finalizing migration"
        );

        debug!(
            ms = ?start.elapsed().as_millis(),
            "migration planning completed"
        );

        Ok(MigrationPlan {
            dataflow_state,
            dmp,
        })
    }
}

fn plan_add_nodes(
    dataflow_state: &mut DfState,
    mut new_nodes: HashSet<NodeIndex>,
    worker: &Option<WorkerIdentifier>,
) -> ReadySetResult<DomainMigrationPlan> {
    let mut topo = topo_order(dataflow_state, &new_nodes);

    // Tracks partially materialized nodes that were duplicated as fully materialized in this
    // planning stage.
    let mut local_redundant_partial: HashMap<NodeIndex, NodeIndex> = Default::default();

    // Shard the graph as desired
    let mut swapped0 = if let Some(shards) = dataflow_state.sharding {
        let (t, swapped) = sharding::shard(
            &mut dataflow_state.ingredients,
            &mut new_nodes,
            &topo,
            shards,
        )?;
        topo = t;

        swapped
    } else {
        HashMap::default()
    };

    // Assign domains
    assignment::assign(dataflow_state, &topo)?;

    // Set up ingress and egress nodes
    let swapped1 = routing::add(dataflow_state, &mut new_nodes, &topo)?;

    // Merge the swap lists
    for ((dst, src), instead) in swapped1 {
        use std::collections::hash_map::Entry;
        match swapped0.entry((dst, src)) {
            Entry::Occupied(mut instead0) => {
                if &instead != instead0.get() {
                    // This can happen if sharding decides to add a Sharder *under* a node,
                    // and routing decides to add an ingress/egress pair between that node
                    // and the Sharder. It's perfectly okay, but we should prefer the
                    // "bottommost" swap to take place (i.e., the node that is *now*
                    // closest to the dst node). This *should* be the sharding node, unless
                    // routing added an ingress *under* the Sharder. We resolve the
                    // collision by looking at which translation currently has an adge from
                    // `src`, and then picking the *other*, since that must then be node
                    // below.
                    if dataflow_state.ingredients.find_edge(src, instead).is_some() {
                        // src -> instead -> instead0 -> [children]
                        // from [children]'s perspective, we should use instead0 for from, so
                        // we can just ignore the `instead` swap.
                    } else {
                        // src -> instead0 -> instead -> [children]
                        // from [children]'s perspective, we should use instead for src, so we
                        // need to prefer the `instead` swap.
                        *instead0.get_mut() = instead;
                    }
                }
            }
            Entry::Vacant(hole) => {
                hole.insert(instead);
            }
        }

        // we may also already have swapped the parents of some node *to* `src`. in
        // swapped0. we want to change that mapping as well, since lookups in swapped
        // aren't recursive.
        for (_, instead0) in swapped0.iter_mut() {
            if *instead0 == src {
                *instead0 = instead;
            }
        }
    }
    let mut swapped = swapped0;
    loop {
        let mut sorted_new = new_nodes.iter().collect::<Vec<_>>();
        sorted_new.sort();

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<DomainIndex> = sorted_new
            .iter()
            .filter(|&&&ni| !dataflow_state.ingredients[ni].is_dropped())
            .map(|&&ni| dataflow_state.ingredients[ni].domain())
            .collect();

        let mut domain_new_nodes = sorted_new
            .iter()
            .filter(|&&&ni| ni != dataflow_state.source)
            .filter(|&&&ni| !dataflow_state.ingredients[ni].is_dropped())
            .map(|&&ni| (dataflow_state.ingredients[ni].domain(), ni))
            .fold(HashMap::new(), |mut dns, (d, ni)| {
                #[allow(clippy::unwrap_or_default)]
                dns.entry(d).or_insert_with(Vec::new).push(ni);
                dns
            });

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_new_nodes {
            // Number of pre-existing nodes
            let mut nnodes = dataflow_state
                .domain_node_index_pairs
                .get(domain)
                .map(HashMap::len)
                .unwrap_or(0);

            if nodes.is_empty() {
                // Nothing to do here
                continue;
            }

            let span = debug_span!("domain", domain = domain.index());
            let _g = span.enter();

            // Give local addresses to every (new) node
            for &ni in nodes.iter() {
                debug!(
                    node_type = ?dataflow_state.ingredients[ni],
                    node = ni.index(),
                    local = nnodes,
                    "assigning local index"
                );
                counter!(
                    recorded::NODE_ADDED,
                    "ntype" => dataflow_state.ingredients[ni].node_type_string(),
                )
                .increment(1);
                dataflow_state
                    .domain_node_index_pairs
                    .entry(*domain)
                    .or_default()
                    .entry(ni)
                    .or_insert_with(|| {
                        // Only make a new local index for the node if it doesn't already have one,
                        // which can happen if we've added an existing node to `new_nodes` due to
                        // remapping (see [remap-nodes], below)
                        let mut ip: IndexPair = ni.into();
                        ip.set_local(LocalNodeIndex::make(nnodes as u32));

                        dataflow_state.ingredients[ni].set_finalized_addr(ip);

                        nnodes += 1;
                        ip
                    });
            }

            // Initialize each new node
            for &ni in nodes.iter() {
                // - Ingredients must contain NodeIndex
                // - domain already exists in dataflow_state
                if dataflow_state.ingredients[ni].is_internal() {
                    // Figure out all the remappings that have happened
                    // NOTE: this has to be *per node*, since a shared parent may be remapped
                    // differently to different children (due to sharding for example). we just
                    // allocate it once though.
                    let mut node_index_mappings =
                        dataflow_state.domain_node_index_pairs[domain].clone();

                    // Parents in other domains have been swapped for ingress nodes.
                    // Those ingress nodes' indices are now local.
                    for (&(dst, src), &instead) in &swapped {
                        if dst != ni {
                            // ignore mappings for other nodes
                            continue;
                        }

                        node_index_mappings.insert(
                            src,
                            dataflow_state.domain_node_index_pairs[domain][&instead],
                        );
                    }

                    trace!(node = ni.index(), "initializing new node");
                    dataflow_state
                        .ingredients
                        .node_weight_mut(ni)
                        .unwrap()
                        .on_commit(&node_index_mappings);
                }
            }
        }

        topo = topo_order(dataflow_state, &new_nodes);

        if let Some(shards) = dataflow_state.sharding {
            sharding::validate(&dataflow_state.ingredients, &topo, shards)?
        };

        // at this point, we've hooked up the graph such that, for any given domain, the graph
        // looks like this:
        //
        //      o (egress)
        //     +.\......................
        //     :  o (ingress)
        //     :  |
        //     :  o-------------+
        //     :  |             |
        //     :  o             o
        //     :  |             |
        //     :  o (egress)    o (egress)
        //     +..|...........+.|..........
        //     :  o (ingress) : o (ingress)
        //     :  |\          :  \
        //     :  | \         :   o
        //
        // etc.
        // println!("{}", dataflow_state.ingredients);

        // Since we are looping, we need to keep the dataflow_state.domain_nodes as it was
        // before the loop.
        // This is not ideal, as we need to clone this each time.
        // TODO(fran): Can we remove the loop and make the modifications in-place?
        //   Even if we don't care about performance, this loop is very hard to reason about.
        let mut domain_nodes = dataflow_state.domain_nodes.clone();

        for &ni in &new_nodes {
            let n = &dataflow_state.ingredients[ni];
            if ni != dataflow_state.source && !n.is_dropped() {
                let di = n.domain();
                domain_nodes
                    .entry(di)
                    .or_default()
                    .insert(n.local_addr(), ni);
            }
        }
        let mut uninformed_domain_nodes: HashMap<_, _> = changed_domains
            .iter()
            .map(|&di| {
                let mut m = domain_nodes[&di].values().cloned().collect::<Vec<_>>();
                m.sort();
                (di, m)
            })
            .collect();

        // Boot up new domains (they'll ignore all updates for now)
        debug!("booting new domains");
        let mut dmp = DomainMigrationPlan::new(
            DomainMigrationMode::Extend,
            dataflow_state.domain_settings(),
        );
        let mut scheduler = Scheduler::new(dataflow_state, worker)?;

        for domain in changed_domains {
            if dataflow_state.domains.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            // uninformed_domain_nodes is built from changed_domains
            #[allow(clippy::unwrap_used)]
            let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
            let worker_shards = scheduler.schedule_domain(domain, &nodes)?;

            for ((shard, replica), worker) in worker_shards.entries() {
                if worker.is_none() {
                    dmp.replica_failed_placement(ReplicaAddress {
                        domain_index: domain,
                        shard,
                        replica,
                    });
                }
            }

            let num_shards = worker_shards.num_rows();
            let num_replicas = worker_shards.row_size();
            dmp.place_domain(domain, worker_shards, nodes);
            dmp.domains.insert(
                domain,
                DomainSettings {
                    num_shards,
                    num_replicas,
                },
            );
        }

        // And now, the last piece of the puzzle -- set up materializations
        debug!("initializing new materializations");

        dataflow_state.materializations.extend(
            &mut dataflow_state.ingredients,
            &new_nodes,
            &dmp,
        )?;

        // Check to see if we've just tried to add a fully materialized node below an existing
        // partially materialized node
        if let Some(InvalidEdge { parent, child }) = dataflow_state
            .materializations
            .validate(&dataflow_state.ingredients, &new_nodes)?
        {
            debug!(
                ?child,
                ?parent,
                "rerouting full node found below partial node",
            );

            // Try to find an existing fully materialized equivalent of that partially materialized
            // parent
            let (duplicate_index, is_new) =
                if let Some(idx) = dataflow_state.materializations.get_redundant(&parent) {
                    (*idx, false)
                } else if let Some(idx) = local_redundant_partial.get(&parent) {
                    (*idx, false)
                } else {
                    // [remap-nodes]
                    // If we cant find one, create a new node in the same domain as old

                    let duplicate_node = dataflow_state.ingredients[parent].duplicate();
                    // add to graph
                    let idx = dataflow_state.ingredients.add_node(duplicate_node);
                    local_redundant_partial.insert(parent, idx);
                    // Add the child node to `new_nodes`, so that on the next iteration of the
                    // loop we make sure that any lookup obligations into the duplicated parent
                    // are satisfied
                    new_nodes.insert(child);
                    dataflow_state.ingredients[child].replace_sibling(parent, idx);
                    (idx, true)
                };

            dataflow_state
                .ingredients
                .add_edge(duplicate_index, child, ());
            if is_new {
                // Recreate edges coming into parent on duplicate
                let incoming: Vec<_> = dataflow_state
                    .ingredients
                    .neighbors_directed(parent, petgraph::EdgeDirection::Incoming)
                    .collect();
                for ni in incoming {
                    dataflow_state.ingredients.add_edge(ni, duplicate_index, ());
                }
                // Add to new nodes for processing in next loop iteration
                new_nodes.insert(duplicate_index);
            }
            // Indicate that the incoming nodes have changed. This entry will be read during
            // the remapping stage in the next iteration of the loop
            swapped.insert((child, parent), duplicate_index);
            // remove old edge
            #[allow(clippy::unwrap_used)]
            // we just found this edge in Materializations::validate()
            let old_edge = dataflow_state.ingredients.find_edge(parent, child).unwrap();
            dataflow_state.ingredients.remove_edge(old_edge);
        } else {
            dataflow_state.domain_nodes = domain_nodes;

            // Add any new nodes to existing domains (they'll also ignore all updates for now)
            debug!("mutating existing domains");
            augmentation::inform(
                dataflow_state,
                &mut dmp,
                uninformed_domain_nodes,
                &new_nodes,
            )?;

            // Set up inter-domain connections
            debug!("bringing up inter-domain connections");
            routing::connect(&dataflow_state.ingredients, &mut dmp, &new_nodes)?;

            dataflow_state.materializations.commit(
                &mut dataflow_state.ingredients,
                &new_nodes,
                &mut dmp,
            )?;

            dataflow_state
                .materializations
                .extend_redundant_partial(local_redundant_partial);
            return Ok(dmp);
        }
    }
}

fn plan_drop_nodes(
    dataflow_state: &mut DfState,
    removals: HashSet<NodeIndex>,
) -> ReadySetResult<DomainMigrationPlan> {
    let mut dmp = DomainMigrationPlan::new(
        DomainMigrationMode::Extend,
        dataflow_state.domain_settings(),
    );
    remove_nodes(dataflow_state, &mut dmp, &removals)?;
    Ok(dmp)
}

fn remove_nodes(
    dataflow_state: &mut DfState,
    dmp: &mut DomainMigrationPlan,
    removals: &HashSet<NodeIndex>,
) -> ReadySetResult<()> {
    // Remove node from controller local state
    let mut domain_removals: HashMap<DomainIndex, Vec<LocalNodeIndex>> = HashMap::default();
    for ni in removals {
        let node = dataflow_state
            .ingredients
            .node_weight_mut(*ni)
            .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?;
        node.remove();
        debug!(node = %ni.index(), "Removed node");
        domain_removals
            .entry(node.domain())
            .or_default()
            .push(node.local_addr())
    }

    // Send messages to domains
    for (domain, nodes) in domain_removals {
        trace!(
            domain_index = %domain.index(),
            "Storing domain request for node removals",
        );

        dmp.stored.push_back(StoredDomainRequest {
            domain,
            shard: None,
            req: DomainRequest::RemoveNodes { nodes },
        });
    }

    Ok(())
}
