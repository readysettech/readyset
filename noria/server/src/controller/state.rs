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

use crate::controller::domain_handle::DomainHandle;
use crate::controller::migrate::materialization::Materializations;
use crate::controller::migrate::scheduling::Scheduler;
use crate::controller::migrate::{routing, DomainMigrationPlan, Migration};
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
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use std::{cell, time};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, instrument, trace, warn};
use vec1::Vec1;

/// Number of concurrent requests to make when making multiple simultaneous requests to domains (eg
/// for replication offsets)
const CONCURRENT_REQUESTS: usize = 16;

/// This structure holds all the dataflow state.
/// It's meant to be handled exclusively by the [`DataflowStateHandle`], which is the structure
/// that guarantees thread-safe access to it.
#[derive(Clone, Serialize, Deserialize)]
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
    persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    pub(super) recipe: Recipe,
    /// Latest replication position for the schema if from replica or binlog
    schema_replication_offset: Option<ReplicationOffset>,
    /// Placement restrictions for nodes and the domains they are placed into.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub(super) node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,

    #[serde(skip)]
    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub(super) domain_nodes: HashMap<DomainIndex, NodeMap<NodeIndex>>,
    #[serde(skip)]
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,

    /// Map from worker URI to the address the worker is listening on for reads.
    #[serde(skip)]
    pub(super) read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    #[serde(skip)]
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,

    /// State between migrations
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    /// Whether to keep prior recipes when modifying data flow state. This should
    /// only be set to true in tests where we are sure it does not break things,
    /// such as logictests where we may OOM from the recipe size.
    // TODO(ENG-838): Remove when dataflow state does not keep entire recipe chain.
    keep_prior_recipes: bool,
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
        keep_prior_recipes: bool,
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
            keep_prior_recipes,
        }
    }

    pub(super) fn schema_replication_offset(&self) -> &Option<ReplicationOffset> {
        &self.schema_replication_offset
    }

    pub(super) fn get_info(&self) -> ReadySetResult<GraphInfo> {
        let mut worker_info = HashMap::new();
        for (di, dh) in self.domains.iter() {
            for (i, shard) in dh.shards.iter().enumerate() {
                worker_info
                    .entry(shard.clone())
                    .or_insert_with(HashMap::new)
                    .entry(DomainKey(*di, i))
                    .or_insert_with(Vec::new)
                    .extend(
                        self.domain_nodes
                            .get(di)
                            .ok_or_else(|| {
                                internal_err(format!("{:?} in domains but not in domain_nodes", di))
                            })?
                            .values(),
                    )
            }
        }
        Ok(GraphInfo {
            workers: worker_info,
        })
    }

    /// Get a map of all known input nodes, mapping the name of the node to that node's
    /// [index](NodeIndex)
    ///
    /// Input nodes are here all nodes of type `Table`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub(super) fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let base = &self.ingredients[n];

                if base.is_dropped() {
                    None
                } else {
                    assert!(base.is_base());
                    Some((base.name().to_owned(), n))
                }
            })
            .collect()
    }

    /// Get a map of all known output nodes, mapping the name of the node to that node's
    /// [index](NodeIndex)
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    pub(super) fn outputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let name = self.ingredients[n].name().to_owned();
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                self.ingredients[n].as_reader().map(|r| {
                    // we want to give the the node address that is being materialized not that of
                    // the reader node itself.
                    (name, r.is_for())
                })
            })
            .collect()
    }

    /// Get a map of all known output nodes, mapping the name of the node to the `SqlQuery`
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration
    pub(super) fn verbose_outputs(&self) -> BTreeMap<String, nom_sql::SqlQuery> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                if self.ingredients[n].is_reader() {
                    #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                    let name = self.ingredients[n].name().to_owned();

                    // Alias should always resolve to an id and id should always resolve to an
                    // expression. However, this mapping will not catch bugs that break this
                    // assumption
                    let id = self.recipe.id_from_alias(&name);
                    let expr = id.and_then(|id| self.recipe.expression(id));
                    expr.map(|e| (name, e.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn find_readers_for(
        &self,
        node: NodeIndex,
        name: &str,
        filter: &Option<ViewFilter>,
    ) -> Vec<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        let mut nodes: Vec<NodeIndex> = Vec::new();
        let mut bfs = Bfs::new(&self.ingredients, node);
        while let Some(child) = bfs.next(&self.ingredients) {
            #[allow(clippy::indexing_slicing)] // just came from self.ingredients
            if self.ingredients[child].is_reader_for(node) && self.ingredients[child].name() == name
            {
                // Check for any filter requirements we can satisfy when
                // traversing the data flow graph, `filter`.
                if let Some(ViewFilter::Workers(w)) = filter {
                    #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                    let domain = self.ingredients[child].domain();
                    for worker in w {
                        if self
                            .domains
                            .get(&domain)
                            .map(|dh| dh.assigned_to_worker(worker))
                            .unwrap_or(false)
                        {
                            nodes.push(child);
                        }
                    }
                } else {
                    nodes.push(child);
                }
            }
        }
        nodes
    }

    /// Obtain a `ViewBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    pub(super) fn view_builder(
        &self,
        view_req: ViewRequest,
    ) -> Result<Option<ViewBuilder>, ReadySetError> {
        // first try to resolve the node via the recipe, which handles aliasing between identical
        // queries.
        let name = view_req.name.as_str();
        let node = match self.recipe.node_addr_for(name) {
            Ok(ni) => ni,
            Err(_) => {
                // if the recipe doesn't know about this query, traverse the graph.
                // we need this do deal with manually constructed graphs (e.g., in tests).
                if let Some(res) = self.outputs().get(name) {
                    *res
                } else {
                    return Ok(None);
                }
            }
        };

        let name = match self.recipe.resolve_alias(name) {
            None => name,
            Some(alias) => alias,
        };

        let readers = self.find_readers_for(node, name, &view_req.filter);
        if readers.is_empty() {
            return Ok(None);
        }
        let mut replicas: Vec<ViewReplica> = Vec::new();
        for r in readers {
            #[allow(clippy::indexing_slicing)] // `find_readers_for` returns valid indices
            let domain_index = self.ingredients[r].domain();
            #[allow(clippy::indexing_slicing)] // `find_readers_for` returns valid indices
            let reader =
                self.ingredients[r]
                    .as_reader()
                    .ok_or_else(|| ReadySetError::InvalidNodeType {
                        node_index: self.ingredients[r].local_addr().id(),
                        expected_type: NodeType::Reader,
                    })?;
            #[allow(clippy::indexing_slicing)] // `find_readers_for` returns valid indices
            let returned_cols = reader
                .post_lookup()
                .returned_cols
                .clone()
                .unwrap_or_else(|| (0..self.ingredients[r].fields().len()).collect());
            #[allow(clippy::indexing_slicing)] // just came from self
            let fields = self.ingredients[r].fields();
            let columns = returned_cols
                .iter()
                .map(|idx| fields.get(*idx).cloned())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| internal_err("Schema expects valid column indices"))?;

            let key_mapping = Vec::from(reader.mapping());

            let schema = self.view_schema(r)?;
            let domain =
                self.domains
                    .get(&domain_index)
                    .ok_or_else(|| ReadySetError::NoSuchDomain {
                        domain_index: domain_index.index(),
                        shard: 0,
                    })?;
            let shards = (0..domain.shards())
                .map(|i| {
                    Ok(ReplicaShard {
                        addr: *self.read_addrs.get(&domain.assignment(i)?).ok_or_else(|| {
                            ReadySetError::UnmappableDomain {
                                domain_index: domain_index.index(),
                            }
                        })?,
                        region: self
                            .workers
                            .get(&domain.assignment(i)?)
                            .ok_or_else(|| ReadySetError::UnmappableDomain {
                                domain_index: domain_index.index(),
                            })?
                            .region
                            .clone(),
                    })
                })
                .collect::<ReadySetResult<Vec<_>>>()?;
            replicas.push(ViewReplica {
                node: r,
                columns: columns.into(),
                schema,
                shards,
                key_mapping,
            });
        }

        Ok(Some(ViewBuilder {
            name: name.to_owned(),
            replicas: Vec1::try_from_vec(replicas)
                .map_err(|_| ReadySetError::ViewNotFound(view_req.name))?,
        }))
    }

    pub(super) fn view_schema(
        &self,
        view_ni: NodeIndex,
    ) -> Result<Option<ViewSchema>, ReadySetError> {
        let n =
            self.ingredients
                .node_weight(view_ni)
                .ok_or_else(|| ReadySetError::NodeNotFound {
                    index: view_ni.index(),
                })?;
        let reader = n
            .as_reader()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: n.local_addr().id(),
                expected_type: NodeType::Reader,
            })?;
        let returned_cols = reader
            .post_lookup()
            .returned_cols
            .clone()
            .unwrap_or_else(|| (0..n.fields().len()).collect());

        let projected_schema = (0..n.fields().len())
            .map(|i| schema::column_schema(&self.ingredients, view_ni, &self.recipe, i))
            .collect::<Result<Vec<_>, ReadySetError>>()?
            .into_iter()
            .collect::<Option<Vec<_>>>();

        let returned_schema = returned_cols
            .iter()
            .map(|idx| schema::column_schema(&self.ingredients, view_ni, &self.recipe, *idx))
            .collect::<Result<Vec<_>, ReadySetError>>()?
            .into_iter()
            .collect::<Option<Vec<_>>>();

        match (projected_schema, returned_schema) {
            (None, _) => Ok(None),
            (_, None) => Ok(None),
            (Some(p), Some(r)) => Ok(Some(ViewSchema::new(r, p))),
        }
    }

    /// Obtain a TableBuilder that can be used to construct a Table to perform writes and deletes
    /// from the given named base node.
    pub(super) fn table_builder(&self, base: &str) -> ReadySetResult<Option<TableBuilder>> {
        let ni = match self.recipe.node_addr_for(base) {
            Ok(ni) => ni,
            Err(_) => *self
                .inputs()
                .get(base)
                .ok_or_else(|| ReadySetError::TableNotFound(base.into()))?,
        };
        let node = self
            .ingredients
            .node_weight(ni)
            .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?;

        trace!(%base, "creating table");

        let mut key = node
            .get_base()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: node.local_addr().id(),
                expected_type: NodeType::Base,
            })?
            .primary_key()
            .map(|k| k.to_owned())
            .unwrap_or_default();

        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col, _) = node.sharded_by() {
                key = vec![col];
            }
        } else {
            is_primary = true;
        }

        let txs = (0..self
            .domains
            .get(&node.domain())
            .ok_or_else(|| ReadySetError::NoSuchDomain {
                domain_index: node.domain().index(),
                shard: 0,
            })?
            .shards())
            .map(|i| {
                self.channel_coordinator
                    .get_addr(&(node.domain(), i))
                    .ok_or_else(|| {
                        internal_err(format!(
                            "failed to get channel coordinator for {}.{}",
                            node.domain().index(),
                            i
                        ))
                    })
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        let base_operator = node
            .get_base()
            .ok_or_else(|| internal_err("asked to get table for non-base node"))?;
        let columns: Vec<String> = node
            .fields()
            .iter()
            .enumerate()
            .filter(|&(n, _)| !base_operator.get_dropped().contains_key(n))
            .map(|(_, s)| s.clone())
            .collect();
        invariant_eq!(
            columns.len(),
            node.fields().len() - base_operator.get_dropped().len()
        );
        let schema = self
            .recipe
            .schema_for(base)
            .map(|s| -> ReadySetResult<_> {
                match s {
                    Schema::Table(s) => Ok(s),
                    _ => internal!("non-base schema {:?} returned for table '{}'", s, base),
                }
            })
            .transpose()?;

        Ok(Some(TableBuilder {
            txs,
            ni: node.global_addr(),
            addr: node.local_addr(),
            key,
            key_is_primary: is_primary,
            dropped: base_operator.get_dropped(),
            table_name: node.name().to_owned(),
            columns,
            schema,
        }))
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub(super) async fn get_statistics(&self) -> ReadySetResult<GraphStats> {
        trace!("asked to get statistics");
        let workers = &self.workers;
        let mut domains = HashMap::new();
        for (&di, s) in self.domains.iter() {
            trace!(di = %di.index(), "requesting stats from domain");
            domains.extend(
                s.send_to_healthy(DomainRequest::GetStatistics, workers)
                    .await?
                    .into_iter()
                    .enumerate()
                    .map(move |(i, s)| ((di, i), s)),
            );
        }

        Ok(GraphStats { domains })
    }

    pub(super) fn get_instances(&self) -> Vec<(WorkerIdentifier, bool)> {
        self.workers
            .iter()
            .map(|(id, status)| (id.clone(), status.healthy))
            .collect()
    }

    pub(super) fn graphviz(&self, detailed: bool) -> String {
        graphviz(&self.ingredients, detailed, &self.materializations)
    }

    pub(super) fn get_failed_nodes(&self, lost_worker: &WorkerIdentifier) -> Vec<NodeIndex> {
        // Find nodes directly impacted by worker failure.
        let mut nodes: Vec<NodeIndex> = self.nodes_on_worker(Some(lost_worker));

        // Add any other downstream nodes.
        let mut failed_nodes = Vec::new();
        while let Some(node) = nodes.pop() {
            failed_nodes.push(node);
            for child in self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            {
                if !nodes.contains(&child) {
                    nodes.push(child);
                }
            }
        }
        failed_nodes
    }

    /// List data-flow nodes, on a specific worker if `worker` specified.
    pub(super) fn nodes_on_worker(&self, worker: Option<&WorkerIdentifier>) -> Vec<NodeIndex> {
        // NOTE(malte): this traverses all graph vertices in order to find those assigned to a
        // domain. We do this to avoid keeping separate state that may get out of sync, but it
        // could become a performance bottleneck in the future (e.g., when recovering large
        // graphs).
        let domain_nodes = |i: DomainIndex| -> Vec<NodeIndex> {
            #[allow(clippy::indexing_slicing)] // indices come from graph
            self.ingredients
                .node_indices()
                .filter(|&ni| ni != self.source)
                .filter(|&ni| !self.ingredients[ni].is_dropped())
                .filter(|&ni| self.ingredients[ni].domain() == i)
                .collect()
        };

        if let Some(worker) = worker {
            self.domains
                .values()
                .filter(|dh| dh.assigned_to_worker(worker))
                .fold(Vec::new(), |mut acc, dh| {
                    acc.extend(domain_nodes(dh.index()));
                    acc
                })
        } else {
            self.domains.values().fold(Vec::new(), |mut acc, dh| {
                acc.extend(domain_nodes(dh.index()));
                acc
            })
        }
    }

    /// Returns a struct containing the set of all replication offsets within the system, including
    /// the replication offset for the schema stored in the controller and the replication offsets
    /// of all base tables
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    pub(super) async fn replication_offsets(&self) -> ReadySetResult<ReplicationOffsets> {
        let domains = self.domains_with_base_tables().await?;
        stream::iter(domains)
            .map(|domain| {
                #[allow(clippy::indexing_slicing)] // came from self.domains
                self.domains[&domain]
                    .send_to_healthy::<NodeMap<Option<ReplicationOffset>>>(
                        DomainRequest::RequestReplicationOffsets,
                        &self.workers,
                    )
                    .map(move |r| -> ReadySetResult<_> { Ok((domain, r?)) })
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .try_fold(
                ReplicationOffsets::with_schema_offset(self.schema_replication_offset.clone()),
                |mut acc, (domain, domain_offs)| async move {
                    for shard in domain_offs {
                        for (lni, offset) in shard {
                            #[allow(clippy::indexing_slicing)] // came from self.domains
                            let ni = self.domain_nodes[&domain].get(lni).ok_or_else(|| {
                                internal_err(format!(
                                    "Domain {} returned nonexistent local node {}",
                                    domain, lni
                                ))
                            })?;
                            #[allow(clippy::indexing_slicing)] // internal invariant
                            let table_name = self.ingredients[*ni].name();
                            acc.tables.insert(table_name.to_owned(), offset); // TODO min of all shards
                        }
                    }
                    Ok(acc)
                },
            )
            .await
    }

    /// Collects a unique list of domains that might contain base tables. Errors out if a domain
    /// retrieved does not appears in self.domains.
    async fn domains_with_base_tables(&self) -> ReadySetResult<HashSet<DomainIndex>> {
        #[allow(clippy::indexing_slicing)] // inputs returns valid node indices
        let domains = self
            .inputs()
            .values()
            .map(|ni| self.ingredients[*ni].domain())
            .collect::<HashSet<_>>();

        for di in domains.iter() {
            if !self.domains.contains_key(di) {
                return Err(ReadySetError::NoSuchDomain {
                    domain_index: di.index(),
                    shard: 0,
                });
            }
        }

        Ok(domains)
    }

    /// Returns a list of all table names that are currently involved in snapshotting.
    pub(super) async fn snapshotting_tables(&self) -> ReadySetResult<HashSet<String>> {
        let domains = self.domains_with_base_tables().await?;

        let table_indices: Vec<(DomainIndex, LocalNodeIndex)> = stream::iter(domains)
            .map(|domain| {
                #[allow(clippy::indexing_slicing)] // validated above
                self.domains[&domain]
                    .send_to_healthy::<Vec<LocalNodeIndex>>(
                        DomainRequest::RequestSnapshottingTables,
                        &self.workers,
                    )
                    .map(move |r| -> ReadySetResult<_> { Ok((domain, r?)) })
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .map_ok(|(di, local_indices)| {
                stream::iter(
                    local_indices
                        .into_iter()
                        .flatten()
                        .map(move |li| -> ReadySetResult<_> { Ok((di, li)) }),
                )
            })
            .try_flatten()
            .try_collect()
            .await?;

        table_indices
            .iter()
            .map(|(di, lni)| -> ReadySetResult<String> {
                #[allow(clippy::indexing_slicing)] // just came from self.domains
                let li = *self.domain_nodes[di].get(*lni).ok_or_else(|| {
                    internal_err(format!("Domain {} returned nonexistent node {}", di, lni))
                })?;
                #[allow(clippy::indexing_slicing)] // internal invariant
                let node = &self.ingredients[li];
                debug_assert!(node.is_base());
                Ok(node.name().to_owned())
            })
            .collect()
    }

    // ** Modify operations **

    /// Perform a new query schema migration.
    #[instrument(level = "info", name = "migrate", skip(self, f))]
    pub(crate) async fn migrate<F, T>(&mut self, f: F) -> Result<T, ReadySetError>
    where
        F: FnOnce(&mut Migration<'_>) -> T,
    {
        info!("starting migration");
        gauge!(recorded::CONTROLLER_MIGRATION_IN_PROGRESS, 1.0);
        let mut m = Migration {
            dataflow_state: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: Default::default(),
            worker: None,
            start: time::Instant::now(),
        };
        let r = f(&mut m);
        m.commit().await?;
        info!("finished migration");
        gauge!(recorded::CONTROLLER_MIGRATION_IN_PROGRESS, 0.0);
        Ok(r)
    }

    pub(super) async fn replicate_readers(
        &mut self,
        spec: ReaderReplicationSpec,
    ) -> ReadySetResult<ReaderReplicationResult> {
        let mut reader_nodes = Vec::new();
        let worker_addr = spec.worker_uri;

        if let Some(ref worker_addr) = worker_addr {
            // If we've been specified to replicate readers into a specific worker,
            // we must then check that the worker is registered in the Controller.
            if !self.workers.contains_key(worker_addr) {
                return Err(ReadySetError::ReplicationUnknownWorker {
                    unknown_uri: worker_addr.clone(),
                });
            }
        }

        // We then proceed to retrieve the node indexes of each
        // query.
        let mut node_indexes = Vec::new();
        for query_name in &spec.queries {
            node_indexes.push((
                query_name,
                self.recipe.node_addr_for(query_name).map_err(|e| {
                    warn!(
                        error = %e,
                        %query_name,
                        "Reader replication failed: no node was found for query",
                    );
                    bad_request_err(format!(
                        "Reader replication failed: no node was found for query '{:?}'",
                        query_name
                    ))
                })?,
            ));
        }

        // Now we look for the reader nodes of each of the query nodes.
        let mut new_readers = HashMap::new();
        for (query_name, node_index) in node_indexes {
            // The logic to find the reader nodes is the same as [`self::find_view_for(NodeIndex,&str)`],
            // but we perform some extra operations here.
            // TODO(Fran): In the future we should try to find a good abstraction to avoid
            // duplicating the logic.
            let mut bfs = Bfs::new(&self.ingredients, node_index);
            while let Some(child_index) = bfs.next(&self.ingredients) {
                #[allow(clippy::indexing_slicing)] // just came out of self.ingredients
                let child: &Node = &self.ingredients[child_index];
                if let Some(r) = child.as_reader() {
                    if r.is_for() == node_index && child.name() == query_name {
                        // Now the child is the reader node of the query we are looking at.
                        // Here, we extract its [`PostLookup`] and use it to create a new
                        // mirror node.
                        let post_lookup = r.post_lookup().clone();
                        #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                        let mut reader_node = self.ingredients[node_index].named_mirror(
                            node::special::Reader::new(node_index, post_lookup),
                            child.name().to_string(),
                        );
                        // We also take the index of the original reader node.
                        if let Some(index) = child.as_reader().and_then(|r| r.index()) {
                            // And set the index on the replicated reader.
                            #[allow(clippy::unwrap_used)] // it must be a reader if it has a key
                            reader_node.as_mut_reader().unwrap().set_index(index);
                        }
                        // We add the replicated reader to the graph.
                        let reader_index = self.ingredients.add_node(reader_node);
                        self.ingredients.add_edge(node_index, reader_index, ());
                        // We keep track of the replicated reader and query node indexes, so
                        // we can use them to run a migration.
                        reader_nodes.push((node_index, reader_index));
                        // We store the reader indexes by query, to use as a reply
                        // to the user.
                        new_readers
                            .entry(query_name)
                            .or_insert_with(Vec::new)
                            .push(reader_index);
                        break;
                    }
                }
            }
        }

        // We run a migration with the new reader nodes.
        // The migration will take care of creating the domains and
        // sending them to the specified worker (or distribute them along all
        // workers if no worker was specified).
        self.migrate(move |mig| {
            mig.worker = worker_addr;
            for (node_index, reader_index) in reader_nodes {
                mig.added.insert(reader_index);
                mig.readers.insert(node_index, reader_index);
            }
        })
        .await?;

        // We retrieve the domain of the replicated readers.
        let mut query_information = HashMap::new();
        for (query_name, reader_indexes) in new_readers {
            let mut domain_mappings = HashMap::new();
            for reader_index in reader_indexes {
                #[allow(clippy::indexing_slicing)] // we just got the index from self
                let reader = &self.ingredients[reader_index];
                domain_mappings
                    .entry(reader.domain())
                    .or_insert_with(|| Vec::new())
                    .push(reader_index)
            }
            query_information.insert(query_name.clone(), domain_mappings);
        }

        // We return information about which replicated readers got in which domain,
        // for which query.
        Ok(ReaderReplicationResult {
            new_readers: query_information,
        })
    }

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
    /// Must be called before any domains have been created.
    #[allow(unused)]
    pub(super) fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    pub(in crate::controller) async fn place_domain(
        &mut self,
        idx: DomainIndex,
        shard_workers: Vec<WorkerIdentifier>,
        nodes: Vec<(NodeIndex, bool)>,
    ) -> ReadySetResult<DomainHandle> {
        // Reader nodes are always assigned to their own domains, so it's good enough to see
        // if any of its nodes is a reader.
        // We check for *any* node (and not *all*) since a reader domain has a reader node and an
        // ingress node.

        // check all nodes actually exist
        for (n, _) in nodes.iter() {
            if self.ingredients.node_weight(*n).is_none() {
                return Err(ReadySetError::NodeNotFound { index: n.index() });
            }
        }

        let domain_nodes: DomainNodes = nodes
            .iter()
            .map(|(ni, _)| {
                #[allow(clippy::unwrap_used)] // checked above
                let node = self
                    .ingredients
                    .node_weight_mut(*ni)
                    .unwrap()
                    .clone()
                    .take();
                node.finalize(&self.ingredients)
            })
            .map(|nd| (nd.local_addr(), cell::RefCell::new(nd)))
            .collect();

        let mut domain_addresses = vec![];
        let mut assignments = vec![];
        let mut new_domain_restrictions = vec![];

        let num_shards = shard_workers.len();
        for (shard, worker_id) in shard_workers.iter().enumerate() {
            let domain = DomainBuilder {
                index: idx,
                shard: if num_shards > 1 { Some(shard) } else { None },
                nshards: num_shards,
                config: self.domain_config.clone(),
                nodes: domain_nodes.clone(),
                persistence_parameters: self.persistence.clone(),
            };

            let w = self
                .workers
                .get(worker_id)
                .ok_or(ReadySetError::NoAvailableWorkers {
                    domain_index: idx.index(),
                    shard,
                })?;

            let idx = domain.index;
            let shard = domain.shard.unwrap_or(0);

            // send domain to worker
            info!(
                "sending domain {}.{} to worker {}",
                idx.index(),
                shard,
                w.uri
            );

            let ret = w
                .rpc::<RunDomainResponse>(WorkerRequestKind::RunDomain(domain))
                .await
                .map_err(|e| ReadySetError::DomainCreationFailed {
                    domain_index: idx.index(),
                    shard,
                    worker_uri: w.uri.clone(),
                    source: Box::new(e),
                })?;

            // Update the domain placement restrictions on nodes in the placed
            // domain if necessary.
            for (n, _) in &nodes {
                #[allow(clippy::indexing_slicing)] // checked above
                let node = &self.ingredients[*n];

                if node.is_base() && w.volume_id.is_some() {
                    new_domain_restrictions.push((
                        node.name().to_owned(),
                        shard,
                        DomainPlacementRestriction {
                            worker_volume: w.volume_id.clone(),
                        },
                    ));
                }
            }

            info!(external_addr = %ret.external_addr, "worker booted domain");

            self.channel_coordinator
                .insert_remote((idx, shard), ret.external_addr)?;
            domain_addresses.push(DomainDescriptor::new(idx, shard, ret.external_addr));
            assignments.push(w.uri.clone());
        }

        // Push all domain placement restrictions to the local controller state. We
        // do this outside the loop to satisfy the borrow checker as this immutably
        // borrows self.
        for (node_name, shard, restrictions) in new_domain_restrictions {
            self.set_domain_placement_local(&node_name, shard, restrictions);
        }

        // Tell all workers about the new domain(s)
        // TODO(jon): figure out how much of the below is still true
        // TODO(malte): this is a hack, and not an especially neat one. In response to a
        // domain boot message, we broadcast information about this new domain to all
        // workers, which inform their ChannelCoordinators about it. This is required so
        // that domains can find each other when starting up.
        // Moreover, it is required for us to do this *here*, since this code runs on
        // the thread that initiated the migration, and which will query domains to ask
        // if they're ready. No domain will be ready until it has found its neighbours,
        // so by sending out the information here, we ensure that we cannot deadlock
        // with the migration waiting for a domain to become ready when trying to send
        // the information. (We used to do this in the controller thread, with the
        // result of a nasty deadlock.)
        for (address, w) in self.workers.iter_mut() {
            for &dd in &domain_addresses {
                info!(worker_uri = %w.uri, "informing worker about newly placed domain");
                if let Err(e) = w
                    .rpc::<()>(WorkerRequestKind::GossipDomainInformation(vec![dd]))
                    .await
                {
                    // TODO(Fran): We need better error handling for workers
                    //   that failed before the controller noticed.
                    error!(
                        ?address,
                        error = ?e,
                        "Worker could not be reached and will be ignored",
                    );
                }
            }
        }

        Ok(DomainHandle {
            idx,
            shards: assignments,
        })
    }

    pub(super) async fn remove_nodes(
        &mut self,
        removals: &[NodeIndex],
    ) -> Result<(), ReadySetError> {
        // Remove node from controller local state
        let mut domain_removals: HashMap<DomainIndex, Vec<LocalNodeIndex>> = HashMap::default();
        for ni in removals {
            let node = self
                .ingredients
                .node_weight_mut(*ni)
                .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?;
            node.remove();
            debug!(node = %ni.index(), "Removed node");
            domain_removals
                .entry(node.domain())
                .or_insert_with(Vec::new)
                .push(node.local_addr())
        }

        // Send messages to domains
        for (domain, nodes) in domain_removals {
            trace!(
                domain_index = %domain.index(),
                "Notifying domain of node removals",
            );

            match self
                .domains
                .get_mut(&domain)
                .ok_or_else(|| ReadySetError::NoSuchDomain {
                    domain_index: domain.index(),
                    shard: 0,
                })?
                .send_to_healthy::<()>(DomainRequest::RemoveNodes { nodes }, &self.workers)
                .await
            {
                // The worker failing is an even more efficient way to remove nodes.
                Ok(_) | Err(ReadySetError::WorkerFailed { .. }) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub(super) async fn remove_leaf(&mut self, mut leaf: NodeIndex) -> Result<(), ReadySetError> {
        let mut removals = vec![];
        let start = leaf;
        if self.ingredients.node_weight(leaf).is_none() {
            return Err(ReadySetError::NodeNotFound {
                index: leaf.index(),
            });
        }
        #[allow(clippy::indexing_slicing)] // checked above
        {
            invariant!(!self.ingredients[leaf].is_source());
        }

        info!(
            node = %leaf.index(),
            "Computing removals for removing node",
        );

        let nchildren = self
            .ingredients
            .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
            .count();
        if nchildren > 0 {
            // This query leaf node has children -- typically, these are readers, but they can also
            // include egress nodes or other, dependent queries. We need to find the actual reader,
            // and remove that.
            if nchildren != 1 {
                internal!(
                    "cannot remove node {}, as it still has multiple children",
                    leaf.index()
                );
            }

            let mut readers = Vec::new();
            let mut bfs = Bfs::new(&self.ingredients, leaf);
            while let Some(child) = bfs.next(&self.ingredients) {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let n = &self.ingredients[child];
                if n.is_reader_for(leaf) {
                    readers.push(child);
                }
            }

            // nodes can have only one reader attached
            invariant_eq!(readers.len(), 1);
            #[allow(clippy::indexing_slicing)]
            let reader = readers[0];
            #[allow(clippy::indexing_slicing)]
            {
                debug!(
                    node = %leaf.index(),
                    really = %reader.index(),
                    "Removing query leaf \"{}\"", self.ingredients[leaf].name()
                );
            }
            removals.push(reader);
            leaf = reader;
        }

        // `node` now does not have any children any more
        assert_eq!(
            self.ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .count(),
            0
        );

        let mut nodes = vec![leaf];
        while let Some(node) = nodes.pop() {
            let mut parents = self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .detach();
            while let Some(parent) = parents.next_node(&self.ingredients) {
                #[allow(clippy::expect_used)]
                let edge = self.ingredients.find_edge(parent, node).expect(
                    "unreachable: neighbors_directed returned something that wasn't a neighbour",
                );
                self.ingredients.remove_edge(edge);

                #[allow(clippy::indexing_slicing)]
                if !self.ingredients[parent].is_source()
                    && !self.ingredients[parent].is_base()
                    // ok to remove original start leaf
                    && (parent == start || !self.recipe.sql_inc().is_leaf_address(parent))
                    && self
                    .ingredients
                    .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                    .count() == 0
                {
                    nodes.push(parent);
                }
            }

            removals.push(node);
        }

        self.remove_nodes(removals.as_slice()).await
    }

    pub(super) fn set_domain_placement_local(
        &mut self,
        node_name: &str,
        shard: usize,
        node_restriction: DomainPlacementRestriction,
    ) {
        self.node_restrictions.insert(
            NodeRestrictionKey {
                node_name: node_name.into(),
                shard,
            },
            node_restriction,
        );
    }

    pub(super) fn set_schema_replication_offset(&mut self, offset: Option<ReplicationOffset>) {
        self.schema_replication_offset = offset;
    }

    pub(super) async fn flush_partial(&mut self) -> ReadySetResult<u64> {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let mut to_evict = Vec::new();
        for (di, s) in self.domains.iter_mut() {
            let domain_to_evict: Vec<(NodeIndex, u64)> = s
                .send_to_healthy::<(DomainStats, HashMap<NodeIndex, NodeStats>)>(
                    DomainRequest::GetStatistics,
                    workers,
                )
                .await?
                .into_iter()
                .flat_map(move |(_, node_stats)| {
                    node_stats
                        .into_iter()
                        .filter_map(|(ni, ns)| match ns.materialized {
                            MaterializationStatus::Partial { .. } => Some((ni, ns.mem_size)),
                            _ => None,
                        })
                })
                .collect();
            to_evict.push((*di, domain_to_evict));
        }

        let mut total_evicted = 0;
        for (di, nodes) in to_evict {
            for (ni, bytes) in nodes {
                let na = self
                    .ingredients
                    .node_weight(ni)
                    .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?
                    .local_addr();
                #[allow(clippy::unwrap_used)] // literally got the `di` from iterating `domains`
                self.domains
                    .get_mut(&di)
                    .unwrap()
                    .send_to_healthy::<()>(
                        DomainRequest::Packet(Packet::Evict {
                            node: Some(na),
                            num_bytes: bytes as usize,
                        }),
                        workers,
                    )
                    .await?;
                total_evicted += bytes;
            }
        }

        warn!(total_evicted, "flushed partial domain state");

        Ok(total_evicted)
    }

    pub(super) async fn apply_recipe(
        &mut self,
        mut new: Recipe,
    ) -> Result<ActivationResult, ReadySetError> {
        new.clone_config_from(&self.recipe);
        let old_recipe = self.recipe.clone();
        let r = self.migrate(|mig| new.activate(mig, old_recipe)).await?;

        match r {
            Ok(ref ra) => {
                let (removed_bases, removed_other): (Vec<_>, Vec<_>) =
                    ra.removed_leaves.iter().cloned().partition(|ni| {
                        self.ingredients
                            .node_weight(*ni)
                            .map(|x| x.is_base())
                            .unwrap_or(false)
                    });

                // first remove query nodes in reverse topological order
                let mut topo_removals = Vec::with_capacity(removed_other.len());
                let mut topo = petgraph::visit::Topo::new(&self.ingredients);
                while let Some(node) = topo.next(&self.ingredients) {
                    if removed_other.contains(&node) {
                        topo_removals.push(node);
                    }
                }
                topo_removals.reverse();

                for leaf in topo_removals {
                    self.remove_leaf(leaf).await?;
                }

                // now remove bases
                for base in removed_bases {
                    // TODO(malte): support removing bases that still have children?

                    // TODO(malte): what about domain crossings? can ingress/egress nodes be left
                    // behind?
                    assert_eq!(
                        self.ingredients
                            .neighbors_directed(base, petgraph::EdgeDirection::Outgoing)
                            .count(),
                        0
                    );
                    let name = self
                        .ingredients
                        .node_weight(base)
                        .ok_or_else(|| ReadySetError::NodeNotFound {
                            index: base.index(),
                        })?
                        .name();
                    debug!(
                        %name,
                        node = %base.index(),
                        "Removing base",
                    );
                    // now drop the (orphaned) base
                    self.remove_nodes(vec![base].as_slice()).await?;
                }

                self.recipe = new;
            }
            Err(ref e) => {
                error!(error = %e, "failed to apply recipe");
            }
        }

        r
    }

    pub(super) async fn extend_recipe(
        &mut self,
        add_txt_spec: RecipeSpec<'_>,
    ) -> Result<ActivationResult, ReadySetError> {
        let mut new = self.recipe.clone();
        let add_txt = add_txt_spec.recipe;

        if let Err(e) = new.extend(add_txt) {
            error!(error = %e, "failed to extend recipe");
            return Err(e);
        }

        match self.apply_recipe(new).await {
            Ok(x) => {
                if let Some(offset) = &add_txt_spec.replication_offset {
                    offset.try_max_into(&mut self.schema_replication_offset)?
                }

                Ok(x)
            }
            Err(e) => Err(e),
        }
    }

    pub(super) async fn install_recipe(
        &mut self,
        r_txt_spec: RecipeSpec<'_>,
    ) -> Result<ActivationResult, ReadySetError> {
        let r_txt = r_txt_spec.recipe;

        match Recipe::from_str(r_txt) {
            Ok(r) => {
                let new = self.recipe.clone().replace(r);
                match self.apply_recipe(new).await {
                    Ok(x) => {
                        self.schema_replication_offset =
                            r_txt_spec.replication_offset.as_deref().cloned();
                        Ok(x)
                    }
                    Err(e) => Err(e),
                }
            }
            Err(error) => {
                error!(%error, "failed to parse recipe");
                internal!("failed to parse recipe: {}", error);
            }
        }
    }

    pub(super) async fn remove_query(&mut self, query_name: &str) -> ReadySetResult<()> {
        let mut new = self.recipe.clone();
        new.remove_query(query_name);

        if let Err(error) = self.apply_recipe(new).await {
            error!(%error, "Failed to apply recipe");
            return Err(error);
        }

        Ok(())
    }

    /// Runs all the necessary steps to recover the full [`DataflowState`], when said state only
    /// has the bare minimum information.
    ///
    /// # Invariants
    /// The following invariants must hold:
    /// - `self.ingredients` must be a valid [`Graph`]. This means it has to be a description of
    /// a valid dataflow graph.
    /// - Each node must have a Domain associated with it (and thus also have a [`LocalNodeIndex`]
    /// and any other associated information).
    /// - `self.domain_nodes` must be valid. This means that all the nodes in `self.ingredients`
    /// (except for `self.source`) must belong to a domain; and there must not be any overlap between
    /// the nodes owned by each domain. All the invariants for Domain assigment from
    /// [`crate::controller::migrate::assignment::assign`] must hold as well.
    ///  - `self.remap` and `self.node_restrictions` must be valid.
    /// - All the other fields should be empty or `[Default::default()]`.
    pub(super) async fn recover(&mut self) -> ReadySetResult<()> {
        invariant!(
            self.domains.is_empty(),
            "there shouldn't be any domain if we are recovering"
        );
        let mut dmp = DomainMigrationPlan::new(self);
        let domain_nodes = self
            .domain_nodes
            .iter()
            .map(|(idx, nm)| (*idx, nm.values().map(|&n| (n, true)).collect::<Vec<_>>()))
            .collect::<HashMap<_, _>>();
        {
            let mut scheduler = Scheduler::new(self, &None)?;
            for (domain, nodes) in domain_nodes.iter() {
                let workers = scheduler.schedule_domain(*domain, &nodes[..])?;
                let shards = workers.len();
                dmp.add_new_domain(*domain, workers, nodes.clone());
                dmp.add_valid_domain(*domain, shards);
            }
        }
        let new = domain_nodes.values().flatten().map(|(n, _)| *n).collect();
        routing::connect(
            &self.ingredients,
            &mut dmp,
            &self.ingredients.node_indices().collect(),
        )?;

        self.materializations.extend(&mut self.ingredients, &new)?;

        self.materializations
            .commit(&mut self.ingredients, &new, &mut dmp)?;

        dmp.apply(self).await
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
    pub(super) fn new(dataflow_state: DataflowState) -> Self {
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
    pub(super) async fn commit(
        &self,
        writer: DataflowStateWriter<'_>,
        authority: &Arc<Authority>,
    ) -> ReadySetResult<()> {
        let persistable_ds = PersistableDataflowState {
            state: writer.state,
        };
        authority
            .update_controller_state(|state: Option<ControllerState>| match state {
                None => {
                    eprintln!("There's no controller state to update");
                    Err(())
                }
                Some(mut state) => {
                    state.dataflow_state = persistable_ds.state.clone();
                    Ok(state)
                }
            })
            .await
            .map_err(|e| internal_err(format!("Unable to update state: {}", e)))?
            .map_err(|_| internal_err("Unable to update state"))?;
        let mut state_guard = self.reader.write().await;
        state_guard.replace(persistable_ds.state);
        Ok(())
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

/// A wrapper around the dataflow state, to be used only in [`DataflowStateWriter::commit`]
/// to send a copy of the state across thread boundaries.
struct PersistableDataflowState {
    state: DataflowState,
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
// Needed to send a copy of the [`DataflowState`] across thread boundaries, when
// we are persisting the state to the [`Authority`].
unsafe impl Sync for PersistableDataflowState {}

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
