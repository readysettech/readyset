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
//! This module provides the structures to store the state of the ReadySet dataflow graph, and
//! to manipulate it in a thread-safe way.

use std::cell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use common::{IndexPair, Tag};
use dataflow::domain::replay_paths::ReplayPath;
use dataflow::node::{self, Column};
use dataflow::payload::{packets::Evict, Eviction};
use dataflow::prelude::{ChannelCoordinator, DomainIndex, DomainNodes, Graph, NodeIndex};
use dataflow::{
    BaseTableState, DomainBuilder, DomainConfig, DomainRequest, DurabilityMode, NodeMap, Packet,
    PersistenceParameters,
};
use failpoint_macros::set_failpoint;
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use futures::{FutureExt, TryFutureExt, TryStream};
use metrics::{counter, gauge, histogram};
use petgraph::visit::{Bfs, IntoNodeReferences};
use petgraph::Direction;
use rand::RngExt;
use readyset_client::builders::{
    ReaderHandleBuilder, ReusedReaderHandleBuilder, TableBuilder, ViewBuilder,
};
use readyset_client::consensus::{Authority, AuthorityControl};
use readyset_client::debug::info::{GraphInfo, MaterializationInfo, NodeSize};
use readyset_client::debug::stats::{DomainStats, GraphStats, NodeStats};
use readyset_client::internal::MaterializationStatus;
use readyset_client::query::QueryId;
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::recipe::{CacheExpr, ExprInfo, ExtendRecipeSpec};
use readyset_client::schema::ViewSchema;
use readyset_client::{
    PersistencePoint, SingleKeyEviction, TableStatus, ViewCreateRequest, ViewFilter, ViewRequest,
};
use readyset_data::{DfValue, Dialect};
use readyset_errors::{
    internal, internal_err, invariant_eq, ErrorNodeType, ReadySetError, ReadySetResult,
};
use readyset_sql::ast::{NonReplicatedRelation, Relation, SqlIdentifier};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use replication_offset::{ReplicationOffset, ReplicationOffsets};
use schema_catalog::SchemaGeneration;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::fs;
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, trace, warn};
use vec1::{vec1, Vec1};

use super::sql::Recipe;
use crate::controller::domain_handle::DomainHandle;
use crate::controller::migrate::materialization::Materializations;
use crate::controller::migrate::scheduling::schedule_domain;
use crate::controller::migrate::{routing, DomainMigrationMode, DomainMigrationPlan, Migration};
use crate::controller::sql::{RecipeExpr, Schema};
use crate::controller::{schema, Worker, WorkerIdentifier};
use crate::internal::LocalNodeIndex;
mod graphviz;

pub(in crate::controller) use self::graphviz::Graphviz;

/// Number of concurrent requests to make when making multiple simultaneous requests to domains (eg
/// for replication offsets)
const CONCURRENT_REQUESTS: usize = 16;

/// Collect all nodes reachable from `start` by traversing edges in the given `direction`.
fn reachable_from(graph: &Graph, start: NodeIndex, direction: Direction) -> HashSet<NodeIndex> {
    let mut reachable = HashSet::new();
    let mut stack = vec![start];
    while let Some(node) = stack.pop() {
        if reachable.insert(node) {
            for next in graph.neighbors_directed(node, direction) {
                if !reachable.contains(&next) {
                    stack.push(next);
                }
            }
        }
    }
    reachable
}

/// This structure holds all the dataflow state.
/// It's meant to be handled exclusively by the [`DfStateHandle`], which is the structure
/// that guarantees thread-safe access to it.
///
/// # Wire format
///
/// Persisted to the Authority's RocksDB via `rmp_serde::to_vec`. Removing or
/// reordering any serde-included field breaks decoding of every older
/// payload. See the matching warning above `Config` in
/// `readyset-server/src/lib.rs` for the full policy and the compat-shim
/// recipe.
#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct DfState {
    pub(super) ingredients: Graph,

    /// ID for the root node in the graph. This is used to retrieve a list of base tables.
    pub(super) source: NodeIndex,
    /// Monotonic counter used to allocate fresh [`DomainIndex`] values. Never decremented, even
    /// when domains are reclaimed via [`Self::reclaim_orphaned_domains`]; `next_domain()` always
    /// hands out a new index.
    pub(super) ndomains: usize,

    pub(super) domain_config: DomainConfig,

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is deleted
    ///     once the `Controller` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory. Useful for
    ///     baseline numbers.
    ///
    /// Note: `pub(super)` visibility is required for syncing this field from CLI config during
    /// leader election with existing state (see `AuthorityLeaderElectionState::update_leader_state`).
    pub(super) persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    pub(super) recipe: Recipe,
    /// Latest replication position for the schema if from replica or binlog
    schema_replication_offset: Option<ReplicationOffset>,
    /// Schema generation number, incremented on every successful migration commit.
    /// Used to detect when cache creation attempts are using stale schema information.
    ///
    /// # Semantics
    /// - Initialized to `SchemaGeneration::INITIAL` (1) on first boot
    /// - Incremented after each successful DDL migration (but NOT for CreateCache-only migrations)
    /// - `None` in `CreateCache::schema_generation_used` indicates "missing/unset"
    /// - Wraps from u64::MAX back to 1 (never 0)
    schema_generation: SchemaGeneration,

    #[serde_as(as = "Vec<(_, _)>")]
    /// Map from local to global node index for each domain
    pub(super) domain_nodes: HashMap<DomainIndex, NodeMap<NodeIndex>>,
    /// Map from global node index to index pair for each domain
    #[serde_as(as = "Vec<(_, _)>")]
    pub(super) domain_node_index_pairs: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    #[serde(skip)]
    pub(super) domains: HashMap<DomainIndex, DomainHandle>,

    #[serde(skip)]
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,

    /// Map from worker URI to the address the worker is listening on for reads.
    #[serde(skip)]
    pub(super) read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    #[serde(skip)]
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,
}

impl DfState {
    /// Creates a new instance of [`DfState`].
    pub(super) fn new(
        ingredients: Graph,
        source: NodeIndex,
        ndomains: usize,
        domain_config: DomainConfig,
        persistence: PersistenceParameters,
        materializations: Materializations,
        recipe: Recipe,
        schema_replication_offset: Option<ReplicationOffset>,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> Self {
        Self {
            ingredients,
            source,
            ndomains,
            domain_config,
            persistence,
            materializations,
            recipe,
            schema_replication_offset,
            schema_generation: SchemaGeneration::INITIAL,
            domains: Default::default(),
            domain_nodes: Default::default(),
            channel_coordinator,
            read_addrs: Default::default(),
            workers: Default::default(),
            domain_node_index_pairs: Default::default(),
        }
    }

    pub(super) fn schema_replication_offset(&self) -> &Option<ReplicationOffset> {
        &self.schema_replication_offset
    }

    fn reader_node_index(&self, query: &Relation) -> ReadySetResult<NodeIndex> {
        self.recipe
            .node_addr_for(query)
            .ok()
            .or_else(|| self.views().get(query).copied())
            .and_then(|leaf| self.find_reader_for(leaf, query, &Default::default()))
            .ok_or_else(|| ReadySetError::QueryNotFound {
                name: query.display_unquoted().to_string(),
            })
    }

    /// Retrieve the current schema generation number, incremented when DDL or other changes are
    /// made that could affect how we rewrite SQL queries.
    ///
    /// Note: This is here instead of on [`SqlIncorporator`] only because we ([`DfState`]) are more
    /// suited to ensuring we increment the generation number on every migration commit (see
    /// [`DfState::apply_recipe`]).
    pub(super) fn schema_generation(&self) -> SchemaGeneration {
        self.schema_generation
    }

    pub(super) fn get_info(&self) -> ReadySetResult<GraphInfo> {
        let mut worker_info = HashMap::new();
        for (di, dh) in self.domains.iter() {
            for (addr, url) in dh.assignments() {
                worker_info
                    .entry(url.clone())
                    .or_insert_with(HashMap::new)
                    .entry(addr)
                    .or_insert_with(Vec::new)
                    .extend(
                        self.domain_nodes
                            .get(di)
                            .ok_or_else(|| {
                                internal_err!("{:?} in domains but not in domain_nodes", di)
                            })?
                            .values(),
                    );
            }
        }
        Ok(GraphInfo {
            workers: worker_info,
        })
    }

    /// Get a map of all known base table nodes, mapping the name of the node to that node's
    /// [index](NodeIndex)
    pub(super) fn tables(&self) -> BTreeMap<Relation, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let node = &self.ingredients[n];

                if node.is_dropped() || node.is_constant() {
                    None
                } else {
                    assert!(node.is_base());
                    Some((node.name().clone(), n))
                }
            })
            .collect()
    }

    /// Return a list of all relations (tables or views) which are known to exist in the upstream
    /// database that we are replicating from, but are not being replicated to ReadySet (which are
    /// recorded via [`Change::AddNonReplicatedRelation`]).
    ///
    /// [`Change::AddNonReplicatedRelation`]: readyset_client::recipe::changelist::Change::AddNonReplicatedRelation
    pub(super) fn non_replicated_relations(&self) -> &HashSet<NonReplicatedRelation> {
        self.recipe.sql_inc().non_replicated_relations()
    }

    /// Get a map of all known views, mapping the name of the view to that node's [index](NodeIndex)
    pub(super) fn views(&self) -> BTreeMap<Relation, NodeIndex> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let name = self.ingredients[n].name().clone();
                self.ingredients[n].as_reader().map(|r| {
                    // we want to give the the node address that is being materialized not that of
                    // the reader node itself.
                    (name, r.is_for())
                })
            })
            .collect()
    }

    /// Get a map of all known views created from `CREATE CACHE` statements, mapping the name of the
    /// view to a tuple of (`SelectStatement`, always) where always is a bool that indicates whether
    /// the `CREATE CACHE` statement was created with the optional `ALWAYS` argument.
    ///
    /// Optionally, search_query_id and search_name can be passed to filter the results. Passing
    /// both will include only results that match at least one.
    pub(super) fn verbose_views(
        &self,
        search_query_id: Option<QueryId>,
        search_name: Option<&Relation>,
    ) -> Vec<CacheExpr> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                if self.ingredients[n].is_reader() {
                    let name = self.ingredients[n].name().clone();

                    // Alias should always resolve to an id and id should always resolve to an
                    // expression. However, this mapping will not catch bugs that break this
                    // assumption
                    let name = self.recipe.resolve_alias(&name)?;
                    let query = self.recipe.expression_by_alias(name)?;

                    // Only return ingredients created from "CREATE CACHE"
                    match query {
                        RecipeExpr::Cache {
                            name,
                            statement,
                            trx_cache_policy,
                            cache_type,
                            policy,
                            query_id,
                            topk_buffer_multiplier,
                        } if (search_query_id.is_none() && search_name.is_none())
                            || (Some(query_id) == search_query_id)
                            || (Some(&name) == search_name) =>
                        {
                            Some(CacheExpr {
                                name,
                                statement,
                                trx_cache_policy,
                                cache_type,
                                policy,
                                query_id,
                                topk_buffer_multiplier,
                            })
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn views_info(
        &self,
        queries: Vec<ViewCreateRequest>,
        dialect: Dialect,
    ) -> Vec<Option<ExprInfo>> {
        queries
            .into_iter()
            .map(|query| {
                self.recipe
                    .expression_info_for_query(query, dialect)
                    .unwrap_or(None)
            })
            .collect()
    }

    pub(super) fn find_reader_for(
        &self,
        node: NodeIndex,
        name: &Relation,
        filter: &Option<ViewFilter>,
    ) -> Option<NodeIndex> {
        // reader should be a child of the given node. once we go beyond depth 1, we may
        // accidentally hit an *unrelated* reader node. to account for this, readers keep track of
        // what node they are "for", and we simply search for the appropriate reader by that
        // metric. since we know that the reader must be relatively close, a BFS search is the way
        // to go.
        let mut bfs = Bfs::new(&self.ingredients, node);
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child].is_reader_for(node) && self.ingredients[child].name() == name
            {
                // Check for any filter requirements we can satisfy when
                // traversing the data flow graph, `filter`.
                if let Some(ViewFilter::Workers(w)) = filter {
                    let domain = self.ingredients[child].domain();
                    for worker in w {
                        if self
                            .domains
                            .get(&domain)
                            .map(|dh| dh.is_assigned_to_worker(worker))
                            .unwrap_or(false)
                        {
                            return Some(child);
                        }
                    }
                } else {
                    return Some(child);
                }
            }
        }
        None
    }

    /// Create a ViewBuilder given the node, the ViewRequest, and an optional name if this view
    /// request is for a reused cache.
    fn view_builder_inner(
        &self,
        node: NodeIndex,
        name: Option<&Relation>,
        view_req: ViewRequest,
    ) -> ReadySetResult<Option<ReaderHandleBuilder>> {
        // This function is provided a name if the view is for a reused cache.
        let name = name.unwrap_or(&view_req.name);
        let name = self.recipe.resolve_alias(name).unwrap_or(name);

        let reader_node = if let Some(r) = self.find_reader_for(node, name, &view_req.filter) {
            r
        } else {
            return Ok(None);
        };

        let domain_index = self.ingredients[reader_node].domain();
        let reader = self.ingredients[reader_node].as_reader().ok_or_else(|| {
            ReadySetError::InvalidNodeType {
                node_index: self.ingredients[reader_node].local_addr().id(),
                expected_type: ErrorNodeType::Reader,
            }
        })?;
        let returned_cols = reader
            .reader_processing()
            .post_processing
            .returned_cols
            .clone()
            .unwrap_or_else(|| (0..self.ingredients[reader_node].columns().len()).collect());
        let columns = self.ingredients[reader_node].columns();
        let columns = returned_cols
            .iter()
            .map(|idx| columns.get(*idx).map(|c| c.name().into()))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| internal_err!("Schema expects valid column indices"))?;

        let key_mapping = Vec::from(reader.mapping());

        let schema = self.view_schema(reader_node)?;
        let domain =
            self.domains
                .get(&domain_index)
                .ok_or_else(|| ReadySetError::UnknownDomain {
                    domain_index: domain_index.index(),
                })?;

        let read_addr = domain
            .worker()
            .map(|worker| {
                self.read_addrs
                    .get(worker)
                    .ok_or_else(|| ReadySetError::UnmappableDomain {
                        domain_index: domain_index.index(),
                    })
                    .copied()
            })
            .transpose()?;

        Ok(Some(ReaderHandleBuilder {
            name: name.clone(),
            node: reader_node,
            columns: columns.into(),
            schema,
            read_addr,
            key_mapping,
            view_request_timeout: self.domain_config.view_request_timeout,
        }))
    }

    /// Obtain a `ViewBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    pub(super) fn view_builder(
        &self,
        view_req: ViewRequest,
    ) -> ReadySetResult<Option<ViewBuilder>> {
        let get_index_or_traverse = |name: &Relation| {
            // first try to resolve the node via the recipe, which handles aliasing between
            // identical queries.
            match self.recipe.node_addr_for(name) {
                Ok(ni) => Some(ni),
                // if the recipe doesn't know about this query, traverse the graph.
                // we need this do deal with manually constructed graphs (e.g., in tests).
                Err(_) => self.views().get(name).copied(),
            }
        };

        match get_index_or_traverse(&view_req.name) {
            // A view for this node exists.
            Some(ni) => self
                .view_builder_inner(ni, None, view_req)
                .map(|opt| opt.map(|vb| ViewBuilder::Single(vb))),
            None => {
                // Does this query reuse another cache?
                match self.recipe.reused_caches(&view_req.name) {
                    Some(reused_caches) => {
                        let mut view_builders = Vec::new();
                        for cache in reused_caches {
                            view_builders.push(
                                get_index_or_traverse(cache.name())
                                    .map(|ni| {
                                        self.view_builder_inner(
                                            ni,
                                            Some(cache.name()),
                                            view_req.clone(),
                                        )
                                    })
                                    .transpose()
                                    .map(|vb| {
                                        vb.flatten().map(|vb| ReusedReaderHandleBuilder {
                                            builder: vb,
                                            key_remapping: cache.key_mapping().clone(),
                                            required_values: cache.required_values().clone(),
                                        })
                                    }),
                            );
                        }
                        let mut last_error = None;
                        let mut builders = Vec::new();
                        for vb in view_builders {
                            match vb {
                                Ok(Some(vb)) => builders.push(vb),
                                Err(e) => last_error = Some(e),
                                Ok(None) => { /* We'll catch this if we have no builders or errors */
                                }
                            }
                        }
                        if builders.is_empty() {
                            if let Some(e) = last_error {
                                Err(e)
                            } else {
                                Ok(None)
                            }
                        } else {
                            #[allow(clippy::unwrap_used)] // builders has at least 1 element
                            Ok(Some(ViewBuilder::MultipleReused(
                                Vec1::try_from(builders).unwrap(),
                            )))
                        }
                    }
                    None => Ok(None),
                }
            }
        }
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
                expected_type: ErrorNodeType::Reader,
            })?;
        let returned_cols = reader
            .reader_processing()
            .post_processing
            .returned_cols
            .clone()
            .unwrap_or_else(|| (0..n.columns().len()).collect());

        let projected_schema = (0..n.columns().len())
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
    pub(super) fn table_builder(&self, name: &Relation) -> ReadySetResult<Option<TableBuilder>> {
        let ni = self
            .recipe
            .node_addr_for(name)
            .map_err(|_| ReadySetError::TableNotFound {
                name: name.name.clone().into(),
                schema: name.schema.clone().map(Into::into),
            })?;
        self.table_builder_by_index(ni)
    }

    pub(super) fn table_builder_by_index(
        &self,
        ni: NodeIndex,
    ) -> ReadySetResult<Option<TableBuilder>> {
        let node = self
            .ingredients
            .node_weight(ni)
            .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?;
        let base = node.name();

        trace!(base = %base.display_unquoted(), "creating table");

        let key = node
            .get_base()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: node.local_addr().id(),
                expected_type: ErrorNodeType::Base,
            })?
            .primary_key()
            .map(|k| k.to_owned())
            .unwrap_or_default();

        let is_primary = !key.is_empty();

        let dh = self
            .domains
            .get(&node.domain())
            .ok_or_else(|| ReadySetError::UnknownDomain {
                domain_index: node.domain().index(),
            })?;

        // Standalone Readyset has at most one worker per domain.
        debug_assert!(dh.is_placed() || dh.worker().is_none());

        let domain = node.domain();
        let tx = self
            .channel_coordinator
            .get_addr(&domain)
            .ok_or_else(|| internal_err!("failed to get channel coordinator for {}", domain))?;

        let base_operator = node
            .get_base()
            .ok_or_else(|| internal_err!("asked to get table for non-base node"))?;
        let columns: Vec<SqlIdentifier> = node
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(n, s)| {
                if base_operator.get_dropped().contains_key(n) {
                    None
                } else {
                    Some(s.name().into())
                }
            })
            .collect();
        invariant_eq!(
            columns.len(),
            node.columns().len() - base_operator.get_dropped().len()
        );
        let schema = self
            .recipe
            .schema_for(base)
            .map(|s| -> ReadySetResult<_> {
                match s {
                    Schema::Table(s) => Ok(s.statement.clone()),
                    _ => internal!(
                        "non-base schema {:?} returned for table {}",
                        s,
                        base.display_unquoted()
                    ),
                }
            })
            .transpose()?;

        Ok(Some(TableBuilder {
            tx,
            ni: node.global_addr(),
            addr: node.local_addr(),
            key,
            key_is_primary: is_primary,
            dropped: base_operator.get_dropped(),
            table_name: node.name().clone(),
            columns,
            schema,
            table_request_timeout: self.domain_config.table_request_timeout,
        }))
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub(super) async fn get_statistics(&self) -> ReadySetResult<GraphStats> {
        trace!("asked to get statistics");
        let workers = &self.workers;
        let mut domains = HashMap::new();
        for (&domain_index, s) in self.domains.iter() {
            trace!(domain = %domain_index.index(), "requesting stats from domain");
            if let Some(stats) = s.try_send(DomainRequest::GetStatistics, workers).await? {
                domains.insert(domain_index, stats);
            }
        }

        Ok(GraphStats { domains })
    }

    pub(super) fn get_instances(&self) -> Vec<(WorkerIdentifier, bool)> {
        self.workers
            .iter()
            .map(|(id, status)| (id.clone(), status.healthy))
            .collect()
    }

    pub(super) fn graphviz(&self, node_sizes: Option<HashMap<NodeIndex, NodeSize>>) -> String {
        Graphviz {
            graph: &self.ingredients,
            node_sizes,
            materializations: &self.materializations,
            domain_nodes: Some(&self.domain_nodes),
            reachable_from: None,
        }
        .to_string()
    }

    pub(super) fn graphviz_for_query(
        &self,
        query: &Relation,
        node_sizes: Option<HashMap<NodeIndex, NodeSize>>,
    ) -> ReadySetResult<String> {
        let ni = self.reader_node_index(query)?;

        Ok(Graphviz {
            graph: &self.ingredients,
            node_sizes,
            materializations: &self.materializations,
            domain_nodes: Some(&self.domain_nodes),
            reachable_from: Some((ni, Direction::Incoming)),
        }
        .to_string())
    }

    /// List data-flow nodes, on a specific worker if `worker` specified.
    pub(super) fn nodes_on_worker(
        &self,
        worker_opt: Option<&WorkerIdentifier>,
    ) -> HashMap<DomainIndex, HashSet<NodeIndex>> {
        self.domains
            .values()
            .filter(|dh| {
                // Either we need all the nodes, because no worker was specified.
                worker_opt.is_none() ||
                // Or we need the domains that belong to the specified worker.
                worker_opt
                    .filter(|w| dh.is_assigned_to_worker(w))
                    .is_some()
            })
            // Accumulate nodes by domain index.
            .fold(HashMap::new(), |mut acc, dh| {
                acc.entry(dh.index()).or_default().extend(
                    self.domain_nodes
                        .get(&dh.index())
                        .map(|nm| nm.values())
                        .into_iter()
                        .flatten(),
                );
                acc
            })
    }

    /// Return a list of information about materializations in the graph
    pub(super) async fn materialization_info(&self) -> ReadySetResult<Vec<MaterializationInfo>> {
        let sizes = self.node_sizes().await?;
        Ok(self.collect_materialization_info(&sizes, None))
    }

    /// Return materialization info filtered to only nodes in the subgraph for the given cache.
    ///
    /// Traverses the dataflow graph upstream from the cache's reader node (via incoming edges)
    /// to collect all ancestor nodes, then queries only the domains containing those nodes for
    /// size information.
    pub(super) async fn materialization_info_for_cache(
        &self,
        cache: &Relation,
    ) -> ReadySetResult<Vec<MaterializationInfo>> {
        let reader_ni = self.reader_node_index(cache)?;
        let reachable = reachable_from(&self.ingredients, reader_ni, Direction::Incoming);

        // Perf: only RPC domains that contain reachable nodes
        let sizes = self
            .node_sizes_for(
                self.domain_node_index_pairs
                    .iter()
                    .filter(|(_, nodes)| nodes.keys().any(|ni| reachable.contains(ni)))
                    .map(|(di, _)| *di),
            )
            .await?;

        Ok(self.collect_materialization_info(&sizes, Some(&reachable)))
    }

    /// Build [`MaterializationInfo`] for materialized nodes, optionally restricted to `filter`.
    fn collect_materialization_info(
        &self,
        sizes: &HashMap<NodeIndex, NodeSize>,
        filter: Option<&HashSet<NodeIndex>>,
    ) -> Vec<MaterializationInfo> {
        let dominated = |ni: &NodeIndex| filter.is_none_or(|f| f.contains(ni));

        self.materializations
            .materialized_non_reader_nodes()
            .filter(dominated)
            .map(|ni| {
                (
                    ni,
                    self.ingredients[ni].name().clone(),
                    self.ingredients[ni].description(),
                    self.materializations
                        .indexes_for(ni)
                        .expect("Node index came from materializations")
                        .clone(),
                )
            })
            .chain(
                self.ingredients
                    .node_references()
                    .filter(move |(ni, _)| dominated(ni))
                    .filter_map(|(ni, n)| {
                        n.as_reader().and_then(|r| r.index()).map(|idx| {
                            (
                                ni,
                                n.name().clone(),
                                n.description(),
                                HashSet::from([idx.clone()]),
                            )
                        })
                    }),
            )
            .map(
                |(node_index, node_name, node_description, indexes)| MaterializationInfo {
                    // TODO(marce): This index might be out of sync if we run readyset distributed
                    domain_index: self
                        .domain_node_index_pairs
                        .iter()
                        .find_map(|(di, nm)| nm.get(&node_index).map(|_| *di))
                        .unwrap_or_else(|| {
                            warn!(
                                "Node {} is not in any domain",
                                self.ingredients[node_index].name().display_unquoted()
                            );
                            DomainIndex::new(usize::MAX)
                        }),
                    node_index,
                    node_name,
                    node_description,
                    size: sizes.get(&node_index).cloned().unwrap_or_default(),
                    partial: self.materializations.is_partial(node_index),
                    indexes,
                },
            )
            .collect()
    }

    /// Issue all of `requests` to their corresponding domains asynchronously, and return a stream
    /// of the results, consisting of one result per domain (potentially in a different order).
    ///
    /// If any domains are not running on a worker, this method will return an error.
    ///
    /// # Invariants
    ///
    /// * All of the domain indices in `requests` must be domains in `self.domains`
    fn query_domains<'a, I, R>(
        &'a self,
        requests: I,
    ) -> impl TryStream<Ok = (DomainIndex, R), Error = ReadySetError> + 'a
    where
        I: IntoIterator<Item = (DomainIndex, DomainRequest)>,
        I::IntoIter: 'a,
        R: DeserializeOwned,
    {
        stream::iter(requests)
            .map(move |(domain, request)| {
                self.domains[&domain]
                    .send::<R>(request, &self.workers)
                    .map(move |r| -> ReadySetResult<_> { Ok((domain, r?)) })
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
    }

    /// Each base table has an offset up to which data has been persisted to disk, and this
    /// method returns the minimum of those offsets. If no base tables have unpersisted data,
    /// this method returns `PersistencePoint::Persisted`.
    ///
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
    pub(super) async fn min_persisted_replication_offset(
        &self,
    ) -> ReadySetResult<PersistencePoint> {
        let domains = self.domains_with_base_tables().await?;
        let mut min_persisted_offsets = self.query_domains::<_, BaseTableState<PersistencePoint>>(
            domains
                .into_iter()
                .map(|domain| (domain, DomainRequest::RequestMinPersistedReplicationOffset)),
        );

        let mut cur_min = PersistencePoint::Persisted;

        while let Some((_idx, offset)) = min_persisted_offsets.try_next().await? {
            let min_persisted_offset_for_domain = match offset {
                BaseTableState::Initialized(persisted_offset) => persisted_offset,
                BaseTableState::Pending => internal!(
                    "At least one table does not have a replication offset because it is \
                    not ready yet. The caller should wait for all tables to be ready before \
                    requesting replication offsets",
                ),
            };

            match (&cur_min, &min_persisted_offset_for_domain) {
                (PersistencePoint::Persisted, _) => cur_min = min_persisted_offset_for_domain,
                (PersistencePoint::UpTo(_), PersistencePoint::Persisted) => {}
                (PersistencePoint::UpTo(min), PersistencePoint::UpTo(persisted_offset)) => {
                    if persisted_offset.try_partial_cmp(min)?.is_lt() {
                        cur_min = PersistencePoint::UpTo(persisted_offset.clone());
                    }
                }
            }
        }

        Ok(cur_min)
    }

    /// Returns a struct containing the set of all replication offsets within the system, including
    /// the replication offset for the schema stored in the controller and the replication offsets
    /// of all base tables
    ///
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
    pub(super) async fn replication_offsets(&self) -> ReadySetResult<ReplicationOffsets> {
        let domains = self.domains_with_base_tables().await?;
        self.query_domains::<_, NodeMap<BaseTableState<Option<ReplicationOffset>>>>(
            domains
                .into_iter()
                .map(|domain| (domain, DomainRequest::RequestReplicationOffsets)),
        )
        .try_fold(
            ReplicationOffsets::with_schema_offset(self.schema_replication_offset.clone()),
            |mut acc, (domain, domain_offs)| async move {
                for (lni, offset) in domain_offs {
                    let ni = self.domain_nodes[&domain].get(lni).ok_or_else(|| {
                        internal_err!("Domain {} returned nonexistent local node {}", domain, lni)
                    })?;

                    if !self.ingredients[*ni].is_base() {
                        continue;
                    }

                    let table_name = self.ingredients[*ni].name();
                    match offset {
                        BaseTableState::Initialized(offset) => {
                            acc.tables.insert(table_name.clone(), offset);
                        }
                        BaseTableState::Pending => {
                            internal!(
                                "Table {} does not have a replication offset because it is \
                                 not ready yet. The caller should wait for all tables to \
                                 be ready before requesting replication offsets",
                                table_name.display_unquoted()
                            );
                        }
                    }
                }
                Ok(acc)
            },
        )
        .await
    }

    pub(super) fn known_domains(&self) -> HashSet<DomainIndex> {
        self.domains.keys().copied().collect()
    }

    /// Collects a unique list of domains that might contain base tables. Errors out if a domain
    /// retrieved does not appears in self.domains.
    async fn domains_with_base_tables(&self) -> ReadySetResult<HashSet<DomainIndex>> {
        let domains = self
            .tables()
            .values()
            .map(|ni| self.ingredients[*ni].domain())
            .collect::<HashSet<_>>();

        for di in domains.iter() {
            if !self.domains.contains_key(di) {
                return Err(ReadySetError::DomainNotFound {
                    domain_index: di.index(),
                });
            }
        }

        Ok(domains)
    }

    /// Have all domains been placed onto workers in the cluster?
    pub(super) fn all_domains_placed(&self) -> bool {
        self.domain_nodes
            .keys()
            .all(|d| self.domains.get(d).is_some_and(|h| h.is_placed()))
    }

    /// Returns a map of nodes for domains which have not yet been placed onto a worker
    pub(super) fn unplaced_domain_nodes(&self) -> HashMap<DomainIndex, HashSet<NodeIndex>> {
        self.domain_nodes
            .iter()
            .filter(|(d, _)| self.domains.get(d).is_none_or(|dh| !dh.is_placed()))
            .map(|(k, v)| (*k, v.values().copied().collect()))
            .collect()
    }

    /// Returns a list of all table names that are currently involved in snapshotting.
    pub(super) async fn snapshotting_tables(&self) -> ReadySetResult<HashSet<Relation>> {
        let domains = self.domains_with_base_tables().await?;
        let table_indices: Vec<(DomainIndex, LocalNodeIndex)> = self
            .query_domains::<_, Vec<LocalNodeIndex>>(
                domains
                    .into_iter()
                    .map(|domain| (domain, DomainRequest::RequestSnapshottingTables)),
            )
            .map_ok(|(di, local_indices)| {
                stream::iter(
                    local_indices
                        .into_iter()
                        .map(move |li| -> ReadySetResult<_> { Ok((di, li)) }),
                )
            })
            .try_flatten()
            .try_collect()
            .await?;

        table_indices
            .iter()
            .map(|(di, lni)| -> ReadySetResult<Relation> {
                let li = *self.domain_nodes[di].get(*lni).ok_or_else(|| {
                    internal_err!("Domain {} returned nonexistent node {}", di, lni)
                })?;
                let node = &self.ingredients[li];
                debug_assert!(node.is_base());
                Ok(node.name().clone())
            })
            .collect()
    }

    pub(super) async fn all_tables_compacted(&self) -> ReadySetResult<bool> {
        let domains = self.domains_with_base_tables().await?;
        let mut stream = self
            .query_domains::<_, bool>(
                domains
                    .into_iter()
                    .map(|domain| (domain, DomainRequest::AllTablesCompacted)),
            )
            .map_ok(|(_, compacted)| compacted);
        while let Some(finished) = stream.next().await {
            if !finished? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Return a map of node indices to key counts.
    pub(super) async fn node_sizes(&self) -> ReadySetResult<HashMap<NodeIndex, NodeSize>> {
        self.node_sizes_for(self.domains.keys().copied()).await
    }

    /// Like [`Self::node_sizes`], but only queries the given domains.
    async fn node_sizes_for(
        &self,
        domains: impl Iterator<Item = DomainIndex>,
    ) -> ReadySetResult<HashMap<NodeIndex, NodeSize>> {
        // Copying the keys into a vec here is a workaround for a higher order
        // lifetime compile error in `external_request` that occurs if we simply
        // use keys().map() to pass into query_domains directly.
        let counts_per_domain: Vec<(DomainIndex, Option<Vec<(NodeIndex, NodeSize)>>)> = {
            let requests = domains
                .map(|di| (di, DomainRequest::RequestNodeSizes))
                .collect::<Vec<_>>();

            stream::iter(requests)
                .map(move |(domain, request)| {
                    let handle = self.domains.get(&domain).ok_or_else(|| {
                        internal_err!(
                            "Domain {domain:?} is not yet available \
                                 — the server may still be recovering"
                        )
                    });
                    async move {
                        let r = handle?
                            .try_send::<Vec<(NodeIndex, NodeSize)>>(request, &self.workers)
                            .await?;
                        Ok::<_, ReadySetError>((domain, r))
                    }
                })
                .buffer_unordered(CONCURRENT_REQUESTS)
        }
        .try_collect()
        .await?;

        let mut res = HashMap::new();
        for (_domain, counts) in counts_per_domain {
            for (node_index, count) in counts.into_iter().flatten() {
                res.entry(node_index)
                    .and_modify(|s| *s += count)
                    .or_insert(count);
            }
        }
        Ok(res)
    }

    // ** Modify operations **

    /// Perform a new query schema migration.
    pub(crate) async fn migrate<F, T>(
        &mut self,
        dry_run: bool,
        dialect: Dialect,
        f: F,
    ) -> ReadySetResult<T>
    where
        F: FnOnce(&mut Migration<'_>) -> ReadySetResult<T>,
    {
        debug!("starting migration");
        gauge!(metric::CONTROLLER_MIGRATION_IN_PROGRESS).set(1.0);
        let mut m = Migration::new(self, dialect);
        let r = f(&mut m)?;
        m.commit(dry_run).await?;
        debug!("finished migration");
        gauge!(metric::CONTROLLER_MIGRATION_IN_PROGRESS).set(0.0);
        Ok(r)
    }

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is deleted
    ///     once the `Controller` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory. Useful for
    ///     baseline numbers.
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
        worker: Option<WorkerIdentifier>,
        nodes: Vec<NodeIndex>,
    ) -> ReadySetResult<DomainHandle> {
        // Reader nodes are always assigned to their own domains, so it's good enough to see
        // if any of its nodes is a reader.
        // We check for *any* node (and not *all*) since a reader domain has a reader node and an
        // ingress node.

        // check all nodes actually exist
        for n in &nodes {
            if self.ingredients.node_weight(*n).is_none() {
                return Err(ReadySetError::NodeNotFound { index: n.index() });
            }
        }

        let domain_nodes: DomainNodes = nodes
            .iter()
            .map(|ni| {
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

        let mut assigned_worker = None;

        if let Some(worker_id) = worker {
            let domain = idx;

            let builder = DomainBuilder {
                index: idx,
                shard: None,
                nshards: 1,
                config: self.domain_config.clone(),
                nodes: domain_nodes.clone(),
                persistence_parameters: self.persistence.clone(),
            };

            let w = self.workers.get(&worker_id).ok_or_else(|| {
                internal_err!("Domain {domain} scheduled onto nonexistent worker {worker_id}")
            })?;

            let idx = builder.index;

            debug!("sending domain {} to worker {}", domain, w.uri);

            w.run_domain(builder)
                .await
                .map_err(|e| ReadySetError::DomainCreationFailed {
                    domain_index: idx.index(),
                    shard: 0,
                    replica: 0,
                    worker_uri: w.uri.clone(),
                    source: Box::new(e),
                })?;

            // The worker registers `(domain, bind_external)` on the shared
            // `ChannelCoordinator` as part of `RunDomain`, so we no longer need a
            // `RunDomainResponse` round-trip to mirror that registration on the
            // controller side.
            debug!(%domain, "worker booted domain");
            assigned_worker = Some(w.uri.clone());
        }

        Ok(DomainHandle::new(idx, assigned_worker))
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
                .or_default()
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
                .get(&domain)
                .ok_or_else(|| ReadySetError::DomainNotFound {
                    domain_index: domain.index(),
                })?
                .try_send::<()>(DomainRequest::RemoveNodes { nodes }, &self.workers)
                .await
            {
                // The worker failing is an even more efficient way to remove nodes.
                Ok(_) | Err(ReadySetError::WorkerFailed { .. }) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub(super) fn set_schema_replication_offset(&mut self, offset: Option<ReplicationOffset>) {
        self.schema_replication_offset = offset;
    }

    pub(super) async fn flush_partial(&mut self) -> ReadySetResult<u64> {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let mut to_evict = Vec::new();
        for (di, s) in &self.domains {
            let domain_to_evict: Vec<(NodeIndex, u64)> = s
                .try_send::<(DomainStats, HashMap<NodeIndex, NodeStats>)>(
                    DomainRequest::GetStatistics,
                    workers,
                )
                .await?
                // Discard results from non-running domains.
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
                    .get(&di)
                    .unwrap()
                    .try_send::<()>(
                        DomainRequest::Packet(Packet::Evict(Evict {
                            req: Eviction::Bytes {
                                node: Some(na),
                                num_bytes: bytes as usize,
                            },
                            barrier: None,
                        })),
                        workers,
                    )
                    .await?;
                total_evicted += bytes;
            }
        }

        warn!(total_evicted, "flushed partial domain state");

        Ok(total_evicted)
    }

    /// List all replay paths from all domains
    pub(super) async fn replay_paths(
        &self,
    ) -> ReadySetResult<Vec<(DomainIndex, Option<BTreeMap<Tag, ReplayPath>>)>> {
        let mut per_domain: Vec<(DomainIndex, Option<BTreeMap<Tag, ReplayPath>>)> = Vec::new();
        for domain in self.domains.keys() {
            let res = self.domains[domain]
                .try_send::<BTreeMap<Tag, ReplayPath>>(
                    DomainRequest::RequestReplayPaths,
                    &self.workers,
                )
                .await?;
            per_domain.push((*domain, res));
        }

        Ok(per_domain)
    }

    /// *Test only API*
    /// Triggers an eviction for a single key based on the provided `SingleKeyEviction` or randomly
    /// if not provided.
    ///
    /// Returns the `SingleKeyEviction` if an eviction occurs. An eviction might not occur if the
    /// randomly selected Tag contains no materialized state.
    pub(super) async fn evict_single(
        &self,
        eviction_request: Option<SingleKeyEviction>,
    ) -> ReadySetResult<Option<SingleKeyEviction>> {
        let (di, tag, key) = match eviction_request {
            Some(SingleKeyEviction {
                domain_idx,
                tag,
                key,
            }) => (
                domain_idx,
                Tag::new(tag),
                Some(key).filter(|k| !k.is_empty()),
            ),
            None => {
                let tags = self.materializations.partial_tags();
                if tags.is_empty() {
                    trace!("Attempted to evict but found no tags for any partial materialization");
                    return Ok(None);
                }

                let idx = {
                    let mut rng = rand::rng();
                    rng.random_range(0..tags.len())
                };

                let (ni, tag) = tags.get(idx).ok_or_else(|| internal_err!())?;
                let di = self
                    .domain_for_node(ni)
                    .ok_or_else(|| internal_err!("Cannot find the Domain for node {:?}", ni))?;
                (di, *tag, None)
            }
        };
        let res = self
            .domains
            .get(&di)
            .ok_or_else(|| internal_err!())?
            .try_send::<Option<Vec<DfValue>>>(
                DomainRequest::Evict { req: Eviction::SingleKey { tag, key } },
                &self.workers,
            )
            .await?
            .flatten(/* Discard results from non-running domains */)
            .map(|key| SingleKeyEviction {
                domain_idx: di,
                tag: u32::from(tag),
                key,
            });

        Ok(res)
    }

    /// Iterate over all nodes stored in `self::domain_nodes` to find the [`Domain`] that owns the
    /// given `node`.
    ///
    /// Note: This is not a fast way to find which Domain owns a Node. If using this function in a
    /// hot path, consider adding additional information to Materializations for efficient indexing.
    fn domain_for_node(&self, node: &NodeIndex) -> Option<DomainIndex> {
        self.domain_nodes
            .iter()
            .flat_map(|(d, nodes)| nodes.into_iter().map(|(_, ni)| (*d, ni)))
            .find(|(_, ni)| *ni == node)
            .map(|(di, _)| di)
    }

    /// Apply the change list to this state.
    ///
    /// Collects any table status changes in the table_statuses map, if provided.
    pub(super) async fn apply_recipe(
        &mut self,
        changelist: ChangeList,
        dry_run: bool,
        table_statuses: Option<&mut HashMap<Relation, TableStatus>>,
    ) -> Result<(), ReadySetError> {
        // I hate this, but there's no way around for now, as migrations
        // are super entangled with the recipe and the graph.
        let mut new = self.recipe.clone();

        // Only DROP-bearing migrations can produce orphaned domains. Skip the (read-only but
        // O(N+E)) reachability scan for pure ADD/ALTER migrations.
        let has_drops = changelist
            .changes()
            .any(|c| matches!(c, Change::Drop { .. }));

        let r = self
            .migrate(dry_run, changelist.dialect, |mig| {
                new.activate(mig, changelist, table_statuses)
            })
            .await;

        match r {
            Ok(res) => {
                self.recipe = new;
                if !dry_run && has_drops {
                    // Domains whose nodes were all dropped by this migration (e.g. the readers
                    // and shard mergers backing a `DROP CACHE`d query) keep their runtime OS
                    // threads alive otherwise, exhausting tracing-subscriber's per-thread slot
                    // pool over time. See REA-5827.
                    //
                    // Reclamation is OS-resource cleanup, not a correctness operation: never let
                    // it demote a successful migration. Any kill failures are logged and the
                    // orphaned controller bookkeeping is purged regardless.
                    if let Err(e) = self.reclaim_orphaned_domains().await {
                        warn!(
                            error = %e,
                            "reclaim_orphaned_domains failed; will retry next migration",
                        );
                    }
                }
                Ok(res)
            }
            Err(e) => {
                debug!(
                    error = %e,
                    "failed to apply recipe. Will retry periodically up to max_processing_minutes."
                );
                Err(e)
            }
        }
    }

    /// Extend our state with a recipe.
    ///
    /// Collects any table status changes in the table_statuses map, if provided.
    pub(super) async fn extend_recipe(
        &mut self,
        recipe_spec: ExtendRecipeSpec<'_>,
        dry_run: bool,
        table_statuses: Option<&mut HashMap<Relation, TableStatus>>,
    ) -> Result<(), ReadySetError> {
        set_failpoint!(failpoints::EXTEND_RECIPE);
        // Drop recipes from the replicator that we have already processed.
        if let (Some(new), Some(current)) = (
            &recipe_spec.replication_offset,
            &self.schema_replication_offset,
        ) {
            if current >= new {
                // no-op
                return Ok(());
            }
        }

        if let Some(create_cache) = recipe_spec
            .changes
            .changes()
            .find_map(|change| match change {
                Change::CreateCache(create_cache)
                    if create_cache
                        .schema_generation_used
                        .is_some_and(|g| g != self.schema_generation) =>
                {
                    Some(create_cache)
                }
                _ => None,
            })
        {
            let err = ReadySetError::SchemaGenerationMismatch {
                used: create_cache.schema_generation_used.map(|g| g.get()),
                current: self.schema_generation.get(),
            };
            counter!(metric::SCHEMA_GENERATION_MISMATCH).increment(1);
            warn!(%err, ?create_cache, "Schema generation mismatch");
            return Err(err);
        }

        let should_increment_schema_generation = recipe_spec
            .changes
            .changes()
            .any(|change| !matches!(change, Change::CreateCache(_)));

        match self
            .apply_recipe(recipe_spec.changes, dry_run, table_statuses)
            .await
        {
            Ok(x) => {
                if let Some(offset) = &recipe_spec.replication_offset {
                    debug!(%offset, "Updating schema replication offset");
                    offset.try_max_into(&mut self.schema_replication_offset)?
                }
                if !dry_run && should_increment_schema_generation {
                    self.schema_generation = self.schema_generation.next();
                    counter!(metric::SCHEMA_CATALOG_GENERATION_INCREMENTED).increment(1);
                    trace!(
                        schema_generation = %self.schema_generation,
                        "Incremented schema generation after successful migration"
                    );
                }

                Ok(x)
            }
            Err(e) => Err(e),
        }
    }

    /// Return 1 if one or more expressions were removed, else return 0.
    /// Someday we may want to return # expressions (and aliases?) dropped.
    pub(super) async fn remove_query(&mut self, query_name: &Relation) -> ReadySetResult<u64> {
        let name = match self.recipe.resolve_alias(query_name) {
            None => return Ok(0),
            Some(name) => name,
        };

        let changelist = ChangeList::from_change(
            Change::Drop {
                name: name.clone(),
                if_exists: false,
            },
            Dialect::DEFAULT_MYSQL,
        );

        if let Err(error) = self.apply_recipe(changelist, false, None).await {
            error!(%error, "Failed to apply recipe");
            return Err(error);
        }

        Ok(1)
    }

    pub(super) async fn remove_all_queries(&mut self) -> ReadySetResult<()> {
        let changes = self
            .recipe
            .cache_names()
            .map(|n| Change::Drop {
                name: n.clone(),
                if_exists: true,
            })
            .collect::<Vec<_>>();

        self.apply_recipe(
            ChangeList::from_changes(changes, Dialect::DEFAULT_MYSQL),
            false,
            None,
        )
        .await
    }

    /// Reset the dataflow state to empty, removing all base tables, caches, views, and domain
    /// state. Preserves configuration (dialect, sql/mir config, persistence, etc.).
    ///
    /// This bypasses the migration system entirely because no domains are running at this point
    /// (called on startup before worker recovery).
    ///
    /// Returns `true` if tables were actually removed, `false` if state was already empty.
    pub(super) fn remove_all_tables(&mut self) -> bool {
        if self.recipe.table_names().next().is_none() {
            return false;
        }

        self.recipe.reset();
        let recipe = self.recipe.clone();

        let mut g = Graph::new();
        let source = g.add_node(node::Node::new::<_, _, Vec<Column>, _>(
            "source",
            Vec::new(),
            node::special::Source,
        ));

        let mut materializations = Materializations::new();
        materializations.set_config(self.materializations.config.clone());

        // Preserve the shared `Arc<ChannelCoordinator>` across the reset so the in-process
        // Worker's registered domain senders stay reachable; the previous owner of `self`
        // is dropped after this line, so cloning before the reassignment is required.
        let channel_coordinator = self.channel_coordinator.clone();
        *self = DfState::new(
            g,
            source,
            0,
            self.domain_config.clone(),
            self.persistence.clone(),
            materializations,
            recipe,
            None,
            channel_coordinator,
        );

        true
    }

    /// Remove dataflow persistent state (`.db` RocksDB directories) from the storage directory
    /// and the working temp directory (if configured and different from storage_dir).
    ///
    /// This is called unconditionally when replication is disabled (shallow-only mode) because
    /// stale files may exist on disk even if the authority state has no tables (e.g. fresh
    /// authority or deserialization failure).
    ///
    /// Only `.db` entries are removed; the standalone authority's `.auth` database (which
    /// stores shallow cache DDL requests) is preserved.
    pub(super) async fn remove_persistent_state(&self) {
        if self.persistence.mode != DurabilityMode::Permanent {
            return;
        }

        if let Some(ref storage_dir) = self.persistence.storage_dir {
            Self::remove_db_entries(storage_dir).await;
        }

        if let Some(ref working_dir) = self.persistence.working_temp_dir {
            if self.persistence.storage_dir.as_ref() != Some(working_dir) {
                Self::remove_db_entries(working_dir).await;
            }
        }
    }

    /// Remove only `.db` entries (dataflow RocksDB directories) from `dir`, preserving
    /// everything else (in particular the standalone authority `.auth` database).
    async fn remove_db_entries(dir: &Path) {
        let mut entries = match fs::read_dir(dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                error!(path = %dir.display(), error = %e, "Failed to read directory");
                return;
            }
        };

        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    error!(path = %dir.display(), error = %e, "Failed to read directory entry");
                    break;
                }
            };

            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "db") {
                if let Err(e) = fs::remove_dir_all(&path).await {
                    error!(
                        path = %path.display(),
                        error = %e,
                        "Failed to remove dataflow state directory"
                    );
                }
            }
        }
    }

    /// Remove all references to the given [`WorkerIdentifier`] from the runtime state of the
    /// dataflow graph, returning a list of domains that were known to be running on that worker.
    pub(super) fn remove_worker(&mut self, wi: &WorkerIdentifier) -> Vec<DomainIndex> {
        self.workers.remove(wi);
        self.read_addrs.remove(wi);
        let mut res = vec![];
        for dh in self.domains.values_mut() {
            if let Some(domain) = dh.remove_worker(wi) {
                self.channel_coordinator.remove(domain);
                res.push(domain);
            }
        }
        res
    }

    /// Return a set of domain indexes that are logically downstream of the given domain in the
    /// graph
    ///
    /// NOTE: this is asymptotically kinda crappy right now, but ideally this is needed rarely
    /// enough and our graphs are small enough in practice that that isn't a huge deal
    pub(super) fn downstream_domains(
        &self,
        domain: DomainIndex,
    ) -> ReadySetResult<HashSet<DomainIndex>> {
        let mut res = HashSet::new();
        for (_, ni) in
            self.domain_nodes
                .get(&domain)
                .ok_or_else(|| ReadySetError::UnknownDomain {
                    domain_index: domain.index(),
                })?
        {
            let mut bfs = petgraph::visit::Bfs::new(&self.ingredients, *ni);
            while let Some(ni) = bfs.next(&self.ingredients) {
                let downstream_domain = self.ingredients[ni].domain();
                if downstream_domain != domain {
                    res.insert(downstream_domain);
                }
            }
        }

        Ok(res)
    }

    /// Send requests to the workers running the given domains to kill them, and remove the domains
    /// from runtime state.
    pub(super) async fn kill_domains<I>(&mut self, domains: I) -> ReadySetResult<()>
    where
        I: IntoIterator<Item = DomainIndex>,
    {
        let mut workers_to_domains: HashMap<_, Vec1<_>> = HashMap::new();
        for di in domains {
            let Some(dh) = self.domains.get(&di) else {
                debug!(domain = %di, "domain not running, not killing");
                continue;
            };

            for (addr, wi) in dh.assignments() {
                workers_to_domains
                    .entry(wi.clone())
                    .and_modify(|v| v.push(addr))
                    .or_insert_with(|| vec1![addr]);
            }
        }

        let mut futs = FuturesUnordered::new();
        for (worker_url, domains) in workers_to_domains {
            let worker = self.workers.get(&worker_url).ok_or_else(|| {
                internal_err!("Worker not found for url {worker_url} to kill domains")
            })?;
            futs.push(worker.kill_domains(domains.clone()).map_ok(|()| domains));
        }
        while let Some(r) = futs.next().await {
            for killed_addr in r? {
                if let Some(dh) = self.domains.get_mut(&killed_addr) {
                    dh.clear_assignment();
                }
            }
        }

        Ok(())
    }

    /// Find domains whose nodes are all orphaned — that is, no non-dropped Reader or Base remains
    /// that reaches them via incoming edges. Used to identify domains whose runtime threads can
    /// be reclaimed after a drop migration (see REA-5827).
    ///
    /// We can't just check `is_dropped()` per node: when a cache is dropped, the drop path only
    /// flags the Reader itself; routing nodes (Ingress, Egress) and Constant (`VALUES`) nodes
    /// feeding that Reader stay un-flagged. So we instead compute reachability backwards from
    /// every live Reader and Base, and consider any domain disjoint from that set as orphaned.
    ///
    /// Bases — but not Constants — anchor reachability even when no live Reader points to them.
    /// A Base survives as long as its underlying table exists (`DROP TABLE` flags it dropped),
    /// because tearing down a Base would force the whole table to be re-replicated from upstream.
    /// Constants are per-query inline data with no such cost: when their owning query is gone,
    /// they should be reclaimed alongside it.
    pub(super) fn find_orphaned_domains(&self) -> HashSet<DomainIndex> {
        let n_nodes = self.ingredients.node_count();
        let mut needed: HashSet<NodeIndex> = HashSet::with_capacity(n_nodes);
        let mut queue: VecDeque<NodeIndex> = VecDeque::with_capacity(n_nodes / 4 + 1);
        for (ni, n) in self.ingredients.node_references() {
            if n.is_dropped() {
                continue;
            }
            if (n.is_reader() || n.is_base()) && needed.insert(ni) {
                queue.push_back(ni);
            }
        }
        while let Some(ni) = queue.pop_front() {
            for parent in self
                .ingredients
                .neighbors_directed(ni, petgraph::Direction::Incoming)
            {
                if !self.ingredients[parent].is_dropped() && needed.insert(parent) {
                    queue.push_back(parent);
                }
            }
        }

        self.domain_nodes
            .iter()
            .filter(|(_, nodes)| nodes.iter().all(|(_, ni)| !needed.contains(ni)))
            .map(|(di, _)| *di)
            .collect()
    }

    /// Kill the runtime threads of any orphaned domains and purge their bookkeeping from the
    /// controller (`self.domains`, `self.domain_nodes`, `self.domain_node_index_pairs`,
    /// [`ChannelCoordinator`], and [`Materializations`] entries) and flag any non-dropped
    /// graph nodes that still belonged to those domains as dropped — closing the orphan loop the
    /// upstream drop path leaves open for routing/Constant nodes (see REA-5827).
    ///
    /// This is best-effort by design: the kill RPCs may fail mid-fan-out (worker partitioned,
    /// timed out, gone). We log and proceed — the migration's logical outcome has already
    /// committed, and any controller-local bookkeeping must be purged regardless to avoid
    /// re-issuing kill RPCs against the same dead domains on every subsequent migration.
    pub(super) async fn reclaim_orphaned_domains(&mut self) -> ReadySetResult<()> {
        let start = Instant::now();
        let orphaned = self.find_orphaned_domains();
        if orphaned.is_empty() {
            return Ok(());
        }

        // Snapshot the assigned domains to evict from the channel coordinator before
        // `kill_domains` clears `DomainHandle::worker` via `remove_assignment`. Note that
        // `dh.assignments()` already filters unscheduled domains, which is what we want —
        // those were never registered with the coordinator.
        let domains_to_evict: Vec<DomainIndex> = orphaned
            .iter()
            .filter_map(|di| self.domains.get(di))
            .flat_map(|dh| dh.assignments().map(|(addr, _)| addr))
            .collect();

        if let Err(e) = self.kill_domains(orphaned.iter().copied()).await {
            warn!(
                error = %e,
                ?orphaned,
                "kill_domains partial failure during reclamation; purging local state anyway",
            );
        }

        for addr in domains_to_evict {
            self.channel_coordinator.remove(addr);
        }

        // Flag any non-dropped graph nodes that lived in the reclaimed domains. This handles
        // routing nodes (Ingress, Egress) that the drop path didn't walk to, and Constant
        // nodes whose only consumer was the dropped Reader. Without this, those nodes stay
        // un-flagged in `self.ingredients` while their `domain()` references a domain that's
        // about to be removed from `self.domains` — a "node has no living domain" invariant
        // violation that breaks recovery and `materialization_info`.
        //
        // We also disconnect their edges to any non-dropped neighbor, otherwise the next
        // migration's `routing::connect` walk would follow an Egress→Ingress edge into a
        // freshly-dropped Ingress and trip the `is_ingress()` invariant.
        let mut nodes_to_flag: Vec<NodeIndex> = Vec::new();
        for di in &orphaned {
            if let Some(nodes) = self.domain_nodes.get(di) {
                nodes_to_flag.extend(nodes.iter().map(|(_, ni)| *ni));
            }
        }
        for ni in &nodes_to_flag {
            let neighbors: Vec<NodeIndex> = self
                .ingredients
                .neighbors_undirected(*ni)
                .filter(|nbr| !self.ingredients[*nbr].is_dropped())
                .collect();
            for nbr in neighbors {
                if let Some(edge) = self.ingredients.find_edge(*ni, nbr) {
                    self.ingredients.remove_edge(edge);
                }
                if let Some(edge) = self.ingredients.find_edge(nbr, *ni) {
                    self.ingredients.remove_edge(edge);
                }
            }
            if let Some(node) = self.ingredients.node_weight_mut(*ni) {
                if !node.is_dropped() {
                    node.remove();
                }
            }
            self.materializations.paths.remove(ni);
            self.materializations.redundant_partial.remove(ni);
        }
        // `redundant_partial` maps partial-parent → full-duplicate; if either side is gone,
        // drop the entry.
        let stale_keys: Vec<NodeIndex> = self
            .materializations
            .redundant_partial
            .iter()
            .filter(|(k, v)| {
                self.ingredients[**k].is_dropped() || self.ingredients[**v].is_dropped()
            })
            .map(|(k, _)| *k)
            .collect();
        for k in stale_keys {
            self.materializations.redundant_partial.remove(&k);
        }

        for di in &orphaned {
            self.domains.remove(di);
            self.domain_nodes.remove(di);
            self.domain_node_index_pairs.remove(di);
        }

        counter!(metric::CONTROLLER_RECLAIMED_DOMAINS).increment(orphaned.len() as u64);
        info!(
            count = orphaned.len(),
            elapsed_us = start.elapsed().as_micros() as u64,
            ?orphaned,
            "Reclaimed orphaned domains",
        );
        Ok(())
    }

    /// Runs all the necessary steps to recover the full [`DfState`], when said state only
    /// has the bare minimum information.
    ///
    /// # Invariants
    /// The following invariants must hold:
    /// - `self.ingredients` must be a valid [`Graph`]. This means it has to be a description of
    ///   a valid dataflow graph.
    /// - Each node must have a Domain associated with it (and thus also have a [`LocalNodeIndex`]
    ///   and any other associated information).
    /// - `self.domain_nodes` must be valid. This means that all the nodes in `self.ingredients`
    ///   (except for `self.source`) must belong to a domain; and there must not be any overlap
    ///   between the nodes owned by each domain. All the invariants for Domain assignment from
    ///   [`crate::controller::migrate::assignment::assign`] must hold as well.
    ///  - `self.remap` must be valid.
    /// - All the other fields should be empty or `[Default::default()]`.
    pub(super) async fn plan_recovery(
        &mut self,
        domain_nodes: &HashMap<DomainIndex, HashSet<NodeIndex>>,
    ) -> ReadySetResult<DomainMigrationPlan> {
        info!("Planning recovery");
        let mut dmp = DomainMigrationPlan::new(DomainMigrationMode::Recover, self.known_domains());
        let domain_nodes = domain_nodes
            .iter()
            .map(|(idx, nm)| (*idx, nm.iter().copied().collect::<Vec<_>>()))
            .collect::<HashMap<_, _>>();
        let mut new = HashSet::new();
        {
            for (domain, nodes) in domain_nodes {
                let worker = schedule_domain(self, &None, domain, &nodes[..])?;

                let already_placed = self.domains.get(&domain).is_some_and(|dh| dh.is_placed());
                if !already_placed && worker.is_none() {
                    dmp.domain_failed_placement(domain);
                }

                dmp.place_domain(domain, worker, nodes.clone());
                dmp.register_domain(domain);
                new.extend(nodes);
            }
        }

        routing::connect(&self.ingredients, &mut dmp, &new)?;

        self.materializations
            .extend(&mut self.ingredients, &new, &dmp)?;

        self.materializations
            .commit(&mut self.ingredients, &new, &mut dmp)?;

        Ok(dmp)
    }
}

/// This structure acts as a wrapper for a [`DfStateReader`] in order to guarantee
/// thread-safe access (read and writes) to ReadySet's dataflow state.
///
/// # Overview
/// Two operations can be performed by this structure.
///
/// ## Reads
/// Reads are performed by taking a read lock on the underlying [`DfStateReader`].
/// This allows any thread to freely get a read-only view of the dataflow state without having
/// to worry about other threads attempting to modify it.
///
/// The choice of using [`tokio::sync::RwLock`] instead of [`std::sync::RwLock`] or
/// [`parking_lot::RwLock`] was made in order to ensure that:
/// 1. The read lock does not starve writers attempting to modify the dataflow state, as the lock is
///    fair and will block reader threads if there are writers waiting for the lock.
/// 2. To ensure that the read lock can be used by multiple threads, since the lock is `Send` and
///    `Sync`.
///
/// ## Writes
/// Writes are performed by following a couple of steps:
/// 1. A mutex is acquired to ensure that only one write is in progress at any time.
/// 2. A copy of the current [`DfState`] (being held by the [`DfStateReader`] is made.
/// 3. A [`DfStateWriter`] is created from the copy and the mutex guard, having the lifetime of the
///    latter.
/// 4. All the computations/modifications are performed on the [`DfStateWriter`] (aka, on the
///    underlying [`DfState`] copy).
/// 5. The [`DfStateWriter`] is then committed to the [`DfState`] by calling
///    [`DfStateWriter::commit`], which replaces the old state by the new one in the
///    [`DfStateReader`].
///
/// As previously mentioned for reads, the choice of using [`tokio::sync::RwLock`] ensures writers
/// fairness and the ability to use the lock by multiple threads.
///
/// Following the three steps to perform a write guarantees that:
/// 1. Writes don't starve readers: when we start a write operation, we take a read lock for the
///    [`DfStateReader`] in order to make a copy of a state. In doing so, we ensure that readers can
///    continue to read the state: no modification has been made yet. Writers can also perform all
///    their computing/modifications (which can be pretty expensive time-wise), and only then the
///    changes can be committed by swapping the old state for the new one, which is the only time
///    readers are forced to wait.
/// 2. If a write operation fails, the state is not modified. TODO(fran): Even though the state is
///    not modified here, we might have sent messages to other workers/domains.   It is worth
///    looking into a better way of handling that (if it's even necessary).   Such a mechanism was
///    never in place to begin with.
/// 3. Writes are transactional: if there is an instance of [`DfStateWriter`] in existence, then all
///    the other writes must wait. This guarantees that the operations performed to the dataflow
///    state are executed transactionally.
///
/// # How to use
/// ## Reading the state
/// To get read-only access to the dataflow state, the [`DfState::read`] method must be used.
/// This method returns a read guard to another wrapper structure, [`DfStateReader`],
/// which only allows reference access to the dataflow state.
///
/// ## Modifying the state
/// To get write and read access to the dataflow state, the [`DfState::write`] method must be
/// used. This method returns a write guard to another wrapper structure, [`DfStateWriter`]
/// which will allow to get a reference or mutable reference to a [`DfState`], which starts
/// off as a copy of the actual dataflow state.
///
/// Once all the computations/modifications are done, the [`DfStateWriter`] must be passed on
/// to the [`DfStateWriter::commit`] to be committed and destroyed.
pub(super) struct DfStateHandle {
    /// A read/write lock protecting the [`DfStateWriter`] from
    /// being accessed directly and in a non-thread-safe way.
    reader: RwLock<DfStateReader>,
    /// A mutex used to ensure that writes are transactional (there's
    /// only one writer at a time holding an instance of [`DfStateWriter`]).
    write_guard: Mutex<()>,
    /// Optional notifier for schema catalog updates.
    schema_change_notifier: Option<crate::controller::events::EventsHandle>,
}

impl DfStateHandle {
    /// Creates a new instance of [`DfStateHandle`].
    pub(super) fn new(
        dataflow_state: DfState,
        schema_change_notifier: Option<crate::controller::events::EventsHandle>,
    ) -> Self {
        Self {
            reader: RwLock::new(DfStateReader {
                state: dataflow_state,
            }),
            write_guard: Mutex::new(()),
            schema_change_notifier,
        }
    }

    /// Acquires a read lock over the dataflow state, and returns the
    /// read guard.
    /// This method will block if there's a [`DfStateHandle::commit`] operation
    /// taking place.
    pub(super) async fn read(&self) -> RwLockReadGuard<'_, DfStateReader> {
        self.reader.read().await
    }

    /// Creates a new instance of a [`DfStateWriter`].
    /// This method will block if there's a [`DfStateHandle::commit`] operation
    /// taking place, or if there exists a thread that owns
    /// an instance of [`DfStateWriter`].
    pub(super) async fn write(&self) -> DfStateWriter<'_> {
        let write_guard = self.write_guard.lock().await;
        let read_guard = self.reader.read().await;
        let start = Instant::now();
        let state_copy = read_guard.state.clone();
        let elapsed = start.elapsed();
        histogram!(metric::DATAFLOW_STATE_CLONE_TIME,).record(elapsed.as_micros() as f64);
        DfStateWriter {
            state: state_copy,
            _guard: write_guard,
        }
    }

    /// Commits the changes made to the dataflow state.
    /// This method will block if there are threads reading the dataflow state.
    pub(super) async fn commit(
        &self,
        writer: DfStateWriter<'_>,
        authority: &Arc<Authority>,
    ) -> ReadySetResult<()> {
        let previous_generation = {
            let guard = self.reader.read().await;
            guard.state.schema_generation()
        };
        let new_state = &writer.state;

        let mut state_guard = self.reader.write().await;
        state_guard.replace(new_state.clone());
        drop(state_guard);

        // The schema replication offset says how far schema replication has progressed.  It
        // must only advance after the schema catalog is durable.
        let mut schema_persisted = true;

        if new_state.schema_generation() != previous_generation {
            if let Some(notifier) = &self.schema_change_notifier {
                // The snapshot stored by `EventsHandle` is a complete `SchemaCatalog`, so partial
                // deltas (if ever added) cannot be accidentally cached.
                let catalog = new_state
                    .recipe
                    .schema_catalog(new_state.schema_generation());
                match notifier.send_schema_catalog_update(catalog) {
                    Ok(()) => {
                        counter!(metric::SCHEMA_CATALOG_UPDATE_SENT).increment(1);
                    }
                    Err(error) => {
                        counter!(metric::SCHEMA_CATALOG_UPDATE_SERIALIZATION_FAILED).increment(1);
                        error!(%error, "Failed to serialize schema catalog update");
                    }
                }
            }

            // Dual-write the persistent schema catalog for future use in recovery.
            let entries = new_state.recipe.to_schema_catalog_entries();
            if let Err(error) = authority.overwrite_schema_catalog(entries).await {
                error!(%error, "Failed to persist schema catalog");
                schema_persisted = false;
            }
            let custom_types = new_state.recipe.to_persisted_custom_types();
            if let Err(error) = authority.overwrite_custom_types(custom_types).await {
                error!(%error, "Failed to persist custom types");
                schema_persisted = false;
            }
            let non_replicated = new_state.recipe.to_persisted_non_replicated_relations();
            if let Err(error) = authority
                .overwrite_non_replicated_relations(non_replicated)
                .await
            {
                error!(%error, "Failed to persist non-replicated relations");
                schema_persisted = false;
            }
        }

        if schema_persisted {
            if let Some(offset) = new_state.schema_replication_offset() {
                if let Err(error) = authority
                    .overwrite_schema_replication_offset(offset.clone())
                    .await
                {
                    error!(%error, "Failed to persist schema replication offset");
                }
            }
        } else {
            warn!("Skipping schema replication offset persist after failed schema catalog write");
        }

        Ok(())
    }
}

/// A read-only wrapper around the dataflow state.
/// This struct implements [`Deref`] in order to provide read-only access to the inner
/// [`DfState`]. No implementation of [`DerefMut`] is provided, nor any other way of accessing
/// the inner [`DfState`] in a mutable way.
pub(super) struct DfStateReader {
    state: DfState,
}

impl DfStateReader {
    /// Replaces the dataflow state with a new one.
    /// This method is meant to be used by the [`DfStateHandle`] only, in order
    /// to atomically swap the dataflow state view exposed to the users.
    // This method MUST NEVER become public, as this guarantees
    // that only the [`DfStateHandle`] can modify it.
    fn replace(&mut self, state: DfState) {
        self.state = state;
    }
}

impl Deref for DfStateReader {
    type Target = DfState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// A read and write wrapper around the dataflow state.
/// This struct implements [`Deref`] to provide read-only access to the inner [`DfState`],
/// as well as [`DerefMut`] to allow mutability.
/// To commit the modifications made to the [`DfState`], use the
/// [`DfStateHandle::commit`] method. Dropping this struct without calling
/// [`DfStateHandle::commit`] will cause the modifications to be discarded, and the lock
/// preventing other writer threads to acquiring an instance to be released.
pub(super) struct DfStateWriter<'handle> {
    state: DfState,
    _guard: MutexGuard<'handle, ()>,
}

impl AsRef<DfState> for DfStateWriter<'_> {
    fn as_ref(&self) -> &DfState {
        &self.state
    }
}

impl AsMut<DfState> for DfStateWriter<'_> {
    fn as_mut(&mut self) -> &mut DfState {
        &mut self.state
    }
}
