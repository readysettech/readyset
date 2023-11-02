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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use array2::Array2;
use common::{IndexPair, Tag};
use dataflow::payload::EvictRequest;
use dataflow::prelude::{ChannelCoordinator, DomainIndex, DomainNodes, Graph, NodeIndex};
use dataflow::{
    BaseTableState, DomainBuilder, DomainConfig, DomainRequest, NodeMap, Packet,
    PersistenceParameters, Sharding,
};
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use futures::{FutureExt, TryFutureExt, TryStream};
use metrics::{gauge, histogram};
use nom_sql::{CreateCacheStatement, NonReplicatedRelation, Relation, SqlIdentifier, SqlQuery};
use petgraph::visit::{Bfs, IntoNodeReferences};
use petgraph::Direction;
use rand::Rng;
use readyset_client::builders::{
    ReaderHandleBuilder, ReusedReaderHandleBuilder, TableBuilder, ViewBuilder,
};
use readyset_client::consensus::{Authority, AuthorityControl, CreateCacheRequest};
use readyset_client::debug::info::{GraphInfo, MaterializationInfo, NodeSize};
use readyset_client::debug::stats::{DomainStats, GraphStats, NodeStats};
use readyset_client::internal::{MaterializationStatus, ReplicaAddress};
use readyset_client::metrics::recorded;
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::recipe::ExtendRecipeSpec;
use readyset_client::{
    PersistencePoint, SingleKeyEviction, TableReplicationStatus, TableStatus, ViewCreateRequest,
    ViewFilter, ViewRequest, ViewSchema,
};
use readyset_data::{DfValue, Dialect};
use readyset_errors::{
    internal, internal_err, invariant_eq, NodeType, ReadySetError, ReadySetResult,
};
use replication_offset::{ReplicationOffset, ReplicationOffsets};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, instrument, trace, warn};
use vec1::{vec1, Vec1};

use super::migrate::DomainSettings;
use super::replication::ReplicationStrategy;
use super::sql::Recipe;
use crate::controller::domain_handle::DomainHandle;
use crate::controller::migrate::materialization::Materializations;
use crate::controller::migrate::scheduling::Scheduler;
use crate::controller::migrate::{routing, DomainMigrationMode, DomainMigrationPlan, Migration};
use crate::controller::sql::Schema;
use crate::controller::{
    schema, ControllerState, DomainPlacementRestriction, NodeRestrictionKey, Worker,
    WorkerIdentifier,
};
use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::internal::LocalNodeIndex;
use crate::worker::WorkerRequestKind;

mod graphviz;

pub(in crate::controller) use self::graphviz::Graphviz;

/// Number of concurrent requests to make when making multiple simultaneous requests to domains (eg
/// for replication offsets)
const CONCURRENT_REQUESTS: usize = 16;

/// Set of relevant changes applied to a recipe during a call to `extend_recipe`
#[derive(Debug, Clone, Default)]
pub(crate) struct RecipeChanges {
    /// List of new cache statements which have been added, along with the schema search path used
    /// when they were created, serialized to a String
    pub(crate) new_cache_statements: Vec<String>,
}

impl RecipeChanges {
    /// Add a new unparsed `create cache` statement to this set of recipe changes
    pub(crate) fn add_cache_statement(
        &mut self,
        unparsed_stmt: String,
        schema_search_path: Vec<SqlIdentifier>,
        dialect: Dialect,
    ) -> ReadySetResult<()> {
        self.new_cache_statements
            .push(serde_json::ser::to_string(&CreateCacheRequest {
                unparsed_stmt,
                schema_search_path,
                dialect,
            })?);

        Ok(())
    }
}

/// This structure holds all the dataflow state.
/// It's meant to be handled exclusively by the [`DfStateHandle`], which is the structure
/// that guarantees thread-safe access to it.
#[derive(Clone, Serialize, Deserialize)]
pub struct DfState {
    pub(super) ingredients: Graph,

    /// ID for the root node in the graph. This is used to retrieve a list of base tables.
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    pub(super) domain_config: DomainConfig,

    pub(super) replication_strategy: ReplicationStrategy,

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

    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    /// Map from local to global node index for each domain
    pub(super) domain_nodes: HashMap<DomainIndex, NodeMap<NodeIndex>>,
    /// Map from global node index to index pair for each domain
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
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
        sharding: Option<usize>,
        domain_config: DomainConfig,
        persistence: PersistenceParameters,
        materializations: Materializations,
        recipe: Recipe,
        schema_replication_offset: Option<ReplicationOffset>,
        node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,
        channel_coordinator: Arc<ChannelCoordinator>,
        replication_strategy: ReplicationStrategy,
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
            domain_node_index_pairs: Default::default(),
            replication_strategy,
        }
    }

    pub(super) fn schema_replication_offset(&self) -> &Option<ReplicationOffset> {
        &self.schema_replication_offset
    }

    pub(super) fn get_info(&self) -> ReadySetResult<GraphInfo> {
        let mut worker_info = HashMap::new();
        for (di, dh) in self.domains.iter() {
            for (shard, replicas) in dh.shards().enumerate() {
                for (replica, url) in replicas.iter().enumerate() {
                    let Some(url) = url else {
                        continue;
                    };
                    worker_info
                        .entry(url.clone())
                        .or_insert_with(HashMap::new)
                        .entry(ReplicaAddress {
                            domain_index: *di,
                            shard,
                            replica,
                        })
                        .or_insert_with(Vec::new)
                        .extend(
                            self.domain_nodes
                                .get(di)
                                .ok_or_else(|| {
                                    internal_err!("{:?} in domains but not in domain_nodes", di)
                                })?
                                .values(),
                        )
                }
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
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let base = &self.ingredients[n];

                if base.is_dropped() {
                    None
                } else {
                    assert!(base.is_base());
                    Some((base.name().clone(), n))
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
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let name = self.ingredients[n].name().clone();
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
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
    pub(super) fn verbose_views(&self) -> Vec<CreateCacheStatement> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                if self.ingredients[n].is_reader() {
                    #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                    let name = self.ingredients[n].name().clone();

                    // Alias should always resolve to an id and id should always resolve to an
                    // expression. However, this mapping will not catch bugs that break this
                    // assumption
                    let name = self.recipe.resolve_alias(&name)?;
                    let query = self.recipe.expression_by_alias(name)?;

                    // Only return ingredients created from "CREATE CACHE"
                    match query {
                        // CacheInner::ID should have been expanded to CacheInner::Statement
                        SqlQuery::CreateCache(stmt) if stmt.inner.is_ok() => Some(stmt),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn view_statuses(
        &self,
        queries: Vec<ViewCreateRequest>,
        dialect: Dialect,
    ) -> Vec<bool> {
        queries
            .into_iter()
            .map(|query| self.recipe.contains(query, dialect).unwrap_or(false))
            .collect()
    }

    pub(super) fn find_reader_for(
        &self,
        node: NodeIndex,
        name: &Relation,
        filter: &Option<ViewFilter>,
    ) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
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

        #[allow(clippy::indexing_slicing)] // `find_reader_for` returns valid indices
        let domain_index = self.ingredients[reader_node].domain();
        #[allow(clippy::indexing_slicing)] // `find_reader_for` returns valid indices
        let reader = self.ingredients[reader_node].as_reader().ok_or_else(|| {
            ReadySetError::InvalidNodeType {
                node_index: self.ingredients[reader_node].local_addr().id(),
                expected_type: NodeType::Reader,
            }
        })?;
        #[allow(clippy::indexing_slicing)] // `find_readers_for` returns valid indices
        let returned_cols = reader
            .reader_processing()
            .post_processing
            .returned_cols
            .clone()
            .unwrap_or_else(|| (0..self.ingredients[reader_node].columns().len()).collect());
        #[allow(clippy::indexing_slicing)] // just came from self
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

        let replicas = (0..domain.num_replicas())
            .map(|replica| {
                (0..domain.num_shards())
                    .map(|shard| {
                        domain
                            .assignment(shard, replica)
                            .map(|worker| {
                                self.read_addrs
                                    .get(worker)
                                    .ok_or_else(|| ReadySetError::UnmappableDomain {
                                        domain_index: domain_index.index(),
                                    })
                                    .copied()
                            })
                            .transpose()
                    })
                    .collect::<ReadySetResult<Vec<_>>>()
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        Ok(Some(ReaderHandleBuilder {
            name: name.clone(),
            node: reader_node,
            columns: columns.into(),
            schema,
            replica_shard_addrs: Array2::from_rows(replicas),
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
                expected_type: NodeType::Reader,
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

        let domain =
            self.domains
                .get(&node.domain())
                .ok_or_else(|| ReadySetError::UnknownDomain {
                    domain_index: node.domain().index(),
                })?;

        invariant_eq!(
            domain.num_replicas(),
            1,
            "Base table domains can't be replicated"
        );

        let txs = (0..domain.num_shards())
            .map(|shard| {
                let replica_addr = ReplicaAddress {
                    domain_index: node.domain(),
                    shard,
                    replica: 0, // Base tables can't currently be replicated
                };
                self.channel_coordinator
                    .get_addr(&replica_addr)
                    .ok_or_else(|| {
                        internal_err!("failed to get channel coordinator for {}", replica_addr)
                    })
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

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
            txs,
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
            domains.extend(
                s.send_to_healthy(DomainRequest::GetStatistics, workers)
                    .await?
                    .into_entries()
                    .map(|((shard, replica), stats)| {
                        (
                            ReplicaAddress {
                                domain_index,
                                shard,
                                replica,
                            },
                            stats,
                        )
                    }),
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

    pub(super) fn graphviz(
        &self,
        detailed: bool,
        node_sizes: Option<HashMap<NodeIndex, NodeSize>>,
    ) -> String {
        Graphviz {
            graph: &self.ingredients,
            detailed,
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
        detailed: bool,
        node_sizes: Option<HashMap<NodeIndex, NodeSize>>,
    ) -> ReadySetResult<String> {
        let ni = self
            .recipe
            .node_addr_for(query)
            .ok()
            .or_else(|| self.views().get(query).copied())
            .and_then(|leaf| self.find_reader_for(leaf, query, &Default::default()))
            .ok_or_else(|| ReadySetError::QueryNotFound {
                name: query.display_unquoted().to_string(),
            })?;

        Ok(Graphviz {
            graph: &self.ingredients,
            detailed,
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

        Ok(self
            .materializations
            .materialized_non_reader_nodes()
            .map(|ni| {
                (
                    ni,
                    self.ingredients[ni].name().clone(),
                    self.ingredients[ni].description(true),
                    self.materializations
                        .indexes_for(ni)
                        .expect("Node index came from materializations")
                        .clone(),
                )
            })
            .chain(self.ingredients.node_references().filter_map(|(ni, n)| {
                n.as_reader().and_then(|r| r.index()).map(|idx| {
                    (
                        ni,
                        n.name().clone(),
                        n.description(true),
                        HashSet::from([idx.clone()]),
                    )
                })
            }))
            .map(
                |(node_index, node_name, node_description, indexes)| MaterializationInfo {
                    node_index,
                    node_name,
                    node_description,
                    size: sizes.get(&node_index).cloned().unwrap_or_default(),
                    partial: self.materializations.is_partial(node_index),
                    indexes,
                },
            )
            .collect())
    }

    /// Issue all of `requests` to their corresponding domains asynchronously, and return a stream
    /// of the results, consisting of shard, then replica, then result (potentially in a different
    /// order).
    ///
    /// If any domains are not running on a worker, this method will return an error.
    ///
    /// # Invariants
    ///
    /// * All of the domain indices in `requests` must be domains in `self.domains`
    fn query_domains<'a, I, R>(
        &'a self,
        requests: I,
    ) -> impl TryStream<Ok = (DomainIndex, Array2<R>), Error = ReadySetError> + 'a
    where
        I: IntoIterator<Item = (DomainIndex, DomainRequest)>,
        I::IntoIter: 'a,
        R: DeserializeOwned,
    {
        stream::iter(requests)
            .map(move |(domain, request)| {
                #[allow(clippy::indexing_slicing)] // came from self.domains
                self.domains[&domain]
                    .send_to_all::<R>(request, &self.workers)
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

        while let Some((_idx, replicas)) = min_persisted_offsets.try_next().await? {
            for offset in replicas.into_cells() {
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
                for replica in domain_offs.into_cells() {
                    for (lni, offset) in replica {
                        #[allow(clippy::indexing_slicing)] // came from self.domains
                        let ni = self.domain_nodes[&domain].get(lni).ok_or_else(|| {
                            internal_err!(
                                "Domain {} returned nonexistent local node {}",
                                domain,
                                lni
                            )
                        })?;

                        if !self.ingredients[*ni].is_base() {
                            continue;
                        }

                        #[allow(clippy::indexing_slicing)] // internal invariant
                        let table_name = self.ingredients[*ni].name();
                        match offset {
                            BaseTableState::Initialized(offset) => {
                                // TODO min of all shards
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
                }
                Ok(acc)
            },
        )
        .await
    }

    pub(super) fn domain_settings(&self) -> HashMap<DomainIndex, DomainSettings> {
        self.domains
            .iter()
            .map(|(idx, hdl)| {
                (
                    *idx,
                    DomainSettings {
                        num_shards: hdl.num_shards(),
                        num_replicas: hdl.num_replicas(),
                    },
                )
            })
            .collect()
    }

    /// Collects a unique list of domains that might contain base tables. Errors out if a domain
    /// retrieved does not appears in self.domains.
    async fn domains_with_base_tables(&self) -> ReadySetResult<HashSet<DomainIndex>> {
        #[allow(clippy::indexing_slicing)] // tables returns valid node indices
        let domains = self
            .tables()
            .values()
            .map(|ni| self.ingredients[*ni].domain())
            .collect::<HashSet<_>>();

        for di in domains.iter() {
            if !self.domains.contains_key(di) {
                return Err(ReadySetError::NoSuchReplica {
                    domain_index: di.index(),
                    shard: 0,
                    replica: 0,
                });
            }
        }

        Ok(domains)
    }

    /// Returns a vector of [`DomainDescriptor`] giving information about the addresses of all
    /// running domains within the cluster
    pub(super) fn domain_addresses(&self) -> Vec<DomainDescriptor> {
        let mut domain_addresses = Vec::new();
        for (domain_index, handle) in &self.domains {
            for shard in 0..handle.num_shards() {
                for replica in 0..handle.num_replicas() {
                    let replica_address = ReplicaAddress {
                        domain_index: *domain_index,
                        shard,
                        replica,
                    };

                    if let Some(socket_addr) = self.channel_coordinator.get_addr(&replica_address) {
                        domain_addresses.push(DomainDescriptor::new(replica_address, socket_addr));
                    }
                }
            }
        }

        domain_addresses
    }

    /// Have all domain replicas been placed onto workers in the cluster?
    pub(super) fn all_replicas_placed(&self) -> bool {
        self.domain_nodes.keys().all(|d| {
            self.domains
                .get(d)
                .map_or(false, |h| h.all_replicas_placed())
        })
    }

    /// Returns a map of nodes for domains which have not yet been placed onto a worker
    pub(super) fn unplaced_domain_nodes(&self) -> HashMap<DomainIndex, HashSet<NodeIndex>> {
        self.domain_nodes
            .iter()
            .filter(|(d, _)| {
                self.domains
                    .get(d)
                    .map_or(true, |dh| !dh.all_replicas_placed())
            })
            .map(|(k, v)| (*k, v.values().copied().collect()))
            .collect()
    }

    /// Query the status of all known tables, including those not replicated by ReadySet
    pub(super) async fn table_statuses(&self) -> ReadySetResult<BTreeMap<Relation, TableStatus>> {
        let known_tables = self.tables();
        let snapshotting_tables = self.snapshotting_tables().await?;
        let non_replicated_relations = self.non_replicated_relations();
        Ok(known_tables
            .into_keys()
            .map(|tbl| {
                let status = TableStatus {
                    replication_status: if snapshotting_tables.contains(&tbl) {
                        TableReplicationStatus::Snapshotting
                    } else {
                        TableReplicationStatus::Snapshotted
                    },
                };
                (tbl, status)
            })
            .chain(non_replicated_relations.iter().cloned().map(|tbl| {
                (
                    tbl.name,
                    TableStatus {
                        replication_status: TableReplicationStatus::NotReplicated(tbl.reason),
                    },
                )
            }))
            .collect())
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
                        .into_cells()
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
            .map(|(di, lni)| -> ReadySetResult<Relation> {
                #[allow(clippy::indexing_slicing)] // just came from self.domains
                let li = *self.domain_nodes[di].get(*lni).ok_or_else(|| {
                    internal_err!("Domain {} returned nonexistent node {}", di, lni)
                })?;
                #[allow(clippy::indexing_slicing)] // internal invariant
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
            .map_ok(|(_, compacted)| compacted.cells().iter().all(|finished| *finished));
        while let Some(finished) = stream.next().await {
            if !finished? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Return a map of node indices to key counts.
    pub(super) async fn node_sizes(&self) -> ReadySetResult<HashMap<NodeIndex, NodeSize>> {
        // Copying the keys into a vec here is a workaround for a higher order
        // lifetime compile error in `external_request` that occurs if we simply
        // use keys().map() to pass into query_domains directly.
        let counts_per_domain: Vec<(DomainIndex, Array2<Option<Vec<(NodeIndex, NodeSize)>>>)> = {
            let requests = self
                .domains
                .keys()
                .map(|di| (*di, DomainRequest::RequestNodeSizes))
                .collect::<Vec<_>>();

            stream::iter(requests)
                .map(move |(domain, request)| {
                    #[allow(clippy::indexing_slicing)] // came from self.domains
                    self.domains[&domain]
                        .send_to_healthy::<Vec<(NodeIndex, NodeSize)>>(request, &self.workers)
                        .map(move |r| -> ReadySetResult<_> { Ok((domain, r?)) })
                })
                .buffer_unordered(CONCURRENT_REQUESTS)
        }
        .try_collect()
        .await?;

        let mut res = HashMap::new();
        let flat_counts = counts_per_domain
            .into_iter()
            .flat_map(|(_domain, per_shard_counts)| {
                per_shard_counts
                    .into_cells()
                    .into_iter()
                    .flatten()
                    .flatten()
            });
        for (node_index, count) in flat_counts {
            // We may have multiple entries for the same node in the case of sharding, so this code
            // adds together the key counts for any duplicate nodes we come across:
            res.entry(node_index)
                .and_modify(|s| *s += count)
                .or_insert(count);
        }
        Ok(res)
    }

    // ** Modify operations **

    /// Perform a new query schema migration.
    #[instrument(level = "info", name = "migrate", skip(self, f, dialect))]
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
        gauge!(recorded::CONTROLLER_MIGRATION_IN_PROGRESS, 1.0);
        let mut m = Migration::new(self, dialect);
        let r = f(&mut m)?;
        m.commit(dry_run).await?;
        debug!("finished migration");
        gauge!(recorded::CONTROLLER_MIGRATION_IN_PROGRESS, 0.0);
        Ok(r)
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
        shard_replica_workers: Array2<Option<WorkerIdentifier>>,
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

        let num_shards = shard_replica_workers.num_rows();

        let mut domain_addresses = vec![];
        let mut assignments = Vec::with_capacity(num_shards);
        let mut new_domain_restrictions = vec![];

        for (shard, replicas) in shard_replica_workers.rows().enumerate() {
            let num_replicas = replicas.len();
            let mut shard_assignments = Vec::with_capacity(num_replicas);
            for (replica, worker_id) in replicas.iter().enumerate() {
                let Some(worker_id) = worker_id else {
                    shard_assignments.push(None);
                    continue;
                };

                let replica_address = ReplicaAddress {
                    domain_index: idx,
                    shard,
                    replica,
                };

                let domain = DomainBuilder {
                    index: idx,
                    shard: if num_shards > 1 { Some(shard) } else { None },
                    replica,
                    nshards: num_shards,
                    config: self.domain_config.clone(),
                    nodes: domain_nodes.clone(),
                    persistence_parameters: self.persistence.clone(),
                };

                let w = self.workers.get(worker_id).ok_or_else(|| {
                    internal_err!(
                        "Domain {replica_address} scheduled onto nonexistent worker {worker_id}"
                    )
                })?;

                let idx = domain.index;

                // send domain to worker
                debug!("sending domain {} to worker {}", replica_address, w.uri);

                let ret = w
                    .rpc::<RunDomainResponse>(WorkerRequestKind::RunDomain(domain))
                    .await
                    .map_err(|e| ReadySetError::DomainCreationFailed {
                        domain_index: idx.index(),
                        shard,
                        replica,
                        worker_uri: w.uri.clone(),
                        source: Box::new(e),
                    })?;

                // Update the domain placement restrictions on nodes in the placed
                // domain if necessary.
                for n in &nodes {
                    #[allow(clippy::indexing_slicing)] // checked above
                    let node = &self.ingredients[*n];

                    if node.is_base() && w.domain_scheduling_config.volume_id.is_some() {
                        new_domain_restrictions.push((
                            node.name().to_owned(),
                            shard,
                            DomainPlacementRestriction {
                                worker_volume: w.domain_scheduling_config.volume_id.clone(),
                            },
                        ));
                    }
                }

                debug!(external_addr = %ret.external_addr, "worker booted domain");

                self.channel_coordinator
                    .insert_remote(replica_address, ret.external_addr);
                domain_addresses.push(DomainDescriptor::new(replica_address, ret.external_addr));
                shard_assignments.push(Some(w.uri.clone()));
            }
            assignments.push(shard_assignments);
        }

        // Push all domain placement restrictions to the local controller state. We
        // do this outside the loop to satisfy the borrow checker as this immutably
        // borrows self.
        for (node_name, shard, restrictions) in new_domain_restrictions {
            self.set_domain_placement_local(node_name, shard, restrictions);
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
                debug!(worker_uri = %w.uri, "informing worker about newly placed domain");
                if let Err(e) = w
                    .rpc::<()>(WorkerRequestKind::GossipDomainInformation(vec![dd]))
                    .await
                {
                    // TODO(Fran): We need better error handling for workers
                    //   that failed before the controller noticed.
                    error!(
                        %address,
                        error = ?e,
                        "Worker could not be reached and will be ignored",
                    );
                }
            }
        }

        Ok(DomainHandle::new(idx, Array2::from_rows(assignments)))
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
                .get(&domain)
                .ok_or_else(|| ReadySetError::NoSuchReplica {
                    domain_index: domain.index(),
                    shard: 0,
                    replica: 0,
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

    pub(super) fn set_domain_placement_local(
        &mut self,
        node_name: Relation,
        shard: usize,
        node_restriction: DomainPlacementRestriction,
    ) {
        self.node_restrictions
            .insert(NodeRestrictionKey { node_name, shard }, node_restriction);
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
                .send_to_healthy::<(DomainStats, HashMap<NodeIndex, NodeStats>)>(
                    DomainRequest::GetStatistics,
                    workers,
                )
                .await?
                .into_cells()
                .into_iter()
                .flatten(/* Discard results from non-running domains */)
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
                    .send_to_healthy::<()>(
                        DomainRequest::Packet(Packet::Evict(EvictRequest::Bytes {
                            node: Some(na),
                            num_bytes: bytes as usize,
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
            }) => (domain_idx, Tag::new(tag), Some(key)),
            None => {
                let tags = self.materializations.partial_tags();
                if tags.is_empty() {
                    trace!("Attempted to evict but found no tags for any partial materialization");
                    return Ok(None);
                }

                let idx = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0, tags.len())
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
            .send_to_healthy::<Option<Vec<DfValue>>>(
                DomainRequest::Evict(EvictRequest::SingleKey { tag, key }),
                &self.workers,
            )
            .await?
            .into_cells()
            .into_iter()
            .flatten(/* Discard results from non-running domains */)
            .next()
            .ok_or_else(|| internal_err!("expected a shard+replica"))?
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

    pub(super) async fn apply_recipe(
        &mut self,
        changelist: ChangeList,
        dry_run: bool,
    ) -> Result<RecipeChanges, ReadySetError> {
        // I hate this, but there's no way around for now, as migrations
        // are super entangled with the recipe and the graph.
        let mut new = self.recipe.clone();

        let r = self
            .migrate(dry_run, changelist.dialect, |mig| {
                new.activate(mig, changelist)
            })
            .await;

        match r {
            Ok(res) => {
                self.recipe = new;
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

    pub(super) async fn extend_recipe(
        &mut self,
        recipe_spec: ExtendRecipeSpec<'_>,
        dry_run: bool,
    ) -> Result<RecipeChanges, ReadySetError> {
        // Drop recipes from the replicator that we have already processed.
        if let (Some(new), Some(current)) = (
            &recipe_spec.replication_offset,
            &self.schema_replication_offset,
        ) {
            if current >= new {
                // no-op
                return Ok(Default::default());
            }
        }

        match self.apply_recipe(recipe_spec.changes, dry_run).await {
            Ok(x) => {
                if let Some(offset) = &recipe_spec.replication_offset {
                    debug!(%offset, "Updating schema replication offset");
                    offset.try_max_into(&mut self.schema_replication_offset)?
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

        if let Err(error) = self.apply_recipe(changelist, false).await {
            error!(%error, "Failed to apply recipe");
            return Err(error);
        }

        Ok(1)
    }

    pub(super) async fn remove_all_queries(&mut self) -> ReadySetResult<RecipeChanges> {
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
        )
        .await
    }

    /// Remove all references to the given [`WorkerIdentifier`] from the runtime state of the
    /// dataflow graph, returning a list of replica addresses that were known to be running on those
    /// workers
    pub(super) fn remove_worker(&mut self, wi: &WorkerIdentifier) -> Vec<ReplicaAddress> {
        self.workers.remove(wi);
        self.read_addrs.remove(wi);
        let mut res = vec![];
        for dh in self.domains.values_mut() {
            for replica_addr in dh.remove_worker(wi) {
                self.channel_coordinator.remove(replica_addr);
                res.push(replica_addr);
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

    /// Send requests to whatever workers are running the given domain replicas to kill those
    /// replicas, and remove them from runtime state.
    pub(super) async fn kill_domains<I>(&mut self, domains: I) -> ReadySetResult<()>
    where
        I: IntoIterator<Item = DomainIndex>,
    {
        let mut workers_to_replicas: HashMap<_, Vec1<_>> = HashMap::new();
        for di in domains {
            let Some(dh) = self.domains.get(&di) else {
                debug!(domain = %di, "domain not running, not killing");
                continue
            };

            for (addr, wi) in dh.assignments() {
                workers_to_replicas
                    .entry(wi.clone())
                    .and_modify(|v| v.push(addr))
                    .or_insert_with(|| vec1![addr]);
            }
        }

        let mut futs = FuturesUnordered::new();
        for (worker_url, replicas) in workers_to_replicas {
            let worker = self.workers.get(&worker_url).ok_or_else(|| {
                internal_err!("Worker not found for url {worker_url} to kill domains")
            })?;
            futs.push(
                worker
                    .rpc(WorkerRequestKind::KillDomains(replicas.clone()))
                    .map_ok(|()| replicas),
            );
        }
        while let Some(r) = futs.next().await {
            for killed_addr in r? {
                if let Some(dh) = self.domains.get_mut(&killed_addr.domain_index) {
                    dh.remove_assignment(killed_addr.shard, killed_addr.replica);
                }
            }
        }

        Ok(())
    }

    /// Runs all the necessary steps to recover the full [`DfState`], when said state only
    /// has the bare minimum information.
    ///
    /// # Invariants
    /// The following invariants must hold:
    /// - `self.ingredients` must be a valid [`Graph`]. This means it has to be a description of
    /// a valid dataflow graph.
    /// - Each node must have a Domain associated with it (and thus also have a [`LocalNodeIndex`]
    /// and any other associated information).
    /// - `self.domain_nodes` must be valid. This means that all the nodes in `self.ingredients`
    /// (except for `self.source`) must belong to a domain; and there must not be any overlap
    /// between the nodes owned by each domain. All the invariants for Domain assignment from
    /// [`crate::controller::migrate::assignment::assign`] must hold as well.
    ///  - `self.remap` and `self.node_restrictions` must be valid.
    /// - All the other fields should be empty or `[Default::default()]`.
    #[instrument(level = "info", skip_all)]
    pub(super) async fn plan_recovery(
        &mut self,
        domain_nodes: &HashMap<DomainIndex, HashSet<NodeIndex>>,
    ) -> ReadySetResult<DomainMigrationPlan> {
        info!("Planning recovery");
        let mut dmp =
            DomainMigrationPlan::new(DomainMigrationMode::Recover, self.domain_settings());
        let domain_nodes = domain_nodes
            .iter()
            .map(|(idx, nm)| (*idx, nm.iter().copied().collect::<Vec<_>>()))
            .collect::<HashMap<_, _>>();
        let mut new = HashSet::new();
        {
            let mut scheduler = Scheduler::new(self, &None)?;
            for (domain, nodes) in domain_nodes {
                let workers = scheduler.schedule_domain(domain, &nodes[..])?;

                for ((shard, replica), worker) in workers.entries() {
                    let not_already_placed = self
                        .domains
                        .get(&domain)
                        .and_then(|dh| dh.assignment(shard, replica))
                        .is_none();

                    if not_already_placed && worker.is_none() {
                        dmp.replica_failed_placement(ReplicaAddress {
                            domain_index: domain,
                            shard,
                            replica,
                        });
                    }
                }

                let num_shards = workers.num_rows();
                let num_replicas = workers[0].len();
                dmp.place_domain(domain, workers, nodes.clone());
                dmp.set_domain_settings(
                    domain,
                    DomainSettings {
                        num_shards,
                        num_replicas,
                    },
                );
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

    /// This method is a hack to make sure the [`ControllerState`] "persisted" in the
    /// [`LocalAuthority`] is stored similarly to the way it would be, if it were serialized and
    /// then deserailized, but without paying the extreme performance penalty actually serializing
    /// it costs. Essentially we either clear or assign defaults to the fields that are marked as
    /// #[serde::skip], thus making sure things are consistent wherever they are stored after
    /// serialization or after this method is applied.
    /// If called prior to serialization or after desiarialization this would effectively be a noop,
    /// so don't bother calling it in authorities that serialize.
    pub(crate) fn touch_up(&mut self) {
        self.domains = Default::default();
        self.channel_coordinator = Default::default();
        self.read_addrs = Default::default();
        self.workers = Default::default();

        let mut new_materializations = Materializations::new();
        new_materializations.paths = self.materializations.paths.clone();
        new_materializations.redundant_partial = self.materializations.redundant_partial.clone();
        new_materializations.tag_generator = self.materializations.tag_generator;
        new_materializations.config = self.materializations.config.clone();

        self.materializations = new_materializations;
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
/// fair and will block reader threads if there are writers waiting for the lock.
/// 2. To ensure that the read lock can be used by multiple threads, since the lock is `Send` and
/// `Sync`.
///
/// ## Writes
/// Writes are performed by following a couple of steps:
/// 1. A mutex is acquired to ensure that only one write is in progress at any time.
/// 2. A copy of the current [`DfState`] (being held by the [`DfStateReader`] is made.
/// 3. A [`DfStateWriter`] is created from the copy and the mutex guard, having the lifetime
/// of the latter.
/// 4. All the computations/modifications are performed on the [`DfStateWriter`] (aka, on the
/// underlying [`DfState`] copy).
/// 5. The [`DfStateWriter`] is then committed to the [`DfState`] by calling
/// [`DfStateWriter::commit`], which replaces the old state by the new one in the
/// [`DfStateReader`].
///
/// As previously mentioned for reads, the choice of using [`tokio::sync::RwLock`] ensures writers
/// fairness and the ability to use the lock by multiple threads.
///
/// Following the three steps to perform a write guarantees that:
/// 1. Writes don't starve readers: when we start a write operation, we take a read lock
/// for the [`DfStateReader`] in order to make a copy of a state. In doing so, we ensure that
/// readers can continue to read the state: no modification has been made yet.
/// Writers can also perform all their computing/modifications (which can be pretty expensive
/// time-wise), and only then the changes can be committed by swapping the old state for the new
/// one, which is the only time readers are forced to wait.
/// 2. If a write operation fails, the state is not modified.
/// TODO(fran): Even though the state is not modified here, we might have sent messages to other
/// workers/domains.   It is worth looking into a better way of handling that (if it's even
/// necessary).   Such a mechanism was never in place to begin with.
/// 3. Writes are transactional: if there is an instance of [`DfStateWriter`] in
/// existence, then all the other writes must wait. This guarantees that the operations performed
/// to the dataflow state are executed transactionally.
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
}

impl DfStateHandle {
    /// Creates a new instance of [`DfStateHandle`].
    pub(super) fn new(dataflow_state: DfState) -> Self {
        Self {
            reader: RwLock::new(DfStateReader {
                state: dataflow_state,
            }),
            write_guard: Mutex::new(()),
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
        histogram!(
            readyset_client::metrics::recorded::DATAFLOW_STATE_CLONE_TIME,
            elapsed.as_micros() as f64,
        );
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
        let new_state = &writer.state;
        if let Some(local) = authority.as_local() {
            local.update_controller_in_place(|state: Option<&mut ControllerState>| match state {
                None => {
                    eprintln!("There's no controller state to update");
                    Err(())
                }
                Some(mut state) => {
                    state.dataflow_state = new_state.clone();
                    state.dataflow_state.touch_up();
                    Ok(())
                }
            })
        } else {
            authority
                .update_controller_state(
                    |state: Option<ControllerState>| match state {
                        None => {
                            eprintln!("There's no controller state to update");
                            Err(())
                        }
                        Some(mut state) => {
                            state.dataflow_state = new_state.clone();
                            Ok(state)
                        }
                    },
                    |state: &mut ControllerState| {
                        state.dataflow_state.touch_up();
                    },
                )
                .await?
                .map(|_| ())
        }
        .map_err(|_| internal_err!("Unable to update state"))?;

        let mut state_guard = self.reader.write().await;
        state_guard.replace(new_state.clone());
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

impl<'handle> AsRef<DfState> for DfStateWriter<'handle> {
    fn as_ref(&self) -> &DfState {
        &self.state
    }
}

impl<'handle> AsMut<DfState> for DfStateWriter<'handle> {
    fn as_mut(&mut self) -> &mut DfState {
        &mut self.state
    }
}

// There is a chain of not thread-safe (not [`Send`] structures at play here:
// [`Graph`] is not [`Send`] (as it might contain a [`reader_map::WriteHandle`]), which
// makes [`DfState`] not [`Send`].
// Because [`DfStateReader`] holds a [`DfState`] instance, the compiler does not
// automatically implement [`Send`] for it. But here is what the compiler does not know:
// 1. Only the [`DfStateHandle`] can instantiate and hold an instance of
// [`DfStateReader`].
//
// 2. Only the [`DfStateHandle`] is able to get a mutable reference to the
// [`DfStateReader`].
//
// 3. The [`DfStateReader`] held by the [`DfStateHandle`] is behind a
// [`tokio::sync::RwLock`], which is only acquired as write in the [`DfStateHandle::commit`]
// method.
//
// Those three conditions guarantee that there are no concurrent modifications to the underlying
// dataflow state.
// So, we explicitly tell the compiler that the [`DfStateReader`] is safe to be moved
// between threads.
unsafe impl Sync for DfStateReader {}
