#![deny(
    clippy::dbg_macro,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use super::NodeRestrictionKey;
use crate::controller::domain_handle::DomainHandle;
use crate::controller::migrate::materialization::Materializations;
use crate::controller::recipe::Schema;
use crate::controller::schema;
use crate::controller::{ControllerState, DomainPlacementRestriction, Migration, Recipe};
use crate::controller::{Worker, WorkerIdentifier};
use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::debug::info::{DomainKey, GraphInfo};
use crate::errors::{bad_request_err, internal_err, ReadySetResult};
use crate::worker::WorkerRequestKind;
use crate::{ReaderReplicationResult, ReaderReplicationSpec, ViewFilter, ViewRequest};
use dataflow::prelude::*;
use dataflow::{node, prelude::Packet, DomainBuilder, DomainConfig, DomainRequest};
use futures::stream::{self, StreamExt, TryStreamExt};
use hyper::Method;
use lazy_static::lazy_static;
use noria::debug::stats::{DomainStats, GraphStats, NodeStats};
use noria::{builders::*, ReplicationOffset, ViewSchema, WorkerDescriptor};
use noria::{
    consensus::{Authority, STATE_KEY},
    RecipeSpec,
};
use noria::{internal, invariant_eq, ActivationResult, ReadySetError};
use petgraph::visit::Bfs;
use regex::Regex;
use reqwest::Url;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{cell, time};
use vec1::Vec1;

/// Number of concurrent requests to make when making multiple simultaneous requests to domains (eg
/// for replication offsets)
const CONCURRENT_REQUESTS: usize = 16;

/// The Noria controller, responsible for making control-plane decisions for the whole of a Noria
/// cluster.
///
/// This runs inside a `ControllerOuter` when it is elected as leader.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct ControllerInner {
    pub(super) ingredients: Graph,
    /// ID for the root node in the graph. This is used to retrieve a list of base tables.
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    pub(super) domain_config: DomainConfig,

    /// Parameters for persistence code.
    pub(super) persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    recipe: Recipe,
    /// Latest replication position for the schema if from replica or binlog
    replication_offset: Option<ReplicationOffset>,
    /// Placement restrictions for nodes and the domains they are placed into.
    pub(super) node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,

    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    pub(in crate::controller) domain_nodes: HashMap<DomainIndex, Vec<NodeIndex>>,
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,

    /// Map from worker URI to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,

    /// State between migrations
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    pending_recovery: Option<(Vec<String>, usize)>,

    quorum: usize,
    controller_uri: Url,

    log: slog::Logger,
}

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

impl ControllerInner {
    #[allow(unused_variables)] // `query` is not used unless debug_assertions is enabled
    pub(super) fn external_request<A: Authority + 'static>(
        &mut self,
        method: hyper::Method,
        path: String,
        query: Option<String>,
        body: hyper::body::Bytes,
        authority: &Arc<A>,
    ) -> ReadySetResult<Vec<u8>> {
        macro_rules! return_serialized {
            ($expr:expr) => {{
                return Ok(::bincode::serialize(&$expr)?);
            }};
        }

        // *** Methods that don't require a quorum ***

        match (&method, path.as_ref()) {
            (&Method::GET, "/simple_graph") => return Ok(self.graphviz(false).into_bytes()),
            (&Method::POST, "/simple_graphviz") => {
                return_serialized!(self.graphviz(false));
            }
            (&Method::GET, "/graph") => return Ok(self.graphviz(true).into_bytes()),
            (&Method::POST, "/graphviz") => {
                return_serialized!(self.graphviz(true));
            }
            (&Method::GET | &Method::POST, "/get_statistics") => {
                return_serialized!(self.get_statistics()?);
            }
            _ => {}
        }

        if self.pending_recovery.is_some() || self.workers.len() < self.quorum {
            return Err(ReadySetError::NoQuorum);
        }

        // *** Methods that do require quorum ***

        match (method, path.as_ref()) {
            (Method::GET, "/flush_partial") => {
                return_serialized!(self.flush_partial()?);
            }
            (Method::POST, "/inputs") => return_serialized!(self.inputs()),
            (Method::POST, "/outputs") => return_serialized!(self.outputs()),
            (Method::GET | Method::POST, "/instances") => {
                return_serialized!(self.get_instances());
            }
            (Method::GET | Method::POST, "/controller_uri") => {
                return_serialized!(self.controller_uri);
            }
            (Method::GET, "/workers") | (Method::POST, "/workers") => {
                return_serialized!(&self.workers.keys().collect::<Vec<_>>())
            }
            (Method::GET, "/healthy_workers") | (Method::POST, "/healthy_workers") => {
                return_serialized!(&self
                    .workers
                    .iter()
                    .filter(|w| w.1.healthy)
                    .map(|w| w.0)
                    .collect::<Vec<_>>());
            }
            #[cfg(debug_assertions)] // don't include this hacky code in prod ~eta
            (Method::GET, "/nodes") => {
                // TODO(malte): this is a pretty yucky hack, but hyper doesn't provide easy access
                // to individual query variables unfortunately. We'll probably want to factor this
                // out into a helper method.
                let nodes = if let Some(query) = query {
                    let mut vars = query.split('&').map(String::from);
                    #[allow(clippy::indexing_slicing)]
                    #[allow(clippy::unwrap_used)]
                    if let Some(n) = &vars.find(|v| v.starts_with("w=")) {
                        self.nodes_on_worker(Some(&n[2..].parse().unwrap()))
                    } else {
                        self.nodes_on_worker(None)
                    }
                } else {
                    // all data-flow nodes
                    self.nodes_on_worker(None)
                };
                return_serialized!(&nodes
                    .into_iter()
                    .filter_map(|ni| {
                        #[allow(clippy::indexing_slicing)]
                        let n = &self.ingredients[ni];
                        if n.is_internal() {
                            Some((ni, n.name(), n.description(true)))
                        } else if n.is_base() {
                            Some((ni, n.name(), "Base table".to_owned()))
                        } else if n.is_reader() {
                            Some((ni, n.name(), "Leaf view".to_owned()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>())
            }
            (Method::POST, "/table_builder") => {
                // NOTE(eta): there is DELIBERATELY no `?` after the `table_builder` call, because
                //            the receiving end expects a `ReadySetResult` to be serialized.
                let body = bincode::deserialize(&body)?;
                let ret = self.table_builder(body);
                return_serialized!(ret);
            }
            (Method::POST, "/view_builder") => {
                // NOTE(eta): same as above applies
                let body = bincode::deserialize(&body)?;
                let ret = self.view_builder(body);
                return_serialized!(ret);
            }
            (Method::POST, "/extend_recipe") => {
                let body = bincode::deserialize(&body)?;
                let ret = self.extend_recipe(authority, body)?;
                return_serialized!(ret);
            }
            (Method::POST, "/install_recipe") => {
                let body = bincode::deserialize(&body)?;
                let ret = self.install_recipe(authority, body)?;
                return_serialized!(ret);
            }
            (Method::POST, "/set_replication_offset") => {
                let body = bincode::deserialize(&body)?;
                let ret = self.set_replication_offset(authority, body)?;
                return_serialized!(ret);
            }
            (Method::POST, "/replicate_readers") => {
                let body = bincode::deserialize(&body)?;
                let ret = self.replicate_readers(body)?;
                return_serialized!(ret);
            }
            (Method::POST, "/get_info") => return_serialized!(self.get_info()?),
            (Method::POST, "/remove_node") => {
                let body = bincode::deserialize(&body)?;
                let ret = self.remove_nodes(vec![body].as_slice())?;
                return_serialized!(ret);
            }
            (Method::POST, "/replication_offset") => {
                // this method can't be `async` since `ControllerInner` isn't Send because `Graph`
                // isn't Send :(
                let res = futures_executor::block_on(self.replication_offset())?;
                return_serialized!(res);
            }
            _ => Err(ReadySetError::UnknownEndpoint),
        }
    }

    pub(super) fn handle_register_from_authority(
        &mut self,
        desc: WorkerDescriptor,
    ) -> ReadySetResult<()> {
        let WorkerDescriptor {
            worker_uri,
            reader_addr,
            region,
            reader_only,
            volume_id,
        } = desc;

        info!(
            self.log,
            "received registration payload from worker: {} (reader address {})",
            worker_uri,
            reader_addr
        );

        let ws = Worker::new(worker_uri.clone(), region, reader_only, volume_id);

        let mut domain_addresses = Vec::new();
        for (index, handle) in &self.domains {
            for i in 0..handle.shards.len() {
                let socket_addr =
                    self.channel_coordinator
                        .get_addr(&(*index, i))
                        .ok_or_else(|| ReadySetError::NoSuchDomain {
                            domain_index: index.index(),
                            shard: i,
                        })?;

                domain_addresses.push(DomainDescriptor::new(*index, i, socket_addr));
            }
        }

        // Can't send this as we are on the controller thread right now and it also
        // has to receive this.
        if let Err(e) = futures_executor::block_on(
            ws.rpc::<()>(WorkerRequestKind::GossipDomainInformation(domain_addresses)),
        ) {
            error!(
                    self.log,
                    "Worker could not be reached and was not updated on domain information. Address: {:?} | Error: {:?}",
                    worker_uri,
                    e
                );
        }

        self.workers.insert(worker_uri.clone(), ws);
        self.read_addrs.insert(worker_uri, reader_addr);

        info!(
            self.log,
            "now have {} of {} required workers",
            self.workers.len(),
            self.quorum
        );

        if self.workers.len() >= self.quorum {
            if let Some((recipes, mut recipe_version)) = self.pending_recovery.take() {
                assert_eq!(self.workers.len(), self.quorum);
                assert_eq!(self.recipe.version(), 0);
                if recipes.len() > recipe_version + 1 {
                    // TODO(eta): this is a terrible stopgap hack
                    crit!(
                        self.log,
                        "{} recipes but recipe version is at {}",
                        recipes.len(),
                        recipe_version
                    );
                    recipe_version = recipes.len() + 1;
                }

                info!(self.log, "Restoring graph configuration");
                self.recipe = Recipe::with_version(
                    recipe_version + 1 - recipes.len(),
                    Some(self.log.clone()),
                );
                for r in recipes {
                    if let Err(e) = self
                        .recipe
                        .clone()
                        .extend(&r)
                        .map_err(|(_, e)| e)
                        .and_then(|r| self.apply_recipe(r))
                    {
                        // TODO(eta): is this the best thing to do?
                        crit!(self.log, "Failed to restore recipe: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn handle_failed_workers(
        &mut self,
        failed: Vec<WorkerIdentifier>,
    ) -> ReadySetResult<()> {
        // first, translate from the affected workers to affected data-flow nodes
        let mut affected_nodes = Vec::new();
        for wi in failed {
            info!(self.log, "handling failure of worker {:?}", wi);
            affected_nodes.extend(self.get_failed_nodes(&wi));
        }

        // then, figure out which queries are affected (and thus must be removed and added again in
        // a migration)
        let affected_queries = self.recipe.queries_for_nodes(affected_nodes);
        let (recovery, mut original) = self.recipe.make_recovery(affected_queries);

        // activate recipe
        self.apply_recipe(recovery)?;

        // we must do this *after* the migration, since the migration itself modifies the recipe in
        // `recovery`, and we currently need to clone it here.
        let tmp = self.recipe.clone();
        original.set_prior(tmp.clone());
        // somewhat awkward, but we must replace the stale `SqlIncorporator` state in `original`
        original.set_sql_inc(tmp.sql_inc().clone());

        // back to original recipe, which should add the query again
        self.apply_recipe(original)?;
        Ok(())
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
                            .iter(),
                    )
            }
        }
        Ok(GraphInfo {
            workers: worker_info,
        })
    }

    pub(super) fn replicate_readers(
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
                self.recipe
                    .node_addr_for(query_name)
                    .map_err(|e| {
                        warn!(self.log, "Reader replication failed: no node was found for query '{:?}'. Error: {:?}",
                        query_name, e);
                        bad_request_err(format!("Reader replication failed: no node was found for query '{:?}'", query_name))
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
                        // We also take the associated keys of the original reader node.
                        let keys_opt = child.as_reader().and_then(|r| r.key());
                        if let Some(keys) = keys_opt {
                            // And set the keys to the replicated reader.
                            #[allow(clippy::unwrap_used)] // it must be a reader if it has a key
                            reader_node.as_mut_reader().unwrap().set_key(keys);
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
        })?;

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

    /// Construct `ControllerInner` with a specified listening interface
    pub(super) fn new(log: slog::Logger, state: ControllerState, controller_uri: Url) -> Self {
        let mut g = petgraph::Graph::new();
        // Create the root node in the graph.
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
        ));

        let mut materializations = Materializations::new(&log);
        if !state.config.partial_enabled {
            materializations.disable_partial()
        }
        materializations.set_frontier_strategy(state.config.frontier_strategy);

        let cc = Arc::new(ChannelCoordinator::new());
        assert_ne!(state.config.quorum, 0);

        let pending_recovery = if !state.recipes.is_empty() {
            Some((state.recipes, state.recipe_version))
        } else {
            None
        };

        let mut recipe = Recipe::blank(Some(log.clone()));
        match state.config.reuse {
            Some(reuse) => recipe.enable_reuse(reuse),
            None => recipe.disable_reuse(),
        }

        ControllerInner {
            ingredients: g,
            source,
            ndomains: 0,

            materializations,
            sharding: state.config.sharding,
            domain_config: state.config.domain_config,
            persistence: state.config.persistence,
            recipe,
            node_restrictions: state.node_restrictions,
            quorum: state.config.quorum,
            log,

            domains: Default::default(),
            domain_nodes: Default::default(),
            channel_coordinator: cc,

            remap: HashMap::default(),

            workers: HashMap::default(),

            pending_recovery,
            read_addrs: Default::default(),
            controller_uri,

            replication_offset: state.replication_offset,
        }
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
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    ///
    /// Must be called before any domains have been created.
    #[allow(unused)]
    fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    pub(in crate::controller) fn place_domain(
        &mut self,
        idx: DomainIndex,
        shard_workers: Vec<WorkerIdentifier>,
        log: &Logger,
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
                let node = self.ingredients.node_weight_mut(*ni).unwrap().take();
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
                log,
                "sending domain {}.{} to worker {}",
                idx.index(),
                shard,
                w.uri
            );

            let ret = futures_executor::block_on(
                w.rpc::<RunDomainResponse>(WorkerRequestKind::RunDomain(domain)),
            )
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

            info!(log, "worker booted domain at {}", ret.external_addr);

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
                info!(
                    log,
                    "informing worker at {} about newly placed domain", w.uri
                );
                if let Err(e) = futures_executor::block_on(
                    w.rpc::<()>(WorkerRequestKind::GossipDomainInformation(vec![dd])),
                ) {
                    // TODO(Fran): We need better error handling for workers
                    //   that failed before the controller noticed.
                    error!(
                        log,
                        "Worker could not be reached and will be ignored. Address: {:?} | Error: {:?}",
                        address,
                        e
                    );
                }
            }
        }

        Ok(DomainHandle {
            idx,
            shards: assignments,
            log: log.clone(),
        })
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    #[allow(unused)]
    fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
        self.materializations.set_logger(&self.log);
    }

    /// Perform a new query schema migration.
    // crate viz for tests
    pub(crate) fn migrate<F, T>(&mut self, f: F) -> Result<T, ReadySetError>
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration");
        let miglog = self.log.new(o!());
        let ingredients = self.ingredients.clone();
        let mut m = Migration {
            ingredients,
            source: self.source,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: Default::default(),
            worker: None,
            start: time::Instant::now(),
            log: miglog,
        };
        let r = f(&mut m);
        m.commit(self)?;
        Ok(r)
    }

    /// Get a map of all known input nodes, mapping the name of the node to that node's
    /// [index](NodeIndex)
    ///
    /// Input nodes are here all nodes of type `Table`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .map(|n| {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let base = &self.ingredients[n];
                assert!(base.is_base());
                (base.name().to_owned(), n)
            })
            .collect()
    }

    /// Get a map of all known output nodes, mapping the name of the node to that node's
    /// [index](NodeIndex)
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    fn outputs(&self) -> BTreeMap<String, NodeIndex> {
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

    fn find_readers_for(
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
    fn view_builder(&self, view_req: ViewRequest) -> Result<Option<ViewBuilder>, ReadySetError> {
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
                        node_index: self.ingredients[r].local_addr(),
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
                columns,
                schema,
                shards,
            });
        }

        Ok(Some(ViewBuilder {
            replicas: Vec1::try_from_vec(replicas)
                .map_err(|_| ReadySetError::ViewNotFound(view_req.name))?,
        }))
    }

    fn view_schema(&self, view_ni: NodeIndex) -> Result<Option<ViewSchema>, ReadySetError> {
        let n =
            self.ingredients
                .node_weight(view_ni)
                .ok_or_else(|| ReadySetError::NodeNotFound {
                    index: view_ni.index(),
                })?;
        let reader = n
            .as_reader()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: n.local_addr(),
                expected_type: NodeType::Reader,
            })?;
        let returned_cols = reader
            .post_lookup()
            .returned_cols
            .clone()
            .unwrap_or_else(|| (0..n.fields().len()).collect());

        let projected_schema = (0..n.fields().len())
            .map(|i| schema::column_schema(&self.ingredients, view_ni, &self.recipe, i, &self.log))
            .collect::<Result<Vec<_>, ReadySetError>>()?
            .into_iter()
            .collect::<Option<Vec<_>>>();

        let returned_schema = returned_cols
            .iter()
            .map(|idx| {
                schema::column_schema(&self.ingredients, view_ni, &self.recipe, *idx, &self.log)
            })
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
    fn table_builder(&self, base: &str) -> ReadySetResult<Option<TableBuilder>> {
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

        trace!(self.log, "creating table"; "for" => base);

        let mut key = node
            .get_base()
            .ok_or_else(|| ReadySetError::InvalidNodeType {
                node_index: node.local_addr(),
                expected_type: NodeType::Base,
            })?
            .key()
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
    fn get_statistics(&mut self) -> ReadySetResult<GraphStats> {
        trace!(self.log, "asked to get statistics");
        let log = &self.log;
        let workers = &self.workers;
        // TODO: request stats from domains in parallel.
        let domains = self
            .domains
            .iter_mut()
            // TODO(eta): error handling impl adds overhead
            .map(|(&di, s)| {
                trace!(log, "requesting stats from domain"; "di" => di.index());
                Ok(
                    s.send_to_healthy_blocking(DomainRequest::GetStatistics, workers)?
                        .into_iter()
                        .enumerate()
                        .map(move |(i, s)| ((di, i), s)),
                )
            })
            .collect::<ReadySetResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(GraphStats { domains })
    }

    fn get_instances(&self) -> Vec<(WorkerIdentifier, bool)> {
        self.workers
            .iter()
            .map(|(id, status)| (id.clone(), status.healthy))
            .collect()
    }

    fn flush_partial(&mut self) -> ReadySetResult<u64> {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let to_evict: Vec<_> = self
            .domains
            .iter_mut()
            .map(|(di, s)| {
                let to_evict: Vec<(NodeIndex, u64)> = s
                    .send_to_healthy_blocking::<(DomainStats, HashMap<NodeIndex, NodeStats>)>(
                        DomainRequest::GetStatistics,
                        workers,
                    )?
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
                Ok((*di, to_evict))
            })
            .collect::<ReadySetResult<_>>()?;

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
                    .send_to_healthy_blocking::<()>(
                        DomainRequest::Packet(Packet::Evict {
                            node: Some(na),
                            num_bytes: bytes as usize,
                        }),
                        workers,
                    )?;
                total_evicted += bytes;
            }
        }

        warn!(
            self.log,
            "flushed {} bytes of partial domain state", total_evicted
        );

        Ok(total_evicted)
    }

    fn apply_recipe(&mut self, mut new: Recipe) -> Result<ActivationResult, ReadySetError> {
        // TODO(eta): if this fails, apply the old one?
        let r = self.migrate(|mig| new.activate(mig))?;

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
                    self.remove_leaf(leaf)?;
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
                    debug!(
                        self.log,
                        "Removing base \"{}\"",
                        self.ingredients.node_weight(base).ok_or_else(|| ReadySetError::NodeNotFound { index: base.index() })?.name();
                        "node" => base.index(),
                    );
                    // now drop the (orphaned) base
                    self.remove_nodes(vec![base].as_slice())?;
                }

                self.recipe = new;
            }
            Err(ref e) => {
                crit!(self.log, "failed to apply recipe: {}", e);
                // TODO(malte): a little yucky, since we don't really need the blank recipe
                let recipe = mem::replace(&mut self.recipe, Recipe::blank(None));
                self.recipe = recipe.revert();
            }
        }

        r
    }

    fn extend_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        add_txt_spec: RecipeSpec,
    ) -> Result<ActivationResult, ReadySetError> {
        let old = self.recipe.clone();
        // needed because self.apply_recipe needs to mutate self.recipe, so can't have it borrowed
        let new = mem::replace(&mut self.recipe, Recipe::blank(None));
        let add_txt = add_txt_spec.recipe;

        match new.extend(add_txt) {
            Ok(new) => match self.apply_recipe(new) {
                Ok(x) => {
                    if let Some(offset) = &add_txt_spec.replication_offset {
                        offset.try_max_into(&mut self.replication_offset)?
                    }

                    if authority
                        .read_modify_write(
                            STATE_KEY,
                            |state: Option<ControllerState>| match state {
                                None => Err(()),
                                Some(mut state) => {
                                    state.node_restrictions = self.node_restrictions.clone();
                                    state.recipe_version = self.recipe.version();
                                    state.recipes.push(add_txt.to_string());
                                    if let Some(offset) = &add_txt_spec.replication_offset {
                                        offset
                                            .try_max_into(&mut state.replication_offset)
                                            .map_err(|_| ())?;
                                    }
                                    Ok(state)
                                }
                            },
                        )
                        .is_err()
                    {
                        noria::internal!("failed to persist recipe extension");
                    }
                    Ok(x)
                }
                Err(e) => {
                    self.recipe = old;
                    Err(e)
                }
            },
            Err((old, e)) => {
                // need to restore the old recipe
                crit!(self.log, "failed to extend recipe: {:?}", e);
                self.recipe = old;
                Err(e)
            }
        }
    }

    fn install_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        r_txt_spec: RecipeSpec,
    ) -> Result<ActivationResult, ReadySetError> {
        let r_txt = r_txt_spec.recipe;

        match Recipe::from_str(r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let _old = self.recipe.clone();
                let old = mem::replace(&mut self.recipe, Recipe::blank(None));
                let new = old
                    .replace(r)
                    .map_err(|e| internal_err(format!("recipe replace failed: {}", e)))?;
                match self.apply_recipe(new) {
                    Ok(x) => {
                        self.replication_offset = r_txt_spec.replication_offset.clone();

                        let install_result = authority.read_modify_write(
                            STATE_KEY,
                            |state: Option<ControllerState>| {
                                match state {
                                    None => Err(()),
                                    Some(mut state) => {
                                        state.node_restrictions = self.node_restrictions.clone();
                                        state.recipe_version = self.recipe.version();
                                        state.recipes = vec![r_txt.to_string()];
                                        // When installing a recipe, the new replication offset overwrites the existing
                                        // offset entirely
                                        state.replication_offset =
                                            r_txt_spec.replication_offset.clone();
                                        Ok(state)
                                    }
                                }
                            },
                        );

                        if let Err(e) = install_result {
                            noria::internal!("failed to persist recipe installation, {}", e)
                        }
                        Ok(x)
                    }
                    Err(e) => {
                        self.recipe = _old;
                        Err(e)
                    }
                }
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                noria::internal!("failed to parse recipe: {:?}", e);
            }
        }
    }

    fn set_replication_offset<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        offset: Option<ReplicationOffset>,
    ) -> Result<(), ReadySetError> {
        self.replication_offset = offset.clone();

        authority
            .read_modify_write::<_, ControllerState, _>(STATE_KEY, |state| match state {
                Some(mut state) => {
                    state.replication_offset = offset.clone();
                    Ok(state)
                }
                None => Err(internal_err("Empty state")),
            })
            .map_err(|_| internal_err("Unable to update state"))??;

        Ok(())
    }

    fn set_domain_placement_local(
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

    fn graphviz(&self, detailed: bool) -> String {
        graphviz(&self.ingredients, detailed, &self.materializations)
    }

    fn remove_leaf(&mut self, mut leaf: NodeIndex) -> Result<(), ReadySetError> {
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
            self.log,
            "Computing removals for removing node {}",
            leaf.index()
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
                    self.log,
                    "Removing query leaf \"{}\"", self.ingredients[leaf].name();
                    "node" => leaf.index(),
                    "really" => reader.index(),
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

        self.remove_nodes(removals.as_slice())
    }

    fn remove_nodes(&mut self, removals: &[NodeIndex]) -> Result<(), ReadySetError> {
        // Remove node from controller local state
        let mut domain_removals: HashMap<DomainIndex, Vec<LocalNodeIndex>> = HashMap::default();
        for ni in removals {
            let node = self
                .ingredients
                .node_weight_mut(*ni)
                .ok_or_else(|| ReadySetError::NodeNotFound { index: ni.index() })?;
            node.remove();
            debug!(self.log, "Removed node {}", ni.index());
            domain_removals
                .entry(node.domain())
                .or_insert_with(Vec::new)
                .push(node.local_addr())
        }

        // Send messages to domains
        for (domain, nodes) in domain_removals {
            trace!(
                self.log,
                "Notifying domain {} of node removals",
                domain.index(),
            );

            self.domains
                .get_mut(&domain)
                .ok_or_else(|| ReadySetError::NoSuchDomain {
                    domain_index: domain.index(),
                    shard: 0,
                })?
                .send_to_healthy_blocking::<()>(
                    DomainRequest::RemoveNodes { nodes },
                    &self.workers,
                )?;
        }

        Ok(())
    }

    fn get_failed_nodes(&self, lost_worker: &WorkerIdentifier) -> Vec<NodeIndex> {
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
    fn nodes_on_worker(&self, worker: Option<&WorkerIdentifier>) -> Vec<NodeIndex> {
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

    /// Returns the maximum replication offset that has been written to any of the tables in this
    /// Noria instance
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    async fn replication_offset(&self) -> ReadySetResult<Option<ReplicationOffset>> {
        // Collect a *unique* list of domains that might contain base tables, to avoid sending
        // multiple requests to a domain that happens to contain multiple base tables
        #[allow(clippy::indexing_slicing)] // inputs returns valid node indices
        let domains = self
            .inputs()
            .values()
            .map(|ni| self.ingredients[*ni].domain())
            .collect::<HashSet<_>>();

        // HACK(eta): validate that all of the domains exist. Doing this inside the future
        // combinator hellscape below is unwieldy enough to merit maybe introducing a TOCTOU bug :P

        for di in domains.iter() {
            if !self.domains.contains_key(di) {
                return Err(ReadySetError::NoSuchDomain {
                    domain_index: di.index(),
                    shard: 0,
                });
            }
        }

        stream::iter(domains)
            .map(|domain| {
                #[allow(clippy::indexing_slicing)] // validated above
                self.domains[&domain].send_to_healthy::<Option<ReplicationOffset>>(
                    DomainRequest::RequestReplicationOffset,
                    &self.workers,
                )
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .try_fold(
                self.replication_offset.clone(),
                |acc: Option<ReplicationOffset>, domain_offs| async move {
                    // NOTE(grfn): domain_offs is a vec per-shard here - ostensibly, every time we
                    // do an update to a replication offset that applies to every shard - meaning
                    // the only case domain_offs *wouldn't* be unique is if we crashed at some
                    // point. Is that a problem?
                    domain_offs
                        .into_iter()
                        .flatten()
                        .chain(acc.into_iter())
                        .try_fold(None, |mut off1, off2| {
                            off2.try_max_into(&mut off1)?;
                            Ok(off1)
                        })
                },
            )
            .await
    }
}
