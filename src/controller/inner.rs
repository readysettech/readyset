use api::debug::stats::GraphStats;
use channel::tcp::TcpSender;
use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::prelude::*;
use dataflow::{node, payload, DomainConfig};

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{io, time};

use api::builders::*;
use api::ActivationResult;
use controller::migrate::materialization::Materializations;
use controller::{
    ControllerState, DomainHandle, Migration, Recipe, WorkerIdentifier, WorkerStatus,
};
use coordination::{CoordinationMessage, CoordinationPayload};

use hyper::{Method, StatusCode};
use mio::net::TcpListener;
use petgraph;
use petgraph::visit::Bfs;
use slog;
use std::mem;

/// `Controller` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct ControllerInner {
    pub(super) ingredients: Graph,
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    pub(super) domain_config: DomainConfig,

    /// Parameters for persistence code.
    pub(super) persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    recipe: Recipe,

    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,
    pub(super) debug_channel: Option<SocketAddr>,

    pub(super) listen_addr: IpAddr,

    /// Map from worker address to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, WorkerStatus>,

    /// State between migrations
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    pub(super) epoch: Epoch,

    pending_recovery: Option<(Vec<String>, usize)>,

    quorum: usize,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    last_checked_workers: Instant,

    log: slog::Logger,
}

pub(crate) fn graphviz(graph: &Graph, materializations: &Materializations) -> String {
    let mut s = String::new();

    let indentln = |s: &mut String| s.push_str("    ");

    // header.
    s.push_str("digraph {{\n");

    // global formatting.
    indentln(&mut s);
    s.push_str("node [shape=record, fontsize=10]\n");

    // node descriptions.
    for index in graph.node_indices() {
        let node = &graph[index];
        let materialization_status = materializations.get_status(&index, node);
        indentln(&mut s);
        s.push_str(&format!("{}", index.index()));
        s.push_str(&node.describe(index, materialization_status));
    }

    // edges.
    for (_, edge) in graph.raw_edges().iter().enumerate() {
        indentln(&mut s);
        s.push_str(&format!(
            "{} -> {}",
            edge.source().index(),
            edge.target().index()
        ));
        s.push_str("\n");
    }

    // footer.
    s.push_str("}}");

    s
}

impl ControllerInner {
    pub fn coordination_message(&mut self, msg: CoordinationMessage) {
        trace!(self.log, "Received {:?}", msg);
        let process = match msg.payload {
            CoordinationPayload::Register {
                ref addr,
                ref read_listen_addr,
                ..
            } => self.handle_register(&msg, addr, read_listen_addr.clone()),
            CoordinationPayload::Heartbeat => self.handle_heartbeat(&msg),
            CoordinationPayload::DomainBooted(..) => Ok(()),
            _ => unimplemented!(),
        };
        match process {
            Ok(_) => (),
            Err(e) => error!(self.log, "failed to handle message {:?}: {:?}", msg, e),
        }

        self.check_worker_liveness();
    }

    pub fn external_request<A: Authority + 'static>(
        &mut self,
        method: Method,
        path: String,
        query: Option<String>,
        body: Vec<u8>,
        authority: &Arc<A>,
    ) -> Result<Result<String, String>, StatusCode> {
        use hyper::Method::*;
        use serde_json as json;

        match (&method, path.as_ref()) {
            (&Get, "/graph") => return Ok(Ok(self.graphviz())),
            (&Post, "/graphviz") => return Ok(Ok(json::to_string(&self.graphviz()).unwrap())),
            (&Get, "/get_statistics") => {
                return Ok(Ok(json::to_string(&self.get_statistics()).unwrap()))
            }
            _ => {}
        }

        if self.pending_recovery.is_some() || self.workers.len() < self.quorum {
            return Err(StatusCode::ServiceUnavailable);
        }

        match (method, path.as_ref()) {
            (Get, "/flush_partial") => Ok(Ok(json::to_string(&self.flush_partial()).unwrap())),
            (Post, "/inputs") => Ok(Ok(json::to_string(&self.inputs()).unwrap())),
            (Post, "/outputs") => Ok(Ok(json::to_string(&self.outputs()).unwrap())),
            (Get, "/instances") => Ok(Ok(json::to_string(&self.get_instances()).unwrap())),
            (Get, "/nodes") => {
                // TODO(malte): this is a pretty yucky hack, but hyper doesn't provide easy access
                // to individual query variables unfortunately. We'll probably want to factor this
                // out into a helper method.
                if let Some(query) = query {
                    let vars: Vec<_> = query.split("&").map(String::from).collect();
                    if let Some(n) = &vars.into_iter().find(|v| v.starts_with("w=")) {
                        return Ok(Ok(json::to_string(
                            &self.nodes_on_worker(Some(&n[2..].parse().unwrap())),
                        ).unwrap()));
                    }
                }
                // all data-flow nodes
                Ok(Ok(json::to_string(&self.nodes_on_worker(None)).unwrap()))
            }
            (Post, "/table_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| Ok(json::to_string(&self.table_builder(args)).unwrap())),
            (Post, "/view_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| Ok(json::to_string(&self.view_builder(args)).unwrap())),
            (Post, "/extend_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| {
                    self.extend_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Post, "/install_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| {
                    self.install_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Post, "/set_security_config") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| {
                    self.set_security_config(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Post, "/create_universe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| {
                    self.create_universe(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Post, "/remove_node") => json::from_slice(&body)
                .map_err(|_| StatusCode::BadRequest)
                .map(|args| self.remove_node(args).map(|r| json::to_string(&r).unwrap())),
            _ => return Err(StatusCode::NotFound),
        }
    }

    fn handle_register(
        &mut self,
        msg: &CoordinationMessage,
        remote: &SocketAddr,
        read_listen_addr: SocketAddr,
    ) -> Result<(), io::Error> {
        info!(
            self.log,
            "new worker registered from {:?}, which listens on {:?}", msg.source, remote
        );

        let sender = Arc::new(Mutex::new(TcpSender::connect(remote)?));
        let ws = WorkerStatus::new(sender.clone());
        self.workers.insert(msg.source.clone(), ws);
        self.read_addrs.insert(msg.source.clone(), read_listen_addr);

        if self.workers.len() >= self.quorum {
            if let Some((recipes, recipe_version)) = self.pending_recovery.take() {
                assert_eq!(self.workers.len(), self.quorum);
                assert_eq!(self.recipe.version(), 0);
                assert!(recipe_version + 1 >= recipes.len());

                info!(self.log, "Restoring graph configuration");
                self.recipe = Recipe::with_version(
                    recipe_version + 1 - recipes.len(),
                    Some(self.log.clone()),
                );
                for r in recipes {
                    self.apply_recipe(self.recipe.clone().extend(&r).unwrap())
                        .unwrap();
                }
            }
        }

        Ok(())
    }

    fn check_worker_liveness(&mut self) {
        let mut any_failed = false;

        // check if there are any newly failed workers
        if self.last_checked_workers.elapsed() > self.healthcheck_every {
            for (_addr, ws) in self.workers.iter() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 4 {
                    any_failed = true;
                }
            }
            self.last_checked_workers = Instant::now();
        }

        // if we have newly failed workers, iterate again to find all workers that have missed >= 3
        // heartbeats. This is necessary so that we correctly handle correlated failures of
        // workers.
        if any_failed {
            let mut failed = Vec::new();
            for (addr, ws) in self.workers.iter_mut() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 3 {
                    error!(self.log, "worker at {:?} has failed!", addr);
                    ws.healthy = false;
                    failed.push(addr.clone());
                }
            }
            self.handle_failed_workers(failed);
        }
    }

    fn handle_failed_workers(&mut self, failed: Vec<WorkerIdentifier>) {
        // first, translate from the affected workers to affected data-flow nodes
        let mut affected_nodes = Vec::new();
        for wi in failed {
            info!(self.log, "handling failure of worker {:?}", wi);
            affected_nodes.extend(self.get_failed_nodes(&wi));
        }

        // then, figure out which queries are affected (and thus must be removed and added again in
        // a migration)
        let affected_queries = self.recipe.queries_for_nodes(affected_nodes);
        let (mut recovery, mut original) = self.recipe.make_recovery(affected_queries);

        // activate recipe
        let _r =
            self.migrate(|mig| {
                recovery
                    .activate(mig)
                    .map_err(|e| format!("failed to activate recovery recipe: {}", e))
            }).unwrap();

        // we must do this *after* the migration, since the migration itself modifies the recipe in
        // `recovery`, and we currently need to clone it here.
        original.set_prior(recovery.clone());
        // somewhat awkward, but we must replace the stale `SqlIncorporator` state in `original`
        original.set_sql_inc(recovery.sql_inc().clone());

        self.recipe = recovery;

        // back to original recipe, which should add the query again
        let _r = self.migrate(|mig| {
            original
                .activate(mig)
                .map_err(|e| format!("failed to activate original recipe: {}", e))
        });

        self.recipe = original;
    }

    fn handle_heartbeat(&mut self, msg: &CoordinationMessage) -> Result<(), io::Error> {
        match self.workers.get_mut(&msg.source) {
            None => crit!(
                self.log,
                "got heartbeat for unknown worker {:?}",
                msg.source
            ),
            Some(ref mut ws) => {
                ws.last_heartbeat = Instant::now();
            }
        }

        Ok(())
    }

    /// Construct `ControllerInner` with a specified listening interface
    pub(super) fn new(listen_addr: IpAddr, log: slog::Logger, state: ControllerState) -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
        ));

        let mut materializations = Materializations::new(&log);
        if !state.config.partial_enabled {
            materializations.disable_partial()
        }

        let cc = Arc::new(ChannelCoordinator::new());
        assert_ne!(state.config.quorum, 0);

        let pending_recovery = if !state.recipes.is_empty() {
            Some((state.recipes, state.recipe_version))
        } else {
            None
        };

        let mut recipe = Recipe::blank(Some(log.clone()));
        recipe.enable_reuse(state.config.reuse);

        ControllerInner {
            ingredients: g,
            source: source,
            ndomains: 0,
            listen_addr,

            materializations,
            sharding: state.config.sharding,
            domain_config: state.config.domain_config,
            persistence: state.config.persistence,
            heartbeat_every: state.config.heartbeat_every,
            healthcheck_every: state.config.healthcheck_every,
            recipe: recipe,
            quorum: state.config.quorum,
            log,

            domains: Default::default(),
            channel_coordinator: cc,
            debug_channel: None,
            epoch: state.epoch,

            remap: HashMap::default(),

            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            pending_recovery,
            last_checked_workers: Instant::now(),
        }
    }

    /// Create a global channel for receiving tracer events.
    ///
    /// Only domains created after this method is called will be able to send trace events.
    ///
    /// This function may only be called once because the receiving end it returned.
    #[allow(unused)]
    pub fn create_tracer_channel(&mut self) -> TcpListener {
        assert!(self.debug_channel.is_none());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();
        self.debug_channel = Some(listener.local_addr().unwrap());
        listener
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
    pub fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    #[allow(unused)]
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
        self.materializations.set_logger(&self.log);
    }

    /// Adds a new user universe.
    /// User universes automatically enforce security policies.
    pub fn add_universe<F, T>(&mut self, context: HashMap<String, DataType>, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration: new soup universe");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: context,
            start: time::Instant::now(),
            log: miglog,
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    /// Perform a new query schema migration.
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: Default::default(),
            start: time::Instant::now(),
            log: miglog,
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    #[cfg(test)]
    pub fn graph(&self) -> &Graph {
        &self.ingredients
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Table`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .map(|n| {
                let base = &self.ingredients[n];
                assert!(base.is_base());
                (base.name().to_owned(), n.into())
            })
            .collect()
    }

    /// Get a Vec of all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    pub fn outputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let name = self.ingredients[n].name().to_owned();
                self.ingredients[n]
                    .with_reader(|r| {
                        // we want to give the the node address that is being materialized not that of
                        // the reader node itself.
                        (name, r.is_for())
                    })
                    .ok()
            })
            .collect()
    }

    fn find_view_for(&self, node: NodeIndex) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        let mut bfs = Bfs::new(&self.ingredients, node);
        let mut reader = None;
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child]
                .with_reader(|r| r.is_for() == node)
                .unwrap_or(false)
            {
                reader = Some(child);
                break;
            }
        }

        reader
    }

    /// Obtain a `ViewBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    pub fn view_builder(&self, name: &str) -> Option<ViewBuilder> {
        // first try to resolve the node via the recipe, which handles aliasing between identical
        // queries.
        let node = match self.recipe.node_addr_for(name) {
            Ok(ni) => ni,
            Err(_) => {
                // if the recipe doesn't know about this query, traverse the graph.
                // we need this do deal with manually constructed graphs (e.g., in tests).
                *self.outputs().get(name)?
            }
        };

        self.find_view_for(node).map(|r| {
            let domain = self.ingredients[r].domain();
            let columns = self.ingredients[r].fields().to_vec();
            let shards = (0..self.domains[&domain].shards())
                .map(|i| self.read_addrs[&self.domains[&domain].assignment(i)].clone())
                .collect();

            ViewBuilder {
                local_ports: vec![],
                node: r,
                columns,
                shards,
            }
        })
    }

    /// Obtain a TableBuild that can be used to construct a Table to perform writes and deletes
    /// from the given named base node.
    pub fn table_builder(&self, base: &str) -> Option<TableBuilder> {
        let ni = match self.recipe.node_addr_for(base) {
            Ok(ni) => ni,
            Err(_) => *self.inputs().get(base)?,
        };
        let node = &self.ingredients[ni];

        trace!(self.log, "creating table"; "for" => base);

        let mut key = self.ingredients[ni]
            .suggest_indexes(ni)
            .remove(&ni)
            .map(|(c, _)| c)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col, _) = self.ingredients[ni].sharded_by() {
                key = vec![col];
            }
        } else {
            is_primary = true;
        }

        let txs = (0..self.domains[&node.domain()].shards())
            .map(|i| {
                self.channel_coordinator
                    .get_addr(&(node.domain(), i))
                    .unwrap()
            })
            .collect();

        let base_operator = node
            .get_base()
            .expect("asked to get table for non-base node");
        let columns: Vec<String> = node
            .fields()
            .iter()
            .enumerate()
            .filter(|&(n, _)| !base_operator.get_dropped().contains_key(n))
            .map(|(_, s)| s.clone())
            .collect();
        assert_eq!(
            columns.len(),
            node.fields().len() - base_operator.get_dropped().len()
        );
        let schema = self.recipe.get_base_schema(base);

        Some(TableBuilder {
            local_port: None,
            txs,
            addr: (*node.local_addr()).into(),
            key: key,
            key_is_primary: is_primary,
            dropped: base_operator.get_dropped(),
            table_name: node.name().to_owned(),
            columns,
            schema,
        })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> GraphStats {
        let workers = &self.workers;
        // TODO: request stats from domains in parallel.
        let domains = self
            .domains
            .iter_mut()
            .flat_map(|(di, s)| {
                s.send_to_healthy(box payload::Packet::GetStatistics, workers)
                    .unwrap();
                s.wait_for_statistics()
                    .unwrap()
                    .into_iter()
                    .enumerate()
                    .map(move |(i, (domain_stats, node_stats))| {
                        let node_map = node_stats
                            .into_iter()
                            .map(|(ni, ns)| (ni.into(), ns))
                            .collect();

                        ((di.clone(), i), (domain_stats, node_map))
                    })
            })
            .collect();

        GraphStats { domains: domains }
    }

    pub fn get_instances(&self) -> Vec<(WorkerIdentifier, bool, Duration)> {
        self.workers
            .iter()
            .map(|(&id, ref status)| (id, status.healthy, status.last_heartbeat.elapsed()))
            .collect()
    }

    pub fn flush_partial(&mut self) -> u64 {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let to_evict: Vec<_> = self
            .domains
            .iter_mut()
            .map(|(di, s)| {
                s.send_to_healthy(box payload::Packet::GetStatistics, workers)
                    .unwrap();
                let to_evict: Vec<(NodeIndex, u64)> = s
                    .wait_for_statistics()
                    .unwrap()
                    .into_iter()
                    .flat_map(move |(_, node_stats)| {
                        node_stats
                            .into_iter()
                            .filter_map(|(ni, ns)| match ns.materialized {
                                MaterializationStatus::Partial => Some((ni, ns.mem_size)),
                                _ => None,
                            })
                    })
                    .collect();
                (*di, to_evict)
            })
            .collect();

        let mut total_evicted = 0;
        for (di, nodes) in to_evict {
            for (ni, bytes) in nodes {
                let na = self.ingredients[ni].local_addr();
                self.domains
                    .get_mut(&di)
                    .unwrap()
                    .send_to_healthy(
                        box payload::Packet::Evict {
                            node: Some(*na),
                            num_bytes: bytes as usize,
                        },
                        workers,
                    )
                    .expect("failed to send domain flush message");
                total_evicted += bytes;
            }
        }

        warn!(
            self.log,
            "flushed {} bytes of partial domain state", total_evicted
        );

        total_evicted
    }

    pub fn create_universe(&mut self, context: HashMap<String, DataType>) -> Result<(), String> {
        let log = self.log.clone();
        let mut r = self.recipe.clone();
        let groups = self.recipe.security_groups();

        let mut universe_groups = HashMap::new();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        let uid = &[uid];
        if context.get("group").is_none() {
            for g in groups {
                let rgb: Option<ViewBuilder> = self.view_builder(&g);
                let mut view = rgb.map(|rgb| rgb.build_exclusive().unwrap()).unwrap();
                let my_groups: Vec<DataType> = view
                    .lookup(uid, true)
                    .unwrap()
                    .iter()
                    .map(|v| v[1].clone())
                    .collect();
                universe_groups.insert(g, my_groups);
            }
        }

        self.add_universe(context.clone(), |mut mig| {
            r.next();
            match r.create_universe(&mut mig, universe_groups) {
                Ok(ar) => {
                    info!(log, "{} expressions added", ar.expressions_added);
                    info!(log, "{} expressions removed", ar.expressions_removed);
                    Ok(())
                }
                Err(e) => {
                    crit!(log, "failed to create universe: {:?}", e);
                    Err("failed to create universe".to_owned())
                }
            }.unwrap();
        });

        self.recipe = r;
        Ok(())
    }

    pub fn set_security_config(&mut self, config: (String, String)) -> Result<(), String> {
        let p = config.0;
        let url = config.1;
        self.recipe.set_security_config(&p, url);
        Ok(())
    }

    fn apply_recipe(&mut self, mut new: Recipe) -> Result<ActivationResult, String> {
        let r = self.migrate(|mig| {
            new.activate(mig)
                .map_err(|e| format!("failed to activate recipe: {}", e))
        });

        match r {
            Ok(ref ra) => {
                for leaf in &ra.removed_leaves {
                    // There should be exactly one reader attached to each "leaf" node. Find it and
                    // remove it along with any now unneeded ancestors.
                    let readers: Vec<_> = self
                        .ingredients
                        .neighbors_directed(*leaf, petgraph::EdgeDirection::Outgoing)
                        .collect();
                    assert!(readers.len() <= 1);
                    if !readers.is_empty() {
                        self.remove_node(readers[0]).unwrap();
                    } else {
                        self.remove_node(*leaf).unwrap();
                    }
                }
                self.recipe = new;
            }
            Err(ref e) => {
                crit!(self.log, "{}", e);
                // TODO(malte): a little yucky, since we don't really need the blank recipe
                let recipe = mem::replace(&mut self.recipe, Recipe::blank(None));
                self.recipe = recipe.revert();
            }
        }

        r
    }

    pub fn extend_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        add_txt: String,
    ) -> Result<ActivationResult, String> {
        // needed because self.apply_recipe needs to mutate self.recipe, so can't have it borrowed
        let new = mem::replace(&mut self.recipe, Recipe::blank(None));
        match new.extend(&add_txt) {
            Ok(new) => {
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes.push(add_txt.clone());
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe extension".to_owned());
                }

                activation_result
            }
            Err((old, e)) => {
                // need to restore the old recipe
                crit!(self.log, "failed to extend recipe: {:?}", e);
                self.recipe = old;
                Err("failed to extend recipe".to_owned())
            }
        }
    }

    pub fn install_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        r_txt: String,
    ) -> Result<ActivationResult, String> {
        match Recipe::from_str(&r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let old = mem::replace(&mut self.recipe, Recipe::blank(None));
                let new = old.replace(r).unwrap();
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes = vec![r_txt.clone()];
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe installation".to_owned());
                }
                activation_result
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                Err("failed to parse recipe".to_owned())
            }
        }
    }

    pub fn graphviz(&self) -> String {
        graphviz(&self.ingredients, &self.materializations)
    }

    pub fn remove_node(&mut self, node: NodeIndex) -> Result<(), String> {
        // Node must not have any children.
        assert_eq!(
            self.ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .count(),
            0
        );
        assert!(!self.ingredients[node].is_source());

        let mut removals = HashMap::new();
        let mut nodes = vec![node];
        while let Some(node) = nodes.pop() {
            let mut parents = self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .detach();
            while let Some(parent) = parents.next_node(&self.ingredients) {
                let edge = self.ingredients.find_edge(parent, node).unwrap();
                self.ingredients.remove_edge(edge);

                // XXX(malte): this is currently overzealous at removing things; it will remove
                // internal views above a reader that have no other dependent views.
                // Should not remove "internal leaf" nodes.
                if !self.ingredients[parent].is_source() && !self.ingredients[parent].is_base()
                    && self
                        .ingredients
                        .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                        .count() == 1
                {
                    nodes.push(parent);
                }
            }

            let domain = self.ingredients[node].domain();
            removals
                .entry(domain)
                .or_insert(Vec::new())
                .push(*self.ingredients[node].local_addr());

            // Remove node from controller local state
            self.domains.get_mut(&domain).unwrap().remove_node(node);
            self.ingredients[node].remove();
        }

        for (domain, nodes) in removals {
            self.domains
                .get_mut(&domain)
                .unwrap()
                .send_to_healthy(box payload::Packet::RemoveNodes { nodes }, &self.workers)
                .unwrap();
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
        if worker.is_some() {
            self.domains
                .values()
                .filter(|dh| dh.assigned_to_worker(worker.unwrap()))
                .flat_map(|dh| dh.nodes().iter())
                .cloned()
                .collect()
        } else {
            self.domains
                .values()
                .flat_map(|dh| dh.nodes().iter())
                .cloned()
                .collect()
        }
    }
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        for (_, d) in &mut self.domains {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send_to_healthy(box payload::Packet::Quit, &self.workers));
            }
        }
    }
}
