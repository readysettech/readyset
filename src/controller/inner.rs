use channel::tcp::TcpSender;
use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::payload::{EgressForBase, IngressFromBase};
use dataflow::prelude::*;
use dataflow::statistics::GraphStats;
use dataflow::{checktable, node, payload, DomainConfig, PersistenceParameters};

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{self, Display};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{io, time};

use controller::migrate::materialization::Materializations;
use controller::mutator::MutatorBuilder;
use controller::recipe::ActivationResult;
use controller::{ControllerState, DomainHandle, Migration, Recipe, RemoteGetterBuilder,
                 WorkerIdentifier, WorkerStatus};
use coordination::{CoordinationMessage, CoordinationPayload};

use hyper::{Method, StatusCode};
use mio::net::TcpListener;
use petgraph;
use petgraph::visit::Bfs;
use slog;
use std::mem;
use tarpc::sync::client::{self, ClientExt};

/// `Controller` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct ControllerInner {
    pub(super) ingredients: petgraph::Graph<node::Node, Edge>,
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) checktable: checktable::CheckTableClient,
    checktable_addr: SocketAddr,
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
    pub(super) deps: HashMap<DomainIndex, (IngressFromBase, EgressForBase)>,
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    pub(super) epoch: Epoch,

    pending_recovery: Option<(Vec<String>, usize)>,

    quorum: usize,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    last_checked_workers: Instant,

    log: slog::Logger,
}

/// Serializable error type for RPC that can fail.
#[derive(Debug, Deserialize, Serialize)]
pub enum RpcError {
    /// Generic error message vessel.
    Other(String),
}

impl Error for RpcError {
    fn description(&self) -> &str {
        match *self {
            RpcError::Other(ref s) => s,
        }
    }
}

impl Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
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
        body: Vec<u8>,
        authority: &Arc<A>,
    ) -> Result<String, StatusCode> {
        use hyper::Method::*;
        use serde_json as json;

        match (&method, path.as_ref()) {
            (&Get, "/graph") => return Ok(self.graphviz()),
            (&Post, "/graphviz") => return Ok(json::to_string(&self.graphviz()).unwrap()),
            (&Get, "/get_statistics") => return Ok(format!("{:?}", self.get_statistics())),
            _ => {}
        }

        if self.pending_recovery.is_some() || self.workers.len() < self.quorum {
            return Err(StatusCode::ServiceUnavailable);
        }

        Ok(match (method, path.as_ref()) {
            (Post, "/inputs") => json::to_string(&self.inputs()).unwrap(),
            (Post, "/outputs") => json::to_string(&self.outputs()).unwrap(),
            (Post, "/mutator_builder") => {
                json::to_string(&self.mutator_builder(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/getter_builder") => {
                json::to_string(&self.getter_builder(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/extend_recipe") => {
                json::to_string(&self.extend_recipe(authority, json::from_slice(&body).unwrap()))
                    .unwrap()
            }
            (Post, "/install_recipe") => {
                json::to_string(&self.install_recipe(authority, json::from_slice(&body).unwrap()))
                    .unwrap()
            }
            (Post, "/set_security_config") => json::to_string(&self.set_security_config(
                json::from_slice(&body).unwrap(),
            )).unwrap(),
            (Post, "/create_universe") => {
                json::to_string(&self.create_universe(json::from_slice(&body).unwrap())).unwrap()
            }
            _ => return Err(StatusCode::NotFound),
        })
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

                info!(self.log, "Recovering from log");
                for (_name, index) in self.inputs().iter() {
                    let node = &self.ingredients[*index];
                    let domain = self.domains.get_mut(&node.domain()).unwrap();
                    domain.send(box payload::Packet::StartRecovery).unwrap();
                    domain.wait_for_ack().unwrap();
                }
                info!(self.log, "Recovery complete");
            }
        }

        Ok(())
    }

    fn check_worker_liveness(&mut self) {
        if self.last_checked_workers.elapsed() > self.healthcheck_every {
            for (addr, ws) in self.workers.iter_mut() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 3 {
                    warn!(self.log, "worker at {:?} has failed!", addr);
                    ws.healthy = false;
                }
            }
            self.last_checked_workers = Instant::now();
        }
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
    pub(super) fn new(
        listen_addr: IpAddr,
        checktable_addr: SocketAddr,
        log: slog::Logger,
        state: ControllerState,
    ) -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
            true,
        ));

        let checktable =
            checktable::CheckTableClient::connect(checktable_addr, client::Options::default())
                .unwrap();

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
            checktable,
            checktable_addr,
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

            deps: HashMap::default(),
            remap: HashMap::default(),

            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            pending_recovery,
            last_checked_workers: Instant::now(),
        }
    }

    /// Use a debug channel. This function may only be called once because the receiving end it
    /// returned.
    #[allow(unused)]
    pub fn create_debug_channel(&mut self) -> TcpListener {
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

    /// Get a boxed function which can be used to validate tokens.
    #[allow(unused)]
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable =
            checktable::CheckTableClient::connect(self.checktable_addr, client::Options::default())
                .unwrap();
        Box::new(move |t: &checktable::Token| checktable.validate_token(t.clone()).unwrap())
    }

    #[cfg(test)]
    pub fn graph(&self) -> &Graph {
        &self.ingredients
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .map(|n| {
                let base = &self.ingredients[n];
                assert!(base.is_internal());
                assert!(base.get_base().is_some());
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
                self.ingredients[n].with_reader(|r| {
                    // we want to give the the node address that is being materialized not that of
                    // the reader node itself.
                    (name, r.is_for())
                })
            })
            .collect()
    }

    fn find_getter_for(&self, node: NodeIndex) -> Option<NodeIndex> {
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

    /// Obtain a `RemoteGetterBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    pub fn getter_builder(&self, name: &str) -> Option<RemoteGetterBuilder> {
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

        self.find_getter_for(node).map(|r| {
            let domain = self.ingredients[r].domain();
            let columns = self.ingredients[r].fields().to_vec();
            let shards = (0..self.domains[&domain].shards())
                .map(|i| self.read_addrs[&self.domains[&domain].assignment(i)].clone())
                .map(|a| {
                    // NOTE: this is where we decide whether assignments are local or not (and
                    // hence whether we should use LocalBypass). currently, we assume that either
                    // *all* assignments are local, or *none* are. this is likely to change, at
                    // which point this has to change too.
                    (a, false)
                })
                .collect();

            RemoteGetterBuilder {
                local_port: None,
                node: r,
                columns,
                shards,
            }
        })
    }

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given named base node.
    pub fn mutator_builder(&self, base: &str) -> Option<MutatorBuilder> {
        let ni = match self.recipe.node_addr_for(base) {
            Ok(ni) => ni,
            Err(_) => *self.inputs().get(base)?,
        };
        let node = &self.ingredients[ni];

        trace!(self.log, "creating mutator"; "for" => base);

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

        let base_operator = node.get_base()
            .expect("asked to get mutator for non-base node");
        let columns: Vec<String> = node.fields()
            .iter()
            .enumerate()
            .filter(|&(n, _)| !base_operator.get_dropped().contains_key(n))
            .map(|(_, s)| s.clone())
            .collect();
        assert_eq!(
            columns.len(),
            node.fields().len() - base_operator.get_dropped().len()
        );

        Some(MutatorBuilder {
            local_port: None,
            txs,
            addr: (*node.local_addr()).into(),
            key: key,
            key_is_primary: is_primary,
            transactional: self.ingredients[ni].is_transactional(),
            dropped: base_operator.get_dropped(),
            table_name: node.name().to_owned(),
            columns,
            is_local: true,
        })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> GraphStats {
        // TODO: request stats from domains in parallel.
        let domains = self.domains
            .iter_mut()
            .flat_map(|(di, s)| {
                s.send(box payload::Packet::GetStatistics).unwrap();
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

    pub fn create_universe(&mut self, context: HashMap<String, DataType>) {
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
                let rgb: Option<RemoteGetterBuilder> = self.getter_builder(&g);
                let mut getter = rgb.map(|rgb| rgb.build_exclusive()).unwrap();
                let my_groups: Vec<DataType> = getter
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
                    Err(RpcError::Other("failed to create universe".to_owned()))
                }
            }.unwrap();
        });

        self.recipe = r;
    }

    pub fn set_security_config(&mut self, config: (String, String)) {
        let p = config.0;
        let url = config.1;
        self.recipe.set_security_config(&p, url);
    }

    fn apply_recipe(&mut self, mut new: Recipe) -> Result<ActivationResult, RpcError> {
        let mut err = Err(RpcError::Other("".to_owned())); // <3 type inference
        self.migrate(|mig| match new.activate(mig, false) {
            Ok(ra) => {
                err = Ok(ra);
            }
            Err(e) => {
                err = Err(RpcError::Other(format!("failed to activate recipe: {}", e)));
            }
        });

        match err {
            Ok(_) => {
                self.recipe = new;
            }
            Err(ref e) => {
                crit!(self.log, "{}", e.description());
                // TODO(malte): a little yucky, since we don't really need the blank recipe
                let recipe = mem::replace(&mut self.recipe, Recipe::blank(None));
                self.recipe = recipe.revert();
            }
        }

        err
    }

    pub fn extend_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        add_txt: String,
    ) -> Result<ActivationResult, RpcError> {
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
                    return Err(RpcError::Other(
                        "Failed to persist recipe extension".to_owned(),
                    ));
                }

                activation_result
            }
            Err((old, e)) => {
                // need to restore the old recipe
                crit!(self.log, "failed to extend recipe: {:?}", e);
                self.recipe = old;
                Err(RpcError::Other("failed to extend recipe".to_owned()))
            }
        }
    }

    pub fn install_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        r_txt: String,
    ) -> Result<ActivationResult, RpcError> {
        match Recipe::from_str(&r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let old = mem::replace(&mut self.recipe, Recipe::blank(None));
                let mut new = old.replace(r).unwrap();
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
                    return Err(RpcError::Other(
                        "Failed to persist recipe installation".to_owned(),
                    ));
                }
                activation_result
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                Err(RpcError::Other("failed to parse recipe".to_owned()))
            }
        }
    }

    pub fn graphviz(&self) -> String {
        let mut s = String::new();

        let indentln = |s: &mut String| s.push_str("    ");

        // header.
        s.push_str("digraph {{\n");

        // global formatting.
        indentln(&mut s);
        s.push_str("node [shape=record, fontsize=10]\n");

        // node descriptions.
        for index in self.ingredients.node_indices() {
            let node = &self.ingredients[index];
            let materialization_status = self.materializations.get_status(&index, node);
            indentln(&mut s);
            s.push_str(&format!("{}", index.index()));
            s.push_str(&node.describe(index, materialization_status));
        }

        // edges.
        for (_, edge) in self.ingredients.raw_edges().iter().enumerate() {
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
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        for (_, d) in &mut self.domains {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send(box payload::Packet::Quit));
            }
        }
    }
}
