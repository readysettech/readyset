use channel::tcp::TcpSender;
use dataflow::{checktable, node, payload, DomainConfig, PersistenceParameters, Readers};
use dataflow::payload::{EgressForBase, IngressFromBase};
use dataflow::prelude::*;
use dataflow::statistics::GraphStats;

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::thread::{self};
use std::sync::{Arc, Mutex, atomic};
use std::{io, time};

use coordination::{CoordinationMessage, CoordinationPayload};
use controller::{ControllerState, DomainHandle, Migration, Recipe, RemoteGetterBuilder,
                 WorkerIdentifier, WorkerStatus};
use controller::migrate::materialization::Materializations;
use controller::mutator::MutatorBuilder;
use controller::sql::reuse::ReuseConfigType;
use souplet::readers;
use worker;

use hyper::{Method, StatusCode};
use mio::net::TcpListener;
use petgraph;
use petgraph::visit::Bfs;
use slog;
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
    read_listen_addr: SocketAddr,
    pub(super) reader_exit: Arc<atomic::AtomicBool>,
    pub(super) readers: Readers,

    /// Map from worker address to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, WorkerStatus>,

    /// State between migrations
    pub(super) deps: HashMap<DomainIndex, (IngressFromBase, EgressForBase)>,
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    /// Local worker pool used for tests
    pub(super) local_pool: Option<worker::WorkerPool>,

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

impl ControllerInner {
    pub fn coordination_message(&mut self, msg: CoordinationMessage) {
        trace!(self.log, "Received {:?}", msg);
        let process = match msg.payload {
            CoordinationPayload::Register {
                ref addr,
                ref read_listen_addr,
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

    pub fn external_request(
        &mut self,
        method: Method,
        path: String,
        body: Vec<u8>,
    ) -> Result<String, StatusCode> {
        use serde_json as json;
        use hyper::Method::*;

        Ok(match (method, path.as_ref()) {
            (Get, "/graph") => self.graphviz(),
            (Post, "/inputs") => json::to_string(&self.inputs()).unwrap(),
            (Post, "/outputs") => json::to_string(&self.outputs()).unwrap(),
            (Post, "/recover") => json::to_string(&self.recover()).unwrap(),
            (Post, "/graphviz") => json::to_string(&self.graphviz()).unwrap(),
            (Post, "/get_statistics") => json::to_string(&self.get_statistics()).unwrap(),
            (Post, "/mutator_builder") => {
                json::to_string(&self.mutator_builder(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/getter_builder") => {
                json::to_string(&self.getter_builder(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/install_recipe") => {
                json::to_string(&self.install_recipe(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/set_security_config") => {
                json::to_string(&self.set_security_config(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/create_universe") => {
                json::to_string(&self.create_universe(json::from_slice(&body).unwrap())).unwrap()
            }
            (Post, "/enable_reuse") => {
                json::to_string(&self.enable_reuse(json::from_slice(&body).unwrap())).unwrap()
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

        let sender = Arc::new(Mutex::new(TcpSender::connect(remote, None)?));
        let ws = WorkerStatus::new(sender.clone());
        self.workers.insert(msg.source.clone(), ws);
        self.read_addrs.insert(msg.source.clone(), read_listen_addr);

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

        let readers: Readers = Arc::default();
        let nreaders = state.config.nreaders;
        let listener = TcpListener::bind(&SocketAddr::new(listen_addr, 0)).unwrap();
        let read_listen_addr = listener.local_addr().unwrap();
        let thread_builder = thread::Builder::new().name("read-dispatcher".to_owned());
        let reader_exit = Arc::new(atomic::AtomicBool::new(false));
        {
            let readers = readers.clone();
            let reader_exit = reader_exit.clone();
            thread_builder
                .spawn(move || readers::serve(listener, readers, nreaders, reader_exit))
                .unwrap();
        }

        let mut materializations = Materializations::new(&log);
        if !state.config.partial_enabled {
            materializations.disable_partial()
        }

        let cc = Arc::new(ChannelCoordinator::new());
        assert!((state.config.nworkers == 0) ^ (state.config.local_workers == 0));
        let local_pool = if state.config.nworkers == 0 {
            Some(
                worker::WorkerPool::new(
                    state.config.local_workers,
                    &log,
                    checktable_addr,
                    cc.clone(),
                ).unwrap(),
            )
        } else {
            None
        };

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
            recipe: Recipe::blank(Some(log.clone())),
            log,

            domains: Default::default(),
            channel_coordinator: cc,
            debug_channel: None,

            deps: HashMap::default(),
            remap: HashMap::default(),

            readers,
            read_listen_addr,
            reader_exit,
            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            local_pool,

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

    /// Initiaties log recovery by sending a
    /// StartRecovery packet to each base node domain.
    pub fn recover(&mut self) {
        info!(self.log, "Recovering from log");
        for (_name, index) in self.inputs().iter() {
            let node = &self.ingredients[*index];
            let domain = self.domains.get_mut(&node.domain()).unwrap();
            domain.send(box payload::Packet::StartRecovery).unwrap();
            domain.wait_for_ack().unwrap();
        }
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
        // presumably only
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
    /// (already maintained) reader node.
    pub fn getter_builder(&self, node: NodeIndex) -> Option<RemoteGetterBuilder> {
        self.find_getter_for(node).map(|r| {
            let domain = self.ingredients[r].domain();
            let shards = (0..self.domains[&domain].shards())
                .map(|i| match self.domains[&domain].assignment(i) {
                    Some(worker) => self.read_addrs[&worker].clone(),
                    None => self.read_listen_addr.clone(),
                })
                .map(|a| {
                    // NOTE: this is where we decide whether assignments are local or not (and
                    // hence whether we should use LocalBypass). currently, we assume that either
                    // *all* assignments are local, or *none* are. this is likely to change, at
                    // which point this has to change too.
                    (a, self.local_pool.is_some())
                })
                .collect();

            RemoteGetterBuilder { node: r, shards }
        })
    }

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given base node.
    pub fn mutator_builder(&self, base: NodeIndex) -> MutatorBuilder {
        let node = &self.ingredients[base];

        trace!(self.log, "creating mutator"; "for" => base.index());

        let mut key = self.ingredients[base]
            .suggest_indexes(base)
            .remove(&base)
            .map(|(c, _)| c)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col, _) = self.ingredients[base].sharded_by() {
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

        let num_fields = node.fields().len();
        let base_operator = node.get_base()
            .expect("asked to get mutator for non-base node");
        MutatorBuilder {
            txs,
            addr: (*node.local_addr()).into(),
            key: key,
            key_is_primary: is_primary,
            transactional: self.ingredients[base].is_transactional(),
            dropped: base_operator.get_dropped(),
            expected_columns: num_fields - base_operator.get_dropped().len(),
            is_local: true,
        }
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

    pub fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.recipe.enable_reuse(reuse_type);
    }

    pub fn create_universe(&mut self, context: HashMap<String, DataType>) {
        let log = self.log.clone();
        let mut r = self.recipe.clone();
        let groups = self.recipe.security_groups();
        let outs = self.outputs();

        let mut universe_groups = HashMap::new();

        if context.get("group").is_none() {
            for g in groups {
                let uid = context.get("id").expect("Universe context must have id");
                let membership = outs[&g];
                let mut getter = self.get_getter(membership).unwrap();
                let my_groups: Vec<DataType> = getter.lookup(uid, true).unwrap().iter().map(|v| v[1].clone()).collect();
                universe_groups.insert(g, my_groups);
            }
        }

        self.add_universe(context, |mut mig| {
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

    pub fn install_recipe(&mut self, r_txt: String) -> Result<(), RpcError> {
        match Recipe::from_str(&r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let old = self.recipe.clone();
                let mut new = old.replace(r).unwrap();
                self.migrate(|mig| match new.activate(mig, false) {
                    Ok(_) => (),
                    Err(e) => panic!("failed to install recipe: {:?}", e),
                });
                self.recipe = new;

                Ok(())
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                Err(RpcError::Other("failed to parse recipe".to_owned()))
            }
        }
    }

    #[cfg(test)]
    pub fn get_mutator(&self, base: NodeIndex) -> ::controller::Mutator {
        self.mutator_builder(base)
            .build("127.0.0.1:0".parse().unwrap())
    }

    pub fn get_getter(&self, node: NodeIndex) -> Option<::controller::RemoteGetter> {
        self.getter_builder(node).map(|g| g.build())
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
        self.reader_exit.store(true, atomic::Ordering::SeqCst);
        for (_, d) in &mut self.domains {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send(box payload::Packet::Quit));
            }
        }
        if let Some(ref mut local_pool) = self.local_pool {
            local_pool.wait();
        }
    }
}
